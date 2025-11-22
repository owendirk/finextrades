#!/usr/bin/env python3
"""
finextrades.py

Backfill historical Bitfinex trades, with global rate limiting and
multi-threaded month-level jobs, plus detection of missing *days*
inside each month for backfilling.

- Symbol: e.g. "tBTCUSD"
- Endpoint: /v2/trades/{symbol}/hist
- Output: ./bitfinex_data/YYYY/MM/{symbol}_YYYY-MM.csv

Each month is written to its own CSV.

Resume logic:
    - If a monthly file exists, we:
        * Inspect it to find:
            - How many trades it has
            - Earliest and latest timestamps for that month
            - Which calendar days (UTC) have at least 1 trade
        * Compute which days in [month_start, month_end) are missing
          (no trades for that date in the file).
        * Backfill each missing day by calling the Bitfinex API
          over [day_start, day_end) and appending trades.

    - If no file exists, the entire month is considered missing,
      so all days in the month are backfilled.

Rate limiting:
    - Global (across all threads) via _request_lock and _next_request_time.
    - Retries with backoff on errors and HTTP 429.
"""

import csv
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Set

import requests

# ==========================
# Configuration
# ==========================

SYMBOL = "tBTCUSD"

# Historical range to backfill (UTC)
START_DATE = datetime(2015, 1, 1, tzinfo=timezone.utc)
END_DATE = datetime.now(timezone.utc)

# Bitfinex API config
BASE_URL = "https://api-pub.bitfinex.com/v2/trades/{symbol}/hist"
MAX_LIMIT = 10000  # max trades per API call

# Rate limiting (global across all threads)
RATE_LIMIT_DELAY = 4.2      # seconds between requests (~14 req/min)
MAX_RATE_LIMIT_DELAY = 20.0 # cap backoff delay
MAX_RETRIES = 5

# Threading
NUM_WORKERS = 8

# Output
OUTPUT_DIR = Path("./bitfinex_data")


# ==========================
# Global rate limiter state
# ==========================

_request_lock = threading.Lock()
_next_request_time = 0.0


@dataclass
class WorkerResult:
    month_label: str
    trades_written: int
    new_first_mts: Optional[int]
    new_last_mts: Optional[int]
    file_path: Path
    errors: int = 0


# ==========================
# Helpers
# ==========================

def dt_to_ms(dt: datetime) -> int:
    """Convert aware datetime to milliseconds since epoch."""
    return int(dt.timestamp() * 1000)


def ms_to_iso(mts: int) -> str:
    """Convert milliseconds since epoch to ISO8601 string (UTC)."""
    return datetime.fromtimestamp(mts / 1000.0, tz=timezone.utc).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def build_month_ranges(start: datetime, end: datetime) -> List[Tuple[datetime, datetime]]:
    """
    Build (month_start, month_end) ranges between start and end.
    month_end is exclusive.

    Returns a list like:
        [(2017-01-01T00:00Z, 2017-02-01T00:00Z),
         (2017-02-01T00:00Z, 2017-03-01T00:00Z),
         ...]
    """
    ranges = []
    # Start at the first day of start's month
    cur = datetime(start.year, start.month, 1, tzinfo=timezone.utc)

    while cur < end:
        if cur.month == 12:
            nxt = datetime(cur.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            nxt = datetime(cur.year, cur.month + 1, 1, tzinfo=timezone.utc)

        range_start = max(cur, start)
        range_end = min(nxt, end)
        if range_start < range_end:
            ranges.append((range_start, range_end))
        cur = nxt

    return ranges


# ==========================
# CSV inspection & gap detection
# ==========================

def inspect_existing_month(
    csv_path: Path,
    month_start: datetime,
    month_end: datetime,
) -> Tuple[Set[datetime.date], Optional[int], Optional[int], int]:
    """
    Inspect an existing monthly CSV file and return:

        present_dates: set of datetime.date with at least 1 trade (UTC),
        min_mts: earliest trade ts (ms since epoch) within [month_start, month_end),
        max_mts: latest trade ts (ms since epoch) within [month_start, month_end),
        num_trades: number of trades in that month in the file.

    If file does not exist or has no data, returns (empty set, None, None, 0).
    """
    present_dates: Set[datetime.date] = set()
    min_mts: Optional[int] = None
    max_mts: Optional[int] = None
    num_trades = 0

    if not csv_path.exists():
        return present_dates, None, None, 0

    with csv_path.open("r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        for row in reader:
            if len(row) < 2:
                continue
            try:
                mts = int(row[1])
            except ValueError:
                continue

            dt = datetime.fromtimestamp(mts / 1000.0, tz=timezone.utc)
            if not (month_start <= dt < month_end):
                # Ignore trades that somehow fall outside this month range
                continue

            num_trades += 1

            if min_mts is None or mts < min_mts:
                min_mts = mts
            if max_mts is None or mts > max_mts:
                max_mts = mts

            present_dates.add(dt.date())

    return present_dates, min_mts, max_mts, num_trades


def compute_missing_days(
    present_dates: Set[datetime.date],
    month_start: datetime,
    month_end: datetime,
) -> List[datetime]:
    """
    Compute which calendar days (UTC) in [month_start, month_end)
    have *no* trades in present_dates.

    Returns a list of datetime objects representing the start of each
    missing day (at 00:00:00 UTC).
    """
    missing_days: List[datetime] = []
    cur_date = month_start.date()
    end_date = month_end.date()  # month_end is exclusive

    while cur_date < end_date:
        if cur_date not in present_dates:
            missing_days.append(
                datetime(cur_date.year, cur_date.month, cur_date.day, tzinfo=timezone.utc)
            )
        cur_date += timedelta(days=1)

    return missing_days


# ==========================
# Rate-limited request
# ==========================

def rate_limited_request(session: requests.Session, url: str, params: dict) -> requests.Response:
    """
    Perform a GET request under a global rate limit, with retry & backoff.

    All threads share _request_lock and _next_request_time.
    """
    global _next_request_time

    attempt = 0
    backoff = RATE_LIMIT_DELAY

    while attempt < MAX_RETRIES:
        # Acquire "slot" in global rate limiter
        while True:
            with _request_lock:
                now = time.time()
                wait = _next_request_time - now
                if wait <= 0:
                    # we can go now
                    _next_request_time = now + RATE_LIMIT_DELAY
                    break
            # outside the lock, sleep if needed
            if wait > 0:
                time.sleep(wait)

        try:
            resp = session.get(url, params=params, timeout=30)
        except requests.RequestException as e:
            attempt += 1
            backoff = min(backoff * 1.5, MAX_RATE_LIMIT_DELAY)
            print(f"[HTTP] Network error {e!r}, retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s")
            time.sleep(backoff)
            continue

        # Handle rate-limit response
        if resp.status_code == 429:
            attempt += 1
            backoff = min(backoff * 2.0, MAX_RATE_LIMIT_DELAY)
            print(f"[HTTP] 429 Rate limit hit, retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s")
            with _request_lock:
                _next_request_time = max(_next_request_time, time.time() + backoff)
            time.sleep(backoff)
            continue

        # Other HTTP errors
        if not resp.ok:
            attempt += 1
            backoff = min(backoff * 1.5, MAX_RATE_LIMIT_DELAY)
            print(f"[HTTP] Error {resp.status_code} '{resp.text[:100]}', "
                  f"retry {attempt}/{MAX_RETRIES} in {backoff:.1f}s")
            time.sleep(backoff)
            continue

        return resp

    raise RuntimeError(f"Max retries exceeded for URL={url} params={params}")


# ==========================
# Backfill a specific time range (e.g., one day)
# ==========================

def backfill_time_range(
    writer: csv.writer,
    session: requests.Session,
    month_label: str,
    range_start: datetime,
    range_end: datetime,
) -> Tuple[int, Optional[int], Optional[int]]:
    """
    Backfill trades for [range_start, range_end) and append to CSV via writer.

    Returns:
        trades_written, first_mts, last_mts
    """
    start_ms = dt_to_ms(range_start)
    end_ms = dt_to_ms(range_end)

    total = 0
    first_mts: Optional[int] = None
    last_mts: Optional[int] = None

    while start_ms < end_ms:
        params = {
            "start": start_ms,
            "end": end_ms,
            "limit": MAX_LIMIT,
            "sort": 1,  # ascending by mts
        }

        try:
            resp = rate_limited_request(session, BASE_URL.format(symbol=SYMBOL), params)
        except Exception as e:
            print(f"[{month_label}] Fatal error during backfill range "
                  f"{ms_to_iso(start_ms)} → {ms_to_iso(end_ms)}: {e!r}")
            return total, first_mts, last_mts

        try:
            data = resp.json()
        except ValueError as e:
            print(f"[{month_label}] JSON parse error: {e!r}, text={resp.text[:200]!r}")
            return total, first_mts, last_mts

        if not data:
            # No more trades in this range
            break

        # trades: [ID, MTS, AMOUNT, PRICE]
        data.sort(key=lambda row: row[1])

        batch_first_mts = data[0][1]
        batch_last_mts = data[-1][1]

        if first_mts is None:
            first_mts = batch_first_mts

        for tid, mts, amount, price in data:
            writer.writerow([tid, mts, ms_to_iso(mts), amount, price])

        total += len(data)
        last_mts = batch_last_mts
        start_ms = batch_last_mts + 1

        print(
            f"[{month_label}]   +{len(data)} trades "
            f"({ms_to_iso(batch_first_mts)} → {ms_to_iso(batch_last_mts)}), "
            f"range_total={total}"
        )

        # If we received less than MAX_LIMIT trades, we probably exhausted this range
        if len(data) < MAX_LIMIT:
            break

    if total == 0:
        print(
            f"[{month_label}]   No trades returned for "
            f"{range_start.date()} ({range_start.isoformat()} → {range_end.isoformat()})"
        )

    return total, first_mts, last_mts


# ==========================
# Worker logic per month
# ==========================

def fetch_month_range(month_start: datetime, month_end: datetime) -> WorkerResult:
    """
    Fetch / backfill trades for [month_start, month_end), detecting missing days
    and filling them.

    Returns a WorkerResult with summary information.
    """
    session = requests.Session()
    year_str = f"{month_start.year:04d}"
    month_str = f"{month_start.month:02d}"
    month_label = f"{year_str}-{month_str}"

    out_dir = OUTPUT_DIR / year_str / month_str
    ensure_dir(out_dir)
    csv_path = out_dir / f"{SYMBOL}_{month_label}.csv"

    # Inspect existing file (if any)
    present_dates, existing_min_mts, existing_max_mts, existing_trades = inspect_existing_month(
        csv_path, month_start, month_end
    )

    if existing_trades > 0:
        print(
            f"[{month_label}] Existing file: trades={existing_trades}, "
            f"earliest={ms_to_iso(existing_min_mts)} "
            f"latest={ms_to_iso(existing_max_mts)}"
        )
    else:
        if csv_path.exists():
            print(f"[{month_label}] Existing file but no trades for this month (header only).")
        else:
            print(f"[{month_label}] No existing file; full month backfill needed.")

    # Compute which days are missing inside this month
    missing_days = compute_missing_days(present_dates, month_start, month_end)
    missing_count = len(missing_days)

    if missing_count == 0:
        print(f"[{month_label}] No missing days; nothing to do.")
        return WorkerResult(
            month_label=month_label,
            trades_written=0,
            new_first_mts=None,
            new_last_mts=None,
            file_path=csv_path,
            errors=0,
        )

    print(
        f"[{month_label}] Missing {missing_count} day(s) in "
        f"{month_start.date()} → {month_end.date()}."
    )

    # Open CSV in append mode; write header if new
    file_exists = csv_path.exists()
    total_trades_written = 0
    new_first_mts: Optional[int] = None
    new_last_mts: Optional[int] = None

    with csv_path.open("a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["id", "mts", "datetime", "amount", "price"])

        # Process missing days in chronological order
        for day_start in sorted(missing_days):
            day_end = min(day_start + timedelta(days=1), month_end)
            print(
                f"[{month_label}] Backfilling day {day_start.date()} "
                f"({day_start.isoformat()} → {day_end.isoformat()})"
            )

            day_trades, day_first_mts, day_last_mts = backfill_time_range(
                writer, session, month_label, day_start, day_end
            )

            if day_trades > 0:
                total_trades_written += day_trades
                if new_first_mts is None or (day_first_mts is not None and day_first_mts < new_first_mts):
                    new_first_mts = day_first_mts
                if new_last_mts is None or (day_last_mts is not None and day_last_mts > new_last_mts):
                    new_last_mts = day_last_mts

    print(
        f"[{month_label}] Done. Trades written this run: {total_trades_written}, "
        f"new_first_mts={new_first_mts}, new_last_mts={new_last_mts}"
    )

    return WorkerResult(
        month_label=month_label,
        trades_written=total_trades_written,
        new_first_mts=new_first_mts,
        new_last_mts=new_last_mts,
        file_path=csv_path,
        errors=0 if total_trades_written >= 0 else 1,
    )


# ==========================
# Main
# ==========================

def main():
    print("Bitfinex Historical Trades Downloader")
    print("=====================================")
    print(f"Symbol      : {SYMBOL}")
    print(f"Start date  : {START_DATE.isoformat()}")
    print(f"End date    : {END_DATE.isoformat()}")
    print(f"Output dir  : {OUTPUT_DIR.resolve()}")
    print(f"Workers     : {NUM_WORKERS}")
    print(f"Rate limit  : ~1 request every {RATE_LIMIT_DELAY:.2f}s\n")

    ensure_dir(OUTPUT_DIR)

    month_ranges = build_month_ranges(START_DATE, END_DATE)
    print("Planned month ranges:")
    for s, e in month_ranges:
        print(f"  {s.strftime('%Y-%m-%d')} → {e.strftime('%Y-%m-%d')}")
    print("")

    futures = []
    results: List[WorkerResult] = []

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for month_start, month_end in month_ranges:
            futures.append(executor.submit(fetch_month_range, month_start, month_end))

        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)

    print("\nSummary:")
    print("========")
    total_trades_all = 0
    for res in sorted(results, key=lambda r: r.month_label):
        total_trades_all += res.trades_written
        print(
            f"{res.month_label}: "
            f"trades_written={res.trades_written}, "
            f"file={res.file_path}, "
            f"errors={res.errors}"
        )

    print(f"\nTotal trades written this run: {total_trades_all}")


if __name__ == "__main__":
    main()
