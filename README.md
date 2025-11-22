# finextrades.py

A small utility script to download historical trade (tick) data from the Bitfinex exchange.

---

## Overview

`finextrades.py` pulls trades for a single Bitfinex symbol from a user-defined start date up to **today’s date**, while respecting Bitfinex’s REST API limits (maximum of 10,000 trades per request).

Data is saved as CSV files, organized by **year** and **month**, under a top-level `bitfinex_data/` directory located next to the script.

---

## Features

- Downloads historical trades/ticks from Bitfinex
- Respects Bitfinex’s 10,000-trades-per-request limit
- Fetches trades from a configurable **start date** up to the current date
- Automatically creates a directory tree:
  - `bitfinex_data/<YEAR>/<MONTH>/`
- One CSV file per month:
- File naming pattern: `SYMBOL_YYYY-MM.csv`

---

## Directory & File Structure

All data is stored relative to the script’s location:

```text
./finextrades.py
./bitfinex_data/
    ├── 2015/
    │   ├── 01/
    │   │   └── tBTCUSD_2015-01.csv
    │   ├── 02/
    │   │   └── tBTCUSD_2015-02.csv
    │   └── ...
    ├── 2016/
    │   └── ...
    └── ...
```

**File naming format:**

```text
SYMBOL_YYYY-MM.csv
# Example:
tBTCUSD_2015-01.csv
```

---

## CSV Format

Each monthly CSV has the following columns:

| Column    | Description                            |
|----------|----------------------------------------|
| `id`     | Trade ID                               |
| `mts`    | Timestamp in milliseconds since epoch  |
| `datetime` | ISO-8601 timestamp (UTC)            |
| `amount` | Trade size (positive = buy, negative = sell) |
| `price`  | Trade price                            |

**Example:**

```csv
id,mts,datetime,amount,price
4489165,1420070400000,2015-01-01T00:00:00+00:00,-7.88069332,322.27
4489166,1420070400000,2015-01-01T00:00:00+00:00,-3.50365591,322.29
4489164,1420070400000,2015-01-01T00:00:00+00:00,-49.84,322.30
4489163,1420070400000,2015-01-01T00:00:00+00:00,-0.77565077,322.31
4489169,1420070433000,2015-01-01T00:00:33+00:00,-0.77213037,322.09
```

---

## Example Output View

![Example directory structure](https://github.com/user-attachments/assets/79bf6611-a1a9-474f-a249-0c3ce60974db)

---

## Usage

1. Make sure you have Python installed (3.8+ recommended).
2. Install any required dependencies (for example, if the script uses `requests`):

   ```bash
   pip install requests
   ```

3. Configure the script:
   - Set your desired **symbol** (e.g. `tBTCUSD`)
   - Set your desired **start date**

4. Run the script from the terminal in the directory where `finextrades.py` lives:

   ```bash
   python finextrades.py
   ```

The script will start downloading trades from the specified start date up to today and will populate the `bitfinex_data/` folder with monthly CSV files.

---

## Notes

- The script adheres to Bitfinex rate limits by requesting at most **10,000 trades per call**.
- If you plan to download many years of data, the first run may take some time due to the amount of historical data.
- You can safely re-run the script; depending on implementation, it may skip or resume already-downloaded data (see code comments for details).
