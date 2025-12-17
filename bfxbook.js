/* USAGE:
npm install ws lodash moment crc-32 parquetjs-lite
# make sure your USB is mounted at /mnt/usb
# sudo mount /dev/sdb1 /mnt/usb

node bfx_book_recorder_parquet.js tBTCUSD

This script:
- Connects to Bitfinex WS v2 book channel
- Maintains an in-memory orderbook
- After EVERY book update, builds a 25-level snapshot
- Buffers snapshots and writes to Parquet in batches
- Rotates Parquet files EVERY HOUR

Parquet schema (wide, fixed depth 25):
  ts:   UTF8 (ISO UTC)
  pair: UTF8
  seq:  INT64 (optional)

  bid_p01..bid_p25 : DOUBLE
  bid_a01..bid_a25 : DOUBLE
  ask_p01..ask_p25 : DOUBLE
  ask_a01..ask_a25 : DOUBLE

File naming (UTC hour-based):
  YYYY_MM_DD_HH_<PAIR>.parquet
  e.g. 2025_12_09_20_tBTCUSD.parquet
*/

'use strict'

const WS = require('ws')
const _ = require('lodash')
const fs = require('fs')
const moment = require('moment')
const CRC = require('crc-32')
const parquet = require('parquetjs-lite')

const pair = process.argv[2]

if (!pair) {
  console.error('Usage: node bfx_book_recorder_parquet.js tBTCUSD')
  process.exit(1)
}

const conf = {
  wshost: 'wss://api.bitfinex.com/ws/2'
}

// ---- Output dir: USB mount ----
const USB_DIR = '/mnt/usb'

// ensure USB dir exists (does NOT mount it, just creates folder if missing)
if (!fs.existsSync(USB_DIR)) {
  fs.mkdirSync(USB_DIR, { recursive: true })
}

const BOOK = {}

console.log('Starting book recorder for', pair, 'on', conf.wshost)
console.log('Writing Parquet files to', USB_DIR)

let connected = false
let connecting = false
let cli
let seq = null   // last seen sequence number (for metadata / debugging only)

const DEPTH = 25

// ---- Parquet writer + rotation settings ----
// Safety cap in case a single hour is insanely busy
const MAX_ROWS_PER_FILE = 1_000_000

// batch flush settings
const FLUSH_BATCH = 500
const FLUSH_INTERVAL_MS = 1000

let buffer = []

let writer = null
let rowsInFile = 0
let currentFilePath = null
let currentHourKey = null  // 'YYYY_MM_DD_HH' for the current file

let diskFull = false  // set true if we ever hit ENOSPC

// ---- Schema ----
function pad2 (n) {
  return String(n).padStart(2, '0')
}

function makeWideSchema (depth) {
  const fields = {
    ts:   { type: 'UTF8' },
    pair: { type: 'UTF8' },
    seq:  { type: 'INT64', optional: true }
  }

  for (let i = 1; i <= depth; i++) {
    const k = pad2(i)
    fields[`bid_p${k}`] = { type: 'DOUBLE', optional: true }
    fields[`bid_a${k}`] = { type: 'DOUBLE', optional: true }
    fields[`ask_p${k}`] = { type: 'DOUBLE', optional: true }
    fields[`ask_a${k}`] = { type: 'DOUBLE', optional: true }
  }

  return new parquet.ParquetSchema(fields)
}

const schema = makeWideSchema(DEPTH)

// hourKey is something like '2025_12_09_20'
function makeParquetFilename (hourKey) {
  return `${USB_DIR}/${hourKey}_${pair}.parquet`
}

async function openNewParquetFile (hourKey) {
  currentHourKey = hourKey
  currentFilePath = makeParquetFilename(hourKey)
  writer = await parquet.ParquetWriter.openFile(schema, currentFilePath, {
    useDataPageV2: false
  })
  rowsInFile = 0
  console.log('[parquet] opened', currentFilePath)
}

async function closeParquetFile () {
  if (writer) {
    try {
      await writer.close()
      console.log('[parquet] closed', currentFilePath, 'rows=', rowsInFile)
    } catch (e) {
      console.error('[parquet] error closing file:', e && e.message ? e.message : e)
    }
  }
  writer = null
  currentFilePath = null
  rowsInFile = 0
  currentHourKey = null
}

// Rotate:
// - when there is no writer yet
// - OR UTC hour changed
// - OR rows exceed MAX_ROWS_PER_FILE
async function rotateIfNeeded () {
  const now = moment.utc()
  const hourKey = now.format('YYYY_MM_DD_HH')

  if (!writer) {
    await openNewParquetFile(hourKey)
    return
  }

  if (hourKey !== currentHourKey || rowsInFile >= MAX_ROWS_PER_FILE) {
    await closeParquetFile()
    await openNewParquetFile(hourKey)
  }
}

async function flushBuffer () {
  if (diskFull) return
  if (buffer.length === 0) return

  try {
    await rotateIfNeeded()

    if (!writer) {
      // could not open writer (e.g. disk full / other error)
      console.error('[parquet] no active writer, dropping buffer to avoid hang')
      buffer = []
      return
    }

    for (const row of buffer) {
      await writer.appendRow(row)
    }

    rowsInFile += buffer.length
    buffer = []
  } catch (err) {
    console.error('[parquet] flush error:', err && err.message ? err.message : err)

    // If disk is full, stop recording to avoid corruption
    if (err && (err.code === 'ENOSPC' || /No space left on device/.test(String(err)))) {
      console.error('[parquet] Disk appears FULL on USB. Stopping recorder to avoid corruption.')
      diskFull = true

      try {
        await closeParquetFile()
      } catch (e2) {
        console.error('[parquet] error while closing after ENOSPC:', e2 && e2.message ? e2.message : e2)
      }

      if (cli) {
        try {
          cli.close()
        } catch (_) {}
      }
    }
  }
}

// periodic flush
setInterval(() => {
  flushBuffer().catch(err => {
    console.error('[parquet] flush error (interval):', err && err.message ? err.message : err)
  })
}, FLUSH_INTERVAL_MS)

async function shutdown (code = 0) {
  console.log('[shutdown] requested with code', code)
  try {
    await flushBuffer()
    await closeParquetFile()
  } catch (e) {
    console.error('[shutdown] error:', e && e.message ? e.message : e)
  } finally {
    process.exit(code)
  }
}

process.on('SIGINT', () => shutdown(0))
process.on('SIGTERM', () => shutdown(0))

// ---- Snapshot row builder (wide) ----
function buildWideRow () {
  if (!BOOK.psnap.bids || !BOOK.psnap.asks) return null

  const bidsPrices = BOOK.psnap.bids.slice(0, DEPTH)
  const asksPrices = BOOK.psnap.asks.slice(0, DEPTH)

  const nowIso = moment.utc().toISOString()

  const row = {
    ts: nowIso,
    pair: pair,
    seq: Number.isFinite(seq) ? BigInt(seq) : null
  }

  // Fill bids
  for (let i = 0; i < DEPTH; i++) {
    const k = pad2(i + 1)
    const priceKey = bidsPrices[i]
    if (priceKey) {
      const pp = BOOK.bids[priceKey]
      row[`bid_p${k}`] = pp ? +pp.price : +priceKey
      row[`bid_a${k}`] = pp ? +pp.amount : 0
    } else {
      row[`bid_p${k}`] = null
      row[`bid_a${k}`] = null
    }
  }

  // Fill asks
  for (let i = 0; i < DEPTH; i++) {
    const k = pad2(i + 1)
    const priceKey = asksPrices[i]
    if (priceKey) {
      const pp = BOOK.asks[priceKey]
      row[`ask_p${k}`] = pp ? +pp.price : +priceKey
      row[`ask_a${k}`] = pp ? +pp.amount : 0
    } else {
      row[`ask_p${k}`] = null
      row[`ask_a${k}`] = null
    }
  }

  return row
}

function enqueueSnapshot () {
  if (diskFull) return

  const row = buildWideRow()
  if (!row) return

  buffer.push(row)

  if (buffer.length >= FLUSH_BATCH) {
    flushBuffer().catch(err => {
      console.error('[parquet] flush error (enqueue):', err && err.message ? err.message : err)
    })
  }
}

// ---- WS + book logic ----
function connect () {
  if (connecting || connected) return
  connecting = true

  cli = new WS(conf.wshost)

  cli.on('open', function open () {
    console.log('WS open')
    connecting = false
    connected = true

    // reset book
    BOOK.bids = {}
    BOOK.asks = {}
    BOOK.psnap = {}
    BOOK.mcnt = 0
    seq = null

    // enable checksum & sequence (65536 + 131072)
    cli.send(JSON.stringify({ event: 'conf', flags: 65536 + 131072 }))

    // subscribe to book with len = 25
    cli.send(JSON.stringify({
      event: 'subscribe',
      channel: 'book',
      pair: pair,
      prec: 'P0',
      len: 25
    }))
  })

  cli.on('close', function () {
    console.log('WS close')
    connecting = false
    connected = false
    seq = null
  })

  cli.on('error', function (err) {
    console.error('WS error:', err.message || err)
  })

  cli.on('message', function (msg) {
    if (diskFull) {
      // if disk is full, ignore further data
      return
    }

    msg = JSON.parse(msg)

    // ignore non-data events
    if (msg.event) return

    // heartbeat: [chanId, 'hb', seq]
    if (msg[1] === 'hb') {
      const hbSeq = +msg[2]
      if (Number.isFinite(hbSeq)) {
        if (seq == null) seq = hbSeq
      }
      return
    }

    // checksum message: [chanId, 'cs', checksum, seq]
    if (msg[1] === 'cs') {
      const checksum = msg[2]
      const csSeq = +msg[3]
      if (Number.isFinite(csSeq)) {
        seq = csSeq
      }

      const csdata = []
      const bids_keys = BOOK.psnap['bids'] || []
      const asks_keys = BOOK.psnap['asks'] || []

      for (let i = 0; i < 25; i++) {
        if (bids_keys[i]) {
          const price = bids_keys[i]
          const pp = BOOK.bids[price]
          if (pp) csdata.push(pp.price, pp.amount)
        }
        if (asks_keys[i]) {
          const price = asks_keys[i]
          const pp = BOOK.asks[price]
          if (pp) csdata.push(pp.price, -pp.amount)
        }
      }

      const cs_str = csdata.join(':')
      const cs_calc = CRC.str(cs_str)

      if (cs_calc !== checksum) {
        console.error('CHECKSUM_FAILED', 'calc=', cs_calc, 'server=', checksum)
        shutdown(-1)
      }
      return
    }

    // data messages (snapshot or update): [chanId, data, seq]
    const data = msg[1]
    const msgSeq = +msg[2]

    if (BOOK.mcnt === 0) {
      // ---- initial snapshot ----
      _.each(data, function (pp) {
        pp = { price: pp[0], cnt: pp[1], amount: pp[2] }
        const side = pp.amount >= 0 ? 'bids' : 'asks'
        pp.amount = Math.abs(pp.amount)
        BOOK[side][pp.price] = pp
      })

      if (Number.isFinite(msgSeq)) {
        seq = msgSeq
      } else {
        seq = null
      }
    } else {
      // ---- incremental update ----
      if (!Array.isArray(data) || data.length < 3) {
        return
      }

      const cseq = msgSeq
      if (Number.isFinite(cseq)) {
        if (seq != null && cseq > seq + 10) {
          console.warn('LARGE SEQ JUMP', 'prev=', seq, 'curr=', cseq)
        }
        seq = cseq
      }

      let pp = { price: data[0], cnt: data[1], amount: data[2] }

      if (!pp.cnt) {
        // delete level
        if (pp.amount > 0) {
          if (BOOK.bids[pp.price]) delete BOOK.bids[pp.price]
        } else if (pp.amount < 0) {
          if (BOOK.asks[pp.price]) delete BOOK.asks[pp.price]
        }
      } else {
        // update / insert
        let side = pp.amount >= 0 ? 'bids' : 'asks'
        pp.amount = Math.abs(pp.amount)
        BOOK[side][pp.price] = pp
      }
    }

    // rebuild price snapshots
    _.each(['bids', 'asks'], function (side) {
      const sbook = BOOK[side]
      const bprices = Object.keys(sbook)

      const prices = bprices.sort(function (a, b) {
        if (side === 'bids') {
          return +a >= +b ? -1 : 1
        } else {
          return +a <= +b ? -1 : 1
        }
      })

      BOOK.psnap[side] = prices
    })

    BOOK.mcnt++

    // enqueue wide snapshot after each processed message
    enqueueSnapshot()
  })
}

// periodically attempt reconnect
setInterval(function () {
  if (connected || diskFull) return
  connect()
}, 3500)

// kick off initial connect
connect()
