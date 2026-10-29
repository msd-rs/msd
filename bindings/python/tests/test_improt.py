from typing import Generator

import pymsd
import logging
import numpy as np
import datetime
import pandas as pd
import time
logger = logging.getLogger("MSD")

MSD_BASE_URL = "http://localhost:50510"

def create_kline_db():
  sqls = """DROP TABLE kline;
create table kline ( ts datetime, open d64, high d64, low d64, close d64, volume DOUBLE, amount DOUBLE) with ( chunkSize = 250, round='1d');
""".split(";")
  for sql in sqls:
    sql = sql.strip()
    if len(sql) == 0:
      continue
    logger.info(f"execute {sql}")
    for _, k, v in pymsd.query(MSD_BASE_URL, sql):
      logger.info("execute done")

def parse_datetime(ts: str) -> int:
  kind = len(ts.split(' '))
  if kind == 1:
    ts += ' 00:00:00 000000'
  elif kind == 2:
    ts += ' 000000'
  return int(datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S %f').timestamp() * 1_000_000) + 8 * 60 * 60 * 1_000_000


def generate_codes(count: int, prefix: str = 'SH') -> list[str]:
  max_len = 8
  width = max_len - len(prefix)
  return [f"{prefix}{i:0{width}d}" for i in range(count)]

def parse_duration(d: str) -> int:
  if d.endswith('us'):
    return int(d[:-2])
  if d.endswith('ms'):
    return int(d[:-2]) * 1_000
  if d.endswith('s'):
    return int(d[:-1]) * 1_000_000
  if d.endswith('m'):
    return int(d[:-1]) * 60 * 1_000_000
  if d.endswith('h'):
    return int(d[:-1]) * 60 * 60 * 1_000_000
  if d.endswith('d'):
    return int(d[:-1]) * 60 * 60 * 24 * 1_000_000
  raise ValueError(f"Invalid duration: {d}")

def generate_kline_frame(
  rows: int, prefix: str, start: str = "1990-01-01", duration: str = "1d"
) -> pd.DataFrame:
  start_ts = parse_datetime(start)
  ts_interval = parse_duration(duration)

  timestamps_int = start_ts + np.arange(rows, dtype=np.int64) * ts_interval

  initial_price = np.random.randint(1_00, 1000_00) / 100.0
  # Generate 4 streams of random multipliers
  multipliers = 1.0 + (np.random.randint(0, 21, size=(rows, 4)) - 10) / 100.0
  step_multipliers = multipliers.mean(axis=1)
  cumulative_growth = np.cumprod(np.insert(step_multipliers[:-1], 0, 1.0))
  base_prices = initial_price * cumulative_growth

  ohlc_values = base_prices[:, np.newaxis] * multipliers

  # Extract Open, High, Low, Close
  _open = ohlc_values[:, 0]
  _close = ohlc_values[:, -1]
  _high = ohlc_values.max(axis=1)
  _low = ohlc_values.min(axis=1)

  # Volume: random integer
  volume = np.random.randint(10_000, 10_000_001, size=rows)

  # Amount: (volume * avg) / 10000.0
  row_avg_prices = base_prices * step_multipliers
  amount = (volume * row_avg_prices) / 10000.0

  return pd.DataFrame(
    {
      "ts": timestamps_int.astype("datetime64[us]"),
      "open": _open.astype("float64"),
      "high": _high.astype("float64"),
      "low": _low.astype("float64"),
      "close": _close.astype("float64"),
      "volume": volume.astype("float64"),
      "amount": amount.astype("float64"),
    }
  )



def kline_frames(
  n: int, rows: int, prefix: str, start: str = "1990-01-01", duration: str = "1d"
) -> Generator[tuple[str, pd.DataFrame], None, None]:
  df = generate_kline_frame(rows, prefix, start, duration)
  for obj in generate_codes(n):
    # use same data for all objects because we want to measure the performance of MSD instead of the performance of data generation
    yield obj, df

def insert_rows_binary(n: int, rows: int, prefix: str = "SH"):
  t1 = time.time()
  logger.info("insert binary start")
  result = pymsd.import_dataframes(MSD_BASE_URL, "kline", kline_frames(n, rows, prefix), {
    "open": 2,
    "high": 2,
    "low": 2,
    "close": 2,
  })
  rows = result["total_rows"]
  server_d = result["time_used_ms"] / 1000.0
  client_d = time.time() - t1
  logger.info(
    f"insert binary done rows: {rows:,} used: {client_d:.3f}/s  speed: {rows / client_d:,.0f} rows/second server_used: {server_d:.3f}"
  )

if __name__ == "__main__":
  insert_rows_binary(100, 100)