import msd
import numpy as np
import pandas as pd
import polars as pl
from io import BytesIO



def gen_datetime(start: str, count: int) -> np.ndarray:
  start = np.datetime64(start) - np.timedelta64(8, "h")
  return np.array([start + np.timedelta64(24 * i, "h") for i in range(count)], dtype="datetime64[us]")

def gen_data(base: float, count: int) -> np.ndarray:
  return np.array([base + i for i in range(count)], dtype=np.float64)



df1 = pd.DataFrame({
    "ts": gen_datetime("2020-01-01", 10),
    "open": gen_data(0, 10),
    "high": gen_data(10, 10),
    "low": gen_data(20, 10),
    "close": gen_data(40, 10),
    "volume": gen_data(50, 10),
})

df2 = pl.DataFrame({
    "ts": gen_datetime("2020-01-01", 10),
    "open": gen_data(0, 10),
    "high": gen_data(10, 10),
    "low": gen_data(20, 10),
    "close": gen_data(40, 10),
    "volume": gen_data(50, 10),
    "amount": gen_data(60, 10),
})

df3 = [
  ("ts", gen_datetime("2020-01-01", 10)),
  ("open", gen_data(0, 10)),
  ("high", gen_data(10, 10)),
  ("low", gen_data(20, 10)),
  ("close", gen_data(40, 10)),
  ("volume", gen_data(50, 10)),
  ("amount", gen_data(60, 10)),
]

print(df1)
print(df2)
print(df3)

msd.import_dataframes("http://127.0.0.1:50510", "kline", [("SH600000", df1), ("SH600001", df2), ("SH600002", df3)])

