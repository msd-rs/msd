import msd
import numpy as np
import pandas as pd
import polars as pl
import io


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
    "amount": gen_data(60, 10),
})

df2 = df1.copy()

df1.set_index("ts", inplace=True)


df3 = pl.DataFrame({
    "ts": gen_datetime("2020-01-01", 10),
    "open": gen_data(0, 10),
    "high": gen_data(10, 10),
    "low": gen_data(20, 10),
    "close": gen_data(40, 10),
    "volume": gen_data(50, 10),
    "amount": gen_data(60, 10),
})

df4 = [
  ("ts", gen_datetime("2020-01-01", 10)),
  ("open", gen_data(0, 10)),
  ("high", gen_data(10, 10)),
  ("low", gen_data(20, 10)),
  ("close", gen_data(40, 10)),
  ("volume", gen_data(50, 10)),
  ("amount", gen_data(60, 10)),
]

def test_pack_dataframe() -> None :
  for df in [df1, df2, df3, df4]:
    content = msd.pack_dataframe("test", df)
    for obj, parsed in msd.parse_reader(io.BytesIO(content)):
      assert obj == "test"
      parsed_df = pd.DataFrame(parsed)
      assert parsed_df.shape == (10, 7)

