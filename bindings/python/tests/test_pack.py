from pymsd.dataframe_adaptor import PandasAdaptor
import pymsd
import numpy as np
import pandas as pd
import polars as pl
import io
import datetime


def gen_datetime(start: str, count: int) -> np.ndarray:
  start_dt = datetime.datetime.fromisoformat(start).microsecond * 1000
  return np.array(
    [start_dt + np.timedelta64(24 * i, "h") for i in range(count)],
    dtype="datetime64[us]",
  )


def gen_data(base: float, count: int) -> np.ndarray:
  return np.array([base + i for i in range(count)], dtype=np.float64)


df1 = pd.DataFrame(
  {
    "ts": gen_datetime("2020-01-01", 10),
    "open": gen_data(0, 10),
    "high": gen_data(10, 10),
    "low": gen_data(20, 10),
    "close": gen_data(40, 10),
    "volume": gen_data(50, 10),
    "amount": gen_data(60, 10),
  }
)

df2 = df1.copy()

df1.set_index("ts", inplace=True)


df3 = pl.DataFrame(
  {
    "ts": gen_datetime("2020-01-01", 10),
    "open": gen_data(0, 10),
    "high": gen_data(10, 10),
    "low": gen_data(20, 10),
    "close": gen_data(40, 10),
    "volume": gen_data(50, 10),
    "amount": gen_data(60, 10),
  }
)

df4 = [
  ("ts", gen_datetime("2020-01-01", 10)),
  ("open", gen_data(0, 10)),
  ("high", gen_data(10, 10)),
  ("low", gen_data(20, 10)),
  ("close", gen_data(40, 10)),
  ("volume", gen_data(50, 10)),
  ("amount", gen_data(60, 10)),
]


def test_pack_dataframe() -> None:
  adaptor = PandasAdaptor()
  for df in [df1, df2, df3, df4]:
    content = pymsd.pack_dataframe("test", df)
    for table, obj, parsed in pymsd.parse_reader(io.BytesIO(content)):
      assert table == ""
      assert obj == "test"
      parsed_df = adaptor.build(parsed)
      assert parsed_df.shape == (10, 7)
