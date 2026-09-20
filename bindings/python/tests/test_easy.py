import pytest
import pandas as pd
import polars as pl
import numpy as np
import pymsd
import os
import time

BASE_URL = "http://localhost:50511"
TEST_TABLE = "stock_kline_1d"


def test_easy_query():
  obj1 = "SH600000"
  obj2 = "SH600004"
  objs = [obj1, obj2]
  pd_client = pymsd.create_msd_pandas(BASE_URL)
  r = pd_client.load(
    objs,
    TEST_TABLE,
  )
  print(r)
  df = r[obj1][TEST_TABLE]
  assert isinstance(df, pd.DataFrame)
  assert not df.empty
  assert df.shape[1] == 7  # ts, open, high, low, close, volume, amount

  pl_client = pymsd.create_msd_polars(BASE_URL)
  r = pl_client.load(
    objs,
    TEST_TABLE,
  )
  print(r)
  df = r[obj1][TEST_TABLE]
  assert isinstance(df, pl.DataFrame)
  assert not df.is_empty()
  assert df.shape[1] == 7  # ts, open, high, low

def test_easy_load_concat():
  client = pymsd.create_msd_polars(BASE_URL)
  t1 = time.time()
  symbols, fields = client.load_concat(
    objs=["SH000001", "SH600000"],
    tables=["stock_kline_1d", "stock_dividend", "stock_shares"],
    base_obj="SH000001",
    join={"stock_dividend": "zero", "*": "backward"},
    start=[2, 1, 1],
  )
  print(f"load_concat used {time.time() - t1}")
  groups = len(symbols)
  bars = len(fields['ts']) // groups
  fields['obj'] = np.concat([np.repeat([obj], bars) for obj in symbols])
  df = pl.DataFrame([pl.Series(name, data) for name, data in fields.items()])

  print(df)



if __name__ == "__main__":
  import logging

  logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s",)
  test_easy_load_concat()
