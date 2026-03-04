import pytest
import pandas as pd
import polars as pl
import numpy as np
import pymsd
import os

BASE_URL = "http://localhost:50510"
TEST_TABLE = "kline"


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


if __name__ == "__main__":
  import logging

  logging.basicConfig(level=logging.INFO)
  test_easy_query()
