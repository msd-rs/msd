import asyncio
import msd
from msd.dataframe_adaptor import PandasAdaptor
import pandas as pd

BASE_URL = "http://localhost:50510"



async def async_query_ok():
  sql = "select * from kline where obj='SH600000' limit 10"
  n = 0
  adaptor = PandasAdaptor()
  async for table, obj, df in msd.async_query(BASE_URL, sql, adaptor.build) :
    assert df.shape[0] == 10
    assert n == 0
    n += 1

def test_async_query_ok():
  asyncio.run(async_query_ok())

if __name__ == "__main__" :
  test_async_query_ok()