import aiohttp
import asyncio
import msd
import pandas as pd

BASE_URL = "http://localhost:50510"

async def async_msd_query(sql: str, h: Handler[R] = None) -> Generator[R, None, None] :
  endpoint = f"{BASE_URL}/data"
  async with aiohttp.ClientSession() as session:
    async with session.post(endpoint, json={"query": sql}, headers={
      "User-Agent": msd.MSD_USER_AGENT
    }) as response :
      if response.status != 200:
        raise Exception(f"Query failed: {response.text}")
      async for t in msd.parse_reader_async(response.content) :
        if h is not None :
          yield h(t)
        else :
          yield t


async def async_query_ok():
  sql = "select * from kline where obj='SH600000' limit 10"
  n = 0
  async for table in async_msd_query(sql, lambda t: pd.DataFrame(t)) :
    assert table.shape[0] == 10
    assert n == 0
    n += 1

def test_async_query_ok():
  asyncio.run(async_query_ok())

if __name__ == "__main__" :
  test_async_query_ok()