from _io import BytesIO
from typing import TypeVar, Callable, Generator, Tuple
import numpy as np
from .reader import parse_reader, parse_reader_async
from .const import *
import logging
type Table = dict[str, np.ndarray]
R = TypeVar("R", default=Table)

type Handler[R] = Callable[[Table], R] 



def query(baseURL: str, sql: str, h: Handler[R] = None) -> Generator[Tuple[str, R], None, None] :
  """
  Query data from msd.

  Args:
    baseURL: The base URL of the msd server.
    sql: The SQL query to execute.
    h: The handler to call for each table, it's used to convert the table to another type, e.g. pandas.DataFrame or polars.DataFrame. 
  Returns:
    A generator of tables.
  """

  try:
    import requests
    import requests.exceptions
  except ImportError:
    raise ImportError("requests is required for msd_query")

  endpoint = f"{baseURL}{MSD_QUERY_PATH}"
  response = requests.post(endpoint, json={"query": sql}, stream=True, headers={
    # msd server will use this to identify the client, and return binary format if it's set.
    "User-Agent": MSD_USER_AGENT,
    # don't compress the response, compress is too slow, when internal network is used, bandwidth is not the bottleneck.
    "Accept-Encoding": "identity",
  })
  if response.status_code != 200:
    raise Exception(f"Query failed: {response.text}")
  try:
    for obj, table in parse_reader(response.raw) :
      if h is not None :
        yield (obj, h(table))
      else :
        yield (obj, table)
  except Exception as e:
    logging.getLogger("MSD").warning("no data received. error: %s", e)



async def async_query(baseURL: str, sql: str, h: Handler[R] = None) -> Generator[Tuple[str, R], None, None] :
  """
  The async version of msd_query.

  Args:
    baseURL: The base URL of the msd server.
    sql: The SQL query to execute.
    h: The handler to call for each table, it's used to convert the table to another type, e.g. pandas.DataFrame or polars.DataFrame. 
  Returns:
    A generator of tables.
  """
  try:
    import aiohttp
  except ImportError:
    raise ImportError("aiohttp is required for async_msd_query")

  endpoint = f"{baseURL}/query"
  async with aiohttp.ClientSession() as session:
    async with session.post(endpoint, json={"query": sql}, headers={
      # msd server will use this to identify the client, and return binary format if it's set.
      "User-Agent": MSD_USER_AGENT,
      "Accept-Encoding": "identity",
    }) as response :
      if response.status != 200:
        raise Exception(f"Query failed: {response.text}")
      async for obj, table in parse_reader_async(response.content) :
        if h is not None :
          yield (obj, h(table))
        else :
          yield (obj, table)