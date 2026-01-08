# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only

from typing import AsyncGenerator, Callable, Generator, Tuple, TypeVar, overload
from .reader import parse_reader, parse_reader_async
from .const import *
import logging

logger = logging.getLogger("MSD")

@overload
def query(baseURL: str, sql: str) -> Generator[Tuple[str, str, MsdTable], None, None] :
    ...

@overload
def query[R](baseURL: str, sql: str, h: Callable[[MsdTable], R]) -> Generator[Tuple[str, str, R], None, None] :
    ...

def query[R](baseURL: str, sql: str, h: Callable[[MsdTable], R] | None = None) -> Generator[Tuple[str, str, R|MsdTable], None, None] :
  """
  Query data from msd.

  Args:
    baseURL: The base URL of the msd server.
    sql: The SQL query to execute.
    h: The handler to call for each table, it's used to convert the table to another type, e.g. pandas.DataFrame or polars.DataFrame. 
  Returns:
    A generator of (table, obj, data)
  """

  try:
    import requests
  except ImportError:
    raise ImportError("requests is required for msd_query")

  logger.info(f"query {baseURL}, sql: {sql}")

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
    for table, obj, data in parse_reader(response.raw) : # type: ignore
      if h is not None :
        yield (table, obj, h(data))
      else :
        yield (table, obj, data)
  except Exception as e:
    logging.getLogger("MSD").warning("no data received. error: %s", e)


@overload
def async_query(baseURL: str, sql: str) -> AsyncGenerator[Tuple[str, str, MsdTable], None] :
    ...

@overload
def async_query[R](baseURL: str, sql: str, h: Callable[[MsdTable], R]) -> AsyncGenerator[Tuple[str, str, R], None] :
    ...

async def async_query[R](baseURL: str, sql: str, h: Callable[[MsdTable], R] | None = None) -> AsyncGenerator[Tuple[str, str, R|MsdTable], None] : # type: ignore
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
      async for table, obj, data in parse_reader_async(response.content) : # type: ignore
        if h is not None :
          yield (table, obj, h(data))
        else :
          yield (table, obj, data)