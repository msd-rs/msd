
from typing import Callable, Generator, TypeVar
import requests
import msd
import pandas as pd
import numpy as np
import polars as pl

BASE_URL = "http://localhost:50510"


type Table = dict[str, np.ndarray]
R = TypeVar("R", default=Table)

type Handler[R] = Callable[[Table], R] 

def msd_query(sql: str, h: Handler[R] = None) -> Generator[R, None, None] :
  endpoint = f"{BASE_URL}/data"
  response = requests.post(endpoint, json={"query": sql}, stream=True, headers={
    "User-Agent": "msd-client"
  })
  if response.status_code != 200:
    raise Exception(f"Query failed: {response.text}")
  for t in msd.parse_reader(response.raw) :
    if h is not None :
      yield h(t)
    else :
      yield t


def test_query_ok():
  n = 0
  for table in msd_query("select * from kline where obj='SH600000' limit 10"):
    df = pd.DataFrame(table)
    assert df.shape[0] == 10
    assert n == 0

    n += 1

def test_query_many_ndarray(benchmark) :
  sql = "select * from kline where obj='SH601*'" 
  want_n = 226

  @benchmark
  def as_ndarray() -> None :
    n = 0
    for table in msd_query(sql) :
      n += 1
    assert n == want_n

def test_query_many_dataframe(benchmark) -> None :
  sql = "select * from kline where obj='SH601*'"
  want_n = 226
  @benchmark
  def as_dataframe() -> None :
    n = 0
    for table in msd_query(sql, lambda t: pd.DataFrame(t)) :
      n += 1
    assert n == want_n

def test_query_many_polars(benchmark) -> None :
  sql = "select * from kline where obj='SH601*'"
  want_n = 226
  @benchmark
  def as_polars() -> None :
    n = 0
    for table in msd_query(sql, lambda t: pl.DataFrame(t)) :
      n += 1
    assert n == want_n

def test_query_concat_pandas(benchmark) -> None :
  sql = "select * from kline where obj='SH601*'"
  want_n = 553990

  @benchmark
  def concat_pandas() -> None :
    dfs = []
    for table in msd_query(sql, lambda t: pd.DataFrame(t)) :
      dfs.append(table)
    df = pd.concat(dfs, ignore_index=True)
    assert df.shape[0] == want_n
  
def test_query_concat_polars(benchmark) -> None :
  sql = "select * from kline where obj='SH601*'"
  want_n = 553990

  @benchmark
  def concat_polars() -> None :
    df = None
    for table in msd_query(sql, lambda t: pl.DataFrame(t)) :
      if df is None :
        df = table
      else :
        df.vstack(table)
    assert df.height == want_n
