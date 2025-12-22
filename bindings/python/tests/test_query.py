
import msd
import pandas as pd
import polars as pl


# Change the following variables to test different environment
BASE_URL = "http://localhost:50510"
RESULT_OBJECTS = 1789
RESULT_ROWS = 6245835
SQL_TO_TEST = "select * from kline where obj='SH60*'"

def test_query_ok():
  n = 0
  for obj, table in msd.query(BASE_URL, "select * from kline where obj='SH600000' limit 10"):
    df = pd.DataFrame(table)
    assert df.shape[0] == 10
    assert n == 0

    n += 1

def test_query_many_ndarray(benchmark) :
  sql = SQL_TO_TEST 

  @benchmark
  def as_ndarray() -> None :
    n = 0
    for obj, table in msd.query(BASE_URL, sql) :
      n += 1
    assert n == RESULT_OBJECTS

def test_query_many_dataframe(benchmark) -> None :
  sql = SQL_TO_TEST 
  @benchmark
  def as_dataframe() -> None :
    n = 0
    for obj, table in msd.query(BASE_URL, sql, lambda t: pd.DataFrame(t)) :
      n += 1
    assert n == RESULT_OBJECTS

def test_query_many_polars(benchmark) -> None :
  sql = SQL_TO_TEST 
  @benchmark
  def as_polars() -> None :
    n = 0
    for obj, table in msd.query(BASE_URL, sql, lambda t: pl.DataFrame(t)) :
      n += 1
    assert n == RESULT_OBJECTS

def test_query_concat_pandas(benchmark) -> None :
  sql = SQL_TO_TEST 

  @benchmark
  def concat_pandas() -> None :
    dfs = []
    for obj, table in msd.query(BASE_URL, sql, lambda t: pd.DataFrame(t)) :
      dfs.append(table)
    df = pd.concat(dfs, ignore_index=True)
    assert df.shape[0] == RESULT_ROWS
  
def test_query_concat_polars(benchmark) -> None :
  sql = SQL_TO_TEST 

  @benchmark
  def concat_polars() -> None :
    dfs = []
    for obj, table in msd.query(BASE_URL, sql, lambda t: pl.DataFrame(t)) :
      dfs.append(table)
    df = pl.concat(dfs)
    assert df.height == RESULT_ROWS
