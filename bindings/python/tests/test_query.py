import pymsd
from pymsd.dataframe_adaptor import PandasAdaptor, PolarsAdaptor
import pandas as pd
import polars as pl


# Change the following variables to test different environment
BASE_URL = "http://localhost:50510"
RESULT_OBJECTS = 1789
RESULT_ROWS = 6245835
SQL_TO_TEST = "select * from kline where obj='SH60*'"


def test_query_ok():
  n = 0
  adaptor = PandasAdaptor()
  for table_name, obj, table in pymsd.query(
    BASE_URL, "select * from kline where obj='SH600000' limit 10", adaptor.build
  ):
    assert table.shape[0] == 10
    assert n == 0

    n += 1


def test_query_many_ndarray(benchmark):
  sql = SQL_TO_TEST

  @benchmark
  def as_ndarray() -> None:
    n = 0
    for table_name, obj, table in pymsd.query(BASE_URL, sql):
      n += 1
    assert n == RESULT_OBJECTS


def test_query_many_dataframe(benchmark) -> None:
  sql = SQL_TO_TEST
  adaptor = PandasAdaptor()

  @benchmark
  def as_dataframe() -> None:
    n = 0
    for table_name, obj, table in pymsd.query(BASE_URL, sql, adaptor.build):
      n += 1
    assert n == RESULT_OBJECTS


def test_query_many_polars(benchmark) -> None:
  sql = SQL_TO_TEST
  adaptor = PolarsAdaptor()

  @benchmark
  def as_polars() -> None:
    n = 0
    for table_name, obj, table in pymsd.query(BASE_URL, sql, adaptor.build):
      n += 1
    assert n == RESULT_OBJECTS


def test_query_concat_pandas(benchmark) -> None:
  sql = SQL_TO_TEST
  adaptor = PandasAdaptor()

  @benchmark
  def concat_pandas() -> None:
    dfs: list[pd.DataFrame] = []
    for table_name, obj, table in pymsd.query(BASE_URL, sql, adaptor.build):
      dfs.append(table)
    df = pd.concat(dfs, ignore_index=True)
    assert df.shape[0] == RESULT_ROWS


def test_query_concat_polars(benchmark) -> None:
  sql = SQL_TO_TEST
  adaptor = PolarsAdaptor()

  @benchmark
  def concat_polars() -> None:
    dfs = []
    for table_name, obj, table in pymsd.query(BASE_URL, sql, adaptor.build):
      dfs.append(table)
    df = pl.concat(dfs)
    assert df.height == RESULT_ROWS
