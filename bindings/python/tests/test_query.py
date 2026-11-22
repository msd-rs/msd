import pandas as pd
import polars as pl
import pymsd


# Change the following variables to test different environment
BASE_URL = "http://localhost:50511"
RESULT_OBJECTS = 1846
RESULT_ROWS = 7438514
SQL_TO_TEST = "select * from stock_kline_1d where obj='SH60*'"


def test_query_ok():
  conn = pymsd.MsdConnection(BASE_URL)
  res = conn.query("select * from stock_kline_1d where obj='SH600000' limit 10")
  assert "SH600000" in res
  df = pd.DataFrame(res["SH600000"]["stock_kline_1d"].to_dict())
  assert df.shape[0] == 10


def test_query_many_ndarray(benchmark):
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def as_ndarray() -> None:
    rows = conn.query_raw(sql)
    assert len(rows) == RESULT_OBJECTS


def test_query_many_dataframe(benchmark) -> None:
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def as_dataframe() -> None:
    n = 0

    def handler(t: pymsd.Table):
      nonlocal n
      _ = pd.DataFrame(t.to_dict())
      n += 1

    conn.query_map(sql, handler)
    assert n == RESULT_OBJECTS


def test_query_many_polars(benchmark) -> None:
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def as_polars() -> None:
    n = 0

    def handler(t: pymsd.Table):
      nonlocal n
      _ = pl.DataFrame(t.to_dict())
      n += 1

    conn.query_map(sql, handler)
    assert n == RESULT_OBJECTS


def test_query_many_table(benchmark):
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def as_table() -> None:
    n = 0

    def handler(t: pymsd.Table):
      nonlocal n
      n += 1

    conn.query_map(sql, handler)
    assert n == RESULT_OBJECTS


def test_query_concat_pandas(benchmark) -> None:
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def concat_pandas() -> None:
    dfs: list[pd.DataFrame] = []

    def handler(t: pymsd.Table):
      dfs.append(pd.DataFrame(t.to_dict()))

    conn.query_map(sql, handler)
    df = pd.concat(dfs, ignore_index=True)
    assert df.shape[0] == RESULT_ROWS


def test_query_concat_polars(benchmark) -> None:
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def concat_polars() -> None:
    dfs = []

    def handler(t: pymsd.Table):
      dfs.append(pl.DataFrame(t.to_dict()))

    conn.query_map(sql, handler)
    df = pl.concat(dfs)
    assert df.height == RESULT_ROWS


def test_query_concat_table(benchmark) -> None:
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def concat_table() -> None:
    table, objs, bars = conn.query_join(sql, "stock_kline_1d", {}, "SH600000")
    assert table.rows == len(objs) * bars


def test_query_concat_table_polars(benchmark) -> None:
  sql = SQL_TO_TEST
  conn = pymsd.MsdConnection(BASE_URL)

  @benchmark
  def concat_table_polars() -> None:
    dfs = []

    def handler(table: pymsd.Table):
      dfs.append(pl.DataFrame(table.to_dict()))

    conn.query_map(sql, handler)
    df = pl.concat(dfs)
    assert df.height == RESULT_ROWS
