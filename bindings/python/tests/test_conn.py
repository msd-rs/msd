import pymsd
import time

from pymsd._msd import Table

# Change the following variables to test different environment
BASE_URL = "http://localhost:50511"
SQL_TO_TEST = """
select * from stock_kline_1d where obj='@stock_block:全部A股' limit -1;
select * from stock_shares where obj='@stock_block:全部A股' limit -1;
select * from stock_capital_flow where obj='@stock_block:全部A股' limit -1;
"""



def test_query_many_ndarray():
  sql = SQL_TO_TEST

  n = 0
  t1 = time.time()
  for table_name, obj, table in pymsd.query(BASE_URL, sql):
    n += 1

  print(n, time.time() - t1)

def test_query_many_conn():
  sql = SQL_TO_TEST

  t1 = time.time() 
  conn = pymsd.MsdConnection(BASE_URL)
  table = conn.snapshot(["stock_kline_1d", "stock_shares", "stock_capital_flow"], ["@stock_block:全部A股"])
  print(table.rows, table.columns, time.time() - t1)

  print(table.header)
  for row in range(0, table.rows):
    for col in range(0, table.columns):
      print(table[row, col], end=' ')
    print()
    if row > 5:
      break

def test_query_kline():
  sql = """
select * from stock_kline_1d where obj='SH600*' limit -1000;
select * from stock_shares where obj='SH600*';
"""

  conn = pymsd.MsdConnection(BASE_URL)
  t1 = time.time() 
  table, objs, bars = conn.query_join(sql, "stock_kline_1d", {"stock_shares": "backward"}, "SH600000")
  t2 = time.time()

  # print(table.header)
  # for i in range(0, table.rows, bars):
  #   for j in range(0, 2):
  #     print(table[i+j, 0], table[i+j, 1], table[i+j, 2], table[i+j, 8], )
  #   for j in range(bars-2, bars):
  #     print(table[i+j, 0], table[i+j, 1], table[i+j, 2], table[i+j, 8], )
  print(table.rows, table.columns, bars, t2 - t1)


def test_query_map():
  sql = """
select * from stock_kline_1d where obj='SH600*' limit -1000;
"""
  conn = pymsd.MsdConnection(BASE_URL)

  objs = 0
  rows = 0
  def handler(table: Table):
    nonlocal objs, rows
    objs += 1
    rows += table.rows

  t1 = time.time()
  conn.query_map(sql, handler)

  print(rows, objs, time.time() - t1)


if __name__ == "__main__":
  #test_query_many_ndarray()
  #test_query_many_conn()
  #test_query_kline()
  test_query_map()
  

