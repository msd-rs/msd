

import msd
import pandas as pd

BASE_URL = "http://localhost:50510"
RESULT_OBJECTS = 1789
RESULT_ROWS = 6245835
SQL_TO_TEST = "select * from kline where obj='SH60*'"


n = 0
for obj, table in msd.msd_query(BASE_URL, "select * from kline where obj='SH600000' limit 10"):
  df = pd.DataFrame(table)
  assert df.shape[0] == 10
  assert n == 0

  n += 1


