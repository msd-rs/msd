

import msd
import pandas as pd

BASE_URL = "http://localhost:50510"
SQL_TO_TEST = "select * from kline where obj='SH60*'"

DATA_TO_IMPORT = "../../dev/demo.with_header.csv"

with open(DATA_TO_IMPORT, "rb") as data:
  result = msd.import_csv(BASE_URL, "kline", data, header=False)
  print("inserted", result)

n = 0
for obj, table in msd.query(BASE_URL, "select * from kline where obj='SH000000' limit 10"):
  df = pd.DataFrame(table)
  assert df.shape[0] == 10
  assert n == 0
  print(obj)
  print(df)
  n += 1


