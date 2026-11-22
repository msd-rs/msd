import pandas as pd
import pymsd

BASE_URL = "http://localhost:50510"
SQL_TO_TEST = "select * from kline where obj='SH60*'"

DATA_TO_IMPORT = "../../dev/demo.with_header.csv"

conn = pymsd.MsdConnection(BASE_URL)
with open(DATA_TO_IMPORT, "rb") as data:
  result = conn.import_csv("kline", data, header=False)
  print("inserted", result)

res = conn.query("select * from kline where obj='SH000000' limit 10")
table = res["SH000000"]["kline"]
df = pd.DataFrame(table.to_dict())
assert df.shape[0] == 10
print("SH000000")
print(df)
