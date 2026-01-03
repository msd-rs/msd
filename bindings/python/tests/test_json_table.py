import msd
from msd.dataframe_adaptor import PolarsAdaptor, PandasAdaptor


sample = """
{"version":1299972097,"columns":[{"name":"ts","kind":"DateTime","metadata":null,"data":{"DateTime":[]}},{"name":"open","kind":"Float64","metadata":null,"data":{"Float64":[]}},{"name":"high","kind":"Float64","metadata":null,"data":{"Float64":[]}},{"name":"low","kind":"Float64","metadata":null,"data":{"Float64":[]}},{"name":"close","kind":"Float64","metadata":null,"data":{"Float64":[]}},{"name":"volume","kind":"Float64","metadata":null,"data":{"Float64":[]}},{"name":"amount","kind":"Float64","metadata":null,"data":{"Float64":[]}}],"metadata":{"round":{"String":"1d"},"chunkSize":{"UInt32":250}}}
"""


def test_parse_json_table():
  table = msd.parse_json_table(sample)
  assert len(table) == 7
  print(table)

  adaptor = PolarsAdaptor()
  df = msd.parse_json_table(sample, adaptor.build)
  assert isinstance(df, pl.DataFrame)
  assert df.shape == (0, 7)
  print(df)

  adaptor = PandasAdaptor()
  df = msd.parse_json_table(sample, adaptor.build)
  assert isinstance(df, pd.DataFrame)
  assert df.shape == (0, 7)
  print(df)




