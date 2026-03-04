import pandas as pd
import polars as pl
from datetime import datetime
from pymsd.dataframe_adaptor import PandasAdaptor, PolarsAdaptor


def test_pandas_concat():
  adaptor = PandasAdaptor()

  # Create sample dataframes
  ts1 = pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"])
  df1 = pd.DataFrame({"ts": ts1, "val": [1, 2, 3]})

  ts2 = pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-04"])
  df2 = pd.DataFrame({"ts": ts2, "val": [4, 5, 6]})

  dfs = {"A": df1, "B": df2}

  # Case 1: Concatenate with default join (nan)
  # Base is A (first one if not specified, or we specify "A")
  res = adaptor.concat(dfs, base="A", join="nan")

  # Expected:
  # A: 2023-01-01, 2023-01-02, 2023-01-03
  # B: Aligned to A's ts.
  # 2023-01-01 matches (4)
  # 2023-01-02 matches (5)
  # 2023-01-03 no match in B (NaN)

  assert isinstance(res, pd.DataFrame)
  assert list(res.columns) == ["obj", "ts", "val"]
  assert len(res) == 6  # 3 for A + 3 for B

  # Check A
  res_a = res[res["obj"] == "A"]
  assert len(res_a) == 3
  assert res_a["val"].tolist() == [1, 2, 3]

  # Check B
  res_b = res[res["obj"] == "B"]
  assert len(res_b) == 3
  # First two should correspond to B's values
  assert res_b.iloc[0]["val"] == 4
  assert res_b.iloc[1]["val"] == 5
  assert pd.isna(res_b.iloc[2]["val"])


def test_pandas_concat_join_methods():
  adaptor = PandasAdaptor()
  ts1 = pd.to_datetime(["2023-01-01", "2023-01-03"])
  df1 = pd.DataFrame({"ts": ts1, "val": [1, 3]})

  ts2 = pd.to_datetime(["2023-01-02"])
  df2 = pd.DataFrame({"ts": ts2, "val": [2]})

  dfs = {"A": df1, "B": df2}

  # Join backward
  # B aligned to A:
  # 2023-01-01 -> backward search in B (<= 2023-01-01). None? B starts at 01-02.
  # 2023-01-03 -> backward search in B (<= 2023-01-03). Match 2023-01-02 (val 2).

  res = adaptor.concat(dfs, base="A", join="backward")
  res_b = res[res["obj"] == "B"]

  # Depending on merge_asof behavior for "backward":
  # "selects the last row in the right DataFrame whose 'on' key is less than or equal to the left's key."
  # For 2023-01-01: B has 2023-01-02. Nothing <= 01-01? So NaN.
  # For 2023-01-03: B has 2023-01-02. Match.

  assert pd.isna(res_b.iloc[0]["val"])
  assert res_b.iloc[1]["val"] == 2


def test_polars_concat():
  adaptor = PolarsAdaptor()

  ts1 = [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 3)]
  df1 = pl.DataFrame({"ts": ts1, "val": [1, 2, 3]})

  ts2 = [datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 4)]
  df2 = pl.DataFrame({"ts": ts2, "val": [4, 5, 6]})

  dfs = {"A": df1, "B": df2}

  res = adaptor.concat(dfs, base="A", join="nan")

  assert isinstance(res, pl.DataFrame)
  assert res.columns[:2] == ["obj", "ts"]
  assert res.height == 6

  res_a = res.filter(pl.col("obj") == "A")
  assert res_a["val"].to_list() == [1, 2, 3]

  res_b = res.filter(pl.col("obj") == "B")
  vals = res_b["val"].to_list()
  assert vals[0] == 4
  assert vals[1] == 5
  assert vals[2] is None  # Polars None for missing
