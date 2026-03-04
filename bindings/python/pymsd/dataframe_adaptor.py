# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only

"""
Adaptors for DataFrames, because msd doesn't force users to use pandas or polars.
It use the adaptor pattern to adapt different DataFrames.
"""

from typing import Any, Generator, Literal, Tuple, Generic, TypeAlias, TypeVar
from .const import MsdTable, MsdTableFrame

JoinMethod: TypeAlias = Literal["backward", "forward", "nearest", "zero", "nan"]

DF = TypeVar("DF")


class DataFrameAdaptor(Generic[DF]):
  """
  Adaptor for DataFrame
  """

  def build(self, table: MsdTable) -> DF:
    """
    create a DataFrame from a table
    """
    ...

  def read_data_file(
    self, file_name: str, /, **kwargs
  ) -> Generator[MsdTableFrame, None, None]:
    """
    read a data file
    """
    ...

  def join_asof(
    self,
    df1: DF,
    df2: DF,
    on: str,
    method: JoinMethod,
  ) -> DF:
    """
    join two DataFrames asof
    """
    ...

  def fields(self, df: DF) -> list[Tuple[str, str]]:
    """
    get fields of a DataFrame
    """
    ...

  def to_msd_table(self, df: DF) -> MsdTable:
    """
    convert a DataFrame to a msd table
    """
    ...

  def is_data_frame(self, df: DF) -> bool:
    """
    check if a variable is a DataFrame
    """
    ...

  def is_data_file(self, p: str) -> bool:
    """
    check if a file is a data file
    """
    return p.endswith((".csv"))

  def concat(self, dfs: dict[str, DF], base: str, join: JoinMethod) -> DF:
    """
    Concatenate the result of load() to a long dataframe
    """
    ...


ADAPTORS: list[DataFrameAdaptor] = []

try:
  import pandas as pd

  class PandasAdaptor(DataFrameAdaptor[pd.DataFrame]):
    def build(self, table: MsdTable) -> pd.DataFrame:
      return pd.DataFrame({col: data for col, data in table})

    def read_data_file(
      self, p: str, /, **kwargs
    ) -> Generator[MsdTableFrame, None, None]:
      if p.endswith(".xlsx") or p.endswith(".xls"):
        read = pd.read_excel(p, **kwargs)
        if isinstance(read, dict):
          for sheet_name, df in read.items():
            yield (sheet_name, df)
        elif isinstance(read, pd.DataFrame):
          first_col = read.columns[0]
          for col, g in read.groupby(first_col):
            yield (str(col), g.drop(columns=[0]))
      else:
        raise ValueError(f"Unsupported file format: {p}")

    def join_asof(
      self,
      df1: pd.DataFrame | pd.Series,
      df2: pd.DataFrame | pd.Series,
      on: str,
      method: JoinMethod,
    ) -> pd.DataFrame:
      if method in ["backward", "forward", "nearest"]:
        return pd.merge_asof(df1, df2, on=on, direction=method)
      elif method == "nan":
        return pd.merge(df1, df2, on=on, how="left")
      elif method == "zero":
        df2_columns = df2.columns
        df = pd.merge(df1, df2, on=on, how="left")
        for col in df2_columns:
          if col != on:
            df[col] = df[col].fillna(0)
        return df
      else:
        raise ValueError(f"Unsupported method: {method}")

    def fields(self, df: pd.DataFrame) -> list[tuple[str, str]]:
      return [(str(col), self.dtype_to_sql(df[col].dtype.kind)) for col in df.columns]

    def is_data_frame(self, df: Any) -> bool:
      return isinstance(df, pd.DataFrame)

    def dtype_to_sql(self, kind: str) -> str:
      if kind in "SU":
        return "String"
      elif kind in "i":
        return "Int"
      elif kind in "u":
        return "UInt"
      elif kind in "f":
        return "Double"
      elif kind in "M":
        return "DateTime"
      elif kind in "b":
        return "Boolean"
      else:
        raise ValueError(f"Unsupported dtype: {kind}")

    def to_msd_table(self, df: pd.DataFrame) -> MsdTable:
      table: MsdTable = []
      if hasattr(df, "index") and df.index.name is not None:
        table.append((str(df.index.name), df.index.to_numpy()))
      for col in df.columns:
        table.append((str(col), df[col].to_numpy()))
      return table

    def concat(
      self, dfs: dict[str, pd.DataFrame], base: str, join: JoinMethod
    ) -> pd.DataFrame:
      if not dfs:
        return pd.DataFrame()

      base_obj = base if base in dfs else next(iter(dfs))
      base_df = dfs[base_obj]

      aligned_dfs = []

      for obj, df in dfs.items():
        if obj == base_obj:
          res_df = df.copy()
        else:
          res_df = self.join_asof(base_df["ts"], df, "ts", join)

        res_df.insert(0, "obj", obj)

        aligned_dfs.append(res_df)

      return pd.concat(aligned_dfs, ignore_index=True)

  ADAPTORS.append(PandasAdaptor())
except ImportError:
  pass


try:
  import polars as pl

  class PolarsAdaptor(DataFrameAdaptor[pl.DataFrame]):
    def build(self, table: MsdTable) -> pl.DataFrame:
      return pl.DataFrame([pl.Series(name, data) for name, data in table])

    def read_data_file(
      self, p: str, /, **kwargs
    ) -> Generator[MsdTableFrame, None, None]:
      if p.endswith(".xlsx") or p.endswith(".xls"):
        read = pl.read_excel(p, **kwargs)
        if isinstance(read, dict):
          for sheet_name, df in read.items():
            yield (sheet_name, df)
        elif isinstance(read, pl.DataFrame):
          first_col = read.columns[0]
          for col, g in read.group_by(first_col):
            yield (str(col), g.drop(first_col))
      else:
        raise ValueError(f"Unsupported file format: {p}")

    def join_asof(
      self,
      df1: pl.DataFrame,
      df2: pl.DataFrame,
      on: str,
      method: JoinMethod,
    ) -> pl.DataFrame:
      if method in ["backward", "forward", "nearest"]:
        return df1.join_asof(df2, on=on, strategy=method)
      elif method == "nan":
        return df1.join(df2, on=on, how="left")
      elif method == "zero":
        df2_columns = df2.columns
        df = df1.join(df2, on=on, how="left")
        for col in df2_columns:
          if col != on:
            df[col] = df[col].fill_nan(0)
        return df
      else:
        raise ValueError(f"Unsupported method: {method}")

    def fields(self, df: pl.DataFrame) -> list[tuple[str, str]]:
      return [(col, self.dtype_to_sql(df[col].dtype)) for col in df.columns]

    def is_data_frame(self, df: Any) -> bool:
      return isinstance(df, pl.DataFrame)

    def dtype_to_sql(self, kind: pl.DataType) -> str:
      if kind == pl.Utf8:
        return "String"
      elif kind in [pl.Int8, pl.Int16, pl.Int32, pl.Int64]:
        return "Int"
      elif kind in [pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64]:
        return "UInt"
      elif kind in [pl.Float32, pl.Float64]:
        return "Double"
      elif kind == pl.Datetime:
        return "DateTime"
      elif kind == pl.Boolean:
        return "Boolean"
      else:
        raise ValueError(f"Unsupported dtype: {kind}")

    def to_msd_table(self, df: pl.DataFrame) -> MsdTable:
      table: MsdTable = []
      for col in df.columns:
        table.append((col, df[col].to_numpy()))
      return table

    def concat(
      self, dfs: dict[str, pl.DataFrame], base: str, join: JoinMethod
    ) -> pl.DataFrame:
      if not dfs:
        return pl.DataFrame()

      base_obj = base if base in dfs else next(iter(dfs))
      base_df = dfs[base_obj]

      aligned_dfs = []
      for obj, df in dfs.items():
        if obj == base_obj:
          res_df = df
        else:
          res_df = self.join_asof(base_df.select("ts"), df, "ts", join)

        # Add obj column
        res_df = res_df.with_columns(pl.lit(obj).alias("obj"))
        # Reorder to make obj first, ts second.
        original_cols = res_df.columns
        cols = ["obj", "ts"] + [c for c in original_cols if c not in ["obj", "ts"]]
        res_df = res_df.select(cols)

        aligned_dfs.append(res_df)

      return pl.concat(aligned_dfs)

  ADAPTORS.append(PolarsAdaptor())
except ImportError:
  pass
