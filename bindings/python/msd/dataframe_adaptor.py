import numpy as npt
from typing import Any, Generator, Literal, Tuple
from os import PathLike
from .const import MsdTable, MsdTableFrame


class DataFrameAdaptor[DataFrame]():

  def build(self, table: MsdTable) -> DataFrame:
    """
    create a DataFrame from a table
    """
    ...

  def read_data_file(self, file_name: str, / , **kwargs) -> Generator[MsdTableFrame, None, None]:
    """
    read a data file
    """
    ...
  
  def join_asof(self, df1: DataFrame, df2: DataFrame, on: str, method: Literal['backward', 'forward', 'nearest']) -> DataFrame:
    """
    join two DataFrames asof
    """
    ...

  def fields(self, df: DataFrame) -> list[tuple[str, str]]:
    """
    get fields of a DataFrame
    """
    ...

  def to_msd_table(self, df: DataFrame) -> MsdTable:
    """
    convert a DataFrame to a msd table
    """
    ...

  def is_data_frame(self, df: DataFrame) -> bool:
    """
    check if a variable is a DataFrame
    """
    ...

  def is_data_file(self, p: str) -> bool:
    """
    check if a file is a data file
    """
    return p.endswith(('.csv', '.json', '.jsonl', '.xlsx', '.xls'))



ADAPTORS = []
try:
  import pandas as pd
  class PandasAdaptor(DataFrameAdaptor[pd.DataFrame]):
    def build(self, table: MsdTable) -> pd.DataFrame:
      return pd.DataFrame({col: data for col, data in table})
    
    def read_data_file(self, p: str, / , **kwargs) -> Generator[MsdTableFrame, None, None]:
      if p.endswith('.xlsx') or p.endswith('.xls'):
        read = pd.read_excel(p, **kwargs)
        if isinstance(read, dict):
          for sheet_name, df in read.items():
            yield (sheet_name, self.to_msd_table(df))
        elif isinstance(read, pd.DataFrame):
          first_col = read.columns[0]
          for col, g in read.groupby(first_col):
            yield (str(col), self.to_msd_table(g.drop(columns=first_col)))
      else:
        raise ValueError(f"Unsupported file format: {p}")
    
    def join_asof(self, df1: pd.DataFrame, df2: pd.DataFrame, on: str, method: Literal['backward', 'forward', 'nearest']
) -> pd.DataFrame:
      return pd.merge_asof(df1, df2, on=on, direction=method)
    
    def fields(self, df: pd.DataFrame) -> list[tuple[str, str]]:
      return [(col, self.dtype_to_sql(df[col].dtype.kind)) for col in df.columns]
    
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
        table.append((col, df[col].to_numpy()))
      return table
  ADAPTORS.append(PandasAdaptor())
except ImportError:
  pass


try:
  import polars as pl
  class PolarsAdaptor(DataFrameAdaptor[pl.DataFrame]):
    def build(self, table: MsdTable) -> pl.DataFrame:
      return pl.DataFrame([pl.Series(name, data) for name, data in table])
    
    def read_data_file(self, p: str, / , **kwargs) -> Generator[MsdTableFrame, None, None]:
      if p.endswith('.xlsx') or p.endswith('.xls'):
        read = pl.read_excel(p, **kwargs)
        if isinstance(read, dict):
          for sheet_name, df in read.items():
            yield (sheet_name, self.to_msd_table(df))
        elif isinstance(read, pl.DataFrame):
          first_col = read.columns[0]
          for col, g in read.group_by(first_col):
            yield (str(col), self.to_msd_table(g.drop(first_col)))
      else:
        raise ValueError(f"Unsupported file format: {p}")
    
    def join_asof(self, df1: pl.DataFrame, df2: pl.DataFrame, on: str, method: Literal['backward', 'forward', 'nearest']) -> pl.DataFrame:
      return df1.join_asof(df2, on=on, strategy=method)
    
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
  ADAPTORS.append(PolarsAdaptor())
except ImportError:
  pass


