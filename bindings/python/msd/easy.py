# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only

"""
A Easy API for msd as pythonic way. Without writing SQL.
"""

import datetime
from .const import MsdTableFrame
from .dataframe_adaptor import DataFrameAdaptor
from .update import import_csv, import_dataframes
from typing import Iterator, Literal, overload
from collections import defaultdict
from .query import query


class MsdClient[DataFrame, Adaptor: DataFrameAdaptor]:
  def __init__(self, baseURL: str, adaptor: Adaptor) -> None:
    self.baseURL = baseURL
    self.adaptor = adaptor



  @overload
  def load(self, 
    objs: list[str] | str, 
    tables: list[str] | str, 
    join: Literal["backward", "forward", "nearest"],
    start: str | datetime.datetime | None = None, 
    end: str | datetime.datetime | None = None, 
    fields : dict[str, list[str]] | list[str] | None = None, 
    ) -> dict[str, DataFrame]:
    ...

  @overload
  def load(
    self,
    objs: list[str] | str, 
    tables: list[str] | str, 
    join: None = None,
    start: str | datetime.datetime | None = None, 
    end: str | datetime.datetime | None = None, 
    fields : dict[str, list[str]] | list[str] | None = None, 
    ) -> dict[str, dict[str, DataFrame]]:
    ...

  def load(self, 
    objs: list[str] | str, 
    tables: list[str] | str, 
    join: Literal["backward", "forward", "nearest"] | None = None,
    start: str | datetime.datetime | None = None, 
    end: str | datetime.datetime | None = None, 
    fields : dict[str, list[str]] | list[str] | None = None, 
    )-> dict[str, dict[str, DataFrame]] | dict[str, DataFrame]:
    """
    Load data from msd, the data will be organized as {obj: {table: DataFrame}} or {obj: DataFrame} if join is specified.

    """
    sql = []
    tables = [tables] if isinstance(tables, str) else tables
    objs = [objs] if isinstance(objs, str) else objs
    fields = {tables[0]: fields} if isinstance(fields, list) and len(tables) == 1 else fields
    for table in tables:
      table_fields = []
      if fields is None:
        table_fields = ["*"]
      elif isinstance(fields, dict):
        table_fields = fields.get(table, [])
        if len(table_fields) == 0:
          table_fields = ["*"]
        else:
          if "ts" not in table_fields:
            table_fields.insert(0, "ts")
          else:
            table_fields.remove("ts")
            table_fields.insert(0, "ts")
      ts_where = []
      if start is not None:
        ts_where.append(f"ts >= '{start}'")
      if end is not None:
        ts_where.append(f"ts < '{end}'")
      if len(ts_where) > 0:
        ts_where = "and " + " and ".join(ts_where)
      else:
        ts_where = ""
      obj_where = ", ".join([f"'{o}'" for o in objs])
      sql.append(f"select {', '.join(table_fields)} from {table} where obj in ({obj_where}) {ts_where};")

    
    # If only one table and one object, and no join, return DataFrame directly
    if len(tables) == 1 and len(objs) == 1 and join is None:
      for table, obj, df in query(self.baseURL, "\n".join(sql), self.adaptor.build):
        return df
    
    result = defaultdict(dict)
    for table, obj, df in query(self.baseURL, "\n".join(sql), self.adaptor.build):
      result[obj][table] = df


    if join is not None:
      joined_result: dict[str, DataFrame] = {}
      for obj in result.values():
        joined_df = None
        for table in obj.values():
          if joined_df is None:
            joined_df = table
          else:
            joined_df = self.adaptor.join_asof(joined_df, table, "ts", join)
        joined_result[obj] = joined_df
      return joined_result
    else:
      return result

  def save(self, table: str, data: Iterator[MsdTableFrame] | str, /, **kwargs) -> dict:
    """
    Save DataFrame or file to a table
    """

    if isinstance(data, str):
      if data.endswith('.csv'):
        with open(data, 'rb') as f:
          return import_csv(self.baseURL, table, f, **kwargs)
      else:
        raise ValueError(f"Unsupported file format: {data}")
    elif isinstance(data, Iterator):
      return import_dataframes(self.baseURL, table, data)


  def tables(self) -> list[str]:
    """
    List available tables
    """
    for _, _, result in query(self.baseURL, ".tables"):
      if isinstance(result, list):
        return result[0][1].tolist()
    return []

  def table_schema(self, table: str) -> DataFrame:
    """
    Get table schema
    """
    for _, _, result in query(self.baseURL, f"desc {table}", self.adaptor.build):
      return result

  def create_table(self, table: str, df: DataFrame):
    """
    Create a table from a DataFrame
    """
    sql = [f"create table {table} ("]
    col_def: list[str] = []
    for name, kind in self.adaptor.fields(df):
      col_def.append(f"{name} {kind}")
    sql.append(",\n".join(col_def))
    sql.append(")")
    for _, _, _ in query(self.baseURL, "\n".join(sql)):
      return


def create_msd_pandas(baseURL: str):
  import pandas
  from msd.dataframe_adaptor import PandasAdaptor
  return MsdClient[pandas.DataFrame, PandasAdaptor](baseURL, PandasAdaptor())


def create_msd_polars(baseURL: str):
  import polars
  from msd.dataframe_adaptor import PolarsAdaptor
  return MsdClient[polars.DataFrame, PolarsAdaptor](baseURL, PolarsAdaptor())
  


if __name__ == "__main__":
  c = create_msd_pandas("http://localhost:50510")
  a = c.load("obj", "table", )