# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only

"""
A Easy API for msd as pythonic way. Without writing SQL.
"""

from pathlib import Path
from .json_table import parse_json_table
import datetime
from .const import MsdTableFrame
from .dataframe_adaptor import DataFrameAdaptor, JoinMethod
from .update import import_csv, import_dataframes
from typing import Iterator, overload
from collections import defaultdict
from .query import query


class MsdClient[DF]:
  """
  A Easy API for msd as pythonic way. Without writing SQL.

  To use it, you need to create a MsdClient instance with a DataFrameAdaptor.
  """

  def __init__(self, baseURL: str, adaptor: DataFrameAdaptor[DF]) -> None:
    self.baseURL = baseURL
    self.adaptor = adaptor
    self._table_schemas: dict[str, DF] = {}

  @overload
  def load(
    self,
    objs: list[str] | str,
    tables: list[str] | str,
    join: JoinMethod | dict[str, JoinMethod],
    start: str | datetime.datetime | None = None,
    end: str | datetime.datetime | None = None,
    fields: dict[str, list[str]] | list[str] | None = None,
  ) -> dict[str, DF]:
    """
    Load data from msd, result will be organized as {obj: DF} because join is specified.
    """
    ...

  @overload
  def load(
    self,
    objs: list[str] | str,
    tables: list[str] | str,
    join: None = None,
    start: str | datetime.datetime | None = None,
    end: str | datetime.datetime | None = None,
    fields: dict[str, list[str]] | list[str] | None = None,
  ) -> dict[str, dict[str, DF]]:
    """
    Load data from msd, result will be organized as {obj: {table: DF}} because join is not specified.
    """
    ...

  def load(
    self,
    objs: list[str] | str,
    tables: list[str] | str,
    join: JoinMethod | dict[str, JoinMethod] | None = None,
    start: str | datetime.datetime | None = None,
    end: str | datetime.datetime | None = None,
    fields: dict[str, list[str]] | list[str] | None = None,
  ) -> dict[str, dict[str, DF]] | dict[str, DF]:
    """
    Load data from msd, the data will be organized as {obj: {table: DF}} or {obj: DF} if join is specified.

    Args:
      objs: list of object names or a single object name
      tables: list of table names or a single table name
      join: always left join on 'ts' column, can be
        - a string of join method
          - "backward" : join asof backward
          - "forward" : join asof forward
          - "nearest" : join asof nearest
          - "zero" : fill non-exist rows with zero
          - "nan" : fill non-exist rows with nan
        - a dict of join method, key is table name, value is join method
          - special key "*" means default join method
          - 'nan' is fallback method when neither table name nor "*" is specified
        - None: no join, result will be organized as {obj: {table: DF}}
      start: start time, can be str or datetime.datetime
      end: end time, can be str or datetime.datetime
      fields: fields to load, can be dict[str, list[str]] or list[str] or None

    Returns:
      dict[str, dict[str, DF]] or dict[str, DF]: the loaded data

    """
    sql = []
    tables = [tables] if isinstance(tables, str) else tables
    objs = [objs] if isinstance(objs, str) else objs
    fields = (
      {tables[0]: fields} if isinstance(fields, list) and len(tables) == 1 else fields
    )
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
      sql.append(
        f"select {', '.join(table_fields)} from {table} where obj in ({obj_where}) {ts_where};"
      )

    result: dict[str, dict[str, DF]] = defaultdict(dict)
    for table, obj, df in query(self.baseURL, "\n".join(sql), self.adaptor.build):
      result[obj][table] = df

    if join is not None:
      joined_result: dict[str, DF] = {}
      for obj, obj_tables in result.items():
        joined_df: DF | None = None
        for table_name in tables:
          df = obj_tables.get(table_name)
          if df is None:
            continue
          if joined_df is None:
            joined_df = df
          else:
            if isinstance(join, dict):
              join_method = join.get(table_name, join.get("*", "nan"))
            elif isinstance(join, str):
              join_method = join
            else:
              raise ValueError("join must be a string or a dict of strings")
            joined_df = self.adaptor.join_asof(joined_df, df, "ts", join_method)
        if joined_df is not None:
          joined_result[obj] = joined_df
      return joined_result
    else:
      return result

  def save(self, table: str, data: Iterator[MsdTableFrame] | str, /, **kwargs) -> dict:
    """
    Save DataFrame or file to a table

    Args:
      table: table name
      data: iterator of MsdTableFrame or csv file path, read 'import csv' for more details
    """

    if isinstance(data, str):
      p = Path(data)
      if p.suffix == ".csv" and p.is_file():
        with open(data, "rb") as f:
          return import_csv(self.baseURL, table, f, **kwargs)
      elif p.is_dir():
        return import_dataframes(
          self.baseURL, table, self.adaptor.read_data_file(data, **kwargs)
        )
      else:
        raise ValueError(f"Unsupported file format: {data}")
    elif isinstance(data, Iterator):
      return import_dataframes(self.baseURL, table, data)
    else:
      raise ValueError(f"Unsupported data type: {type(data)}")

  def tables(self) -> list[str]:
    """
    List available tables
    """
    for _, _, result in query(self.baseURL, ".tables"):
      if len(result) != 2:
        raise ValueError("Unexpected result from .tables")
      for name, schema in zip(result[0][1], result[1][1]):
        df = parse_json_table(schema, self.adaptor.build)
        self._table_schemas[name] = df

    return list(self._table_schemas.keys())

  def table_schema(self, table: str) -> DF:
    """
    Get table schema
    """
    if table in self._table_schemas:
      return self._table_schemas[table]
    for _, _, result in query(self.baseURL, f"desc {table}", self.adaptor.build):
      self._table_schemas[table] = result
      return result

  def create_table(self, table: str, df: DF):
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
  """
  Create a MsdClient instance with pandas DataFrame
  """
  import pandas
  from .dataframe_adaptor import PandasAdaptor

  return MsdClient[pandas.DataFrame](baseURL, PandasAdaptor())  # type: ignore


def create_msd_polars(baseURL: str):
  """
  Create a MsdClient instance with polars DataFrame
  """
  import polars
  from .dataframe_adaptor import PolarsAdaptor

  return MsdClient[polars.DataFrame](baseURL, PolarsAdaptor())  # type: ignore


if __name__ == "__main__":
  c = create_msd_pandas("http://localhost:50510")
  a = c.load(
    "obj",
    "table",
  )
