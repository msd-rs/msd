# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only

"""
A Easy API for msd as pythonic way. Without writing SQL.
"""

from typing import Callable
from typing import Tuple
from pathlib import Path
from .json_table import parse_json_table
import datetime
from .const import MsdTableFrame
from .dataframe_adaptor import DataFrameAdaptor, JoinMethod
from .update import import_csv, import_dataframes
from typing import Iterator, overload, Generic, TypeVar
from collections import defaultdict
from .query import query
import logging
import numpy as np


logger = logging.getLogger("MSD")

DF = TypeVar("DF")

type RangeType = str | datetime.datetime | int

class MsdClient(Generic[DF]):
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
    start: RangeType | list[RangeType] | None = None,
    end: str | datetime.datetime | None = None,
    fields: dict[str, list[str]] | list[str] | None = None,
    pre_join_hook: Callable[[str, DF], DF] | None = None,
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
    start: RangeType | list[RangeType] | None = None,
    end: str | datetime.datetime | None = None,
    fields: dict[str, list[str]] | list[str] | None = None,
    pre_join_hook: Callable[[str, DF], DF] | None = None,
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
    start: RangeType | list[RangeType] | None = None,
    end: str | datetime.datetime | None = None,
    fields: dict[str, list[str]] | list[str] | None = None,
    pre_join_hook: Callable[[str, DF], DF] | None = None,
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
      pre_join_hook: a function that takes (table_name, df) and returns df, it will be called before join, it's useful for data preprocessing, for example, you can calculate some derived fields like YoY or MoM before join. First parameter is table name, second parameter is dataframe.

    Returns:
      dict[str, dict[str, DF]] or dict[str, DF]: the loaded data

    """
    sql = []
    tables = [tables] if isinstance(tables, str) else tables
    objs = [objs] if isinstance(objs, str) else objs
    fields = (
      {tables[0]: fields} if isinstance(fields, list) and len(tables) == 1 else fields
    )
    starts = []
    if start is None:
      starts = [None] * len(tables)
    elif not isinstance(start, list):
      starts = [start] * len(tables)
    else:
      starts = start

    for table, start in zip(tables, starts):
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
      # only filter date on the first table
      if start is not None and not isinstance(start, int):
        ts_where.append(f"ts >= '{start}'")
      if end is not None:
        if isinstance(end, str):
          if end.startswith("="):
            ts_where.append(f"ts <= '{end[1:]}'")
          else:
            ts_where.append(f"ts < '{end}'")
        else:
          ts_where.append(f"ts < '{end}'")
      if len(ts_where) > 0:
        ts_where = "and " + " and ".join(ts_where)
      else:
        ts_where = ""
      obj_where = ", ".join([f"'{o}'" for o in objs])
      limit = ""
      if isinstance(start, int):
        limit = f"limit -{start}"
      sql.append(
        f"select {', '.join(table_fields)} from {table} where obj in ({obj_where}) {ts_where} {limit};"
      )

    sql = "\n".join(sql)
    logger.debug(sql)
    if join is not None and pre_join_hook is None:
      # Fast path: vectorized join
      raw_results = defaultdict(dict)
      for table_name, obj, msd_table in query(self.baseURL, sql):
        raw_results[obj][table_name] = msd_table

      # Group objects by their start table index
      groups = defaultdict(list)
      for obj, obj_tables in raw_results.items():
        for idx, table_name in enumerate(tables):
          if table_name in obj_tables:
            groups[idx].append(obj)
            break

      joined_result: dict[str, DF] = {}

      def build_combined_df(table_name: str, group_objs: list[str]) -> DF | None:
        combined_columns = defaultdict(list)
        obj_arrays = []
        schema_cols = []

        for obj in group_objs:
          msd_table = raw_results[obj].get(table_name)
          if msd_table is None:
            continue
          if not schema_cols:
            schema_cols = [col_name for col_name, _ in msd_table]

          length = len(msd_table[0][1]) if msd_table else 0
          if length == 0:
            continue

          obj_arrays.append(np.repeat(obj, length))
          for col_name, array in msd_table:
            combined_columns[col_name].append(array)

        if not obj_arrays:
          return None

        concat_obj = np.concatenate(obj_arrays)
        concat_msd_table = []
        for col_name in schema_cols:
          arrays = combined_columns[col_name]
          if arrays:
            concat_msd_table.append((col_name, np.concatenate(arrays)))
        concat_msd_table.append(("obj", concat_obj))
        return self.adaptor.build(concat_msd_table)

      for start_idx, group_objs in groups.items():
        base_table_name = tables[start_idx]
        joined_df = build_combined_df(base_table_name, group_objs)
        if joined_df is None:
          continue

        joined_df = self.adaptor.sort(joined_df, "ts")

        for table_name in tables[start_idx+1:]:
          other_df = build_combined_df(table_name, group_objs)
          if other_df is None:
            continue

          other_df = self.adaptor.sort(other_df, "ts")

          if isinstance(join, dict):
            join_method = join.get(table_name, join.get("*", "nan"))
          elif isinstance(join, str):
            join_method = join
          else:
            raise ValueError("join must be a string or a dict of strings")

          joined_df = self.adaptor.join_asof(joined_df, other_df, "ts", join_method, by="obj")

        # Partition joined_df back by "obj"
        partitioned = self.adaptor.partition(joined_df, "obj")
        for obj, df in partitioned.items():
          joined_result[obj] = df

      # Handle any objects that were completely empty across all tables
      missing_objs = [obj for obj in raw_results if obj not in joined_result]
      if missing_objs:
        for obj in missing_objs:
          obj_tables = raw_results[obj]
          joined_df = None
          for table_name in tables:
            msd_table = obj_tables.get(table_name)
            if msd_table is None:
              continue
            df = self.adaptor.build(msd_table)
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
      # Fallback path: sequential join
      result: dict[str, dict[str, DF]] = defaultdict(dict)
      for table, obj, df in query(self.baseURL, sql, self.adaptor.build):
        if pre_join_hook is not None:
          df = pre_join_hook(table, df)
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

  def concat(
    self, dfs: dict[str, DF], /, base: str = "", join: JoinMethod = "nan"
  ) -> Tuple[DF | None, list[str]]:
    """
    Concatenate the result of load() to a long dataframe

    So the result can be passed to some analysis functions.

    Args:
      dfs: dict of dataframes, key is obj, value is dataframe
      base: base obj name, it's 'ts' column will be used as the join key, if empty or not in dfs, the first obj will be used as the base
      join: join method, used to join the dataframes

    Returns:
      dict[str, np.ndarray]: the concatenated dataframe, key is column name, value have length of len(dfs) * len(base), order by symbols
      list[str]: the symbols, first is base, then sorted symbols
    """
    return self.adaptor.concat(dfs, base, join)

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

  def table_schema(self, table: str) -> DF | None:
    """
    Get table schema, return None if table not exists
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
