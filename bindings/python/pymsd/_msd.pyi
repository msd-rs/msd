from typing import Any, BinaryIO, Callable, Iterable, Tuple
from pathlib import Path
from .const import MsdTable, MsdTableFrame
import numpy as np

def pack_table_frame(obj: str, table: MsdTable, decimal_fields: dict[str, int] | None = None) -> bytes:
  """
  Pack a table frame into a bytes stream.
  """
  ...

def check_table_frame(frame: bytes) -> bool:
  """
  Check if the bytes stream is a valid table frame.
  """
  ...

def parse_table_frame(frame: bytes) -> Tuple[str, str, MsdTable]:
  """
  Parse a bytes stream into a table frame.
  """
  ...

def aligned_index(left: np.ndarray, right: np.ndarray, method: int) -> np.ndarray:
  """
  join right to left use method, return the index of right
  """
  ...



class Table:
  """
  Columnar table representation returned by MSD operations.
  """
  @property
  def rows(self) -> int:
    """Number of rows in the table."""
    ...
  @property
  def columns(self) -> int:
    """Number of columns in the table."""
    ...
  @property
  def name(self) -> str:
    """Table name stored in metadata."""
    ...
  @property
  def obj(self) -> str:
    """Object name stored in metadata."""
    ...
  @property
  def header(self) -> list[str]:
    """List of column names in the table."""
    ...
  def __getitem__(self, index: tuple[int, int]) -> Any:
    """
    Get cell value at (row, column) index.
    """
    ...
  def to_list(self) -> MsdTable:
    """
    Transform this table to a list of (name, data) tuples, where data is a typed numpy ndarray.
    After the transform, this Table object will be reset/cleared.
    """
    ...

  def to_dict(self) -> dict[str, np.ndarray]:
    """
    Transform this table to a dict of (name, data) , where data is a typed numpy ndarray.
    After the transform, this Table object will be reset/cleared.

    For duplicate field name, the first is kept, others will be ignored 

    Python 3.7+ dict implementation ensure the dict insert order
    """
    ...

class MsdConnection:
  """
  Client connection for querying and snapshotting MSD database tables.
  """
  def __init__(self, url: str, token: str = ..., compress: bool = ...) -> None:
    """
    Initialize a new MSD connection.

    Args:
      url: Base URL of the MSD server.
      token: Optional Bearer authentication token.
      compress: Whether to enable zstd compression on responses.
    """
    ...
  def query(self, sql: str) -> dict[str, dict[str, Table]]:
    """
    Execute raw query and group the results into a 2-level dict:
    level 1: obj -> level 2: table name -> Table.
    """
    ...

  def query_map(self, sql: str, h: Callable[[Table],Any]) -> None:
    """
    Execute raw query with a handler which can process each result table
    """
    ...
    
  def query_join(
    self,
    sql: str,
    base_table: str,
    fill: dict[str, str],
    base_obj: str,
  ) -> tuple[Table, list[str], int]:
    """
    Query and join the result to one table.
    The join result will have:
    - rows: count_of_objects * base_table.rows
    - columns: sum columns of join_asof for base_obj's tables
    Returns (Table, list[str], bars) as 
      - full data
      - objs
      - each obj data len
      it means len(objs) * bars == full_data.rows
    Raises error when base_obj or base_table is not found.

    Args:
      sql: SQL query statement.
      base_table: Base table name whose time series will be aligned against.
      fill: Map of table name to fill method ('zero', 'nan', 'backward', 'forward'). Key '*' can be used as fallback.
      base_obj: Base object key.
    """
    ...
  def snapshot(self, tables: list[str], objects: list[str]) -> Table:
    """
    Take last snapshot of multiple tables for a set of objects,
    joining them into one table keyed by object.

    Args:
      tables: List of table names or 'name: field1, field2'.
      objects: List of objects (single, wildcard pattern, or block like '@stock_block:...').
    """
    ...

  def import_csv(
    self,
    table: str,
    data: BinaryIO | bytes | bytearray | str | Path,
    header: bool = ...,
    delimiter: str | None = ...,
  ) -> dict[str, Any]:
    """
    Import CSV data into an MSD table.

    Args:
      table: The name of the table to import data to.
      data: CSV data to import (bytes, file-like object with read(), file path string, or Path).
      header: Whether the CSV data contains a header row. Defaults to True.
      delimiter: Optional CSV delimiter character.

    Returns:
      Dict containing import statistics (e.g. total_rows, time_used_ms, rows_per_sec).
    """
    ...

  def import_table(
    self,
    table: str,
    data: BinaryIO | bytes | bytearray | str | Path | Table,
  ) -> dict[str, Any]:
    """
    Import table frame data into an MSD table.

    Args:
      table: The name of the table to import data to.
      data: Table frame data to import (Table, bytes, file-like object with read(), file path string, or Path).

    Returns:
      Dict containing import statistics (e.g. total_rows, time_used_ms, rows_per_sec).
    """
    ...

  def import_dataframes(
    self,
    table: str,
    data: Iterable[MsdTableFrame | bytes] | MsdTableFrame,
    decimal_fields: dict[str, int] | None = ...,
  ) -> dict[str, Any]:
    """
    Import dataframes or table frames into an MSD table.

    Args:
      table: The name of the table to import data to.
      data: Iterable of (object name, dataframe) tuples, single (object name, dataframe) tuple, or pre-packed bytes.
      decimal_fields: Optional dictionary mapping decimal field names to scale.

    Returns:
      Dict containing import statistics (e.g. total_rows, time_used_ms, rows_per_sec).
    """
    ...

  def import_dataframe(
    self,
    table: str,
    data: Iterable[MsdTableFrame | bytes] | MsdTableFrame,
    decimal_fields: dict[str, int] | None = ...,
  ) -> dict[str, Any]:
    """
    Alias for import_dataframes.
    """
    ...
