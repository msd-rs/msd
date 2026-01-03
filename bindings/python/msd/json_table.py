

from typing import overload
from .const import MsdTable, MSD_TABLE_VERSION
from typing import Callable
import json
import numpy as np



@overload
def parse_json_table[DF](text: str, builder: Callable[[MsdTable], DF]) -> DF:
  ...

@overload
def parse_json_table[DF](text: str) -> MsdTable:
  ...

def parse_json_table[DF](text: str, builder: Callable[[MsdTable], DF] | None = None) -> DF | MsdTable:
  """
  Parse a json table to a DataFrame or MsdTable
  
  the json table usually comes from '.tables' query or JSON response of query 

  Args:
    text: json table string
    builder: builder function to convert MsdTable to DataFrame
  """
  obj = json.loads(text)
  version = obj.get("version", 0)
  if version != MSD_TABLE_VERSION:
    raise ValueError(f"version mismatch: expected {MSD_TABLE_VERSION}, got {version}")
  columns = obj.get("columns", [])
  
  cols = []
  for col in columns:
    name = col.get("name", "")
    kind = col.get("kind", "")
    if len(name) == 0 or len(kind) == 0:
      raise ValueError("column name or kind is empty")
    cols.append((name, empty_ndarray_by_kind(kind)))

  if builder is not None:
    return builder(cols)
  return cols


def empty_ndarray_by_kind(kind: str) -> np.ndarray:
  if kind == "DateTime":
    return np.empty(0, dtype="datetime64[us]")
  if kind == "Float64":
    return np.empty(0, dtype="float64")
  if kind == "Float32":
    return np.empty(0, dtype="float32")
  if kind == "Int64":
    return np.empty(0, dtype="int64")
  if kind == "Int32":
    return np.empty(0, dtype="int32")
  if kind == "Int16":
    return np.empty(0, dtype="int16")
  if kind == "Int8":
    return np.empty(0, dtype="int8")
  if kind == "UInt64":
    return np.empty(0, dtype="uint64")
  if kind == "UInt32":
    return np.empty(0, dtype="uint32")
  if kind == "UInt16":
    return np.empty(0, dtype="uint16")
  if kind == "UInt8":
    return np.empty(0, dtype="uint8")
  raise ValueError(f"unknown kind: {kind}")  



