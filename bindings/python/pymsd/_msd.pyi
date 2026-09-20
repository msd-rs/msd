from typing import Tuple
from .const import MsdTable
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