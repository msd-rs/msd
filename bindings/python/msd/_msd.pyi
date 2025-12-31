
from typing import Tuple
from .const import MsdTable, MsdTableFrame

def pack_table_frame(obj: str, table: MsdTable) -> bytes:
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
