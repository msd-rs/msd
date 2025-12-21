
from .msd import check_table_frame, parse_table_frame
import io
from typing import Generator
import numpy
import asyncio

type Table = dict[str, numpy.ndarray]

def parse_reader(r: io.BytesIO) -> Generator[Tuple[str, Table], None, None]:   
  """
  Parse a table frame from a bytes stream.

  Args:
    r: A bytes stream.

  Yields:
    A Dict, keys are field names, values are numpy arrays.
  """
  while True:
    header = r.read(8)
    if len(header) == 0:
      break
    size = check_table_frame(header)
    data = r.read(size)
    if len(data) == 0:
      break
    yield parse_table_frame(data)


async def parse_reader_async(r: asyncio.StreamReader) -> Generator[Tuple[str, Table], None, None]:
  """
  Parse a table frame from a bytes stream.

  Args:
    r: A bytes stream.

  Yields:
    A Dict, keys are field names, values are numpy arrays.
  """
  while True:
    header = await r.read(8)
    if len(header) == 0:
      break
    size = check_table_frame(header)
    data = await r.read(size)
    if len(data) == 0:
      break
    yield parse_table_frame(data)