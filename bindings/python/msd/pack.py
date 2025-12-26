

import pandas
from typing import TYPE_CHECKING
import numpy as np
from .msd import pack_table_frame

type DataFrame = list[tuple[str, np.ndarray|list]]

if TYPE_CHECKING:
  have_pandas = False
  have_polars = False
  try:
    import pandas
    have_pandas = True
  except ImportError:
    pass

  try:
    import polars
    have_polars = True
  except ImportError:
    pass

  if have_pandas and have_polars:
    type DataFrame = list[tuple[str, np.ndarray|list]] | pandas.DataFrame | polars.DataFrame 
  elif have_pandas:
    type DataFrame = list[tuple[str, np.ndarray|list]] | pandas.DataFrame
  elif have_polars:
    type DataFrame = list[tuple[str, np.ndarray|list]] | polars.DataFrame
  else:
    type DataFrame = list[tuple[str, np.ndarray|list]]

type DataFrameGenerator = Generator[(str, DataFrame), None, None]

def pack_dataframe(obj: str, df: DataFrame) -> bytes:
  """
  Pack a DataFrame into a binary format.

  Args:
    obj (str): The object name.
    df (DataFrame): The DataFrame to pack. It can be a list of (name, ndarray), a pandas DataFrame, or a polars DataFrame.

  Note:
    If df is a pandas DataFrame, the index will be packed as a column when it has a name, e.g. df.index.name = "ts".
    When a pandas DataFrame created without explicit index name, the default index doesn't have a name, so the index will be ignored.

  Returns:
    bytes: The packed DataFrame.
  """

  if type(df).__name__ == "DataFrame":
    cols = []
    if hasattr(df, "index") and df.index.name is not None:
      cols.append((df.index.name, df.index.to_numpy()))
    cols.extend([(k, df[k].to_numpy()) for k in df.columns])
    df = cols
  elif isinstance(df, list):
    for col in df:
      if not isinstance(col, tuple) or len(col) != 2:
        raise ValueError("df must be a list of (name, array)")
      if not isinstance(col[0], str):
        raise ValueError("df must be a list of (name, array)")
      if not isinstance(col[1], np.ndarray) and not isinstance(col[1], list):
        raise ValueError("df must be a list of (name, array)")
  else:
    raise ValueError("df must be a pandas DataFrame, a polars DataFrame, or a list of (name, array)")

    
  # convert object and string arrays to lists
  for i in range(len(df)):
    if isinstance(df[i][1], np.ndarray) and df[i][1].dtype.kind in "SUO":
      df[i] = (df[i][0], df[i][1].tolist())
  
  return pack_table_frame(obj, df)



  