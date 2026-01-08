# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only



from .const import SupportedDataFrame
import numpy as np
from ._msd import pack_table_frame
from .dataframe_adaptor import ADAPTORS




def pack_dataframe(obj: str, df: SupportedDataFrame) -> bytes:
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

  for adaptor in ADAPTORS:
    if adaptor.is_data_frame(df):
      df = adaptor.to_msd_table(df)
      return pack_table_frame(obj, df)

  if type(df) is list:
    # convert object and string arrays to lists
    for i in range(len(df)):
      if isinstance(df[i][1], np.ndarray) and df[i][1].dtype.kind in "SUO":
        df[i] = (df[i][0], df[i][1].tolist())
    return pack_table_frame(obj, df)

  raise ValueError(f"Unsupported DataFrame type: {type(df)}")



  