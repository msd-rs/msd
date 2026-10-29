# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only


from typing import BinaryIO, Iterator
from .dataframe_adaptor import MsdTableFrame
from ._msd import MsdConnection


def import_csv(
  baseURL: str, table_name: str, data: BinaryIO, header: bool = True
) -> dict:
  """
  Import data from csv file to msd.

  Args:
    baseURL (str): The base URL of the msd server.
    table_name (str): The name of the table to import data to.
    data (BinaryIO): The data to import, e.g. a file object opened in binary mode.
    header (bool, optional): Whether the data has a header. Defaults to True.

  Returns:
    dict: The result of the import operation.
  """
  conn = MsdConnection(baseURL)
  return conn.import_csv(table_name, data, header=header)


def import_dataframes(
  baseURL: str, table_name: str, data: Iterator[MsdTableFrame], decimal_fields: dict | None = None
) -> dict:
  """
  Import data from a generator of (object name, data) to msd.

  Args:
    baseURL (str): The base URL of the msd server.
    table_name (str): The name of the table to import data to.
    data (Iterator[Tuple[str, DataFrame]]): The data to import.

  Returns:
    dict: The result of the import operation.
  """
  conn = MsdConnection(baseURL)
  return conn.import_dataframes(table_name, data, decimal_fields=decimal_fields)
