

from typing import BinaryIO
from .const import *

def import_csv(baseURL: str, table_name: str, data: BinaryIO,  header: bool = True) -> dict:
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
  try:
    import requests
  except ImportError:
    raise ImportError("requests is required for msd_import_csv")

  skip = 1 if header else 0
  endpoint = f"{baseURL}{MSD_IMPORT_PATH.format(table_name=table_name)}?skip={skip}"
  response = requests.put(endpoint, data=data, headers={
    "User-Agent": MSD_USER_AGENT,
    "Content-Type": "text/csv",
  })
  if response.status_code != 200:
    raise Exception(f"Import failed: {response.text}")

  return response.json()