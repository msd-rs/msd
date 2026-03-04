# Copyright 2026 MSD-RS Project LiJia
# SPDX-License-Identifier: agpl-3.0-only

from typing import Any, Tuple, TYPE_CHECKING, TypeAlias
import numpy as np

MSD_USER_AGENT = "msd-client"
MSD_QUERY_PATH = "/query"
MSD_IMPORT_PATH = "/table/{table_name}"
MSD_TABLE_VERSION = 1299972097

MsdTable: TypeAlias = list[tuple[str, np.ndarray]]


if TYPE_CHECKING:
  import pandas
  import polars

  # 静态检查器会看到完整的类型提示
  SupportedDataFrame: TypeAlias = MsdTable | pandas.DataFrame | polars.DataFrame
else:
  SupportedDataFrame: TypeAlias = Any

MsdTableFrame: TypeAlias = Tuple[str, SupportedDataFrame]
