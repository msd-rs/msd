from typing import Any

import numpy as np
from pymsd import aligned_index, parse_table_frame
from pymsd.easy import JoinMethod




class Aligner:
  def __init__(self,left_index: np.ndarray, right_index: np.ndarray, method: JoinMethod ) -> None:
    self.method = self.parse_method(method)
    self.aligned_right_index = aligned_index(left_index, right_index, self.method)
    self.valid_aligned_right_index = self.aligned_right_index < len(right_index)

  def parse_method(self, method: JoinMethod) -> int:
    match method:
      case 'zero': return 0
      case 'nan': return 1
      case 'backward': return 2
      case 'forward': return 4
    return 0

  def apply_zero(self, v: np.ndarray) -> Any:
    r = np.full(self.aligned_right_index.shape, 0, dtype=v.dtype)
    r[self.valid_aligned_right_index] = v[self.aligned_right_index[self.valid_aligned_right_index]]
    return r

  def apply_nan(self, v: np.ndarray) -> Any:
    r = np.full(self.aligned_right_index.shape, np.nan, dtype=v.dtype)
    r[self.valid_aligned_right_index] = v[self.aligned_right_index[self.valid_aligned_right_index]]
    return r


left = np.arange(0, 10)
right = np.arange(0, 10, 2)

aligner = Aligner(left, right, 'backward')

v = np.arange(0, 5, dtype=np.float64)
print(aligner.apply_zero(v))
