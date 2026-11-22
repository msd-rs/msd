from typing import Any, Literal, TypeAlias

import numpy as np
from pymsd import aligned_index

JoinMethod: TypeAlias = Literal["backward", "forward", "zero", "nan"]


class Aligner:
  def __init__(self, left_index: np.ndarray, right_index: np.ndarray, method: JoinMethod) -> None:
    self.method = self.parse_method(method)
    self.aligned_right_index = aligned_index(left_index, right_index, self.method)
    self.valid_aligned_right_index = self.aligned_right_index < len(right_index)

  def parse_method(self, method: JoinMethod) -> int:
    match method:
      case "zero":
        return 0
      case "nan":
        return 1
      case "backward":
        return 2
      case "forward":
        return 4
    return 0

  def apply_zero(self, v: np.ndarray) -> Any:
    r = np.full(self.aligned_right_index.shape, 0, dtype=v.dtype)
    r[self.valid_aligned_right_index] = v[self.aligned_right_index[self.valid_aligned_right_index]]
    return r

  def apply_nan(self, v: np.ndarray) -> Any:
    r = np.full(self.aligned_right_index.shape, np.nan, dtype=v.dtype)
    r[self.valid_aligned_right_index] = v[self.aligned_right_index[self.valid_aligned_right_index]]
    return r


def test_aligned_index() -> None:
  left = np.arange(0, 10, dtype=np.int64)
  right = np.arange(0, 10, 2, dtype=np.int64)

  aligner = Aligner(left, right, "backward")
  v = np.arange(0, 5, dtype=np.float64)
  res = aligner.apply_zero(v)
  assert np.array_equal(res, np.array([0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 3.0, 3.0, 4.0, 4.0]))
