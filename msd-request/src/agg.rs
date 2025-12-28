// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::str::FromStr;

#[derive(Debug, Clone)]
#[repr(u16)]
pub enum AggStateId {
  Sum,
  Count,
  Min,
  Max,
  Avg,
  UniqCount,
  First,
  Prev,
  DiffPrev,
  DiffFirst,
}

impl AggStateId {
  pub fn from_u16(value: u16) -> Option<Self> {
    match value {
      0 => Some(AggStateId::Sum),
      1 => Some(AggStateId::Count),
      2 => Some(AggStateId::Min),
      3 => Some(AggStateId::Max),
      4 => Some(AggStateId::Avg),
      5 => Some(AggStateId::First),
      6 => Some(AggStateId::UniqCount),
      7 => Some(AggStateId::Prev),
      8 => Some(AggStateId::DiffPrev),
      9 => Some(AggStateId::DiffFirst),
      _ => None,
    }
  }
}

impl FromStr for AggStateId {
  type Err = ();

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    match s {
      "sum" => Ok(AggStateId::Sum),
      "count" => Ok(AggStateId::Count),
      "min" => Ok(AggStateId::Min),
      "max" => Ok(AggStateId::Max),
      "avg" => Ok(AggStateId::Avg),
      "first" => Ok(AggStateId::First),
      "uniq_count" => Ok(AggStateId::UniqCount),
      "prev" => Ok(AggStateId::Prev),
      "diff_prev" => Ok(AggStateId::DiffPrev),
      "diff_first" => Ok(AggStateId::DiffFirst),
      _ => Err(()),
    }
  }
}
impl ToString for AggStateId {
  fn to_string(&self) -> String {
    match self {
      AggStateId::Sum => "sum".to_string(),
      AggStateId::Count => "count".to_string(),
      AggStateId::Min => "min".to_string(),
      AggStateId::Max => "max".to_string(),
      AggStateId::Avg => "avg".to_string(),
      AggStateId::First => "first".to_string(),
      AggStateId::UniqCount => "uniq_count".to_string(),
      AggStateId::Prev => "prev".to_string(),
      AggStateId::DiffPrev => "diff_prev".to_string(),
      AggStateId::DiffFirst => "diff_first".to_string(),
    }
  }
}
