use core::f64;
use std::str::FromStr;

use msd_table::Variant;
use rustc_hash::FxHashSet;

type AggStateKey = u64;

#[derive(Debug, Clone)]
pub enum AggState {
  Sum(Variant, usize), // (current sum, count)
  Count(usize),
  Min(Variant),
  Max(Variant),
  Avg(Variant, usize), // (current sum, count)
  First(Variant),
  Uniq(FxHashSet<Variant>), // set of unique values
}

#[derive(Debug, Clone)]
#[repr(u16)]
pub enum AggStateId {
  Sum,
  Count,
  Min,
  Max,
  Avg,
  Uniq,
  First,
  Prev,
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
      6 => Some(AggStateId::Uniq),
      7 => Some(AggStateId::Prev),
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
      "uniq" => Ok(AggStateId::Uniq),
      "prev" => Ok(AggStateId::Prev),
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
      AggStateId::Uniq => "uniq".to_string(),
      AggStateId::Prev => "prev".to_string(),
    }
  }
}

impl From<AggStateId> for AggState {
  fn from(id: AggStateId) -> Self {
    match id {
      AggStateId::Sum => AggState::Sum(Variant::Null, 0),
      AggStateId::Count => AggState::Count(0),
      AggStateId::Min => AggState::Min(Variant::Null),
      AggStateId::Max => AggState::Max(Variant::Null),
      AggStateId::Avg => AggState::Avg(Variant::Null, 0),
      AggStateId::First => AggState::First(Variant::Null),
      AggStateId::Uniq => AggState::Uniq(FxHashSet::default()),
      AggStateId::Prev => AggState::First(Variant::Null),
    }
  }
}

impl AggState {
  pub fn id(&self) -> AggStateId {
    match self {
      AggState::Sum(_, _) => AggStateId::Sum,
      AggState::Count(_) => AggStateId::Count,
      AggState::Min(_) => AggStateId::Min,
      AggState::Max(_) => AggStateId::Max,
      AggState::Avg(_, _) => AggStateId::Avg,
      AggState::First(_) => AggStateId::First,
      AggState::Uniq(_) => AggStateId::Uniq,
    }
  }
}

impl AggState {
  pub fn update(&mut self, value: &Variant) {
    match self {
      AggState::Sum(current_sum, count) => {
        *current_sum = add_variants(current_sum, value);
        *count += 1;
      }
      AggState::Count(count) => {
        *count += 1;
      }
      AggState::Min(current_min) => {
        if value < current_min {
          *current_min = value.clone();
        }
      }
      AggState::Max(current_max) => {
        if value > current_max {
          *current_max = value.clone();
        }
      }
      AggState::Avg(current_sum, count) => {
        *current_sum = add_variants(current_sum, value);
        *count += 1;
      }
      AggState::First(first_value) => {
        if let Variant::Null = first_value {
          *first_value = value.clone();
        }
      }
      AggState::Uniq(uniq_set) => {
        uniq_set.insert(value.clone());
      }
    }
  }
}

pub fn agg_state_key(col_index: usize, agg_id: AggStateId) -> AggStateKey {
  ((col_index as u64) << 32) | (agg_id as u64)
}
