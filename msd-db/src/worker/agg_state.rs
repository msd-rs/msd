use std::str::FromStr;

use msd_table::Variant;
use rustc_hash::FxHashSet;

#[derive(Debug, Clone)]
pub enum AggState {
  Sum(Variant, usize), // (current sum, count)
  Count(usize),        // count
  Min(Variant),
  Max(Variant),
  Avg(Variant, usize),      // (current sum, count)
  First(Variant),           // first value
  Uniq(FxHashSet<Variant>), // set of unique values
  Prev(Variant),            // previous value
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
      AggState::Prev(_) => AggStateId::Prev,
    }
  }
}

impl AggState {
  pub fn update(&mut self, value: &Variant) {
    match self {
      AggState::Sum(current_sum, count) => {
        if current_sum.is_null() {
          *current_sum = value.clone();
        } else {
          *current_sum += value
        }
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
        *current_sum += value;
        *count += 1;
      }
      AggState::First(first_value) => {
        if first_value.is_null() {
          *first_value = value.clone();
        }
      }
      AggState::Uniq(uniq_set) => {
        uniq_set.insert(value.clone());
      }
      AggState::Prev(prev_value) => {
        *prev_value = value.clone();
      }
    }
  }

  pub fn reset(&mut self) {
    match self {
      AggState::Sum(_, _) => {
        *self = AggState::Sum(Variant::Null, 0);
      }
      AggState::Count(_) => {
        *self = AggState::Count(0);
      }
      AggState::Min(_) => {
        *self = AggState::Min(Variant::Null);
      }
      AggState::Max(_) => {
        *self = AggState::Max(Variant::Null);
      }
      AggState::Avg(_, _) => {
        *self = AggState::Avg(Variant::Null, 0);
      }
      AggState::First(_) => {
        *self = AggState::First(Variant::Null);
      }
      AggState::Uniq(_) => {
        *self = AggState::Uniq(FxHashSet::default());
      }
      AggState::Prev(_) => {
        *self = AggState::Prev(Variant::Null);
      }
    }
  }

  pub fn get(&self) -> Variant {
    match self {
      AggState::Sum(current_sum, _) => current_sum.clone(),
      AggState::Count(count) => Variant::from(*count),
      AggState::Min(current_min) => current_min.clone(),
      AggState::Max(current_max) => current_max.clone(),
      AggState::Avg(current_sum, count) => {
        if *count == 0 {
          Variant::Null
        } else {
          current_sum.clone() / Variant::from(*count)
        }
      }
      AggState::First(first_value) => first_value.clone(),
      AggState::Uniq(uniq_set) => Variant::Int64(uniq_set.len() as i64),
      AggState::Prev(prev_value) => prev_value.clone(),
    }
  }
}
