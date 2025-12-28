// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::str::FromStr;

use msd_request::AggStateId;
use msd_table::{Field, Table, Variant};
use rustc_hash::FxHashSet;

use crate::errors::DbError;

#[derive(Debug, Clone)]
pub enum AggState {
  Sum(Variant, usize), // (current sum, count)
  Count(usize),        // count
  Min(Variant),
  Max(Variant),
  Avg(Variant, usize),         // (current sum, count)
  First(Variant),              // first value
  Uniq(FxHashSet<Variant>),    // set of unique values
  Prev(Variant),               // previous value
  DiffPrev(Variant, Variant),  // diff from previous value
  DiffFirst(Variant, Variant), // diff from first value
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
      AggStateId::UniqCount => AggState::Uniq(FxHashSet::default()),
      AggStateId::Prev => AggState::Prev(Variant::Null),
      AggStateId::DiffPrev => AggState::DiffPrev(Variant::Null, Variant::Null),
      AggStateId::DiffFirst => AggState::DiffFirst(Variant::Null, Variant::Null),
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
      AggState::Uniq(_) => AggStateId::UniqCount,
      AggState::Prev(_) => AggStateId::Prev,
      AggState::DiffPrev(_, _) => AggStateId::DiffPrev,
      AggState::DiffFirst(_, _) => AggStateId::DiffFirst,
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
      AggState::DiffPrev(prev, diff_prev) => {
        *diff_prev = value - prev;
        *prev = value.clone();
      }
      AggState::DiffFirst(first, diff_first) => {
        if first.is_null() {
          *first = value.clone();
          *diff_first = value.clone();
        } else {
          *diff_first = value - first;
        }
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
      AggState::DiffPrev(_, _) => {
        *self = AggState::DiffPrev(Variant::Null, Variant::Null);
      }
      AggState::DiffFirst(_, _) => {
        *self = AggState::DiffFirst(Variant::Null, Variant::Null);
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
      AggState::DiffPrev(_, diff_prev) => diff_prev.clone(),
      AggState::DiffFirst(_, diff_first) => diff_first.clone(),
    }
  }
}

impl TryFrom<&Field> for AggState {
  type Error = DbError;
  fn try_from(field: &Field) -> Result<Self, Self::Error> {
    field
      .get_metadata("agg")
      .and_then(|v| v.get_str())
      .and_then(|s| AggStateId::from_str(s).ok())
      .map(|id| AggState::from(id))
      .ok_or(DbError::InvalidAgg(field.name.clone()))
  }
}

impl AggState {
  pub fn table_states(table: &Table) -> Vec<Option<AggState>> {
    table
      .columns()
      .iter()
      .map(|f| AggState::try_from(f).ok())
      .collect()
  }
}
