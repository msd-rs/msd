// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::{fmt::Display, hash::Hash, sync::OnceLock};

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub struct RequestKey {
  pub table: SmolStr,
  pub obj: SmolStr,
}

pub fn broadcast_key() -> &'static RequestKey {
  static BROADCAST_KEY: OnceLock<RequestKey> = OnceLock::new();
  BROADCAST_KEY.get_or_init(|| RequestKey {
    table: "__broadcast__".into(),
    obj: "__broadcast__".into(),
  })
}

pub fn once_key() -> &'static RequestKey {
  static ONCE_KEY: OnceLock<RequestKey> = OnceLock::new();
  ONCE_KEY.get_or_init(|| RequestKey {
    table: "__once__".into(),
    obj: "__once__".into(),
  })
}

impl RequestKey {
  pub fn new<S: Into<SmolStr>, T: Into<SmolStr>>(table: T, obj: S) -> Self {
    Self {
      table: table.into(),
      obj: obj.into(),
    }
  }
  pub fn is_broadcast(&self) -> bool {
    self == broadcast_key()
  }
}

impl Display for RequestKey {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}:{}", self.table, self.obj)
  }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Copy, PartialEq, Eq)]
pub struct DateRange {
  /// start (timestamp, inclusive)
  pub start: Option<(i64, bool)>,
  /// end (timestamp, inclusive)
  pub end: Option<(i64, bool)>,
  #[serde(default)]
  pub year: Option<i32>,
  #[serde(default)]
  pub month: Option<u8>,
  #[serde(default)]
  pub day: Option<u8>,
  #[serde(default)]
  pub hour: Option<u8>,
  #[serde(default)]
  pub minute: Option<u8>,
  #[serde(default)]
  pub second: Option<u8>,
  #[serde(default)]
  pub weekday: Option<u8>,
}

impl DateRange {
  pub fn has_feature_filter(&self) -> bool {
    self.year.is_some()
      || self.month.is_some()
      || self.day.is_some()
      || self.hour.is_some()
      || self.minute.is_some()
      || self.second.is_some()
      || self.weekday.is_some()
  }

  // Check if the timestamp is within the range, default to true if range is not set
  pub fn contains(&self, ts: i64) -> bool {
    if !self.is_set() {
      return true;
    }
    let start = self
      .start
      .as_ref()
      .map(|(ts, inclusive)| if *inclusive { *ts } else { *ts - 1 })
      .unwrap_or(0);
    let end = self
      .end
      .as_ref()
      .map(|(ts, inclusive)| if *inclusive { *ts } else { *ts + 1 })
      .unwrap_or(i64::MAX);
    if start > ts || end < ts {
      return false;
    }
    if self.has_feature_filter() {
      let dt = msd_table::to_datetime(ts);
      if let Some(y) = self.year
        && dt.year() != y
      {
        return false;
      }
      if let Some(m) = self.month
        && dt.month() as u8 != m
      {
        return false;
      }
      if let Some(d) = self.day
        && dt.day() != d
      {
        return false;
      }
      if let Some(h) = self.hour
        && dt.hour() != h
      {
        return false;
      }
      if let Some(min) = self.minute
        && dt.minute() != min
      {
        return false;
      }
      if let Some(s) = self.second
        && dt.second() != s
      {
        return false;
      }
      if let Some(w) = self.weekday
        && dt.weekday().number_from_monday() != w
      {
        return false;
      }
    }
    true
  }
  // Check if any start, end or feature filter is set
  pub fn is_set(&self) -> bool {
    self.start.is_some() || self.end.is_some() || self.has_feature_filter()
  }
}
