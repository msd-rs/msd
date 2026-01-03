// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! 系统内日期时间的相关辅助方法

use std::{
  convert::TryInto,
  sync::atomic::{AtomicI32, Ordering},
};

// re-export time crate
pub use time::{
  Duration, Month, OffsetDateTime, UtcOffset,
  format_description::FormatItem,
  macros::{format_description, offset},
};

use super::errors::TableError;

static TZ_LOCAL: AtomicI32 = AtomicI32::new(8 * 60 * 60);
pub const RFC3399_DATETIME: &[FormatItem] =
  format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]");

/// 设置时间解析的默认的时区，出于安全性和性能考虑，不使用 localtime_r API，而是由配置指定, 默认的时区是 +8 即中国时区，参考 [`crate::MsdConf`]
pub fn set_default_timezone(tz: UtcOffset) {
  TZ_LOCAL.store(tz.whole_seconds(), Ordering::Relaxed);
}

pub fn get_local_offset() -> UtcOffset {
  let tz = TZ_LOCAL.load(Ordering::Relaxed);
  UtcOffset::from_whole_seconds(tz).unwrap()
}

fn parse_i64(s: &[u8]) -> i64 {
  let mut n = 0;
  for &b in s {
    n *= 10;
    n += (b - b'0') as i64;
  }
  n
}

#[derive(Debug, Clone, Copy)]
struct DateTimePart {
  pub n: i64,
  pub len: u32,
  pub sep: Option<u8>,
}

impl DateTimePart {
  fn into_tz_hour(&self) -> i64 {
    match self.sep {
      Some(b'+') => self.n,
      Some(b'-') => -self.n,
      _ => self.n,
    }
  }

  fn new(s: &[u8], sep: Option<u8>) -> Self {
    DateTimePart {
      n: parse_i64(s),
      len: s.len() as u32,
      sep,
    }
  }
}

impl Into<i64> for &DateTimePart {
  fn into(self) -> i64 {
    self.n
  }
}

impl Into<u32> for &DateTimePart {
  fn into(self) -> u32 {
    self.n as u32
  }
}

impl Into<i32> for &DateTimePart {
  fn into(self) -> i32 {
    self.n as i32
  }
}

impl Into<u8> for &DateTimePart {
  fn into(self) -> u8 {
    if self.n < 0 {
      (-self.n) as u8
    } else {
      self.n as u8
    }
  }
}

impl Into<Month> for &DateTimePart {
  fn into(self) -> Month {
    let m = if self.n < 0 {
      (-self.n) as u8
    } else {
      self.n as u8
    };
    m.try_into().unwrap_or(Month::January)
  }
}

#[derive(Debug, Clone)]
struct SplitWithSep<'a, T: 'a, P>
where
  P: FnMut(&T) -> bool,
{
  v: &'a [T],
  pred: P,
  finished: bool,
}

impl<'a, T: 'a, P: FnMut(&T) -> bool> SplitWithSep<'a, T, P> {
  #[inline]
  pub(super) fn new(slice: &'a [T], pred: P) -> Self {
    Self {
      v: slice,
      pred,
      finished: false,
    }
  }
  #[inline]
  fn finish(&mut self) -> Option<(&'a [T], Option<&'a T>)> {
    if self.finished {
      None
    } else {
      self.finished = true;
      Some((self.v, None))
    }
  }
}

impl<'a, T, P> Iterator for SplitWithSep<'a, T, P>
where
  P: FnMut(&T) -> bool,
{
  type Item = (&'a [T], Option<&'a T>);

  #[inline]
  fn next(&mut self) -> Option<Self::Item> {
    if self.finished {
      return None;
    }

    match self.v.iter().position(|x| (self.pred)(x)) {
      None => self.finish(),
      Some(idx) => {
        let (left, right) =
                    // SAFETY: if v.iter().position returns Some(idx), that
                    // idx is definitely a valid index for v
                    unsafe { (self.v.get_unchecked(..idx), self.v.get_unchecked(idx + 1..)) };
        let ret = Some((left, Some(&self.v[idx])));
        self.v = right;
        ret
      }
    }
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    if self.finished {
      (0, Some(0))
    } else {
      // If the predicate doesn't match anything, we yield one slice.
      // If it matches every element, we yield `len() + 1` empty slices.
      (1, Some(self.v.len() + 1))
    }
  }
}

/// 解析时间到微秒的时间戳, 基于当前时区
pub fn parse_datetime(s: &str) -> Result<i64, TableError> {
  parse_datetime_with_tz(s, get_local_offset())
}

/// 解析时间到微秒的时间戳, 基于指定时区
///
/// 支持如下几种形式的字符串
/// - 全为数字, 此时, 输入被理解成时间戳, 并根据其大小, 猜测单位并进行转换, 例如
///     - 1640966400 被认为是秒数
///     - 1640966400000 被认为是毫秒数
///     - 1640966400000000000 被认为是纳秒数
/// - 2021-01-02, 按日期解析
/// - 2021-01-02 15:03:04, 按日期时间解析
/// - 2021-01-02 15:03:04.999999, 按日期时间解析, 带有纳秒
pub fn parse_datetime_with_tz(s: &str, tz: UtcOffset) -> Result<i64, TableError> {
  if s.is_empty() {
    return Err(TableError::BadDatetimeFormat(s.into()));
  }

  const MAX_PARTS: usize = 9;

  let mut a = [DateTimePart {
    n: -1,
    len: 0,
    sep: None,
  }; MAX_PARTS];
  let mut n = 0;
  let mut sep = None;

  let split = SplitWithSep::new(s.as_bytes().trim_ascii(), |c| !(*c >= b'0' && *c <= b'9'));

  split
    .filter(|(s, _)| !s.trim_ascii().is_empty())
    .take(MAX_PARTS)
    .zip(a.iter_mut())
    .for_each(|(s, p)| {
      *p = DateTimePart::new(s.0, sep);
      sep = s.1.copied();
      n += 1;
    });

  match n {
    1 => {
      let ts = (&a[0]).into();
      if ts < 10_000_000_000 {
        // 2286-11-20
        // second -> microsecond
        Ok(ts * 1_000_000)
      } else if ts < 10_000_000_000_000 {
        // millisecond -> microsecond
        Ok(ts * 1_000)
      } else if ts >= 10_000_000_000_000_000 {
        // nanosecond -> microsecond
        Ok(ts / 1_000)
      } else {
        Ok(ts)
      }
    }
    2 => {
      // floating point timestamp like 1639447901.25218743
      let int_part: i64 = (&a[0]).into();
      let frac_part: i64 = (&a[1]).into();
      Ok(int_part * 1_000_000 + frac_part)
    }
    // 2021-01-02
    3 =>
    //Ok(Local.ymd(parse_i32(a[0]), parse_u32(a[1]), parse_u32(a[2])).and_hms(0, 0, 0).timestamp_nanos()),
    {
      time::Date::from_calendar_date((&a[0]).into(), (&a[1]).into(), (&a[2]).into())
        .and_then(|d| d.with_hms(0, 0, 0))
        .map(|dt| (dt.assume_offset(tz).unix_timestamp_nanos() / 1_000) as i64)
        .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into())
    }
    // 2021-01-02 03:04:05
    6 => time::Date::from_calendar_date((&a[0]).into(), (&a[1]).into(), (&a[2]).into())
      .and_then(|d| d.with_hms((&a[3]).into(), (&a[4]).into(), (&a[5]).into()))
      .map(|dt| (dt.assume_offset(tz).unix_timestamp_nanos() / 1_000) as i64)
      .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into()),
    // 2021-01-02 03:04:05.999999 or 2021-01-02 03:04:05+08
    7 => {
      let (microsecond, hour_fix) = match (&a[6]).len {
        1..=2 => (
          0,
          (tz.whole_seconds() as i64 - (&a[6]).into_tz_hour() * 3600) * 1_000_000,
        ),
        3 => ((&a[6]).n * 1_000, 0),
        4 => ((&a[6]).n * 1_000_0, 0),
        5 => ((&a[6]).n * 1_000_00, 0),
        6 => ((&a[6]).n, 0),
        7 => ((&a[6]).n / 10, 0),
        8 => ((&a[6]).n / 100, 0),
        9 => ((&a[6]).n / 1000, 0),
        _ => return Err(TableError::BadDatetimeFormat(s.to_string()).into()),
      };
      time::Date::from_calendar_date((&a[0]).into(), (&a[1]).into(), (&a[2]).into())
        .and_then(|d| {
          d.with_hms_micro(
            (&a[3]).into(),
            (&a[4]).into(),
            (&a[5]).into(),
            microsecond as u32,
          )
        })
        .map(|dt| (dt.assume_offset(tz).unix_timestamp_nanos() / 1_000) as i64 + hour_fix)
        .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into())
    }
    // 2021-01-02 03:04:05.999999+08
    8 => {
      let hour_fix = (tz.whole_seconds() as i64 - (&a[7]).into_tz_hour() * 3600) * 1_000_000;
      let microsecond = match (&a[6]).len {
        3 => (&a[6]).n * 1_000,
        4 => (&a[6]).n * 1_000_0,
        5 => (&a[6]).n * 1_000_00,
        6 => (&a[6]).n,
        7 => (&a[6]).n / 10,
        8 => (&a[6]).n / 100,
        9 => (&a[6]).n / 1000,
        _ => return Err(TableError::BadDatetimeFormat(s.to_string()).into()),
      };
      time::Date::from_calendar_date((&a[0]).into(), (&a[1]).into(), (&a[2]).into())
        .and_then(|d| {
          d.with_hms_micro(
            (&a[3]).into(),
            (&a[4]).into(),
            (&a[5]).into(),
            microsecond as u32,
          )
        })
        .map(|dt| (dt.assume_offset(tz).unix_timestamp_nanos() / 1_000) as i64 + hour_fix)
        .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into())
    }

    _ => Err(TableError::BadDatetimeFormat(s.to_string()).into()),
  }
}

/// 解析时间间隔
pub fn parse_duration(s: &str) -> Result<Duration, String> {
  if s.is_empty() {
    return Err("empty duration".to_string());
  }

  let unit = match s.as_bytes().last() {
    Some(b) => match b {
      b'm' => 60,
      b'h' => 60 * 60,
      b'd' => 60 * 60 * 24,
      _ => 1,
    },
    None => 1,
  };

  let num = s
    .as_bytes()
    .iter()
    .filter(|b| b.is_ascii_digit() || b'.'.eq(*b))
    .map(|b| *b)
    .collect::<Vec<_>>();
  let num = String::from_utf8_lossy(&num);

  let num = num
    .as_ref()
    .parse::<f64>()
    .map_err(|e| format!("{} can't parsed to duration: {:?}", s, e))?;
  let seconds = (num * unit as f64) as i64;
  Ok(Duration::new(seconds, 0))
}

/// parse time unit string like "1s", "5m", "2h", "1d", "1w", "1M", "1y"
///
/// - for "s", "m", "h", "d", the returned value is in microseconds
/// - for "w", "M", "y", the returned value is (n, unit char)
pub fn parse_unit(s: &str) -> Result<(i64, u8), bool> {
  if s.is_empty() {
    return Err(false);
  }

  let s = s.as_bytes();

  let unit: i64 = match s[s.len() - 1] {
    b's' => 1 * 1_000_000,
    b'm' => 60 * 1_000_000,
    b'h' => 60 * 60 * 1_000_000,
    b'd' => 60 * 60 * 24 * 1_000_000,
    b'w' | b'M' | b'y' => 1, // special handling
    _ => -1,
  };

  if unit == -1 {
    return Err(false);
  }

  let mut n = 0;
  for &b in s {
    if b.is_ascii_digit() {
      n *= 10;
      n += (b - b'0') as i64;
    }
  }
  Ok((n * unit, s[s.len() - 1]))
}

/// round the timestamp `src` to the given time unit parsed by [`parse_unit`]
pub fn round_ts_with_tz(src: i64, unit: &(i64, u8), tz: UtcOffset) -> Result<i64, bool> {
  // no need to round
  if unit.0 == 1 && unit.1 == b's' {
    return Ok(src);
  }

  const ONE_DAY: i64 = 24 * 60 * 60 * 1_000_000;
  let (num, kind) = unit; //parse_unit(unit.as_bytes())?;

  let offset = tz.whole_seconds() as i64 * 1_000_000;

  match kind {
    b'w' => {
      // week
      let src = src - ((src + offset) % ONE_DAY);
      let dt = to_datetime_with_tz(src, tz);
      let days = dt.weekday().number_days_from_monday() as i64;
      return Ok(src - days * ONE_DAY);
    }
    b'M' => {
      // month
      let src = src - ((src + offset) % ONE_DAY);
      let dt = to_datetime_with_tz(src, tz);
      let days = (dt.day() - 1) as i64;
      return Ok(src - days * ONE_DAY);
    }
    b'y' => {
      // year
      let src = src - ((src + offset) % ONE_DAY);
      let dt = to_datetime_with_tz(src, tz);
      let days = (dt.ordinal() - 1) as i64;
      return Ok(src - days * ONE_DAY);
    }
    b'd' => {
      // day
      return Ok(src - ((src + offset) % ONE_DAY));
    }
    _ => {
      // other fixed unit
      return Ok(src - (src % num));
    }
  }
}

pub fn round_ts(src: i64, unit: &(i64, u8)) -> Result<i64, bool> {
  round_ts_with_tz(src, unit, get_local_offset())
}

pub fn add_duration(dt: i64, duration: Option<&str>) -> i64 {
  dt + duration
    .and_then(|s| parse_unit(s).ok())
    .map(|(n, _)| n)
    .unwrap_or_default()
}

/// 将一个微秒秒转换成基于指定时区的 [`time::OffsetDateTime`]
pub fn to_datetime_with_tz(us: i64, tz: UtcOffset) -> OffsetDateTime {
  OffsetDateTime::from_unix_timestamp_nanos(us as i128 * 1_000)
    .map(|dt| dt.to_offset(tz))
    .unwrap_or(OffsetDateTime::now_utc())
}

/// 将一个微秒转换成基于本地时间的 [`time::OffsetDateTime`]
pub fn to_datetime(us: i64) -> OffsetDateTime {
  to_datetime_with_tz(us, get_local_offset())
}

/// 将一个微秒转换称 RFC3399 格式的字符串
pub fn to_datetime_str_with_tz(us: i64, tz: UtcOffset) -> String {
  to_datetime_with_tz(us, tz)
    .format(RFC3399_DATETIME)
    .unwrap_or_default()
}

/// 将一个纳秒转换称基于本地时间的字符 RFC3399
pub fn to_datetime_str(ns: i64) -> String {
  to_datetime_str_with_tz(ns, get_local_offset())
}

/// 返回给定时区的 unix micro second
pub fn now_with_tz(tz: UtcOffset) -> i64 {
  OffsetDateTime::now_utc()
    .to_offset(tz)
    .unix_timestamp_nanos() as i64
    / 1_000
}

/// 返回当前时区的 unix micro second
pub fn now() -> i64 {
  now_with_tz(get_local_offset())
}

/// 返回给定时区的时间
pub fn now_datetime_with_tz(tz: UtcOffset) -> OffsetDateTime {
  OffsetDateTime::now_utc().to_offset(tz)
}

/// 返回当前时区的时间
pub fn now_datetime() -> OffsetDateTime {
  now_datetime_with_tz(get_local_offset())
}

#[cfg(test)]
mod tests {

  use time::{
    format_description,
    macros::{datetime, offset},
  };

  use super::*;
  use anyhow::Result;

  #[test]
  fn test_time_lib() -> Result<()> {
    let format =
      format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour]")?;
    let local_offset = offset!(+8);

    let d2021_01_02_00_00_00 = OffsetDateTime::parse("2021-01-02 00:00:00 08", &format)?;

    let n2021_01_02_00_00_00 = d2021_01_02_00_00_00.unix_timestamp();

    let r2021_01_02_00_00_00 =
      OffsetDateTime::from_unix_timestamp(n2021_01_02_00_00_00)?.to_offset(local_offset);

    assert_eq!(d2021_01_02_00_00_00, r2021_01_02_00_00_00);

    assert_eq!(
      "2021-01-02 00:00:00 08",
      &d2021_01_02_00_00_00.format(&format)?
    );

    Ok(())
  }

  #[test]
  fn test_date_str() -> Result<()> {
    let d1 = parse_datetime("2021-01-02 00:00:00.000000")?;
    let d2 = datetime!(2021-01-02 00:00:00.000000 +8);

    let s1 = to_datetime_str(d1);
    let s2 = d2.format(RFC3399_DATETIME)?;

    assert_eq!("2021-01-02T00:00:00.000000", &s1);
    assert_eq!("2021-01-02T00:00:00.000000", &s2);

    Ok(())
  }

  #[test]
  fn test_parse() {
    set_default_timezone(offset!(+8));

    assert_eq!(
      to_datetime(parse_datetime("1639447901252").unwrap()),
      datetime!(2021-12-14 10:11:41.252 +8) //Local.ymd(2021, 12, 14).and_hms_milli(10, 11, 41, 252)
    );

    assert_eq!(
      to_datetime(parse_datetime("1639447901252187").unwrap()),
      datetime!(2021-12-14 10:11:41.252187 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("1639447901252187000").unwrap()),
      datetime!(2021-12-14 10:11:41.252187000 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("1639447901").unwrap()),
      datetime!(2021-12-14 10:11:41 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14").unwrap()),
      datetime!(2021-12-14 0:00:00 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41").unwrap()),
      datetime!(2021-12-14 10:11:41 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021/12/14 10:11:41").unwrap()),
      datetime!(2021-12-14 10:11:41 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.252").unwrap()),
      datetime!(2021-12-14 10:11:41.252 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.252187").unwrap()),
      datetime!(2021-12-14 10:11:41.252187 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.252187").unwrap()),
      datetime!(2021-12-14 10:11:41.252187000 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.25218743").unwrap()),
      datetime!(2021-12-14 10:11:41.252187 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41+05").unwrap()),
      datetime!(2021-12-14 13:11:41 +8)
    );
    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41-05").unwrap()),
      datetime!(2021-12-14 23:11:41 +8)
    );
  }

  #[test]
  fn test_round_ts() -> Result<()> {
    let dt = parse_datetime("1650572430000")?;

    let r1h = round_ts(dt, &parse_unit("1h").unwrap()).unwrap();
    let r1d = round_ts(dt, &parse_unit("1d").unwrap()).unwrap();

    assert_eq!(to_datetime(r1h), datetime!(2022-04-22 04:00:00 +8));

    assert_eq!(to_datetime(r1d), datetime!(2022-04-22 00:00:00 +8));

    assert_eq!(
      to_datetime(round_ts(dt, &parse_unit("1w").unwrap()).unwrap()),
      datetime!(2022-04-18 00:00:00 +8)
    );

    assert_eq!(
      to_datetime(round_ts(dt, &parse_unit("1M").unwrap()).unwrap()),
      datetime!(2022-04-01 00:00:00 +8)
    );

    assert_eq!(
      to_datetime(round_ts(dt, &parse_unit("1y").unwrap()).unwrap()),
      datetime!(2022-01-01 00:00:00 +8)
    );

    //println!("{} {} {}", dt / 1_000_000, r1h / 1_000_000, r1d / 1_000_000);
    Ok(())
  }
}
