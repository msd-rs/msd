//! 系统内日期时间的相关辅助方法

use std::{
  convert::TryInto,
  sync::atomic::{AtomicI8, Ordering},
};

use time::{
  Duration, Month, OffsetDateTime, UtcOffset, format_description::FormatItem,
  macros::format_description,
};

use super::errors::TableError;

static TZ_LOCAL: AtomicI8 = AtomicI8::new(8);
pub const RFC3399_DATETIME: &[FormatItem] =
  format_description!("[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:6]");

/// 设置时间解析的默认的时区，出于安全性和性能考虑，不使用 localtime_r API，而是由配置指定, 默认的时区是 +8 即中国时区，参考 [`crate::MsdConf`]
pub fn set_default_timezone(tz: i8) {
  TZ_LOCAL.store(tz, Ordering::Relaxed);
}

fn parse_i64(s: &[u8]) -> i64 {
  let mut n = 0;
  for &b in s {
    n *= 10;
    n += (b - b'0') as i64;
  }
  n
}

fn parse_i32(s: &[u8]) -> i32 {
  parse_i64(s) as i32
}

fn parse_u32(s: &[u8]) -> u32 {
  parse_i64(s) as u32
}
fn parse_u8(s: &[u8]) -> u8 {
  parse_i64(s) as u8
}

fn parse_month(s: &[u8]) -> Month {
  parse_u8(s).try_into().unwrap_or(Month::January)
}

/// 解析时间到微秒的时间戳, 基于本地时间
///
/// 支持如下几种形式的字符串
/// - 全为数字, 此时, 输入被理解成时间戳, 并根据其大小, 猜测单位并进行转换, 例如
///     - 1640966400 被认为是秒数
///     - 1640966400000 被认为是毫秒数
///     - 1640966400000000000 被认为是纳秒数
/// - 2021-01-02, 按日期解析
/// - 2021-01-02 15:03:04, 按日期时间解析
/// - 2021-01-02 15:03:04.999999, 按日期时间解析, 带有纳秒
pub fn parse_datetime(s: &str) -> Result<i64, TableError> {
  if s.is_empty() {
    return Err(TableError::BadDatetimeFormat(s.into()));
  }

  let a: Vec<&[u8]> = s
    .trim()
    .as_bytes()
    .split(|&c| !(c >= b'0' && c <= b'9'))
    .collect();

  let n = a.len();

  let local_offset: UtcOffset =
    UtcOffset::from_hms(TZ_LOCAL.load(Ordering::Relaxed), 0, 0).unwrap_or(UtcOffset::UTC);

  match n {
    1 => {
      let ts = parse_i64(a[0]);
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
      let int_part = parse_i64(a[0]);
      let frac_part = if a[0].len() > 6 {
        parse_i64(&a[1][0..6])
      } else {
        parse_i64(a[1])
      };
      Ok(int_part * 1_000_000 + frac_part)
    }
    // 2021-01-02
    3 =>
    //Ok(Local.ymd(parse_i32(a[0]), parse_u32(a[1]), parse_u32(a[2])).and_hms(0, 0, 0).timestamp_nanos()),
    {
      time::Date::from_calendar_date(parse_i32(a[0]), parse_month(a[1]), parse_u8(a[2]))
        .and_then(|d| d.with_hms(0, 0, 0))
        .map(|dt| (dt.assume_offset(local_offset).unix_timestamp_nanos() / 1_000) as i64)
        .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into())
    }
    // 2021-01-02 03:04:05
    6 => time::Date::from_calendar_date(parse_i32(a[0]), parse_month(a[1]), parse_u8(a[2]))
      .and_then(|d| d.with_hms(parse_u8(a[3]), parse_u8(a[4]), parse_u8(a[5])))
      .map(|dt| (dt.assume_offset(local_offset).unix_timestamp_nanos() / 1_000) as i64)
      .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into()),
    // 2021-01-02 03:04:05.999999 or 2021-01-02 03:04+08
    7 => {
      if a[6].len() == 2 {
        // FIXME 对时区的处理
        let _zone = parse_u8(a[6]);
        time::Date::from_calendar_date(parse_i32(a[0]), parse_month(a[1]), parse_u8(a[2]))
          .and_then(|d| d.with_hms(parse_u8(a[3]), parse_u8(a[4]), parse_u8(a[5])))
          .map(|dt| (dt.assume_offset(local_offset).unix_timestamp_nanos() / 1_000) as i64)
          .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into())
      } else if a[6].len() < 10 {
        let ns = parse_u32(a[6]) * 10_u32.pow(9 - a[6].len() as u32);
        //Ok(Local.ymd(parse_i32(a[0]), parse_u32(a[1]), parse_u32(a[2])).and_hms_nano(parse_u32(a[3]), parse_u32(a[4]), parse_u32(a[5]), ns).timestamp_nanos())
        time::Date::from_calendar_date(parse_i32(a[0]), parse_month(a[1]), parse_u8(a[2]))
          .and_then(|d| d.with_hms_nano(parse_u8(a[3]), parse_u8(a[4]), parse_u8(a[5]), ns))
          .map(|dt| (dt.assume_offset(local_offset).unix_timestamp_nanos() / 1_000) as i64)
          .map_err(|e| TableError::BadDatetimeFormat(e.to_string()).into())
      } else {
        Err(TableError::BadDatetimeFormat(s.to_string()).into())
      }
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

fn parse_unit(s: &[u8]) -> Result<i64, bool> {
  if s.is_empty() {
    return Err(false);
  }

  let unit: i64 = match s[s.len() - 1] {
    b's' => 1,
    b'm' => 60,
    b'h' => 60 * 60,
    b'd' => 60 * 60 * 24,
    _ => -1,
  };
  if unit < 0 {
    return Err(false);
  }

  let mut n = 0;
  for &b in s {
    if b.is_ascii_digit() {
      n *= 10;
      n += (b - b'0') as i64;
    }
  }
  Ok(n * unit * 1_000_000)
}

pub fn round_ts(src: i64, unit: &str) -> Result<i64, bool> {
  const ONE_DAY: i64 = 24 * 60 * 60 * 1_000_000;

  if unit.ends_with('w') {
    let offset = TZ_LOCAL.load(Ordering::Relaxed) as i64 * 3600_000_000;
    let src = src - ((src + offset) % ONE_DAY);
    let dt = to_datetime(src);
    let ndays = dt.weekday().number_days_from_monday() as i64;
    return Ok(src - ndays * ONE_DAY);
  } else if unit.ends_with('M') {
    let offset = TZ_LOCAL.load(Ordering::Relaxed) as i64 * 3600_000_000;
    let src = src - ((src + offset) % ONE_DAY);
    let dt = to_datetime(src);
    let ndays = (dt.day() - 1) as i64;
    return Ok(src - ndays * ONE_DAY);
  } else if unit.ends_with('y') {
    let offset = TZ_LOCAL.load(Ordering::Relaxed) as i64 * 3600_000_000;
    let src = src - ((src + offset) % ONE_DAY);
    let dt = to_datetime(src);
    let ndays = (dt.ordinal() - 1) as i64;
    return Ok(src - ndays * ONE_DAY);
  }

  let unit = unit.as_bytes();
  let offset = unit
    .last()
    .map(|b| {
      if b'd'.eq(b) {
        (TZ_LOCAL.load(Ordering::Relaxed)) as i64 * 3600_000_000
      } else {
        0
      }
    })
    .unwrap_or(0);
  let unit = parse_unit(unit)?;
  Ok(src - ((src + offset) % unit))
}

pub fn add_duration(dt: i64, duration: Option<&str>) -> i64 {
  dt + duration
    .and_then(|s| parse_unit(s.as_bytes()).ok())
    .unwrap_or_default()
}

/// 将一个纳秒转换成基于本地时间的 [`time::OffsetDateTime`]
pub fn to_datetime(ns: i64) -> OffsetDateTime {
  let local_offset: UtcOffset =
    UtcOffset::from_hms(TZ_LOCAL.load(Ordering::Relaxed), 0, 0).unwrap_or(UtcOffset::UTC);
  OffsetDateTime::from_unix_timestamp_nanos(ns as i128 * 1_000)
    .map(|dt| dt.to_offset(local_offset))
    .unwrap_or(OffsetDateTime::now_utc())
}

/// 将一个纳秒转换称基于本地时间的字符 RFC3399
pub fn to_datetime_str(ns: i64) -> String {
  let local_offset: UtcOffset =
    UtcOffset::from_hms(TZ_LOCAL.load(Ordering::Relaxed), 0, 0).unwrap_or(UtcOffset::UTC);
  OffsetDateTime::from_unix_timestamp_nanos(ns as i128 * 1_000)
    .map(|dt| dt.to_offset(local_offset))
    .map(|dt| dt.format(RFC3399_DATETIME).unwrap_or_default())
    .unwrap_or_default()
}

/// 返回当前时区的 unix nano second
pub fn now() -> i64 {
  let local_offset: UtcOffset =
    UtcOffset::from_hms(TZ_LOCAL.load(Ordering::Relaxed), 0, 0).unwrap_or(UtcOffset::UTC);
  OffsetDateTime::now_utc()
    .to_offset(local_offset)
    .unix_timestamp_nanos() as i64
    / 1_000
}

/// 返回当前时区的时间
pub fn now_datetime() -> OffsetDateTime {
  let local_offset: UtcOffset =
    UtcOffset::from_hms(TZ_LOCAL.load(Ordering::Relaxed), 0, 0).unwrap_or(UtcOffset::UTC);
  OffsetDateTime::now_utc().to_offset(local_offset)
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
    set_default_timezone(8);

    assert_eq!(
      to_datetime(parse_datetime("1639447901252").unwrap()),
      datetime!(2021-12-14 10:11:41.252 +8) //Local.ymd(2021, 12, 14).and_hms_milli(10, 11, 41, 252)
    );

    assert_eq!(
      to_datetime(parse_datetime("1639447901252187").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms_micro(10, 11, 41, 252187)
      datetime!(2021-12-14 10:11:41.252187 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("1639447901252187000").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms_nano(10, 11, 41, 252187000)
      datetime!(2021-12-14 10:11:41.252187000 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("1639447901").unwrap()),
      //.Local.ymd(2021, 12, 14).and_hms(10, 11, 41)
      datetime!(2021-12-14 10:11:41 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms(0, 0, 0)
      datetime!(2021-12-14 0:00:00 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms(10, 11, 41)
      datetime!(2021-12-14 10:11:41 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021/12/14 10:11:41").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms(10, 11, 41)
      datetime!(2021-12-14 10:11:41 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.252").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms_milli(10, 11, 41, 252)
      datetime!(2021-12-14 10:11:41.252 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.252187").unwrap()),
      // Local.ymd(2021, 12, 14).and_hms_micro(10, 11, 41, 252_187)
      datetime!(2021-12-14 10:11:41.252187 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.252187").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms_nano(10, 11, 41, 252_187_000)
      datetime!(2021-12-14 10:11:41.252187000 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41.25218743").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms_nano(10, 11, 41, 252_187_430)
      datetime!(2021-12-14 10:11:41.252187 +8)
    );

    assert_eq!(
      to_datetime(parse_datetime("2021-12-14T10:11:41+05").unwrap()),
      //Local.ymd(2021, 12, 14).and_hms_nano(10, 11, 41, 252_187_430)
      datetime!(2021-12-14 10:11:41 +8)
    );
  }

  #[test]
  fn test_round_ts() -> Result<()> {
    let dt = parse_datetime("1650572430000")?;

    let r1h = round_ts(dt, "1h").unwrap();
    let r1d = round_ts(dt, "1d").unwrap();

    assert_eq!(to_datetime(r1h), datetime!(2022-04-22 04:00:00 +8));

    assert_eq!(to_datetime(r1d), datetime!(2022-04-22 00:00:00 +8));

    assert_eq!(
      to_datetime(round_ts(dt, "1w").unwrap()),
      datetime!(2022-04-18 00:00:00 +8)
    );

    assert_eq!(
      to_datetime(round_ts(dt, "1M").unwrap()),
      datetime!(2022-04-01 00:00:00 +8)
    );

    assert_eq!(
      to_datetime(round_ts(dt, "1y").unwrap()),
      datetime!(2022-01-01 00:00:00 +8)
    );

    //println!("{} {} {}", dt / 1_000_000, r1h / 1_000_000, r1d / 1_000_000);
    Ok(())
  }
}
