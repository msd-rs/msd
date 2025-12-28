//! 定义64位的 Decimal 类型
//!
//! Decimal 即的精确小数位数的浮点数, 主要用于表达金额,价格等数据, D64 是一个宽度位 64bit 的 Decimal,
//! 在64位系统下, 正好可以放置到一个整数中, 但其限制16个有效数字, 超出的将会溢出.
//!
//! 数据的格式如下, 从低到高位
//! - 0    : 是否负数
//! - 1    : 是否INF
//! - 2    : 是否NAN
//! - 3    : 未使用
//! - 4~8  : 小数位数
//! - 8~64 : 原始数值*10的小数位次方
//!
//! D64 定义了基本的四则运算和比较运算, 以及到 i64 和 f64 转换
//!
//! D64 也定义了从字符串解析和生成的相关方法

use std::{
  fmt::Display,
  hash::Hash,
  ops::{Add, Div, Mul, Sub},
  str::FromStr,
};

use serde::{Deserialize, Serialize};

/// 针对10的次方的性能优化
fn pow10(n: usize) -> f64 {
  const TABLE: [f64; 12] = [
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0,
    10000000000.0,
    100000000000.0,
  ];

  if n < TABLE.len() {
    TABLE[n]
  } else {
    10.0_f64.powi(n as i32)
  }
}

const LEN: usize = 8;

/// flag layout
/// 0: is neg
/// 1: is inf
/// 2: is nan
/// 3: no used
/// 4~8: decimal number
const FLAG: usize = 7;

/// D64 实际的存储
#[derive(Clone, Copy, Default, bincode::Encode, bincode::Decode)]
pub struct D64 {
  /// 8 个 byte
  v: [u8; LEN],
}

impl std::fmt::Debug for D64 {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if self.is_nan() {
      return f.write_str("NAN");
    }
    if self.is_inf() {
      return f.write_str("INF");
    }
    let v: f64 = self.into();
    f.write_fmt(format_args!(
      "{:.precision$}",
      v,
      precision = self.dec_num()
    ))
  }
}

impl Serialize for D64 {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    serializer.serialize_u64(self.into())
  }
}

impl<'de> Deserialize<'de> for D64 {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    let u = u64::deserialize(deserializer)?;
    Ok(D64::from(u))
  }
}

impl D64 {
  /// 是否为 INF
  pub fn is_inf(&self) -> bool {
    let b = self.v[FLAG] & 0b0000_0100;
    return b != 0;
  }

  /// 是否为 NAN
  pub fn is_nan(&self) -> bool {
    let b = self.v[FLAG] & 0b0000_0010;
    return b != 0;
  }

  /// 是否为 负数
  pub fn is_neg(&self) -> bool {
    let b = self.v[FLAG] & 0b0000_0001;
    return b != 0;
  }

  /// 获取小数位数
  pub fn dec_num(&self) -> usize {
    (self.v[FLAG] >> 4) as usize
  }
}

/// #生成方法
impl D64 {
  /// 从浮点数生成，指定浮点数和小数位数
  /// `let d = from_f64(1.23, 3); // 为 "1.230"`
  pub fn from_f64(v: f64, dec: usize) -> D64 {
    let vi = (v * pow10(dec)) as i64;
    Self::from_i64(vi, dec)
  }

  /// 从整数生成，指定整数值数和小数位数
  /// `let d = from_i64(12, 2); // 为 "12.00"`
  pub fn from_i64(v: i64, dec: usize) -> D64 {
    let (n, flag) = if v < 0 {
      (-v as u64, (dec << 4 | 1) as u64)
    } else {
      (v as u64, (dec << 4) as u64)
    };
    D64::from(n << 8 | flag)
  }

  /// 根据当前的小数位数，创建给定浮点数的 D64
  /// ```
  /// let d1 = from_i64(12, 2);      // 为 "12.00"
  /// let d2 = d1.with_f64(12.0123); // 为 "12.01"
  /// ```
  pub fn with_f64(&self, v: f64) -> D64 {
    Self::from_f64(v, self.dec_num())
  }

  /// 根据当前的小数位数，创建给定整数的 D64
  /// ```
  /// let d1 = from_i64(12, 2); // 为 "12.00"
  /// let d2 = d1.with_i64(13); // 为 "13.00"
  /// ```
  pub fn with_i64(&self, v: i64) -> D64 {
    Self::from_i64(v, self.dec_num())
  }

  /// 根据当前的小数位数，创建零值
  /// ```
  /// let d1 = from_i64(12, 2); // 为 "12.00"
  /// let d2 = d1.to_zero();    // 为 "0.00"
  /// ```
  pub fn to_zero(&self) -> D64 {
    let dec_num = self.dec_num() as u64;
    D64 {
      v: (dec_num << 4).to_be_bytes(),
    }
  }

  /// 判断当前值是否为零值
  pub fn is_zero(&self) -> bool {
    let v: u64 = self.into();
    (v >> 8) == 0
  }
}

impl D64 {
  pub fn set_nan(&mut self) {
    self.v = D64_NAN.v
  }

  pub fn set_zero(&mut self) {
    let dec_num = self.dec_num() as u64;
    self.v = (dec_num << 4).to_be_bytes()
  }

  pub fn set_i64(&mut self, n: i64) {
    let (n, flag) = if n < 0 {
      (-n as u64, (self.dec_num() << 4 | 1) as u64)
    } else {
      (n as u64, (self.dec_num() << 4) as u64)
    };
    self.v = (n << 8 | flag).to_be_bytes()
  }
}

impl Display for D64 {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if self.is_nan() {
      return f.write_str("NAN");
    }
    if self.is_inf() {
      return f.write_str("INF");
    }
    let v: f64 = self.into();
    f.write_fmt(format_args!(
      "{:.precision$}",
      v,
      precision = self.dec_num()
    ))
  }
}

pub const PFE_EMPTY: i64 = 1;
pub const PFE_INVALID: i64 = 2;
pub const PFE_OVERFLOW: i64 = 3;
pub const D64_NAN: D64 = D64 {
  v: [0, 0, 0, 0, 0, 0, 0, 2],
};
pub const D64_INF: D64 = D64 {
  v: [0, 0, 0, 0, 0, 0, 0, 4],
};
//pub const D64_ZERO: D64 = D64{v:[0, 0, 0, 0, 0, 0, 0, 0]};

impl FromStr for D64 {
  type Err = i64;

  fn from_str(s: &str) -> Result<D64, Self::Err> {
    let s = s.trim();
    if s.len() == 3 {
      if s.eq_ignore_ascii_case("NAN") {
        return Ok(D64_NAN);
      }
      if s.eq_ignore_ascii_case("INF") {
        return Ok(D64_INF);
      }
    }
    if s.len() == 0 {
      return Err(PFE_EMPTY);
    }
    let s = s.as_bytes();
    let mut dec = s.len() - 1;
    let mut val: u64 = 0;
    let mut is_neg = false;

    for (i, b) in s.iter().enumerate() {
      match b {
        b'0'..=b'9' => val = val * 10 + (*b - b'0') as u64,
        b'.' => dec = i,
        b'-' => is_neg = true,
        _ => return Err(PFE_INVALID),
      }
    }

    if val > 0xFF_FFFF_FFFF_FFFF {
      return Err(PFE_OVERFLOW);
    }

    dec = s.len() - 1 - dec;

    let flag = (dec as u8) << 4 | (is_neg as u8);

    let mut d: D64 = (val << 8).into();
    d.v[FLAG] = flag;
    Ok(d)
  }
}

impl From<&D64> for u64 {
  fn from(d: &D64) -> Self {
    u64::from_be_bytes(d.v)
  }
}

impl From<D64> for u64 {
  fn from(d: D64) -> Self {
    u64::from_be_bytes(d.v)
  }
}

impl From<u64> for D64 {
  fn from(u: u64) -> Self {
    D64 { v: u.to_be_bytes() }
  }
}

impl From<&D64> for i64 {
  fn from(d: &D64) -> Self {
    let n = i64::from_be_bytes(d.v) >> 8;
    if d.is_neg() { -n } else { n }
  }
}

impl From<D64> for i64 {
  fn from(d: D64) -> Self {
    let n = i64::from_be_bytes(d.v) >> 8;
    if d.is_neg() { -n } else { n }
  }
}

impl From<&D64> for f64 {
  fn from(d: &D64) -> Self {
    d.clone().into()
  }
}
impl From<D64> for f64 {
  fn from(d: D64) -> Self {
    let n: u64 = d.into();
    let n = (n >> 8) as i64;
    let n = if d.is_neg() { -n } else { n } as f64;
    match d.dec_num() {
      0 => n,
      p => n / pow10(p),
    }
  }
}

impl Add for D64 {
  type Output = Self;

  fn add(self, rhs: Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs + rhs, self.dec_num())
  }
}

impl Add<&D64> for D64 {
  type Output = Self;

  fn add(self, rhs: &Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs + rhs, self.dec_num())
  }
}

impl Add<f64> for D64 {
  type Output = Self;

  fn add(self, rhs: f64) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    D64::from_f64(lhs + rhs, self.dec_num())
  }
}

impl Sub for D64 {
  type Output = Self;

  fn sub(self, rhs: Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs - rhs, self.dec_num())
  }
}

impl Sub<&D64> for D64 {
  type Output = Self;

  fn sub(self, rhs: &Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs - rhs, self.dec_num())
  }
}

impl Sub<f64> for D64 {
  type Output = Self;

  fn sub(self, rhs: f64) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    D64::from_f64(lhs - rhs, self.dec_num())
  }
}

impl Mul for D64 {
  type Output = Self;

  fn mul(self, rhs: Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs * rhs, self.dec_num())
  }
}

impl Mul<&D64> for D64 {
  type Output = Self;

  fn mul(self, rhs: &Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs * rhs, self.dec_num())
  }
}

impl Mul<f64> for D64 {
  type Output = Self;

  fn mul(self, rhs: f64) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    D64::from_f64(lhs * rhs, self.dec_num())
  }
}

impl Div for D64 {
  type Output = Self;

  fn div(self, rhs: Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs / rhs, self.dec_num())
  }
}

impl Div<&D64> for D64 {
  type Output = Self;

  fn div(self, rhs: &Self) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    let rhs: f64 = rhs.into();
    D64::from_f64(lhs / rhs, self.dec_num())
  }
}

impl Div<f64> for D64 {
  type Output = Self;

  fn div(self, rhs: f64) -> Self::Output {
    if self.is_nan() || rhs.is_nan() {
      return D64_NAN;
    }
    let lhs: f64 = self.into();
    D64::from_f64(lhs / rhs, self.dec_num())
  }
}

impl PartialEq for D64 {
  fn eq(&self, other: &Self) -> bool {
    if self.v == other.v {
      true
    } else {
      if self.dec_num() == other.dec_num() {
        let a: i64 = self.into();
        let b: i64 = self.into();
        a == b
      } else {
        let a: f64 = self.into();
        let b: f64 = self.into();
        a == b
      }
    }
  }
}

impl PartialOrd for D64 {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    if self.v == other.v {
      Some(std::cmp::Ordering::Equal)
    } else {
      if self.dec_num() == other.dec_num() {
        let a: i64 = self.into();
        let b: i64 = other.into();
        a.partial_cmp(&b)
      } else {
        let a: f64 = self.into();
        let b: f64 = other.into();
        a.partial_cmp(&b)
      }
    }
  }
}

impl Hash for D64 {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.v.hash(state);
  }
}

impl From<&super::D128> for D64 {
  fn from(d: &super::D128) -> Self {
    let dec_num = d.scale() as usize;
    let n = d.mantissa() as i64;
    D64::from_i64(n, dec_num)
  }
}

impl From<&D64> for super::D128 {
  fn from(d: &D64) -> Self {
    let scale = d.dec_num() as u32;
    let num = d.into();
    super::D128::new(num, scale)
  }
}

#[cfg(test)]
mod tests {

  use super::D64;

  #[test]
  fn test_from() {
    let d1 = D64::from_f64(1.23, 2);
    let d2 = D64::from_i64(-123, 2);

    let n1: i64 = d1.into();
    let n2: i64 = d2.into();

    let u1: u64 = d1.into();
    let u2: u64 = d2.into();
    println!("{d1}, {d2} {n1} {n2} {u1} {u2}");
  }

  #[test]
  fn test_math_operator() {
    let d1 = "123.450".parse::<D64>().unwrap();
    let d2 = "-121.45".parse::<D64>().unwrap();
    let d3 = "8".parse::<D64>().unwrap();
    let d4 = "6".parse::<D64>().unwrap();

    let dr = (d1 + d2) * d3 / d4;

    assert_eq!(
      "2.666".to_string(),
      dr.to_string(),
      "({d1} + {d2}) * {d3} / {d4} = {dr}"
    );
  }

  #[test]
  fn test_cmp_operator() -> Result<(), i64> {
    let d1: D64 = "123.45".parse()?;
    let d2: D64 = "123.45".parse()?;
    let d3: D64 = "123.46".parse()?;

    assert_eq!(d1, d2, "{d1} == {d2}");
    assert_eq!(true, d1 < d3, "{d1} < {d3}");

    Ok(())
  }
}
