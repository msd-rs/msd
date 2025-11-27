use super::Variant;
use crate::{D64, D128};

macro_rules! impl_variant_from {
  ($name:ident, $type:ty) => {
    impl From<$type> for Variant {
      fn from(value: $type) -> Self {
        Variant::$name(value)
      }
    }

    impl From<&$type> for Variant {
      fn from(value: &$type) -> Self {
        Variant::$name(value.clone())
      }
    }
  };
}

impl_variant_from!(Int32, i32);
impl_variant_from!(UInt32, u32);
impl_variant_from!(Int64, i64);
impl_variant_from!(UInt64, u64);
impl_variant_from!(Float32, f32);
impl_variant_from!(Float64, f64);
impl_variant_from!(String, String);
impl_variant_from!(Bytes, Vec<u8>);
impl_variant_from!(Bool, bool);
impl_variant_from!(Decimal64, D64);
impl_variant_from!(Decimal128, D128);

impl From<&str> for Variant {
  fn from(value: &str) -> Self {
    Variant::String(value.to_string())
  }
}

impl From<&[u8]> for Variant {
  fn from(value: &[u8]) -> Self {
    Variant::Bytes(value.to_vec())
  }
}

impl From<usize> for Variant {
  fn from(value: usize) -> Self {
    Variant::UInt64(value as u64)
  }
}
