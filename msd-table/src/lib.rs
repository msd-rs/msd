/// Macros for creating `Variant` instances.
/// This macro allows you to create a `Variant` from a single value or a list of values.
////// Usage:
/// - `v!(value)` creates a `Variant` from a single value.
/// - `v!(value1, value2, ...)` creates a `Vec<Variant>` from multiple values.
///
#[macro_export]
macro_rules! v {
  ($val:expr) => {
    Variant::from($val)
  };
  ($($element:expr),+) => {
    {
      let mut a = Vec::new();
      $(a.push(Variant::from($element));)*
      a
    }
  };
}

#[macro_export]
macro_rules! s {
  ($val:expr) => {
    Series::from($val)
  };
  ($($element:expr),+) => {
    {
      let mut a = Vec::new();
      $(a.push($element);)*
      Series::from(a)
    }
  };
}

mod d64;
mod errors;
mod serde;
mod table;

pub use d64::D64;
pub use errors::TableError;
pub use rust_decimal::Decimal as D128;
pub use table::{DataType, Field, Series, Table, TableColumn, Variant, VariantMutRef, VariantRef};

impl From<&D128> for D64 {
  fn from(d: &D128) -> Self {
    let dec_num = d.scale() as usize;
    let n = d.mantissa() as i64;
    D64::from_i64(n, dec_num)
  }
}

impl From<&D64> for D128 {
  fn from(d: &D64) -> Self {
    let scale = d.dec_num() as u32;
    let num = d.into();
    D128::new(num, scale)
  }
}
