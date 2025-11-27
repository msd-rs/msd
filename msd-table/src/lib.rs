#[macro_use]
mod macros;

mod d64;
mod errors;
mod serde;
mod table;
mod updater;
mod variant;

pub use d64::D64;
pub use errors::TableError;
pub use rust_decimal::Decimal as D128;
pub use table::{DataType, Field, Series, Table, TableColumn};
pub use updater::*;
pub use variant::{Variant, VariantMutRef, VariantRef};

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
