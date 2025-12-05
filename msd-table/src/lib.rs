#[macro_use]
mod macros;

mod d64;
mod date;
mod errors;
mod serde;
mod table;
mod updater;
mod variant;

pub use csv::*;
pub use d64::D64;
pub use date::*;
pub use errors::TableError;
pub use rust_decimal::Decimal as D128;
pub use serde::csv::*;
pub use table::{DataType, Field, Series, Table};
pub use updater::*;
pub use variant::{Variant, VariantMutRef, VariantRef};
