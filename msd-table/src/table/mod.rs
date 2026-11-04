// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

mod datatype;
mod field;
mod rows_table;
mod series;
mod table;
mod table_ref;

pub use datatype::DataType;
pub use field::Field;
pub use rows_table::RowsTable;
pub use series::Series;
pub(crate) use table::{PosSliceReader, SkipVariant};
pub use table::{
  FillMethod, LazyChunk, Table, compute_joined_indices, decode_projected_series,
  extend_series_with_fill, joined_index,
};
pub use table_ref::{FieldRef, SeriesRef, TableRef};
