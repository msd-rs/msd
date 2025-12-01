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

macro_rules! getter {
  ($class:ident, $name:ident, $kind:ident, $type:ty) => {
    pub fn $name(&self) -> Option<&$type> {
      if let $class::$kind(v) = self {
        Some(v)
      } else {
        None
      }
    }
  };
}

macro_rules! getter_mut {
  ($class:ident, $name:ident, $kind:ident, $type:ty) => {
    pub fn $name(&mut self) -> Option<&mut $type> {
      if let $class::$kind(v) = self {
        Some(v)
      } else {
        None
      }
    }
  };
}

/// Macro for creating a `Table` from column definitions.
///
/// # Usage
///
/// ```rust
/// use msd_table::{table, Table, DataType};
///
/// // Create a table with columns (data is optional, defaults to empty Vec)
/// let t = table!(
///   { name: "id", kind: i64, data: vec![1i64, 2, 3] },
///   { name: "value", kind: f64, data: vec![1.0, 2.0, 3.0] },
///   { name: "label", kind: string }  // data defaults to Vec::default()
/// );
/// ```
///
/// ## Supported Types
///
/// The `type` field accepts the following identifiers (matching `DataType` variants):
/// - `i32` -> `DataType::Int32`
/// - `u32` -> `DataType::UInt32`
/// - `i64` -> `DataType::Int64`
/// - `u64` -> `DataType::UInt64`
/// - `f32` -> `DataType::Float32`
/// - `f64` -> `DataType::Float64`
/// - `d64` -> `DataType::Decimal64`
/// - `d128` -> `DataType::Decimal128`
/// - `bool` -> `DataType::Bool`
/// - `string` -> `DataType::String`
/// - `bytes` -> `DataType::Bytes`
/// - `datetime` -> `DataType::DateTime`
/// - `null` -> `DataType::Null`
///
#[macro_export]
macro_rules! table {
  // Internal rule: parse type identifier to DataType
  (@type i32) => { $crate::DataType::Int32 };
  (@type u32) => { $crate::DataType::UInt32 };
  (@type i64) => { $crate::DataType::Int64 };
  (@type u64) => { $crate::DataType::UInt64 };
  (@type f32) => { $crate::DataType::Float32 };
  (@type f64) => { $crate::DataType::Float64 };
  (@type d64) => { $crate::DataType::Decimal64 };
  (@type d128) => { $crate::DataType::Decimal128 };
  (@type bool) => { $crate::DataType::Bool };
  (@type string) => { $crate::DataType::String };
  (@type bytes) => { $crate::DataType::Bytes };
  (@type datetime) => { $crate::DataType::DateTime };
  (@type null) => { $crate::DataType::Null };

  // Internal rule: create default Series for a DataType
  (@default_series i32) => { $crate::Series::Int32(Vec::default()) };
  (@default_series u32) => { $crate::Series::UInt32(Vec::default()) };
  (@default_series i64) => { $crate::Series::Int64(Vec::default()) };
  (@default_series u64) => { $crate::Series::UInt64(Vec::default()) };
  (@default_series f32) => { $crate::Series::Float32(Vec::default()) };
  (@default_series f64) => { $crate::Series::Float64(Vec::default()) };
  (@default_series d64) => { $crate::Series::Decimal64(Vec::default()) };
  (@default_series d128) => { $crate::Series::Decimal128(Vec::default()) };
  (@default_series bool) => { $crate::Series::Bool(Vec::default()) };
  (@default_series string) => { $crate::Series::String(Vec::default()) };
  (@default_series bytes) => { $crate::Series::Bytes(Vec::default()) };
  (@default_series datetime) => { $crate::Series::DateTime(Vec::default()) };
  (@default_series null) => { $crate::Series::Null };

  // Internal rule: column with data
  (@column { name: $name:expr, kind: $kind:ident, data: $data:expr }) => {
    $crate::TableColumn::new(
      $crate::Field::new($name, table!(@type $kind)),
      $crate::Series::from($data),
    )
  };

  // Internal rule: column without data (use default empty Vec)
  (@column { name: $name:expr, kind: $kind:ident }) => {
    $crate::TableColumn::new (
      $crate::Field::new($name, table!(@type $kind)),
      table!(@default_series $kind),
    )
  };

  // Main entry: create Table from multiple column definitions
  ($( $col:tt ),* $(,)?) => {
    {
      let columns = vec![
        $( table!(@column $col) ),*
      ];
      $crate::Table::from_columns(columns)
    }
  };
}
