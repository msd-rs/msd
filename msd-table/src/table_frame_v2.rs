// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! this module provides a streaming table frame (known as v2) serialization format,
//! it is defined as follows:
//!
//! TABLE_FRAME := TABLE_HEADER + OBJ_NAME + TABLE_NAME + COLUMN_COUNT + ROWS_COUNT  + TABLE_DATA
//! TABLE_HEADER := MAGIC + VERSION + DATA_SIZE
//! MAGIC := 0x4d7c as u16
//! VERSION := 0x0200 as u16
//! OBJ_NAME := STRING
//! COLUMN_COUNT := count(columns) as u32
//! ROW_COUNT := count(rows) as u32
//! TABLE_DATA := [COLUMN_NAME + COLUMN_TYPE + [DATA]] * ROWS_COUNT] * COLUMN_COUNT
//! COLUMN_NAME := STRING
//! COLUMN_TYPE := u8
//! DATA := column's values
//! STRING := LENGTH + BYTES

use crate::{D64, DataType, Field, Series, Table, TableFrameError};
use std::convert::TryInto;

const MAGIC: u16 = 0x4d7c;
const VERSION: u16 = 0x0200;

/// Pack a table frame
///
/// # Arguments
///
/// * `table` - The table to pack
///
/// # Returns
///
/// * `Vec<u8>` - The packed table frame
///
pub fn pack_table_frame_v2(table: &Table) -> Vec<u8> {
  let mut frame = Vec::new();
  frame.extend_from_slice(&MAGIC.to_le_bytes());
  frame.extend_from_slice(&VERSION.to_le_bytes());
  frame.extend_from_slice(&[0u8; 4]); // will be updated later
  write_string(
    &mut frame,
    table
      .get_table_meta("obj")
      .and_then(|v| v.get_str())
      .unwrap_or(""),
  );
  write_string(
    &mut frame,
    table
      .get_table_meta("table")
      .and_then(|v| v.get_str())
      .unwrap_or(""),
  );
  frame.extend_from_slice(&(table.column_count() as u32).to_le_bytes());
  frame.extend_from_slice(&(table.row_count() as u32).to_le_bytes());
  for field in table.columns() {
    write_string(&mut frame, &field.name);
    frame.extend_from_slice(&[field.kind as u8]);
    match &field.data {
      Series::Null => {}
      Series::String(v) => v.iter().for_each(|s| write_string(&mut frame, s)),
      Series::Bytes(v) => v.iter().for_each(|b| write_bytes(&mut frame, b)),
      Series::Int32(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&v.to_le_bytes())),
      Series::UInt32(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&v.to_le_bytes())),
      Series::Int64(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&v.to_le_bytes())),
      Series::UInt64(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&v.to_le_bytes())),
      Series::Float32(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&v.to_le_bytes())),
      Series::Float64(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&v.to_le_bytes())),
      Series::Decimal64(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&(f64::from(*v)).to_le_bytes())),
      Series::Bool(v) => v.iter().for_each(|v| frame.push(if *v { 1 } else { 0 })),
      Series::DateTime(v) => v
        .iter()
        .for_each(|v| frame.extend_from_slice(&((*v as f64) / 1000.0).to_le_bytes())),
    }
  }

  let data_size = frame.len() - 8;
  frame[4..8].copy_from_slice(&(data_size as u32).to_le_bytes());

  frame
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
  buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
  buf.extend_from_slice(s.as_bytes());
}

fn write_bytes(buf: &mut Vec<u8>, b: &[u8]) {
  buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
  buf.extend_from_slice(b);
}

/// Check if the buffer is a valid table frame
///
/// # Arguments
///
/// * `buf` - The buffer to check
///
/// # Returns
///
/// * `Result<(usize, usize), TableFrameError>` - The size of the (header, data) tuple
///
pub fn check_table_frame_v2(buf: &[u8]) -> Result<(usize, usize), TableFrameError> {
  if buf.len() < 8 {
    return Err(TableFrameError::BufferTooSmall(8, buf.len()));
  }

  let magic = u16::from_le_bytes(buf[0..2].try_into().unwrap());
  if magic != MAGIC {
    return Err(TableFrameError::InvalidTableFrame(format!(
      "Invalid magic: {}",
      magic
    )));
  }

  let version = u16::from_le_bytes(buf[2..4].try_into().unwrap());
  if version != VERSION {
    return Err(TableFrameError::InvalidTableFrame(format!(
      "Invalid version: {}",
      version
    )));
  }

  let frame_size = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
  Ok((8, frame_size))
}

/// Unpack a table frame
///
/// # Arguments
///
/// * `buf` - The buffer to unpack
/// * `skip_header` - Whether to skip the header
///
/// # Returns
///
/// * `Result<Table, TableFrameError>` - The unpacked table frame
///
pub fn unpack_table_frame_v2(buf: &[u8], skip_header: bool) -> Result<Table, TableFrameError> {
  let table_data = if skip_header {
    buf
  } else {
    if buf.len() < 8 {
      return Err(TableFrameError::BufferTooSmall(8, buf.len()));
    }

    let magic = u16::from_le_bytes(buf[0..2].try_into().unwrap());
    if magic != MAGIC {
      return Err(TableFrameError::InvalidTableFrame(format!(
        "Invalid magic: {}",
        magic
      )));
    }

    let version = u16::from_le_bytes(buf[2..4].try_into().unwrap());
    if version != VERSION {
      return Err(TableFrameError::InvalidTableFrame(format!(
        "Invalid version: {}",
        version
      )));
    }

    let frame_size = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
    if buf.len() < 8 + frame_size {
      return Err(TableFrameError::BufferTooSmall(8 + frame_size, buf.len()));
    }
    &buf[8..8 + frame_size]
  };

  let mut offset = 0;

  let obj = read_string(table_data, &mut offset);
  let table = read_string(table_data, &mut offset);
  let cols = read_u32(table_data, &mut offset);
  let rows = read_u32(table_data, &mut offset);

  let mut columns = Vec::with_capacity(cols as usize);
  for _ in 0..cols {
    let name = read_string(table_data, &mut offset);
    let kind = read_u8(table_data, &mut offset);

    let kind = DataType::try_from(kind).map_err(|e| {
      TableFrameError::InvalidTableFrame(format!("column '{}' has invalid data type {}", name, e))
    })?;

    let field = match kind {
      DataType::Null => Field {
        name,
        kind,
        metadata: None,
        data: Series::Null,
      },
      DataType::DateTime => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_i64(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::DateTime(vec),
          metadata: None,
        }
      }
      DataType::String => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_string(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::String(vec),
          metadata: None,
        }
      }
      DataType::Bytes => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_bytes(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Bytes(vec),
          metadata: None,
        }
      }
      DataType::Int32 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_int32(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Int32(vec),
          metadata: None,
        }
      }
      DataType::UInt32 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_u32(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::UInt32(vec),
          metadata: None,
        }
      }
      DataType::Int64 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_i64(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Int64(vec),
          metadata: None,
        }
      }
      DataType::UInt64 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_u64(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::UInt64(vec),
          metadata: None,
        }
      }
      DataType::Float32 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_f32(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Float32(vec),
          metadata: None,
        }
      }
      DataType::Float64 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_f64(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Float64(vec),
          metadata: None,
        }
      }
      DataType::Decimal64 => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_d64(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Decimal64(vec),
          metadata: None,
        }
      }
      DataType::Bool => {
        let mut vec = Vec::with_capacity(rows as usize);
        for _ in 0..rows {
          let v = read_bool(table_data, &mut offset);
          vec.push(v);
        }
        Field {
          name,
          kind,
          data: Series::Bool(vec),
          metadata: None,
        }
      }
    };

    columns.push(field);
  }

  Ok(Table::from_columns(columns).replace_metadata([("obj", obj), ("table", table)]))
}

fn read_string(buf: &[u8], offset: &mut usize) -> String {
  let len = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
  *offset += len + 4;
  String::from_utf8(buf[*offset - 4..*offset].to_vec()).unwrap()
}

fn read_bytes(buf: &[u8], offset: &mut usize) -> Vec<u8> {
  let len = u32::from_le_bytes(buf[*offset..*offset + 4].try_into().unwrap()) as usize;
  *offset += len + 4;
  buf[*offset - 4..*offset].to_vec()
}

fn read_u8(buf: &[u8], offset: &mut usize) -> u8 {
  *offset += 1;
  buf[*offset - 1]
}

fn read_int32(buf: &[u8], offset: &mut usize) -> i32 {
  *offset += 4;
  i32::from_le_bytes(buf[*offset - 4..*offset].try_into().unwrap())
}

fn read_i64(buf: &[u8], offset: &mut usize) -> i64 {
  *offset += 8;
  i64::from_le_bytes(buf[*offset - 8..*offset].try_into().unwrap())
}

fn read_u32(buf: &[u8], offset: &mut usize) -> u32 {
  *offset += 4;
  u32::from_le_bytes(buf[*offset - 4..*offset].try_into().unwrap())
}

fn read_u64(buf: &[u8], offset: &mut usize) -> u64 {
  *offset += 8;
  u64::from_le_bytes(buf[*offset - 8..*offset].try_into().unwrap())
}

fn read_f32(buf: &[u8], offset: &mut usize) -> f32 {
  *offset += 4;
  f32::from_le_bytes(buf[*offset - 4..*offset].try_into().unwrap())
}

fn read_f64(buf: &[u8], offset: &mut usize) -> f64 {
  *offset += 8;
  f64::from_le_bytes(buf[*offset - 8..*offset].try_into().unwrap())
}

fn read_d64(buf: &[u8], offset: &mut usize) -> D64 {
  let v = read_f64(buf, offset);
  D64::from_f64(v, 2)
}

fn read_bool(buf: &[u8], offset: &mut usize) -> bool {
  let v = buf[*offset];
  *offset += 1;
  v == 1
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::{DataType, Field};

  #[test]
  fn test_pack_unpack_roundtrip() {
    let columns = vec![
      Field::new("id", DataType::Int64, 0),
      Field::new("name", DataType::String, 0),
    ];
    let table = Table::from_columns(columns);
    // Note: Empty table for now, or populate it if needed for deeper test

    let packed = pack_table_frame_v2(&table);
    let unpacked_table = unpack_table_frame_v2(&packed, false).unwrap();

    assert_eq!(unpacked_table.column_count(), table.column_count());
    // Add more assertions as needed
  }

  #[test]
  fn test_invalid_magic() {
    let mut packed = pack_table_frame_v2(&Table::default());
    packed[0] = 0x00; // Corrupt magic
    let err = unpack_table_frame_v2(&packed, false).unwrap_err();
    match err {
      TableFrameError::InvalidTableFrame(_) => (),
      _ => panic!("Expected InvalidTableFrame"),
    }
  }
}
