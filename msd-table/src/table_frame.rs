// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! this module provides a simple table binary format,
//! the format is defined as follows:
//!
//! TABLE_FRAME := TABLE_HEADER + TABLE_DATA + TABLE_FOOTER
//! TABLE_HEADER := MAGIC + VERSION + FRAME_SIZE
//! MAGIC := 0x4d7c as u16
//! VERSION := 0x0001 as u16
//! FRAME_SIZE := sizeof(TABLE_DATA + TABLE_FOOTER) as u32
//! TABLE_DATA := binary of Table
//! TABLE_FOOTER := CRC32 of TABLE_DATA

use crate::{Series, Table, TableRef, Variant};
use std::convert::TryInto;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TableFrameError {
  #[error("Invalid table frame: {0}")]
  InvalidTableFrame(String),
  #[error("Buffer too small, want {0} got {1}")]
  BufferTooSmall(usize, usize),
  #[error("Table frame CRC32 check failed")]
  InvalidCrc,
}

const MAGIC: u16 = 0x4d7c;
const VERSION: u16 = 0x0001;

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
pub fn pack_table_frame(table: &Table) -> Vec<u8> {
  let mut frame = Vec::with_capacity(256);
  pack_table_frame_into(table, &mut frame);
  frame
}

pub fn pack_table_frame_into(table: &Table, frame: &mut Vec<u8>) {
  let header_pos = frame.len();
  frame.extend_from_slice(&MAGIC.to_le_bytes());
  frame.extend_from_slice(&VERSION.to_le_bytes());
  frame.extend_from_slice(&0u32.to_le_bytes());

  let data_start = frame.len();
  bincode_next::encode_into_std_write(table, &mut *frame, bincode_next::config::standard())
    .expect("Failed to serialize table");

  let data_len = frame.len() - data_start;
  let crc = crc32(&frame[data_start..]);
  let frame_size = (data_len + 4) as u32;

  frame[header_pos + 4..header_pos + 8].copy_from_slice(&frame_size.to_le_bytes());
  frame.extend_from_slice(&crc.to_le_bytes());
}

/// Pre-encodes the invariant schema parts of a projected `Table` (`version`, `columns.len()`,
/// each column's `(name, kind, metadata)` header, and the `Table.metadata` prefix) so that
/// packing many objects' tables in a batch only copies pre-encoded headers and encodes `Series` + `obj`.
pub struct BatchFramePacker {
  table_prefix: Vec<u8>,
  col_headers: Vec<Vec<u8>>,
  meta_prefix: Vec<u8>,
  is_kv: bool,
}

impl BatchFramePacker {
  pub fn new(template: &Table, table_name: &str) -> Self {
    use bincode_next::Encode;
    use bincode_next::enc::Encoder;

    let cfg = bincode_next::config::standard();

    let mut table_prefix = Vec::with_capacity(8);
    {
      let mut enc = bincode_next::enc::EncoderImpl::new(&mut table_prefix, cfg);
      enc.encode_struct_header(4).unwrap();
      template.version().encode(&mut enc).unwrap();
      enc.encode_slice_len(template.column_count()).unwrap();
    }

    let mut col_headers = Vec::with_capacity(template.column_count());
    for col in template.columns() {
      let mut hdr = Vec::with_capacity(32);
      {
        let mut enc = bincode_next::enc::EncoderImpl::new(&mut hdr, cfg);
        enc.encode_struct_header(4).unwrap();
        col.name.encode(&mut enc).unwrap();
        col.kind.encode(&mut enc).unwrap();
        col.metadata.encode(&mut enc).unwrap();
      }
      col_headers.push(hdr);
    }

    let mut meta_prefix = Vec::with_capacity(32);
    {
      let mut enc = bincode_next::enc::EncoderImpl::new(&mut meta_prefix, cfg);
      // Option::Some discriminant = 1u32
      1u32.encode(&mut enc).unwrap();
      enc.encode_struct_header(1).unwrap();
      // Only "table" and "obj" metadata keys, matching `result.replace_metadata([("obj", ...), ("table", ...)])`
      enc.encode_map_len(2).unwrap();
      "table".encode(&mut enc).unwrap();
      Variant::String(table_name.to_string()).encode(&mut enc).unwrap();
      // Encode key "obj" and Variant::String discriminant (5u32) so only `obj: &str` needs to be appended per object
      "obj".encode(&mut enc).unwrap();
      5u32.encode(&mut enc).unwrap();
      enc.encode_struct_header(1).unwrap();
    }

    Self {
      table_prefix,
      col_headers,
      meta_prefix,
      is_kv: template.is_kv(),
    }
  }

  #[inline]
  pub fn pack_series_into(&self, cols: &[Series], obj: &str, frame: &mut Vec<u8>) {
    use bincode_next::Encode;
    use bincode_next::enc::Encoder;
    use bincode_next::enc::write::Writer;

    let cfg = bincode_next::config::standard();
    let header_pos = frame.len();
    frame.extend_from_slice(&MAGIC.to_le_bytes());
    frame.extend_from_slice(&VERSION.to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes());

    let data_start = frame.len();
    frame.extend_from_slice(&self.table_prefix);

    let mut enc = bincode_next::enc::EncoderImpl::new(&mut *frame, cfg);
    for (hdr, series) in self.col_headers.iter().zip(cols.iter()) {
      enc.writer().write(hdr).unwrap();
      series.encode(&mut enc).unwrap();
    }
    enc.writer().write(&self.meta_prefix).unwrap();
    obj.encode(&mut enc).unwrap();
    self.is_kv.encode(&mut enc).unwrap();

    let data_len = frame.len() - data_start;
    let crc = crc32(&frame[data_start..]);
    let frame_size = (data_len + 4) as u32;

    frame[header_pos + 4..header_pos + 8].copy_from_slice(&frame_size.to_le_bytes());
    frame.extend_from_slice(&crc.to_le_bytes());
  }
}

pub fn pack_table_ref_frame(table: &TableRef) -> Vec<u8> {
  let mut table_data = Vec::new();

  // binary of Table
  let table_bytes = bincode_next::encode_to_vec(table, bincode_next::config::standard())
    .expect("Failed to serialize table");
  table_data.extend_from_slice(&table_bytes);

  let crc = crc32(&table_data);

  // FRAME_SIZE := sizeof(TABLE_DATA + TABLE_FOOTER) as u32
  // TABLE_FOOTER is CRC32 (4 bytes)
  let frame_size = (table_data.len() + 4) as u32;

  let mut frame = Vec::new();
  frame.extend_from_slice(&MAGIC.to_le_bytes());
  frame.extend_from_slice(&VERSION.to_le_bytes());
  frame.extend_from_slice(&frame_size.to_le_bytes());
  frame.extend_from_slice(&table_data);
  frame.extend_from_slice(&crc.to_le_bytes());

  frame
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
pub fn check_table_frame(buf: &[u8]) -> Result<(usize, usize), TableFrameError> {
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
pub fn unpack_table_frame(buf: &[u8], skip_header: bool) -> Result<Table, TableFrameError> {
  let content_and_footer = if skip_header {
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

  // TABLE_DATA + FOOTER (CRC)
  if content_and_footer.len() < 4 {
    return Err(TableFrameError::InvalidTableFrame(
      "Content and footer too short".to_string(),
    ));
  }
  let (table_data, footer) = content_and_footer.split_at(content_and_footer.len() - 4);

  let stored_crc = u32::from_le_bytes(footer.try_into().unwrap());
  let computed_crc = crc32(table_data);

  if stored_crc != computed_crc {
    return Err(TableFrameError::InvalidCrc);
  }

  // Parse Table
  let table_bytes = &table_data;
  let (table, _): (Table, usize) =
    bincode_next::decode_from_slice(table_bytes, bincode_next::config::standard())
      .map_err(|e| TableFrameError::InvalidTableFrame(format!("Failed to decode table: {}", e)))?;

  Ok(table)
}

fn crc32(buf: &[u8]) -> u32 {
  crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, buf) as u32
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

    let packed = pack_table_frame(&table);
    let unpacked_table = unpack_table_frame(&packed, false).unwrap();

    assert_eq!(unpacked_table.column_count(), table.column_count());
    // Add more assertions as needed
  }

  #[test]
  fn test_invalid_magic() {
    let mut packed = pack_table_frame(&Table::default());
    packed[0] = 0x00; // Corrupt magic
    let err = unpack_table_frame(&packed, false).unwrap_err();
    match err {
      TableFrameError::InvalidTableFrame(_) => (),
      _ => panic!("Expected InvalidTableFrame"),
    }
  }

  #[test]
  fn test_crc_failure() {
    let mut packed = pack_table_frame(&Table::default());
    // corrupt a byte in the data section (which is after header)
    // HEADER is 8 bytes.
    let len = packed.len();
    packed[len - 5] ^= 0xFF; // Corrupt last byte of data (before CRC footer)

    let err = unpack_table_frame(&packed, false).unwrap_err();
    match err {
      TableFrameError::InvalidCrc => (),
      _ => panic!("Expected InvalidCrc, got {:?}", err),
    }
  }

  #[test]
  fn test_batch_frame_packer_and_lazy_chunk() {
    use crate::{D64, LazyChunk, decode_projected_series};

    let mut f0 = Field::new_with_data("ts", DataType::DateTime, Series::DateTime(vec![1000, 2000, 3000]));
    f0.add_metadata("primary_key".to_string(), Variant::Bool(true));
    let mut f1 = Field::new_with_data(
      "price",
      DataType::Decimal64,
      Series::Decimal64(vec![
        D64::from_f64(10.5, 2),
        D64::from_f64(11.2, 2),
        D64::from_f64(10.8, 2),
      ]),
    );
    f1.add_metadata("decimal".to_string(), Variant::Int32(2));
    let f2 = Field::new_with_data("vol", DataType::Int64, Series::Int64(vec![100, 200, 300]));

    let mut table = Table::from_columns(vec![f0, f1, f2]);
    table.set_table_meta("obj", Variant::String("000001.SZ".to_string())).unwrap();
    table.set_table_meta("table", Variant::String("stock_financial".to_string())).unwrap();

    // Test LazyChunk
    let raw = bincode_next::encode_to_vec(&table, bincode_next::config::standard()).unwrap();
    let mut lazy = LazyChunk::new(raw.clone().into_boxed_slice());
    assert_eq!(lazy.row_count().unwrap(), 3);
    assert_eq!(lazy.get_col(2).unwrap(), &table.columns()[2].data);
    assert_eq!(lazy.get_col(0).unwrap(), &table.columns()[0].data);
    assert_eq!(lazy.get_col(1).unwrap(), &table.columns()[1].data);

    // Test decode_projected_series
    let (rows, proj) = decode_projected_series(&raw, &[0, 2]).unwrap();
    assert_eq!(rows, 3);
    assert_eq!(proj.len(), 2);
    assert_eq!(&proj[0], &table.columns()[0].data);
    assert_eq!(&proj[1], &table.columns()[2].data);

    // Test BatchFramePacker matches unpack_table_frame
    let template = table.to_empty_projected(&[0, 1, 2], 0);
    let packer = BatchFramePacker::new(&template, "stock_financial");
    let cols: Vec<Series> = table.columns().iter().map(|c| c.data.clone()).collect();
    let mut frame = Vec::new();
    packer.pack_series_into(&cols, "000001.SZ", &mut frame);

    let unpacked = unpack_table_frame(&frame, false).unwrap();
    assert_eq!(unpacked.column_count(), 3);
    assert_eq!(unpacked.row_count(), 3);
    assert_eq!(unpacked.columns()[0].name, "ts");
    assert_eq!(unpacked.columns()[0].is_pk(), true);
    assert_eq!(unpacked.columns()[0].data, table.columns()[0].data);
    assert_eq!(unpacked.columns()[1].data, table.columns()[1].data);
    assert_eq!(unpacked.columns()[2].data, table.columns()[2].data);
    assert_eq!(
      unpacked.get_table_meta("obj"),
      Some(&Variant::String("000001.SZ".to_string()))
    );
    assert_eq!(
      unpacked.get_table_meta("table"),
      Some(&Variant::String("stock_financial".to_string()))
    );
  }
}
