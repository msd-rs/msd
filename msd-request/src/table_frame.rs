//! this module provides a simple table binary format,
//! the format is defined as follows:
//!
//! TABLE_FRAME := TABLE_HEADER + TABLE_DATA + TABLE_FOOTER
//! TABLE_HEADER := MAGIC + VERSION + FRAME_SIZE
//! MAGIC := 0x4d7c as u16
//! VERSION := 0x0001 as u16
//! FRAME_SIZE := sizeof(TABLE_DATA + TABLE_FOOTER) as u32
//! TABLE_DATA := OBJECT + binary of Table
//! OBJECT := STRING
//! STRING := sizeof(len) as u32 + len as u8
//! TABLE_FOOTER := CRC32 of TABLE_DATA

use crate::errors::TableFrameError;
use msd_table::Table;
use std::convert::TryInto;

const MAGIC: u16 = 0x4d7c;
const VERSION: u16 = 0x0001;

pub fn pack_table_frame(obj: &str, table: &Table) -> Vec<u8> {
  let mut table_data = Vec::new();

  // OBJECT := STRING
  // STRING := sizeof(len) as u32 + len as u8
  let obj_bytes = obj.as_bytes();
  table_data.extend_from_slice(&(obj_bytes.len() as u32).to_le_bytes());
  table_data.extend_from_slice(obj_bytes);

  // binary of Table
  let table_bytes = bincode::serde::encode_to_vec(table, bincode::config::standard())
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

pub fn unpack_table_frame(buf: &[u8]) -> Result<(String, Table), TableFrameError> {
  if buf.len() < 8 {
    return Err(TableFrameError::BufferTooSmall(8, buf.len()));
  }

  let magic = u16::from_le_bytes(buf[0..2].try_into().unwrap());
  if magic != MAGIC {
    return Err(TableFrameError::InvalidTableFrame);
  }

  let version = u16::from_le_bytes(buf[2..4].try_into().unwrap());
  if version != VERSION {
    return Err(TableFrameError::InvalidTableFrame);
  }

  let frame_size = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
  if buf.len() < 8 + frame_size {
    return Err(TableFrameError::BufferTooSmall(8 + frame_size, buf.len()));
  }

  // TABLE_DATA + FOOTER (CRC)
  let content_and_footer = &buf[8..8 + frame_size];
  if content_and_footer.len() < 4 {
    return Err(TableFrameError::InvalidTableFrame);
  }
  let (table_data, footer) = content_and_footer.split_at(content_and_footer.len() - 4);

  let stored_crc = u32::from_le_bytes(footer.try_into().unwrap());
  let computed_crc = crc32(table_data);

  if stored_crc != computed_crc {
    return Err(TableFrameError::InvalidCrc);
  }

  // Parse OBJECT
  if table_data.len() < 4 {
    return Err(TableFrameError::InvalidTableFrame);
  }
  let str_len = u32::from_le_bytes(table_data[0..4].try_into().unwrap()) as usize;
  if table_data.len() < 4 + str_len {
    return Err(TableFrameError::InvalidTableFrame);
  }
  let str_bytes = &table_data[4..4 + str_len];
  let obj_str =
    String::from_utf8(str_bytes.to_vec()).map_err(|_| TableFrameError::InvalidTableFrame)?;

  // Parse Table
  let table_bytes = &table_data[4 + str_len..];
  let (table, _): (Table, usize) =
    bincode::serde::decode_from_slice(table_bytes, bincode::config::standard())
      .map_err(|_| TableFrameError::InvalidTableFrame)?;

  Ok((obj_str, table))
}

fn crc32(buf: &[u8]) -> u32 {
  crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32IsoHdlc, buf) as u32
}

#[cfg(test)]
mod tests {
  use super::*;
  use msd_table::{DataType, Field};

  #[test]
  fn test_pack_unpack_roundtrip() {
    let obj_name = "test_object";
    let columns = vec![
      Field::new("id", DataType::Int64, 0),
      Field::new("name", DataType::String, 0),
    ];
    let table = Table::from_columns(columns);
    // Note: Empty table for now, or populate it if needed for deeper test

    let packed = pack_table_frame(obj_name, &table);
    let (unpacked_obj, unpacked_table) = unpack_table_frame(&packed).unwrap();

    assert_eq!(unpacked_obj, obj_name);
    assert_eq!(unpacked_table.column_count(), table.column_count());
    // Add more assertions as needed
  }

  #[test]
  fn test_invalid_magic() {
    let mut packed = pack_table_frame("obj", &Table::default());
    packed[0] = 0x00; // Corrupt magic
    let err = unpack_table_frame(&packed).unwrap_err();
    match err {
      TableFrameError::InvalidTableFrame => (),
      _ => panic!("Expected InvalidTableFrame"),
    }
  }

  #[test]
  fn test_crc_failure() {
    let mut packed = pack_table_frame("obj", &Table::default());
    // corrupt a byte in the data section (which is after header)
    // HEADER is 8 bytes.
    let len = packed.len();
    packed[len - 5] ^= 0xFF; // Corrupt last byte of data (before CRC footer)

    let err = unpack_table_frame(&packed).unwrap_err();
    match err {
      TableFrameError::InvalidCrc => (),
      _ => panic!("Expected InvalidCrc, got {:?}", err),
    }
  }
}
