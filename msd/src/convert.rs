// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::{
  io::{BufRead, BufReader, BufWriter, Read, Write},
  path::Path,
};

use crate::app_config::ConvertOptions;
use anyhow::{Result, bail};
use msd_table::Table;

#[derive(Debug)]
enum Format {
  CSV,
  JSON,
  JSONL,
  TableFrame,
  TableFrameV2,
}

impl Format {
  fn from_file_ext<P: AsRef<Path>>(path: &P) -> Result<Self> {
    let ext = path.as_ref().extension().and_then(|s| s.to_str());
    match ext {
      Some("csv") | Some("txt") => Ok(Self::CSV),
      Some("json") => Ok(Self::JSON),
      Some("jsonl") => Ok(Self::JSONL),
      Some("msd") => Ok(Self::TableFrame),
      Some("msd2") => Ok(Self::TableFrameV2),
      _ => anyhow::bail!("Unknown format: {:?}", ext),
    }
  }

  fn from_file_content<P: AsRef<Path>>(path: &P) -> Result<Self> {
    let mut reader = std::fs::File::open(path)?;
    let mut magic = [0u8; 2];
    let mut version = [0u8; 2];
    reader.read_exact(&mut magic)?;
    reader.read_exact(&mut version)?;
    if u16::from_le_bytes(magic) == 0x4d7c {
      let ver = u16::from_le_bytes(version);
      match ver {
        0x0001 => return Ok(Format::TableFrame),
        0x0200 => return Ok(Format::TableFrameV2),
        _ => anyhow::bail!("Unknown version: {}", ver),
      }
    }
    if &magic[..] == b"{\n" {
      return Ok(Self::JSON);
    }
    if magic[0] == b'{' {
      return Ok(Self::JSONL);
    }

    anyhow::bail!("Unknown format")
  }

  fn from_file<P: AsRef<Path>>(path: &P) -> Result<Self> {
    if let Ok(format) = Self::from_file_ext(path) {
      return Ok(format);
    }
    Self::from_file_content(path)
  }
}

pub fn run(options: ConvertOptions) -> Result<()> {
  let input_format = Format::from_file(&options.source)?;
  let tables = read_tables(options.source, input_format)?;
  let output_format = Format::from_file_ext(&options.destination).unwrap_or(Format::CSV);
  match output_format {
    Format::CSV => write_csv(&options.destination, tables)?,
    Format::JSON => write_json(&options.destination, tables)?,
    Format::JSONL => write_jsonl(&options.destination, tables)?,
    Format::TableFrame => write_msd(&options.destination, tables)?,
    Format::TableFrameV2 => write_msd2(&options.destination, tables)?,
  }
  Ok(())
}

fn read_tables<P: AsRef<Path>>(path: P, format: Format) -> Result<Vec<Table>> {
  let mut tables = vec![];
  match format {
    Format::CSV => bail!("CSV format should not be used as input format"),
    Format::JSON => {
      let body = std::fs::read(&path)?;
      let table = serde_json::from_slice::<Table>(&body)?;
      tables.push(table);
    }
    Format::JSONL => {
      let fp = std::fs::File::open(&path)?;
      let buf = BufReader::new(fp);
      for line in buf.lines() {
        let line = line?;
        let table = serde_json::from_str::<Table>(&line)?;
        tables.push(table);
      }
    }
    Format::TableFrame => {
      let mut reader = std::fs::File::open(&path)?;
      let mut header = [0u8; 8];
      let mut data = Vec::new();
      while reader.read_exact(&mut header).is_ok() {
        let (_, data_size) = msd_table::check_table_frame(&header)?;
        data.resize(data_size, 0);
        reader.read_exact(&mut data)?;
        let table = msd_table::unpack_table_frame(&data, true)?;
        tables.push(table);
      }
    }
    Format::TableFrameV2 => {
      let mut reader = std::fs::File::open(&path)?;
      let mut header = [0u8; 8];
      let mut data = Vec::new();
      let mut offset = 0;
      while reader.read_exact(&mut header).is_ok() {
        let (_, data_size) = msd_table::check_table_frame_v2(&header)?;
        println!(
          "table at offset {} with size {}+8={}",
          offset,
          data_size,
          8 + data_size
        );
        offset += 8 + data_size;
        data.resize(data_size, 0);
        reader.read_exact(&mut data)?;
        let table = msd_table::unpack_table_frame_v2(&data, true)?;
        tables.push(table);
      }
    }
  }
  Ok(tables)
}

fn write_csv_file<P: AsRef<Path>>(path: P, table: &Table) -> Result<()> {
  let writer = std::fs::File::create(path)?;
  let obj = table
    .get_table_meta("obj")
    .map(|v| v.to_string())
    .unwrap_or(String::default());
  let mut writer = BufWriter::new(writer);
  let mut first_line = true;
  for row in table.rows(false) {
    if !first_line {
      writer.write(b"\n")?;
    }
    first_line = false;
    writer.write(obj.as_bytes())?;
    writer.write(b",")?;
    for col in row.iter() {
      writer.write(col.to_string().as_bytes())?;
      writer.write(b",")?;
    }
  }
  writer.flush()?;
  Ok(())
}

fn write_csv<P: AsRef<Path>>(path: P, tables: Vec<Table>) -> Result<()> {
  let path = path.as_ref();
  if path.is_dir() {
    for table in tables.iter() {
      let name = table
        .get_table_meta("table")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
      let file_name = path.join(format!("{}.csv", name));
      write_csv_file(file_name, table)?;
    }
    return Ok(());
  } else {
    let dir = path.parent();
    let file_name = path.file_name().unwrap();
    for table in tables.iter() {
      let name = table
        .get_table_meta("table")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
      let file_name = if let Some(dir) = dir {
        dir.join(format!("{}.{}", name, file_name.to_string_lossy()))
      } else {
        format!("{}.{}", name, file_name.to_string_lossy()).into()
      };
      write_csv_file(file_name, table)?;
    }
    return Ok(());
  }
}

fn write_json_file<P: AsRef<Path>>(path: P, table: &Table) -> Result<()> {
  let writer = std::fs::File::create(path)?;
  serde_json::to_writer_pretty(writer, table)?;
  Ok(())
}

fn write_json<P: AsRef<Path>>(path: P, tables: Vec<Table>) -> Result<()> {
  let path = path.as_ref();
  if path.is_dir() {
    for table in tables.iter() {
      let name = table
        .get_table_meta("table")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
      let file_name = path.join(format!("{}.json", name));
      write_json_file(file_name, table)?;
    }
    return Ok(());
  } else {
    let dir = path.parent();
    let file_name = path.file_name().unwrap();
    for table in tables.iter() {
      let name = table
        .get_table_meta("name")
        .map(|v| v.to_string())
        .unwrap_or_else(|| "unknown".to_string());
      let file_name = if let Some(dir) = dir {
        dir.join(format!("{}.{}", name, file_name.to_string_lossy()))
      } else {
        format!("{}.{}", name, file_name.to_string_lossy()).into()
      };
      write_json_file(file_name, table)?;
    }
    return Ok(());
  }
}

fn write_jsonl<P: AsRef<Path>>(path: P, tables: Vec<Table>) -> Result<()> {
  let mut writer = std::fs::File::create(path)?;
  let mut first = true;
  for table in tables.iter() {
    if !first {
      writer.write(b"\n")?;
    }
    first = false;
    serde_json::to_writer(&mut writer, table)?;
  }
  Ok(())
}

fn write_msd<P: AsRef<Path>>(path: P, tables: Vec<Table>) -> Result<()> {
  let mut writer = std::fs::File::create(path)?;

  for table in tables.iter() {
    let data = msd_table::pack_table_frame(table);
    writer.write(data.as_slice())?;
  }
  Ok(())
}

fn write_msd2<P: AsRef<Path>>(path: P, tables: Vec<Table>) -> Result<()> {
  let mut writer = std::fs::File::create(path)?;

  for table in tables.iter() {
    let data = msd_table::pack_table_frame_v2(table);
    writer.write(data.as_slice())?;
  }
  Ok(())
}
