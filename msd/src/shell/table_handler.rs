// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::{
  cell::{Cell, RefCell},
  fmt::Write as FmtWrite,
  io::Write,
};

use anyhow::Result;
use colored::Colorize;
use msd_table::Table;

use crate::app_config::ShellOptions;

/// build table handler based on shell options
/// currently only PrintHandler is supported
pub fn build_table_handler(opts: &ShellOptions) -> impl TableHandler {
  PrintHandler::from(opts)
}

pub fn build_schema_handler() -> impl TableHandler {
  SchemaHandler
}

pub fn build_list_tables_handler() -> impl TableHandler {
  ListTablesHandler
}

pub trait TableHandler {
  fn handle(&self, table: &Table) -> Result<()>;
}

#[derive(Debug)]
pub struct ListTablesHandler;

impl TableHandler for ListTablesHandler {
  fn handle(&self, table: &Table) -> Result<()> {
    if table.row_count() == 0 {
      return Ok(());
    }
    let Some(name_col) = table.column("name").or_else(|| table.column_by_index(0)) else {
      return Ok(());
    };

    let Some(schema_col) = table.column("schema").or_else(|| table.column_by_index(1)) else {
      return Ok(());
    };

    let mut rows = vec![vec![
      "no".to_string(),
      "name".to_string(),
      "kind".to_string(),
      "desc".to_string(),
    ]];
    for i in 0..table.row_count() {
      let name = name_col
        .data
        .get(i)
        .map(|v| v.to_string())
        .unwrap_or_default();

      let schema = schema_col
        .data
        .get(i)
        .and_then(|v| {
          v.get_str()
            .and_then(|s| serde_json::from_str::<Table>(s).ok())
        })
        .unwrap_or_default();

      let desc = schema
        .get_table_meta("desc")
        .map(|v| v.to_string())
        .unwrap_or_default();
      let kind = if schema.is_kv() {
        "kv".to_string()
      } else {
        "ts".to_string()
      };
      rows.push(vec![format!("{}", i + 1), name, kind, desc]);
    }

    let col_count = rows[0].len();
    let mut col_widths = vec![0; col_count];
    for row in &rows {
      for (i, col) in row.iter().enumerate() {
        col_widths[i] = col_widths[i].max(col.len()).min(40);
      }
    }

    let sep = "|";
    let mut out = String::with_capacity(rows.len() * col_count * 16);
    for (i, row) in rows.iter().enumerate() {
      out.push_str(sep);
      for (col, width) in row.iter().zip(col_widths.iter()) {
        let col = if i == 0 {
          col.color("blue").bold()
        } else {
          col.white()
        };
        let _ = write!(out, "{:>width$}{}", col, sep);
      }
      out.push('\n');
    }
    print!("{}", out);
    Ok(())
  }
}

#[derive(Debug)]
pub struct SchemaHandler;

impl TableHandler for SchemaHandler {
  fn handle(&self, table: &Table) -> Result<()> {
    let mut rows = vec![vec![
      "no".to_string(),
      "column".to_string(),
      "type".to_string(),
      "agg".to_string(),
      "desc".to_string(),
    ]];

    for (i, col) in table.columns().iter().enumerate() {
      let agg = col
        .get_metadata("agg")
        .and_then(|v| v.get_str())
        .unwrap_or_default()
        .to_string();
      let desc = col
        .get_metadata("desc")
        .and_then(|v| v.get_str())
        .unwrap_or_default()
        .to_string();
      rows.push(vec![
        format!("{}", i + 1),
        col.name.clone(),
        col.kind.to_string(),
        agg,
        desc,
      ]);
    }

    let col_count = rows[0].len();
    let mut col_widths = vec![0; col_count];
    for row in &rows {
      for (i, col) in row.iter().enumerate() {
        col_widths[i] = col_widths[i].max(col.len()).min(40);
      }
    }

    let sep = "|";
    let mut out = String::with_capacity(rows.len() * col_count * 16);
    for (i, row) in rows.iter().enumerate() {
      out.push_str(sep);
      for (col, width) in row.iter().zip(col_widths.iter()) {
        let col = if i == 0 {
          col.color("blue").bold()
        } else {
          col.white()
        };
        let _ = write!(out, "{:>width$}{}", col, sep);
      }
      out.push('\n');
    }
    print!("{}", out);
    Ok(())
  }
}

#[derive(Debug)]
pub struct PrintHandler {
  print_rows: usize,
  header_printed: Cell<bool>,
  col_widths: RefCell<Vec<usize>>,
}

impl From<&ShellOptions> for PrintHandler {
  fn from(opts: &ShellOptions) -> Self {
    Self {
      print_rows: opts.reactive_rows,
      header_printed: Cell::new(false),
      col_widths: RefCell::new(Vec::new()),
    }
  }
}

impl TableHandler for PrintHandler {
  fn handle(&self, table: &Table) -> Result<()> {
    if self.print_rows == 0 || table.row_count() == 0 {
      return Ok(());
    }
    let print_header = !self.header_printed.get();
    let rows = table_to_text(table, self.print_rows, print_header);
    self.header_printed.set(true);

    let col_count = if rows.is_empty() { 0 } else { rows[0].len() };
    let mut col_widths = self.col_widths.borrow_mut();
    if col_widths.len() < col_count {
      col_widths.resize(col_count, 0);
    }

    for row in &rows {
      for (i, col) in row.iter().enumerate() {
        if i < col_widths.len() {
          col_widths[i] = col_widths[i].max(col.len()).min(30);
        }
      }
    }
    let sep = "|";
    let mut out = String::with_capacity(rows.len() * col_count * 16);
    for (i, row) in rows.iter().enumerate() {
      out.push_str(sep);
      for (col, width) in row.iter().zip(col_widths.iter()) {
        let col = if print_header {
          match i {
            0 => col.color("blue").bold(),
            1 => col.color("green"),
            _ => col.white(),
          }
        } else {
          col.white()
        };
        let _ = write!(out, "{:>width$}{}", col, sep);
      }
      out.push('\n');
    }
    print!("{}", out);
    Ok(())
  }
}

pub struct CsvHandler {
  writer: RefCell<csv::Writer<Box<dyn Write>>>,
}

impl CsvHandler {
  pub fn new(writer: Box<dyn Write>) -> Self {
    let writer = csv::WriterBuilder::new()
      .has_headers(false)
      .from_writer(writer);
    Self {
      writer: RefCell::new(writer),
    }
  }
}

impl TableHandler for CsvHandler {
  fn handle(&self, table: &Table) -> Result<()> {
    let mut wtr = self.writer.borrow_mut();
    let obj = table
      .get_table_meta("obj")
      .and_then(|v| v.get_str())
      .map(|s| s.to_string())
      .unwrap_or_default();
    for row in table.rows(false) {
      let record = row
        .iter()
        .map(|v| v.to_string())
        .fold(vec![obj.clone()], |mut acc, value| {
          acc.push(value);
          acc
        });

      wtr.write_record(&record)?;
    }
    wtr.flush()?;
    Ok(())
  }
}

/// convert table to row based text
/// first row: column names (if print_header is true)
/// second row: column types (if print_header is true)
/// other rows: column rows as strings
fn table_to_text(table: &Table, max_row: usize, print_header: bool) -> Vec<Vec<String>> {
  let mut rows = vec![];

  let obj = table
    .get_table_meta("obj")
    .and_then(|v| v.get_str())
    .map(|s| s.to_string())
    .unwrap_or_default();

  if print_header {
    let row = table.columns().iter().map(|c| c.name.clone()).fold(
      vec!["no".into(), "obj".into()],
      |mut acc, name| {
        acc.push(name);
        acc
      },
    );
    rows.push(row);

    let row = table.columns().iter().map(|c| c.kind.to_string()).fold(
      vec!["int".into(), "string".into()],
      |mut acc, kind| {
        acc.push(kind);
        acc
      },
    );
    rows.push(row);
  }

  let top_rows = max_row / 2;
  let bottom_rows = table.row_count().saturating_sub(max_row - top_rows);

  for (i, row) in table.rows(false).enumerate() {
    if i < top_rows || i >= bottom_rows {
      rows.push(row.iter().map(|v| v.to_string()).fold(
        vec![format!("{}", i + 1), obj.clone()],
        |mut acc, value| {
          acc.push(value);
          acc
        },
      ));
    }
  }
  rows
}
