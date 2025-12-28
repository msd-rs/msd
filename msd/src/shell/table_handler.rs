// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::{cell::RefCell, io::Write};

use anyhow::Result;
use colored::Colorize;
use msd_table::Table;

use crate::app_config::ShellOptions;

/// build table handler based on shell options
/// currently only PrintHandler is supported
pub fn build_table_handler(opts: &ShellOptions) -> impl TableHandler {
  PrintHandler::from(opts)
}

pub trait TableHandler {
  fn handle(&self, table: &Table) -> Result<()>;
}

#[derive(Debug)]
pub struct PrintHandler {
  print_rows: usize,
}

impl From<&ShellOptions> for PrintHandler {
  fn from(opts: &ShellOptions) -> Self {
    Self {
      print_rows: opts.reactive_rows,
    }
  }
}

impl TableHandler for PrintHandler {
  fn handle(&self, table: &Table) -> Result<()> {
    let rows = table_to_text(table, self.print_rows);
    let col_count = if rows.is_empty() { 0 } else { rows[0].len() };
    let mut col_widths = vec![0; col_count];

    for row in &rows {
      for (i, col) in row.iter().enumerate() {
        if i < col_widths.len() {
          col_widths[i] = col_widths[i].max(col.len()).min(30);
        }
      }
    }
    let sep = "|";
    for (i, row) in rows.iter().enumerate() {
      print!("{}", sep);
      for (col, width) in row.iter().zip(col_widths.iter()) {
        let col = match i {
          0 => col.color("blue").bold(),
          1 => col.color("green"),
          _ => col.white(),
        };
        print!("{:>width$}{}", col, sep);
      }
      print!("\n");
    }
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
/// first row: column names
/// second row: column types
/// other rows: column rows as strings
fn table_to_text(table: &Table, max_row: usize) -> Vec<Vec<String>> {
  let mut rows = vec![];

  let obj = table
    .get_table_meta("obj")
    .and_then(|v| v.get_str())
    .map(|s| s.to_string())
    .unwrap_or_default();

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
