use anyhow::Result;
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
          col_widths[i] = col_widths[i].max(col.len());
        }
      }
    }
    for row in rows {
      print!("|");
      for (col, width) in row.iter().zip(col_widths.iter()) {
        print!("{:>width$}|", col);
      }
      print!("\n");
    }
    Ok(())
  }
}

/// convert table to row based text
/// first row: column names
/// second row: column types
/// other rows: column rows as strings
fn table_to_text(table: &Table, max_row: usize) -> Vec<Vec<String>> {
  let mut rows = vec![];

  let mut row = table
    .columns()
    .iter()
    .map(|c| c.name.clone())
    .collect::<Vec<String>>();
  row.insert(0, "".into());
  rows.push(row);

  let mut row = table
    .columns()
    .iter()
    .map(|c| c.kind.to_string())
    .collect::<Vec<String>>();
  row.insert(0, "".into());
  rows.push(row);

  let top_rows = max_row / 2;
  let bottom_rows = table.row_count().saturating_sub(max_row - top_rows);

  for (i, row) in table.rows(false).enumerate() {
    if i < top_rows || i >= bottom_rows {
      let mut row = row.iter().map(|v| v.to_string()).collect::<Vec<String>>();
      row.insert(0, (i + 1).to_string());
      rows.push(row);
    }
  }
  rows
}
