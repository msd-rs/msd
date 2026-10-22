// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

mod read;
mod single;

use crate::app_config::ShellOptions;
use anyhow::Result;
use read::strip_compression_suffix;
use single::{import_csv, import_table};

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ImportFormat {
  Csv,
  Table,
  Unknown,
}

pub(crate) fn detect_import_format(file_path: &str) -> ImportFormat {
  let clean_path = file_path.split('?').next().unwrap_or(file_path);
  let clean_path = clean_path.split('#').next().unwrap_or(clean_path);
  let uncompressed_path = strip_compression_suffix(clean_path);

  if uncompressed_path.ends_with(".tbl") || file_path == "---" {
    ImportFormat::Table
  } else if uncompressed_path.ends_with(".csv")
    || uncompressed_path.ends_with(".txt")
    || file_path == "--"
    || file_path.ends_with(".gz")
    || file_path.ends_with(".zstd")
    || file_path.ends_with(".br")
  {
    ImportFormat::Csv
  } else {
    ImportFormat::Unknown
  }
}

pub async fn execute(
  opts: &ShellOptions,
  table: &str,
  file_path: &str,
  skip: usize,
  delimiter: u8,
) -> Result<()> {
  match detect_import_format(file_path) {
    ImportFormat::Table => import_table(opts, table, file_path, skip, delimiter).await,
    ImportFormat::Csv => import_csv(opts, table, file_path, skip, delimiter).await,
    ImportFormat::Unknown => Ok(()),
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_detect_import_format() {
    assert_eq!(detect_import_format("data.csv.gz"), ImportFormat::Csv);
    assert_eq!(detect_import_format("data.txt.zstd"), ImportFormat::Csv);
    assert_eq!(detect_import_format("data.tbl.br"), ImportFormat::Table);
    assert_eq!(detect_import_format("data.csv"), ImportFormat::Csv);
    assert_eq!(detect_import_format("data.tbl"), ImportFormat::Table);
    assert_eq!(detect_import_format("--"), ImportFormat::Csv);
    assert_eq!(detect_import_format("---"), ImportFormat::Table);
    assert_eq!(detect_import_format("data.gz"), ImportFormat::Csv);
    assert_eq!(
      detect_import_format("http://example.com/test.csv.gz?auth=1"),
      ImportFormat::Csv
    );
    assert_eq!(
      detect_import_format("https://example.com/test.tbl.zstd#section"),
      ImportFormat::Table
    );
    assert_eq!(detect_import_format("random.bin"), ImportFormat::Unknown);
  }
}
