use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use msd_db::{DbBinary, MsdDb, db};
use msd_store::{MsdStore, RocksDbStore};
use msd_table::Table;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Options {
  /// Name of the table to operate on
  table: String,

  /// Optional name to operate on
  key: Option<String>,

  /// Sets a custom config file
  #[arg(short, long, value_name = "DB_PATH", env = "MSD_DB_PATH")]
  db_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
  dotenvy::dotenv_override().ok();

  let options = Options::parse();

  let db_path = PathBuf::from(&options.db_path);

  if !db_path.exists() || !db_path.is_dir() || !db_path.join("CURRENT").exists() {
    panic!("Database {} is invalid", options.db_path);
  }

  let store = RocksDbStore::new(&options.db_path)?;
  match options.table.as_str() {
    "__SCHEMA__" => {
      let key = options.key.as_ref().map(String::as_str).unwrap_or("");
      view_schema(&store, key).await?;
    }
    _ => {
      println!("Table: {}", options.table);
      println!("Key: {}", options.key.unwrap_or_default());
    }
  }

  Ok(())
}

async fn view_schema(store: &RocksDbStore, key: &str) -> Result<()> {
  store.prefix_with(key, None, "__SCHEMA__", false, |k, v| {
    println!("Key: {}", String::from_utf8_lossy(k));
    print!("Value: ");
    match Table::from_bytes(v) {
      Ok(t) => {
        serde_json::to_writer_pretty(std::io::stdout(), &t).unwrap();
      }
      Err(e) => {
        println!("Failed to decode value: {}", e);
      }
    };
    true
  })?;
  Ok(())
}
