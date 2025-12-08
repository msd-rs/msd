use std::{collections::HashSet, path::PathBuf};

use anyhow::Result;
use clap::Parser;
use msd_db::{DbBinary, index::IndexItem};
use msd_request::Key;
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
      println!("[");
      let key = options.key.as_ref().map(String::as_str).unwrap_or("");
      view_schema(&store, key).await?;
      println!("]");
    }
    _ => {
      println!("[");
      view_table(
        &store,
        options.table.as_str(),
        options.key.unwrap_or_default().as_str(),
      )
      .await?;
      println!("]");
    }
  }

  Ok(())
}

async fn view_schema(store: &RocksDbStore, key: &str) -> Result<()> {
  println!("{{");
  store.prefix_with(key, None, "__SCHEMA__", false, |k, v| {
    println!("\"Key\": \"{}\"", String::from_utf8_lossy(k));
    print!("\"Value\": ");
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
  println!("}}");
  Ok(())
}

async fn view_table(store: &RocksDbStore, table: &str, key: &str) -> Result<()> {
  let mut is_first = true;

  let mut objects = HashSet::new();

  store.prefix_with(key, None, table, false, |k, v| {
    let index_key = match Key::try_from(k) {
      Ok(k) => k,
      Err(e) => {
        println!("Failed to decode key {:?}: {}", k, e);
        return true;
      }
    };
    if index_key.is_index() {
      // skip index keys here, they will be handled together with data keys
      return true;
    }
    if is_first {
      is_first = false;
      println!();
    } else {
      println!(",");
    }
    if !index_key.is_index() {
      if !objects.contains(index_key.get_obj()) {
        println!("{{");
        objects.insert(index_key.get_obj().to_string());
        println!("\"Key\": \"{}\",", index_key.get_obj());
        println!("\"Kind\": \"Index\",");
        print!("\"Value\": ");
        match store.get(&Key::new_index(index_key.get_obj()), table) {
          Ok(Some(v)) => match Vec::<IndexItem>::from_bytes(&v) {
            Ok(v) => {
              serde_json::to_writer_pretty(std::io::stdout(), &v).unwrap();
              println!();
            }
            Err(e) => {
              println!("Failed to decode value: {}", e);
            }
          },
          Ok(None) => {
            println!("No index found for object {}", index_key.get_obj());
          }
          Err(e) => {
            println!(
              "Failed to get index for object {}: {}",
              index_key.get_obj(),
              e
            );
          }
        }
        println!("}},");
      }
      println!("{{");
      println!("\"Key\": \"{}\",", index_key);
      println!("\"Kind\": \"Data\",");
      print!("\"Value\": ");
      match Table::from_bytes(v) {
        Ok(t) => {
          serde_json::to_writer_pretty(std::io::stdout(), &t).unwrap();
          println!();
        }
        Err(e) => {
          println!("Failed to decode value: {}", e);
        }
      }
      print!("}}");
    };

    true
  })?;
  println!();
  Ok(())
}
