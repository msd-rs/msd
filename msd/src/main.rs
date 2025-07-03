use msd_store::RocksDbStore;

fn main() {
  println!("Hello, world!");

  let db = RocksDbStore::new("/tmp/msd_db").unwrap();
}
