use msd_table::DataType;

use super::{MsdStore, Worker};
use crate::errors::DbError;
use crate::index::IndexItem;
use crate::request::InsertRequest;
use crate::worker::cache::CacheValue;

impl<S: MsdStore> Worker<S> {
  pub(super) fn handle_insert(&mut self, req: InsertRequest) -> Result<(), DbError> {
    let exist = self.ensure_cache_initialized(&req.key)?;
    if !exist {
      return self.on_insert_new(req);
    }

    Ok(())
  }

  fn on_insert_new(&mut self, req: InsertRequest) -> Result<(), DbError> {
    // create empty table with schema
    let table = self
      .schema
      .get(&req.key.table)
      .map(|t| t.to_empty())
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;
    let pk_col = table.pk_column();

    // convert insert data to table, insert data should order by pk ascending
    let mut table = req.data.to_table(&table)?;
    table.sort_by_pk(false);

    // split table into chunks and build index
    let chunk_size = table
      .get_table_meta("chunkSize")
      .and_then(|v| v.cast(DataType::UInt32).and_then(|v| v.get_u32().copied()))
      .unwrap_or(200) as usize;
    let mut chunks = table.chunks(chunk_size);
    let index = chunks
      .iter()
      .map(|t| {
        let start = t.cell(0, pk_col).get_u64().copied().unwrap_or(0);
        let end = t
          .cell(t.row_count() - 1, pk_col)
          .get_u64()
          .copied()
          .unwrap_or(0);
        assert!(end != 0 && end >= start, "Invalid primary key range");
        IndexItem {
          start,
          end,
          count: t.row_count() as u64,
        }
      })
      .collect();

    // all required data persisted, update cache
    self.flush_index(&req.key, &index)?;
    for (seq, chunk) in chunks.iter().enumerate() {
      self.flush_chunk(&req.key, chunk, seq as u32)?;
    }
    let cached = chunks.pop().expect("chunk is empty");
    let cache = CacheValue {
      index,
      cached,
      state: Default::default(),
    };
    self.cache.insert(req.key.clone(), cache);
    Ok(())
  }
}
