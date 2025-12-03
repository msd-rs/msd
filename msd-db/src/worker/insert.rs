use msd_table::DataType;

use super::{MsdStore, Worker};
use crate::errors::DbError;
use crate::index::IndexItem;
use crate::request::InsertRequest;
use crate::worker::agg_state::AggState;
use crate::worker::cache::CacheValue;

impl<S: MsdStore> Worker<S> {
  pub(super) fn handle_insert(&mut self, req: InsertRequest) -> Result<(), DbError> {
    let exist = self.ensure_cache_initialized(&req.key)?;
    if !exist {
      self.on_insert_new(req)
    } else {
      self.on_insert_existing(req)
    }
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
    let state = AggState::table_states(&cached);
    let cache = CacheValue {
      index,
      cached,
      state,
    };
    self.cache.insert(req.key.clone(), cache);
    Ok(())
  }

  fn on_insert_existing(&mut self, req: InsertRequest) -> Result<(), DbError> {
    /* TODO: merge insert data into existing cache and flush to storage
    1. order insert data by pk ascending
    2. dispose incoming rows that less than cached min pk with warning log
    3. merge insert data into cached table, rotating chunks as when up to chunk size
      - use `round_ts` to round the incoming pk when table have `round` meta
      - when rounded pk same as last pk in cached table, update each column value by it's agg state if any
      - when rotate chunk, save the old chunk to storage and create new empty chunk, update index accordingly and clear agg states
    4. update index accordingly
    5. flush updated index and new chunks to storage
    */

    todo!()
  }
}
