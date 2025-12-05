use msd_table::{DataType, Variant, round_ts};
use tracing::warn;

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
        let start = t
          .cell(0, pk_col)
          .get_datetime()
          .copied()
          .unwrap_or_default();
        let end = t
          .cell(t.row_count() - 1, pk_col)
          .get_datetime()
          .copied()
          .unwrap_or_default();
        assert!(
          start <= end,
          "Invalid primary key range start: {}, end: {}",
          start,
          end
        );
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
    // Get schema for this table
    let schema = self
      .schema
      .get(&req.key.table)
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;
    let pk_col = schema.pk_column();

    // Convert insert data to table and sort by pk ascending
    let mut incoming = req.data.to_table(schema)?;
    incoming.sort_by_pk(false);

    // Get round unit from table metadata
    let round_unit = schema
      .get_table_meta("round")
      .and_then(|v| v.get_str())
      .map(|s| s.to_string());

    // Get chunk size from metadata
    let chunk_size = schema
      .get_table_meta("chunkSize")
      .and_then(|v| v.cast(DataType::UInt32).and_then(|v| v.get_u32().copied()))
      .unwrap_or(200) as usize;

    // Get cache for this key
    let cache = self
      .cache
      .get_mut(&req.key)
      .ok_or_else(|| DbError::CacheNotFound(req.key.clone()))?;

    // Get the min pk from cached table (first row's pk)
    let cached_min_pk = if cache.cached.row_count() > 0 {
      cache.cached.cell(0, pk_col).get_u64().copied().unwrap_or(0)
    } else {
      0
    };

    // Track new chunks to flush
    let mut new_chunks: Vec<(u32, msd_table::Table)> = Vec::new();

    // Process each incoming row
    for row in incoming.rows(false) {
      // Get the incoming pk and optionally round it
      let raw_pk = row[pk_col].get_datetime().copied().unwrap_or(0);
      let pk = if let Some(ref unit) = round_unit {
        round_ts(raw_pk, unit).unwrap_or(raw_pk)
      } else {
        raw_pk
      };

      // Skip rows with pk less than cached min pk
      if pk < cached_min_pk as i64 {
        warn!(
          key = ?req.key,
          incoming_pk = pk,
          cached_min_pk = cached_min_pk,
          "Skipping row with pk older than cached min"
        );
        continue;
      }

      // Get the last pk in cached table
      let cached_row_count = cache.cached.row_count();
      let last_cached_pk = if cached_row_count > 0 {
        cache
          .cached
          .cell(cached_row_count - 1, pk_col)
          .get_datetime()
          .copied()
          .unwrap_or(0)
      } else {
        0
      };

      if pk == last_cached_pk && cached_row_count > 0 {
        // Update existing row using agg states
        let last_row_idx = cached_row_count - 1;
        for (col_idx, cell_ref) in row.iter().enumerate() {
          if col_idx == pk_col {
            continue; // Skip pk column
          }
          let cell_value: Variant = cell_ref.clone().into();

          // Update agg state and set the aggregated value
          if let Some(Some(agg_state)) = cache.state.get_mut(col_idx) {
            agg_state.update(&cell_value);
            let agg_value = agg_state.get();
            if let Some(cell_mut) = cache.cached.get_cell_mut(last_row_idx, col_idx) {
              let _ = cell_mut.set(agg_value);
            }
          } else {
            // No agg state, just overwrite with the new value
            if let Some(cell_mut) = cache.cached.get_cell_mut(last_row_idx, col_idx) {
              let _ = cell_mut.set(cell_value);
            }
          }
        }
      } else {
        // Append new row
        // First check if we need to rotate chunk
        if cache.cached.row_count() >= chunk_size {
          // Rotate chunk: save current chunk and create new empty one
          let seq = cache.index.len() as u32;
          let old_chunk = std::mem::replace(&mut cache.cached, schema.to_empty());

          // Update index for the old chunk
          let old_start = old_chunk
            .cell(0, pk_col)
            .get_datetime()
            .copied()
            .unwrap_or(0);
          let old_end = old_chunk
            .cell(old_chunk.row_count() - 1, pk_col)
            .get_datetime()
            .copied()
            .unwrap_or(0);
          cache.index.push(IndexItem {
            start: old_start,
            end: old_end,
            count: old_chunk.row_count() as u64,
          });

          // Queue chunk for flushing
          new_chunks.push((seq, old_chunk));

          // Reset agg states
          for state in cache.state.iter_mut() {
            if let Some(s) = state {
              s.reset();
            }
          }
        }

        // Build the row with rounded pk
        let mut new_row: Vec<Variant> = row.iter().map(|r| r.clone().into()).collect();
        new_row[pk_col] = Variant::DateTime(pk);

        // Update agg states for this new row
        for (col_idx, cell_value) in new_row.iter().enumerate() {
          if col_idx == pk_col {
            continue;
          }
          if let Some(Some(agg_state)) = cache.state.get_mut(col_idx) {
            agg_state.update(cell_value);
          }
        }

        cache.cached.push_row(new_row).map_err(DbError::from)?;
      }
    }

    // Flush all new chunks to storage
    for (seq, chunk) in new_chunks.iter() {
      self.flush_chunk(&req.key, chunk, *seq)?;
    }

    // Update the last index item based on current cached state
    // Note: The index for the current cached chunk is NOT stored yet
    // It will be stored when the chunk is rotated or flushed

    // Flush updated index to storage
    let cache = self.cache.get(&req.key).unwrap();
    self.flush_index(&req.key, &cache.index)?;

    Ok(())
  }
}
