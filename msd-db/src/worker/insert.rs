use std::vec;

use msd_request::{InsertResponse, RequestError};
use msd_table::{DataType, Table, Variant, parse_unit, round_ts};
use tracing::{debug, warn};

use super::{MsdStore, Worker};
use crate::errors::DbError;
use crate::index::IndexItem;
use crate::request::InsertRequest;
use crate::worker::agg_state::AggState;
use crate::worker::cache::CacheValue;

impl<S: MsdStore> Worker<S> {
  pub(super) fn handle_insert(&mut self, req: InsertRequest) -> Result<InsertResponse, DbError> {
    if req.key.obj.is_empty() {
      return Err(DbError::RequestError(RequestError::InvalidRequest(
        "Object name is required".to_string(),
      )));
    }
    self.ensure_cache_initialized(&req.key)?;
    self.on_insert_existing(req)
  }

  fn on_insert_existing(&mut self, mut req: InsertRequest) -> Result<InsertResponse, DbError> {
    // Get schema for this table
    let schema = self
      .schema
      .get(&req.key.table)
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;
    let pk_col = schema.pk_column();

    // Convert insert data to table and sort by pk ascending
    let mut incoming = req.take_table()?;
    incoming.sort_by_pk(false);

    // Get round unit from table metadata
    let round_unit = schema
      .get_table_meta("round")
      .and_then(|v| v.get_str())
      .and_then(|s| parse_unit(s).ok())
      .unwrap_or((1, b's'));

    // Get chunk size from metadata
    let chunk_size = schema
      .get_table_meta("chunkSize")
      .and_then(|v| v.cast(DataType::UInt32).and_then(|v| v.get_u32().copied()))
      .unwrap_or(200) as usize;

    // Get cache for this key
    let cache = self.cache.entry(req.key.clone()).or_insert(CacheValue {
      index: vec![IndexItem::default()],
      cached: schema.to_empty(),
      state: AggState::table_states(&schema),
    });

    // Get the min pk from cached table (first row's pk)
    let cached_min_pk = if cache.cached.row_count() > 0 {
      cache
        .cached
        .cell(0, pk_col)
        .get_datetime()
        .copied()
        .unwrap_or(0)
    } else {
      0
    };

    debug!(
      key = ?req.key,
      cached_min_pk = cached_min_pk,
      "Found existing cache for insert"
    );

    // Track new chunks to flush
    let mut new_chunks: Vec<(u32, msd_table::Table)> = Vec::new();

    // Process each incoming row
    for row in incoming.rows(false) {
      // Get the incoming pk and optionally round it
      let raw_pk = row[pk_col].get_datetime().copied().unwrap_or(0);
      let pk = round_ts(raw_pk, &round_unit).unwrap_or(raw_pk);

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
      debug!(
        key = %req.key,
        raw_pk,
        pk,
        last_cached_pk,
        "Processing incoming row"
      );

      if pk == last_cached_pk && cached_row_count > 0 {
        // Update existing row using agg states
        let last_row_idx = cached_row_count - 1;
        for (col_idx, cell_value) in row.iter().enumerate() {
          if col_idx == pk_col {
            continue; // Skip pk column
          }

          // Skip empty cells
          if cell_value.is_empty() {
            continue;
          }

          let cell_value: Variant = cell_value.to_variant();

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
              let _ = cell_mut.set(cell_value.clone());
            }
          }
        }
      } else {
        // Append new row
        // First check if we need to rotate chunk
        if cache.cached.row_count() >= chunk_size {
          // Rotate chunk: save current chunk and create new empty one
          assert!(
            cache.index.len() > 0,
            "Cache index should have at least one item when rotating chunk"
          );
          let seq = (cache.index.len() - 1) as u32;
          let old_chunk = std::mem::replace(&mut cache.cached, schema.to_empty());

          // Queue chunk for flushing
          new_chunks.push((seq, old_chunk));
        }
        // Reset agg states
        for state in cache.state.iter_mut() {
          if let Some(s) = state {
            s.reset();
          }
        }
        // Add new index item
        cache.index.push(IndexItem::default());

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
        cache.index.last_mut().map(|item| {
          if item.count == 0 {
            item.start = pk;
          }
          item.count += 1;
          item.end = pk;
        });
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
    self.flush_chunk(&req.key, &cache.cached, (cache.index.len() - 1) as u32)?;

    Ok(Table::default())
  }
}
