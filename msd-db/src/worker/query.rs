use std::ops::Deref;

use msd_table::{Field, Table, Variant};

use crate::{errors::DbError, keys::Key, request::QueryRequest, serde::DbBinary};

use super::{MsdStore, Worker};

/// Name of the timestamp column in the result table.
const TS_COLUMN: &str = "ts";

impl<S: MsdStore> Worker<S> {
  /// Handle a query request and return the matching data as a Table.
  ///
  /// The query process:
  /// 1. Ensure cache is initialized (loads index and last chunk if needed)
  /// 2. Determine which chunks overlap with the query time range
  /// 3. Load and merge data from relevant chunks
  /// 4. Apply field filtering, time range filtering, ordering, and limit
  pub(crate) fn handle_query(&mut self, req: QueryRequest) -> Result<Table, DbError> {
    let exist = self.ensure_cache_initialized(&req)?;
    if !exist {
      return Err(DbError::NotFound(req.deref().clone()));
    }

    let cache = self.cache.get(&req.key).unwrap();
    let index = &cache.index;

    // Determine the query time range
    let query_start = req.start.map(|(ts, _)| ts).unwrap_or(0);
    let query_end = req.end.map(|(ts, _)| ts).unwrap_or(u64::MAX);
    let start_inclusive = req.start.map(|(_, inc)| inc).unwrap_or(true);
    let end_inclusive = req.end.map(|(_, inc)| inc).unwrap_or(true);

    // Find chunks that overlap with the query range
    // IndexItem: start (inclusive), end (exclusive)
    let mut chunk_indices: Vec<usize> = index
      .iter()
      .enumerate()
      .filter(|(_, item)| {
        // Chunk overlaps if: chunk.start < query_end && chunk.end > query_start
        item.start < query_end && item.end > query_start
      })
      .map(|(i, _)| i)
      .collect();

    if chunk_indices.is_empty() {
      // Return empty table with schema from cache
      return Ok(self.create_empty_result_table(&cache.cached, &req.fields));
    }

    // Sort chunk indices by time order (ascending for now, we'll reverse at the end if needed)
    chunk_indices.sort();

    let last_chunk_idx = index.len() - 1;

    // Collect all matching rows from chunks
    let mut result_rows: Vec<(u64, Vec<Variant>)> = Vec::new();

    for &chunk_idx in &chunk_indices {
      let chunk_table = if chunk_idx == last_chunk_idx {
        // Use cached table for the last chunk
        &cache.cached
      } else {
        // Load chunk from store
        let data_key = Key::new_data(&req.key.obj, chunk_idx as u32);
        let data = self
          .store
          .get(data_key, &req.key.table)?
          .ok_or(DbError::ChunkMissing(req.key.clone(), chunk_idx as u32))?;
        let table: Table = DbBinary::from_bytes(&data)?;
        // We need to process this table immediately since we can't store the reference
        self.collect_rows_from_chunk(
          &table,
          query_start,
          query_end,
          start_inclusive,
          end_inclusive,
          &mut result_rows,
        )?;
        continue;
      };

      self.collect_rows_from_chunk(
        chunk_table,
        query_start,
        query_end,
        start_inclusive,
        end_inclusive,
        &mut result_rows,
      )?;
    }

    // Sort by timestamp
    let ascending = req.ascending.unwrap_or(true);
    if ascending {
      result_rows.sort_by_key(|(ts, _)| *ts);
    } else {
      result_rows.sort_by_key(|(ts, _)| std::cmp::Reverse(*ts));
    }

    // Apply limit
    if let Some(limit) = req.limit {
      result_rows.truncate(limit);
    }

    // Build result table
    self.build_result_table(&cache.cached, &req.fields, result_rows)
  }

  /// Collect rows from a chunk that match the time range filter.
  fn collect_rows_from_chunk(
    &self,
    chunk: &Table,
    query_start: u64,
    query_end: u64,
    start_inclusive: bool,
    end_inclusive: bool,
    result_rows: &mut Vec<(u64, Vec<Variant>)>,
  ) -> Result<(), DbError> {
    // Find the timestamp column (first column is assumed to be timestamp)
    let ts_col = chunk.column_by_index(0).ok_or_else(|| {
      DbError::TableError(msd_table::TableError::IndexOutOfBounds(
        0,
        chunk.column_count(),
      ))
    })?;

    let ts_series = ts_col.data.get_uint64().ok_or_else(|| {
      DbError::TableError(msd_table::TableError::TypeMismatch(
        msd_table::DataType::UInt64,
        ts_col.data.data_type(),
      ))
    })?;

    for row_idx in 0..chunk.row_count() {
      let ts = ts_series[row_idx];

      // Apply time range filter
      let start_ok = if start_inclusive {
        ts >= query_start
      } else {
        ts > query_start
      };
      let end_ok = if end_inclusive {
        ts <= query_end
      } else {
        ts < query_end
      };

      if start_ok && end_ok {
        // Collect all column values for this row
        let row: Vec<Variant> = chunk
          .columns()
          .iter()
          .map(|col| {
            col
              .data
              .get(row_idx)
              .map(|v| v.to_variant())
              .unwrap_or(Variant::Null)
          })
          .collect();
        result_rows.push((ts, row));
      }
    }

    Ok(())
  }

  /// Create an empty result table with the appropriate schema.
  fn create_empty_result_table(&self, source: &Table, fields: &Option<Vec<String>>) -> Table {
    let columns: Vec<Field> = if let Some(field_names) = fields {
      source
        .columns()
        .iter()
        .filter(|col| col.schema.name == TS_COLUMN || field_names.contains(&col.schema.name))
        .map(|col| col.schema.clone())
        .collect()
    } else {
      source
        .columns()
        .iter()
        .map(|col| col.schema.clone())
        .collect()
    };
    Table::new(columns, 0)
  }

  /// Build the result table from collected rows.
  fn build_result_table(
    &self,
    source: &Table,
    fields: &Option<Vec<String>>,
    rows: Vec<(u64, Vec<Variant>)>,
  ) -> Result<Table, DbError> {
    if rows.is_empty() {
      return Ok(self.create_empty_result_table(source, fields));
    }

    // Determine which columns to include
    let column_indices: Vec<usize> = if let Some(field_names) = fields {
      source
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, col)| col.schema.name == TS_COLUMN || field_names.contains(&col.schema.name))
        .map(|(i, _)| i)
        .collect()
    } else {
      (0..source.column_count()).collect()
    };

    // Create result table
    let columns: Vec<Field> = column_indices
      .iter()
      .map(|&i| source.column_by_index(i).unwrap().schema.clone())
      .collect();
    let mut result = Table::new(columns, 0);

    // Add rows
    for (_, row) in rows {
      let filtered_row: Vec<Variant> = column_indices
        .iter()
        .map(|&i| row.get(i).cloned().unwrap_or(Variant::Null))
        .collect();
      result.push_row(filtered_row)?;
    }

    Ok(result)
  }
}
