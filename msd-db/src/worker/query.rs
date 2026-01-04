// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::ops::Deref;

use msd_table::{Table, VariantRef};
use tracing::{debug, span, trace};

use crate::{errors::DbError, request::QueryRequest, serde::DbBinary};
use msd_request::{DateRange, Key};

use super::{MsdStore, Worker};

impl<S: MsdStore> Worker<S> {
  /// Handle a query request and return the matching data as a Table.
  ///
  /// The query process:
  /// 1. Ensure cache is initialized (loads index and last chunk if needed)
  /// 2. Determine which chunks overlap with the query time range
  /// 3. Load and merge data from relevant chunks
  /// 4. Apply field filtering, time range filtering, ordering, and limit
  pub(super) fn handle_query(&mut self, req: QueryRequest) -> Result<Table, DbError> {
    debug!(?req, id = self.id, "Handling query request");
    let exist = self.ensure_cache_initialized(&req)?;
    if !exist {
      return Err(DbError::NotFound(req.deref().clone()));
    }

    let cache = self
      .cache
      .get(&req.key)
      .ok_or(DbError::NotFound(req.deref().clone()))?;

    let index = &cache.index;

    // setup condition with defaults
    let descending = !req.ascending.unwrap_or(true);
    let limit = req.limit.unwrap_or(usize::MAX);

    // Collect chunk seq from index that overlap with query range
    let (first_query_seq, last_query_seq) = index
      .iter()
      .enumerate()
      .filter_map(|(idx, item)| {
        if req.in_range(item.start) || req.in_range(item.end) {
          Some(idx)
        } else {
          None
        }
      })
      .fold((index.len(), 0), |(first, last), idx| {
        (first.min(idx), last.max(idx))
      });

    // Result table to accumulate query results
    let mut result = Table::default();

    // No overlapping chunks
    if first_query_seq > last_query_seq {
      return Ok(result);
    }

    let including_last = last_query_seq == index.len() - 1;

    // start from the last chunk so iteration will go through chunks in descending seq order
    let start_key = Key::new_data(
      &req.key.obj,
      if descending {
        last_query_seq
      } else {
        first_query_seq
      } as u32,
    );
    // include the separator after object name so prefix covers `obj.`
    let prefix_len = req.key.obj.len() + 1;

    // Holder for any error that occurs inside the closure (can't use `?` inside)
    let mut inner_err: Option<DbError> = None;

    // Base on data key design, chunk keys for the same object are stored contiguously and in reverse order
    let _scan_span = span!(tracing::Level::TRACE, "query_scan").entered();
    let pk_col = cache.cached.pk_column();

    let mut row_filter = RowFilter::new(limit, req.date_range, pk_col);

    if including_last && descending {
      let mut cached = cache.cached.clone();
      Self::filter_table_columns(&mut cached, &req);
      if result.column_count() == 0 {
        result = cached.to_empty();
      }
      result.extend_filtered(&cached, descending, |row| row_filter.apply(row))?;
    }

    self.store.prefix_with(
      start_key,
      Some(prefix_len),
      &req.key.table,
      !descending,
      |k: &[u8], v: &[u8]| {
        if row_filter.is_collected() {
          return false;
        }
        trace!(key=?k, "start");
        // parse key and get sequence
        let key = match Key::try_from(k) {
          Ok(k) => k,
          Err(e) => {
            trace!(key=?k, error=%e, "Failed to parse key in query");
            inner_err = Some(DbError::from(e));
            return false;
          }
        };
        if key.is_index() {
          // got less boundary
          return false;
        }
        let seq = key.get_seq() as usize;
        if seq < first_query_seq {
          trace!(%key, first_query_seq, last_query_seq, "Reached beyond needed chunks");
          // reached beyond needed chunks
          return false;
        }
        if seq == last_query_seq && seq == index.len() - 1 {
          trace!(%key, last_query_seq, "last chunk will use cache");
          return true;
        }

        let mut table: Table = match DbBinary::from_bytes(v) {
          Ok(t) => t,
          Err(e) => {
            trace!(key=%key, error=%e, "Failed to deserialize table in query");
            inner_err = Some(e);
            return true;
          }
        };
        Self::filter_table_columns(&mut table, &req);

        if result.column_count() == 0 {
          // first chunk being processed, initialize result table with its schema
          result = table.to_empty();
        }

        // collect rows from this chunk that match the time range
        trace!(%key, collected_rows = row_filter.collected_rows, limit, descending, "begin filtering rows in chunk");
        let status = result.extend_filtered(&table, descending, |row| row_filter.apply(row));
        if let Err(e) = status {
          trace!(%key, error=%e, "Failed to extend filtered rows in query");
          inner_err = Some(DbError::TableError(e));
        }
        trace!(%key, collected_rows = row_filter.collected_rows, limit, "finished filtering rows in chunk");
        true
      },
    )?;

    if including_last && !descending {
      let mut cached = cache.cached.clone();
      Self::filter_table_columns(&mut cached, &req);
      if result.column_count() == 0 {
        result = cached.to_empty();
      }
      result.extend_filtered(&cached, descending, |row| row_filter.apply(row))?;
    }

    if let Some(e) = inner_err {
      return Err(e);
    }
    result = result.replace_metadata([("obj", &req.key.obj), ("table", &req.key.table)]);
    debug!(id = self.id, rows = result.row_count(), "Query completed");
    Ok(result)
  }

  fn filter_table_columns(table: &mut Table, req: &QueryRequest) {
    match req.fields.as_ref() {
      Some(fields) => {
        table.retain_columns_by(|col| fields.iter().any(|f| f.eq_ignore_ascii_case(&col.name)));
      }
      None => {}
    }
  }
}

#[derive(Debug, Clone, Copy)]
struct RowFilter {
  limit: usize,
  data_range: DateRange,
  pk_col: usize,
  collected_rows: usize,
}

impl RowFilter {
  fn new(limit: usize, data_range: DateRange, pk_col: usize) -> Self {
    Self {
      limit,
      data_range,
      pk_col,
      collected_rows: 0,
    }
  }

  fn apply(&mut self, row: &Vec<VariantRef<'_>>) -> bool {
    if self.is_collected() {
      return false;
    }
    let ts = match row.get(self.pk_col).and_then(|v| v.get_datetime()) {
      Some(v) => *v,
      None => return false,
    };
    let collected = self.data_range.contains(ts);
    if collected {
      self.collected_rows += 1;
    }
    collected
  }

  fn is_collected(&self) -> bool {
    self.collected_rows >= self.limit
  }
}
