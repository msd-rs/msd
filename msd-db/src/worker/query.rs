// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::ops::Deref;

use msd_table::Table;
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
  pub(super) fn handle_query(&mut self, req: &QueryRequest) -> Result<Table, DbError> {
    debug!(?req, id = self.id, "Handling query request");
    let exist = self.ensure_cache_initialized(&req)?;
    if !exist {
      return self.empty_table(&req);
    }

    let cache = self
      .cache
      .get(&req.key)
      .ok_or(DbError::NotFound(req.deref().clone()))?;

    let index = &cache.index;

    // setup condition with defaults
    let limit = req
      .limit
      .map(|n| if n >= 0 { n as usize } else { -n as usize })
      .unwrap_or(usize::MAX);
    let is_last_mode = req.limit.map(|n| n < 0).unwrap_or(false);
    let descending = if is_last_mode {
      !(!req.ascending.unwrap_or(true))
    } else {
      !req.ascending.unwrap_or(true)
    };

    // Collect chunk seq from index that overlap with query range
    let (first_query_seq, last_query_seq) = index
      .iter()
      .enumerate()
      .filter_map(|(idx, item)| {
        if item.overlap(&req.date_range) {
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

    // include the separator after object name so prefix covers `obj.`
    let prefix_len = req.key.obj.len() + 1;

    // Holder for any error that occurs inside the closure (can't use `?` inside)
    let mut inner_err: Option<DbError> = None;

    // Base on data key design, chunk keys for the same object are stored contiguously and in reverse order
    let _scan_span = span!(tracing::Level::TRACE, "query_scan").entered();
    let pk_col = cache.cached.pk_column();

    let mut row_filter = RowFilter::new(limit, req.date_range, pk_col);

    if including_last && descending {
      if result.column_count() == 0 {
        result = cache.cached.to_empty();
      }
      Self::collect_rows_from_table(&mut result, &cache.cached, descending, &mut row_filter)?;
    }

    let need_store_scan =
      !row_filter.is_collected() && (!including_last || first_query_seq < last_query_seq);

    if need_store_scan {
      let scan_start_seq = if descending && including_last {
        last_query_seq.saturating_sub(1)
      } else if descending {
        last_query_seq
      } else {
        first_query_seq
      };
      let start_key = Key::new_data(&req.key.obj, scan_start_seq as u32);

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

          let table: Table = match DbBinary::from_bytes(v) {
            Ok(t) => t,
            Err(e) => {
              trace!(key=%key, error=%e, "Failed to deserialize table in query");
              inner_err = Some(e);
              return true;
            }
          };

          if result.column_count() == 0 {
            // first chunk being processed, initialize result table with its schema
            result = table.to_empty();
          }

          // collect rows from this chunk that match the time range
          trace!(%key, collected_rows = row_filter.collected_rows, limit, descending, "begin filtering rows in chunk");
          let status = Self::collect_rows_from_table(&mut result, &table, descending, &mut row_filter);
          if let Err(e) = status {
            trace!(%key, error=%e, "Failed to extend filtered rows in query");
            inner_err = Some(e);
          }
          trace!(%key, collected_rows = row_filter.collected_rows, limit, "finished filtering rows in chunk");
          !row_filter.is_collected()
        },
      )?;
    }

    if including_last && !descending && !row_filter.is_collected() {
      if result.column_count() == 0 {
        result = cache.cached.to_empty();
      }
      Self::collect_rows_from_table(&mut result, &cache.cached, descending, &mut row_filter)?;
    }

    if let Some(e) = inner_err {
      return Err(e);
    }
    Self::filter_table_columns(&mut result, &req);
    if is_last_mode && result.row_count() > 1 {
      result.sort_by_pk(!descending);
    }
    result = result.replace_metadata([
      ("obj", req.key.obj.as_str()),
      ("table", req.key.table.as_str()),
    ]);
    debug!(id = self.id, rows = result.row_count(), "Query completed");
    Ok(result)
  }

  fn collect_rows_from_table(
    result: &mut Table,
    source: &Table,
    descending: bool,
    row_filter: &mut RowFilter,
  ) -> Result<(), DbError> {
    let row_count = source.row_count();
    if row_count == 0 || row_filter.is_collected() {
      return Ok(());
    }
    let pk_col = row_filter.pk_col;
    let pk_series = match source.column_by_index(pk_col) {
      Some(col) => &col.data,
      None => return Ok(()),
    };

    let src_cols = source.columns();
    let dst_cols = result.columns_mut();

    if descending {
      for r in (0..row_count).rev() {
        if row_filter.is_collected() {
          break;
        }
        if let Some(ts) = pk_series.get(r).and_then(|v| v.get_datetime().copied()) {
          if row_filter.check_ts(ts) {
            for (dst, src) in dst_cols.iter_mut().zip(src_cols.iter()) {
              if let Some(cell) = src.data.get(r) {
                dst.data.push(cell.to_variant())?;
              }
            }
          }
        }
      }
    } else {
      for r in 0..row_count {
        if row_filter.is_collected() {
          break;
        }
        if let Some(ts) = pk_series.get(r).and_then(|v| v.get_datetime().copied()) {
          if row_filter.check_ts(ts) {
            for (dst, src) in dst_cols.iter_mut().zip(src_cols.iter()) {
              if let Some(cell) = src.data.get(r) {
                dst.data.push(cell.to_variant())?;
              }
            }
          }
        }
      }
    }
    Ok(())
  }

  fn filter_table_columns(table: &mut Table, req: &QueryRequest) {
    match req.fields.as_ref() {
      Some(fields) => {
        table.retain_columns_by(|col| fields.iter().any(|f| f.eq_ignore_ascii_case(&col.name)));
      }
      None => {}
    }
  }

  fn empty_table(&self, req: &QueryRequest) -> Result<Table, DbError> {
    let schema = self
      .schema
      .get(&req.key.table)
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;

    let mut table = schema.to_empty();
    Self::filter_table_columns(&mut table, req);
    Ok(table.replace_metadata([
      ("obj", req.key.obj.as_str()),
      ("table", req.key.table.as_str()),
    ]))
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

  fn check_ts(&mut self, ts: i64) -> bool {
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
