// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::ops::Deref;

use msd_table::{
  BatchFramePacker, LazyChunk, Series, Table, decode_projected_series, pack_table_frame_into,
};
use smol_str::SmolStr;
use tracing::debug;

use crate::{errors::DbError, index::IndexItem, request::QueryRequest, serde::DbBinary};
use msd_request::{DateRange, Key};

use super::{MsdStore, Worker};

impl<S: MsdStore> Worker<S> {
  /// Handle a query request and return the matching data as a Table.
  pub(super) fn handle_query(&mut self, req: &QueryRequest) -> Result<Table, DbError> {
    debug!(?req, id = self.id, "Handling query request");
    let exist = self.ensure_cache_initialized(req)?;
    let schema = self
      .schema
      .get(&req.key.table)
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;

    let proj_cols = Self::compute_projected_columns(schema, req);
    let pk_col = schema.pk_column();
    let extra_filter_cols = Self::resolve_extra_filters(schema, &req.ts_filters);

    if !exist {
      let mut table = schema.to_empty_projected(&proj_cols, 0);
      table.set_obj_table_meta(req.key.obj.as_str(), req.key.table.as_str());
      return Ok(table);
    }

    let limit_cap = req
      .limit
      .map(|n| n.unsigned_abs() as usize)
      .unwrap_or(64)
      .min(256);
    let mut result = schema.to_empty_projected(&proj_cols, limit_cap);
    let mut dst_cols: Vec<Series> = result
      .columns_mut()
      .iter_mut()
      .map(|f| std::mem::replace(&mut f.data, Series::Null))
      .collect();
    let mut matched_indices = Vec::new();

    let has_overlap = self.query_object_into_series(
      req,
      pk_col,
      &proj_cols,
      extra_filter_cols.as_deref(),
      &mut dst_cols,
      &mut matched_indices,
    )?;

    if !has_overlap {
      return Ok(Table::default());
    }

    for (col, data) in result.columns_mut().iter_mut().zip(dst_cols.into_iter()) {
      col.data = data;
    }
    result.set_obj_table_meta(req.key.obj.as_str(), req.key.table.as_str());

    debug!(id = self.id, rows = result.row_count(), "Query completed");
    Ok(result)
  }

  /// Handle a batch of object queries for the same table/query template and return packed table frames.
  pub(super) fn handle_batch_query_frame(
    &mut self,
    req: &mut QueryRequest,
    objects: Vec<SmolStr>,
  ) -> Result<Vec<u8>, DbError> {
    let schema = self
      .schema
      .get(&req.key.table)
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;

    let proj_cols = Self::compute_projected_columns(schema, req);
    let pk_col = schema.pk_column();
    let extra_filter_cols = Self::resolve_extra_filters(schema, &req.ts_filters);

    let limit_cap = req
      .limit
      .map(|n| n.unsigned_abs() as usize)
      .unwrap_or(64)
      .min(256);
    let template = schema.to_empty_projected(&proj_cols, limit_cap);
    let packer = BatchFramePacker::new(&template, req.key.table.as_str());

    let mut scratch_cols: Vec<Series> = template
      .columns()
      .iter()
      .map(|f| Series::with_capacity(f.kind, limit_cap))
      .collect();
    let mut matched_indices = Vec::new();
    let mut frame_buf = Vec::with_capacity(objects.len() * (proj_cols.len() * 64 + 64));

    for obj in objects {
      req.key.obj = obj;
      for col in &mut scratch_cols {
        col.clear();
      }
      let exist = self.ensure_cache_initialized(req)?;
      if !exist {
        packer.pack_series_into(&scratch_cols, req.key.obj.as_str(), &mut frame_buf);
        continue;
      }

      let has_overlap = self.query_object_into_series(
        req,
        pk_col,
        &proj_cols,
        extra_filter_cols.as_deref(),
        &mut scratch_cols,
        &mut matched_indices,
      )?;

      if !has_overlap {
        pack_table_frame_into(&Table::default(), &mut frame_buf);
      } else {
        packer.pack_series_into(&scratch_cols, req.key.obj.as_str(), &mut frame_buf);
      }
    }

    Ok(frame_buf)
  }

  fn query_object_into_series(
    &mut self,
    req: &QueryRequest,
    pk_col: usize,
    proj_cols: &[usize],
    extra_filter_cols: Option<&[(usize, DateRange)]>,
    dst_cols: &mut [Series],
    matched_indices: &mut Vec<usize>,
  ) -> Result<bool, DbError> {
    let cache = self
      .cache
      .get_mut(&req.key)
      .ok_or_else(|| DbError::NotFound(req.deref().clone()))?;

    let index_len = cache.index.len();
    if index_len == 0 {
      return Ok(false);
    }

    let limit = req
      .limit
      .map(|n| n.unsigned_abs() as usize)
      .unwrap_or(usize::MAX);
    let is_last_mode = req.limit.map(|n| n < 0).unwrap_or(false);
    let descending = if is_last_mode {
      req.ascending.unwrap_or(true)
    } else {
      !req.ascending.unwrap_or(true)
    };

    let (first_query_seq, last_query_seq) = if !req.date_range.is_set() {
      (0, index_len - 1)
    } else {
      cache
        .index
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
          if item.overlap(&req.date_range) {
            Some(idx)
          } else {
            None
          }
        })
        .fold((index_len, 0), |(first, last), idx| {
          (first.min(idx), last.max(idx))
        })
    };

    if first_query_seq > last_query_seq {
      return Ok(false);
    }

    // If any ts_filter references a non-existent column, 0 rows match
    let extra_cols = match extra_filter_cols {
      Some(cols) => cols,
      None => return Ok(true),
    };

    let including_last = last_query_seq == index_len - 1;
    let mut row_filter = RowFilter::new(limit, req.date_range, pk_col);

    if including_last && descending {
      let last_item = cache.index.get(index_len - 1);
      if let Some(ref mut lazy_last) = cache.lazy_last {
        collect_from_lazy_chunk(
          lazy_last,
          last_item,
          proj_cols,
          extra_cols,
          descending,
          &mut row_filter,
          dst_cols,
          matched_indices,
        )?;
      } else {
        collect_from_table(
          &cache.cached,
          last_item,
          proj_cols,
          extra_cols,
          descending,
          &mut row_filter,
          dst_cols,
          matched_indices,
        )?;
      }
    }

    let need_store_scan =
      !row_filter.is_collected() && (!including_last || first_query_seq < last_query_seq);

    if need_store_scan {
      if descending {
        let scan_start_seq = if including_last {
          last_query_seq.saturating_sub(1)
        } else {
          last_query_seq
        };
        for seq in (first_query_seq..=scan_start_seq).rev() {
          if row_filter.is_collected() {
            break;
          }
          if !cache.index[seq].overlap(&req.date_range) {
            continue;
          }
          if seq + 2 == index_len {
            if cache.lazy_prev.as_ref().map(|(s, _)| *s) != Some(seq as u32) {
              let data_key = Key::new_data(&req.key.obj, seq as u32);
              if let Some(bytes) = self.store.get(data_key, &req.key.table)? {
                cache.lazy_prev = Some((seq as u32, LazyChunk::new(bytes.into_boxed_slice())));
              }
            }
            if let Some((_, ref mut prev_chunk)) = cache.lazy_prev {
              collect_from_lazy_chunk(
                prev_chunk,
                cache.index.get(seq),
                proj_cols,
                extra_cols,
                descending,
                &mut row_filter,
                dst_cols,
                matched_indices,
              )?;
            }
          } else {
            let data_key = Key::new_data(&req.key.obj, seq as u32);
            let chunk_item = cache.index.get(seq);
            let mut scan_err: Option<DbError> = None;
            self.store.get_with(data_key, &req.key.table, |raw| {
              if let Err(e) = collect_from_raw_bytes(
                raw,
                chunk_item,
                proj_cols,
                extra_cols,
                descending,
                &mut row_filter,
                dst_cols,
                matched_indices,
              ) {
                scan_err = Some(e);
              }
            })?;
            if let Some(e) = scan_err {
              return Err(e);
            }
          }
        }
      } else {
        let scan_end_seq = if including_last {
          last_query_seq.saturating_sub(1)
        } else {
          last_query_seq
        };
        for seq in first_query_seq..=scan_end_seq {
          if row_filter.is_collected() {
            break;
          }
          if !cache.index[seq].overlap(&req.date_range) {
            continue;
          }
          if seq + 2 == index_len && cache.lazy_prev.as_ref().map(|(s, _)| *s) == Some(seq as u32) {
            if let Some((_, ref mut prev_chunk)) = cache.lazy_prev {
              collect_from_lazy_chunk(
                prev_chunk,
                cache.index.get(seq),
                proj_cols,
                extra_cols,
                descending,
                &mut row_filter,
                dst_cols,
                matched_indices,
              )?;
            }
          } else {
            let data_key = Key::new_data(&req.key.obj, seq as u32);
            let chunk_item = cache.index.get(seq);
            let mut scan_err: Option<DbError> = None;
            self.store.get_with(data_key, &req.key.table, |raw| {
              if let Err(e) = collect_from_raw_bytes(
                raw,
                chunk_item,
                proj_cols,
                extra_cols,
                descending,
                &mut row_filter,
                dst_cols,
                matched_indices,
              ) {
                scan_err = Some(e);
              }
            })?;
            if let Some(e) = scan_err {
              return Err(e);
            }
          }
        }
      }
    }

    if including_last && !descending && !row_filter.is_collected() {
      let last_item = cache.index.get(index_len - 1);
      if let Some(ref mut lazy_last) = cache.lazy_last {
        collect_from_lazy_chunk(
          lazy_last,
          last_item,
          proj_cols,
          extra_cols,
          descending,
          &mut row_filter,
          dst_cols,
          matched_indices,
        )?;
      } else {
        collect_from_table(
          &cache.cached,
          last_item,
          proj_cols,
          extra_cols,
          descending,
          &mut row_filter,
          dst_cols,
          matched_indices,
        )?;
      }
    }

    if is_last_mode && dst_cols.first().map_or(0, |s| s.len()) > 1 {
      for col in dst_cols.iter_mut() {
        col.reverse();
      }
    }

    Ok(true)
  }

  fn compute_projected_columns(schema: &Table, req: &QueryRequest) -> Vec<usize> {
    let pk_col = schema.pk_column();
    match req.fields.as_ref() {
      Some(fields) => schema
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
          if idx == pk_col || fields.iter().any(|f| f.eq_ignore_ascii_case(&col.name)) {
            Some(idx)
          } else {
            None
          }
        })
        .collect(),
      None => (0..schema.column_count()).collect(),
    }
  }

  fn resolve_extra_filters(
    schema: &Table,
    extra_filters: &[(SmolStr, DateRange)],
  ) -> Option<Vec<(usize, DateRange)>> {
    if extra_filters.is_empty() {
      return Some(Vec::new());
    }
    let mut out = Vec::with_capacity(extra_filters.len());
    for (col_name, range) in extra_filters {
      let idx = schema
        .columns()
        .iter()
        .position(|col| col.name.eq_ignore_ascii_case(col_name))?;
      out.push((idx, *range));
    }
    Some(out)
  }
}

#[inline]
fn is_chunk_fully_in_range(chunk_item: Option<&IndexItem>, range: &DateRange) -> bool {
  if !range.is_set() {
    return true;
  }
  match chunk_item {
    Some(item) if item.count > 0 => range.contains(item.start) && range.contains(item.end),
    _ => false,
  }
}

fn collect_from_lazy_chunk(
  chunk: &mut LazyChunk,
  chunk_item: Option<&IndexItem>,
  proj_cols: &[usize],
  extra_cols: &[(usize, DateRange)],
  descending: bool,
  row_filter: &mut RowFilter,
  dst_cols: &mut [Series],
  matched_indices: &mut Vec<usize>,
) -> Result<(), DbError> {
  let row_count = chunk.row_count()?;
  if row_count == 0 || row_filter.is_collected() {
    return Ok(());
  }

  if extra_cols.is_empty() && is_chunk_fully_in_range(chunk_item, &row_filter.data_range) {
    let rem = row_filter.limit - row_filter.collected_rows;
    let take = rem.min(row_count);
    let (start, end) = if descending {
      (row_count - take, row_count)
    } else {
      (0, take)
    };
    row_filter.collected_rows += take;
    for (dst, &col_idx) in dst_cols.iter_mut().zip(proj_cols.iter()) {
      let src = chunk.get_col(col_idx)?;
      dst.extend_range(src, start, end, descending)?;
    }
    return Ok(());
  }

  matched_indices.clear();
  if extra_cols.is_empty() {
    let pk_series = chunk.get_col(row_filter.pk_col)?;
    let Some(pk_slice) = pk_series.get_datetime() else {
      return Ok(());
    };
    if descending {
      for r in (0..row_count).rev() {
        if row_filter.is_collected() {
          break;
        }
        if row_filter.data_range.contains(pk_slice[r]) {
          row_filter.collected_rows += 1;
          matched_indices.push(r);
        }
      }
    } else {
      for r in 0..row_count {
        if row_filter.is_collected() {
          break;
        }
        if row_filter.data_range.contains(pk_slice[r]) {
          row_filter.collected_rows += 1;
          matched_indices.push(r);
        }
      }
    }
  } else {
    let _ = chunk.get_col(row_filter.pk_col)?;
    for &(col_idx, _) in extra_cols {
      let _ = chunk.get_col(col_idx)?;
    }
    if descending {
      for r in (0..row_count).rev() {
        if row_filter.is_collected() {
          break;
        }
        let pk_ok = chunk
          .get_col(row_filter.pk_col)?
          .get_datetime()
          .is_some_and(|s| row_filter.data_range.contains(s[r]));
        if !pk_ok {
          continue;
        }
        let mut extra_ok = true;
        for &(col_idx, ref range) in extra_cols {
          let ok = chunk
            .get_col(col_idx)?
            .get_datetime()
            .is_some_and(|s| range.contains(s[r]));
          if !ok {
            extra_ok = false;
            break;
          }
        }
        if extra_ok {
          row_filter.collected_rows += 1;
          matched_indices.push(r);
        }
      }
    } else {
      for r in 0..row_count {
        if row_filter.is_collected() {
          break;
        }
        let pk_ok = chunk
          .get_col(row_filter.pk_col)?
          .get_datetime()
          .is_some_and(|s| row_filter.data_range.contains(s[r]));
        if !pk_ok {
          continue;
        }
        let mut extra_ok = true;
        for &(col_idx, ref range) in extra_cols {
          let ok = chunk
            .get_col(col_idx)?
            .get_datetime()
            .is_some_and(|s| range.contains(s[r]));
          if !ok {
            extra_ok = false;
            break;
          }
        }
        if extra_ok {
          row_filter.collected_rows += 1;
          matched_indices.push(r);
        }
      }
    }
  }

  if !matched_indices.is_empty() {
    for (dst, &col_idx) in dst_cols.iter_mut().zip(proj_cols.iter()) {
      let src = chunk.get_col(col_idx)?;
      dst.extend_by_indices(src, matched_indices)?;
    }
  }
  Ok(())
}

fn collect_from_table(
  table: &Table,
  chunk_item: Option<&IndexItem>,
  proj_cols: &[usize],
  extra_cols: &[(usize, DateRange)],
  descending: bool,
  row_filter: &mut RowFilter,
  dst_cols: &mut [Series],
  matched_indices: &mut Vec<usize>,
) -> Result<(), DbError> {
  let row_count = table.row_count();
  if row_count == 0 || row_filter.is_collected() {
    return Ok(());
  }

  let src_cols = table.columns();
  if extra_cols.is_empty() && is_chunk_fully_in_range(chunk_item, &row_filter.data_range) {
    let rem = row_filter.limit - row_filter.collected_rows;
    let take = rem.min(row_count);
    let (start, end) = if descending {
      (row_count - take, row_count)
    } else {
      (0, take)
    };
    row_filter.collected_rows += take;
    for (dst, &col_idx) in dst_cols.iter_mut().zip(proj_cols.iter()) {
      if let Some(src) = src_cols.get(col_idx) {
        dst.extend_range(&src.data, start, end, descending)?;
      }
    }
    return Ok(());
  }

  let Some(pk_slice) = src_cols
    .get(row_filter.pk_col)
    .and_then(|c| c.data.get_datetime())
  else {
    return Ok(());
  };

  let mut extra_slices = Vec::with_capacity(extra_cols.len());
  for &(col_idx, ref range) in extra_cols {
    let Some(s) = src_cols.get(col_idx).and_then(|c| c.data.get_datetime()) else {
      return Ok(());
    };
    extra_slices.push((s, range));
  }

  matched_indices.clear();
  if descending {
    for r in (0..row_count).rev() {
      if row_filter.is_collected() {
        break;
      }
      if row_filter.data_range.contains(pk_slice[r])
        && extra_slices
          .iter()
          .all(|(s, range)| range.contains(s[r]))
      {
        row_filter.collected_rows += 1;
        matched_indices.push(r);
      }
    }
  } else {
    for r in 0..row_count {
      if row_filter.is_collected() {
        break;
      }
      if row_filter.data_range.contains(pk_slice[r])
        && extra_slices
          .iter()
          .all(|(s, range)| range.contains(s[r]))
      {
        row_filter.collected_rows += 1;
        matched_indices.push(r);
      }
    }
  }

  if !matched_indices.is_empty() {
    for (dst, &col_idx) in dst_cols.iter_mut().zip(proj_cols.iter()) {
      if let Some(src) = src_cols.get(col_idx) {
        dst.extend_by_indices(&src.data, matched_indices)?;
      }
    }
  }
  Ok(())
}

fn collect_from_raw_bytes(
  raw: &[u8],
  chunk_item: Option<&IndexItem>,
  proj_cols: &[usize],
  extra_cols: &[(usize, DateRange)],
  descending: bool,
  row_filter: &mut RowFilter,
  dst_cols: &mut [Series],
  matched_indices: &mut Vec<usize>,
) -> Result<(), DbError> {
  if row_filter.is_collected() {
    return Ok(());
  }

  if extra_cols.is_empty() {
    let (row_count, proj_series) = decode_projected_series(raw, proj_cols)?;
    if row_count == 0 {
      return Ok(());
    }
    if is_chunk_fully_in_range(chunk_item, &row_filter.data_range) {
      let rem = row_filter.limit - row_filter.collected_rows;
      let take = rem.min(row_count);
      let (start, end) = if descending {
        (row_count - take, row_count)
      } else {
        (0, take)
      };
      row_filter.collected_rows += take;
      for (dst, src) in dst_cols.iter_mut().zip(proj_series.iter()) {
        dst.extend_range(src, start, end, descending)?;
      }
      return Ok(());
    }

    let Some(pk_pos) = proj_cols.iter().position(|&c| c == row_filter.pk_col) else {
      return Ok(());
    };
    let Some(pk_slice) = proj_series[pk_pos].get_datetime() else {
      return Ok(());
    };
    matched_indices.clear();
    if descending {
      for r in (0..row_count).rev() {
        if row_filter.is_collected() {
          break;
        }
        if row_filter.data_range.contains(pk_slice[r]) {
          row_filter.collected_rows += 1;
          matched_indices.push(r);
        }
      }
    } else {
      for r in 0..row_count {
        if row_filter.is_collected() {
          break;
        }
        if row_filter.data_range.contains(pk_slice[r]) {
          row_filter.collected_rows += 1;
          matched_indices.push(r);
        }
      }
    }
    if !matched_indices.is_empty() {
      for (dst, src) in dst_cols.iter_mut().zip(proj_series.iter()) {
        dst.extend_by_indices(src, matched_indices)?;
      }
    }
    return Ok(());
  }

  let table: Table = DbBinary::from_bytes(raw)?;
  collect_from_table(
    &table,
    chunk_item,
    proj_cols,
    extra_cols,
    descending,
    row_filter,
    dst_cols,
    matched_indices,
  )
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

  #[inline]
  fn is_collected(&self) -> bool {
    self.collected_rows >= self.limit
  }
}
