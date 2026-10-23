// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::vec;

use msd_request::{InsertResponse, Key, RequestError, RequestKey};
use msd_table::{
  DataType, Table, Variant, VariantRef, get_local_offset, now_with_tz, parse_unit,
  round_ts_with_tz,
};
use tracing::{debug, trace, warn};

use super::{MsdStore, Worker};
use crate::errors::DbError;
use crate::index::IndexItem;
use crate::request::{InsertRequest, MsdRequest};
use crate::serde::DbBinary;
use crate::worker::agg_state::AggState;
use crate::worker::cache::CacheValue;
use crate::worker::chan::Chan;

impl<S: MsdStore> Worker<S> {
  pub(super) fn handle_insert(&mut self, req: InsertRequest) -> Result<InsertResponse, DbError> {
    if req.key.obj.is_empty() {
      return Err(DbError::RequestError(RequestError::InvalidRequest(
        "Object name is required".to_string(),
      )));
    }

    let key = req.key.clone();
    self.ensure_cache_initialized(&req.key)?;

    if req.truncate.unwrap_or(false) {
      match self.cache.remove(&req.key) {
        Some(item) => {
          Self::delete_cache_item(&self.store, &req.key, &item)?;
        }
        None => {}
      }
    }

    let (resp, chan_table) = self.on_insert_existing(req)?;

    match chan_table {
      Some(table) => {
        self.on_insert_chan(&key, table)?;
      }
      None => {}
    }
    Ok(resp)
  }

  fn on_insert_existing(
    &mut self,
    mut req: InsertRequest,
  ) -> Result<(InsertResponse, Option<Vec<Option<Table>>>), DbError> {
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

    let tz_offset = get_local_offset();

    // Get chunk size from metadata
    let chunk_size = schema
      .get_table_meta("chunkSize")
      .and_then(|v| v.cast(DataType::UInt32).and_then(|v| v.get_u32().copied()))
      .unwrap_or(200) as usize;

    // Get cache for this key
    let cache = self.cache.entry(req.key.clone()).or_insert(CacheValue {
      index: vec![IndexItem::default()],
      cached: schema.to_empty(),
      state: AggState::table_states(schema),
      chan: Chan::parse_from_table(schema),
      last_changed: 0,
    });

    // Get the min pk from cached table (first row's pk)
    let cached_last_pk = cache.last_pk();

    debug!(
      key = ?req.key,
      cached_last_pk,
      "Found existing cache for insert"
    );

    // Track new or modified historical chunks to flush
    let mut new_chunks: Vec<(u32, msd_table::Table)> = Vec::new();
    // Currently loaded historical chunk: (seq, table, is_dirty)
    let mut loaded_chunk: Option<(u32, msd_table::Table, bool)> = None;

    let mut chan_table = cache.chan.as_ref().map(|a| {
      a.iter()
        .map(|c| {
          c.table()
            .and_then(|name| self.schema.get(name))
            .map(|schema| schema.to_empty())
        })
        .collect::<Vec<_>>()
    });

    // Process each incoming row
    let mut incoming_iter = incoming.rows(false);
    while let Some(row) = incoming_iter.next_cached() {
      debug!(?row, "Processing incoming row");
      // Get the incoming pk and optionally round it
      let raw_pk = row[pk_col].get_datetime().copied().unwrap_or(0);
      let pk = round_ts_with_tz(raw_pk, &round_unit, tz_offset).unwrap_or(raw_pk);
      let cached_last_pk = cache.last_pk();
      let cached_row_count = cache.cached.row_count();

      // Handle historical rows with pk less than cached last pk
      if cached_row_count > 0 && pk < cached_last_pk {
        let cached_first_pk = cache.first_pk();
        if pk >= cached_first_pk {
          // Case 1: pk falls within the range of cache.cached
          if let Some(row_idx) = Self::find_pk_row(&cache.cached, pk_col, pk) {
            Self::overwrite_row(&mut cache.cached, row_idx, pk_col, row);
          } else {
            warn!(
              key = ?req.key,
              incoming_pk = pk,
              cached_last_pk,
              "Skipping row with non-existent pk in cache"
            );
          }
        } else {
          // Case 2: pk is older than cache.cached, search historical chunks in index
          let hist_index = &cache.index[..cache.index.len().saturating_sub(1)];
          let target_seq = hist_index
            .binary_search_by(|item| {
              if pk < item.start {
                std::cmp::Ordering::Greater
              } else if pk > item.end {
                std::cmp::Ordering::Less
              } else {
                std::cmp::Ordering::Equal
              }
            })
            .ok()
            .map(|idx| idx as u32);

          if let Some(seq) = target_seq {
            if let Some((loaded_seq, _, _)) = &loaded_chunk {
              if *loaded_seq != seq {
                if let Some((old_seq, old_table, true)) = loaded_chunk.take() {
                  new_chunks.push((old_seq, old_table));
                } else {
                  loaded_chunk = None;
                }
              }
            }

            if loaded_chunk.is_none() {
              let data_key = Key::new_data(&req.key.obj, seq);
              let data = self
                .store
                .get(data_key, &req.key.table)?
                .ok_or_else(|| DbError::ChunkMissing(req.key.clone(), seq))?;
              let chunk_table: Table = DbBinary::from_bytes(&data)?;
              loaded_chunk = Some((seq, chunk_table, false));
            }

            if let Some((_, chunk_table, dirty)) = loaded_chunk.as_mut() {
              if let Some(row_idx) = Self::find_pk_row(chunk_table, pk_col, pk) {
                Self::overwrite_row(chunk_table, row_idx, pk_col, row);
                *dirty = true;
              } else {
                warn!(
                  key = ?req.key,
                  incoming_pk = pk,
                  seq,
                  "Skipping row with non-existent pk in historical chunk"
                );
              }
            }
          } else {
            // Case 3: pk does not fall into any existing chunk
            warn!(
              key = ?req.key,
              incoming_pk = pk,
              cached_last_pk,
              "Skipping row with non-existent pk"
            );
          }
        }
        continue;
      }

      trace!(
        key = %req.key,
        raw_pk,
        pk,
        cached_last_pk,
        "Processing incoming row"
      );

      if pk == cached_last_pk && cached_row_count > 0 {
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
          // Add new index item
          cache.index.push(IndexItem::default());

          // Queue chunk for flushing
          new_chunks.push((seq, old_chunk));
        }
        // Reset agg states
        for state in cache.state.iter_mut() {
          if let Some(s) = state {
            s.reset();
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

        cache
          .chan
          .as_mut()
          .zip(chan_table.as_mut())
          .map(|(chans, tables)| {
            chans
              .iter_mut()
              .zip(tables.iter_mut())
              .for_each(|(chan, table)| {
                if let Some(table) = table {
                  match table.push_row(chan.apply(&new_row)) {
                    Ok(_) => {}
                    Err(err) => {
                      debug!(
                        key = ?req.key,
                        %err,
                        "Failed to push row to chan table"
                      );
                    }
                  }
                }
              })
          });

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

    if let Some((seq, chunk, true)) = loaded_chunk.take() {
      new_chunks.push((seq, chunk));
    }

    // Flush all new or modified historical chunks to storage
    for (seq, chunk) in new_chunks.iter() {
      Self::flush_chunk(self.store.clone(), &req.key, chunk, *seq)?;
    }
    // Update last changed time, this will trigger flush in next flush interval
    cache.last_changed = now_with_tz(tz_offset);
    Ok((Table::default(), chan_table))
  }

  fn find_pk_row(table: &Table, pk_col: usize, pk: i64) -> Option<usize> {
    table
      .column_by_index(pk_col)
      .and_then(|col| col.data.get_datetime())
      .and_then(|ts| ts.binary_search(&pk).ok())
  }

  fn overwrite_row(table: &mut Table, row_idx: usize, pk_col: usize, row: &[VariantRef<'_>]) {
    for (col_idx, cell_value) in row.iter().enumerate() {
      if col_idx == pk_col || cell_value.is_empty() {
        continue;
      }
      if let Some(cell_mut) = table.get_cell_mut(row_idx, col_idx) {
        let _ = cell_mut.set(cell_value.to_variant());
      }
    }
  }

  fn on_insert_chan(
    &mut self,
    key: &RequestKey,
    chan_tables: Vec<Option<Table>>,
  ) -> Result<(), DbError> {
    if let Some(chans) = self.cache.get(key).and_then(|cache| cache.chan.as_ref()) {
      for (chan, chan_table) in chans.iter().zip(chan_tables.iter()) {
        if let Some(chan_table) = chan_table {
          for table in chan.tables() {
            let chan_request = InsertRequest {
              key: RequestKey {
                table: table.clone(),
                obj: key.obj.clone(),
              },
              data: msd_request::InsertData::Table(chan_table.clone()),
              truncate: None,
            };

            let (chan_request, _rx) = MsdRequest::insert(chan_request);

            match self.tx.send(chan_request) {
              Ok(_) => {
                debug!(from = %key, to = table.as_str(), "Sent chan insert request");
              }
              Err(e) => {
                warn!("Failed to send chan insert request: {}", e);
              }
            }
          }
        }
      }
    }

    // match self
    //   .cache
    //   .get(key)
    //   .and_then(|cache| cache.chan.as_ref())
    //   .map(|chan| chan.tables())
    // {
    //   Some(tables) => {
    //     for table in tables {
    //       let chan_request = InsertRequest {
    //         key: RequestKey {
    //           table: table.clone(),
    //           obj: key.obj.clone(),
    //         },
    //         data: msd_request::InsertData::Table(chan_table.clone()),
    //       };

    //       let (chan_request, _rx) = MsdRequest::insert(chan_request);

    //       match self.tx.try_send(chan_request) {
    //         Ok(_) => {
    //           debug!(from=%key, to=table, "Sent chan insert request");
    //         }
    //         Err(e) => {
    //           warn!("Failed to send chan insert request: {}", e);
    //         }
    //       }
    //     }
    //   }
    //   None => {}
    // }
    Ok(())
  }
}
