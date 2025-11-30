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

    // setup condition with defaults
    let descending = !req.ascending.unwrap_or(true);
    let limit = req.limit.unwrap_or(usize::MAX);

    // Collect chunk id from index that overlap with query range
    let (first_seq, last_seq) = index
      .iter()
      .enumerate()
      .filter_map(|(idx, item)| {
        if req.in_range(item.start) || req.in_range(item.end) {
          Some(idx)
        } else {
          None
        }
      })
      .fold((index.len(), 0), |(mut first, mut last), idx| {
        if idx < first {
          first = idx;
        }
        if idx > last {
          last = idx;
        }
        (first, last)
      });

    // Result table to accumulate query results
    let mut result = Table::default();

    // No overlapping chunks
    if first_seq > last_seq {
      return Ok(result);
    }

    // start from the last chunk so iteration will go through chunks in descending seq order
    let start_key = Key::new_data(&req.key.obj, last_seq as u32);
    // include the separator after object name so prefix covers `obj.`
    let prefix_len = req.key.obj.len() + 1;

    // Holder for any error that occurs inside the closure (can't use `?` inside)
    let mut inner_err: Option<DbError> = None;

    self.store.prefix_with(
      start_key,
      Some(prefix_len),
      &req.key.table,
      |k: &[u8], v: &[u8]| {
        // parse key and get sequence
        let key = match Key::try_from(k) {
          Ok(k) => k,
          Err(e) => {
            inner_err = Some(e);
            return false;
          }
        };
        let seq = key.get_seq() as usize;
        if seq < first_seq {
          // reached beyond needed chunks
          return false;
        }

        let table: Table = match DbBinary::from_bytes(v) {
          Ok(t) => t,
          Err(e) => {
            inner_err = Some(e);
            return true;
          }
        };

        if result.column_count() == 0 {
          result = table.to_empty();
        }

        // collect rows from this chunk that match the time range
        let pk_col = table.pk_column();
        let mut collected_rows = result.row_count();
        let status = result.extend_filtered(&table, descending, |row| {
          if collected_rows >= limit {
            return false;
          }
          let ts = match row.get(pk_col).and_then(|v| v.get_u64()) {
            Some(v) => *v,
            None => return false,
          };
          let collected = req.in_range(ts);
          if collected {
            collected_rows += 1;
          }
          collected
        });
        if let Err(e) = status {
          inner_err = Some(DbError::TableError(e));
        }
        true
      },
    )?;

    if let Some(e) = inner_err {
      return Err(e);
    }
    Ok(result)
  }
}
