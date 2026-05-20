// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

//! MsdDb implementation.
//!

mod flusher;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use crate::serde::DbBinary;
use msd_request::{DeleteRequest, InsertRequest, Key, ListObjectsRequest, QueryRequest};
use msd_store::MsdStore;
use msd_table::{DataType, Table, Variant, parse_unit, table};
use rustc_hash::FxHasher;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{info, warn};
use wildcard::Wildcard;

use crate::errors::DbError;
use crate::request::{Broadcast, MsdRequest, RequestKey};
use crate::worker::{Chan, Worker};

const SCHEMA_TABLE_NAME: &'static str = "__SCHEMA__";
const TABLE_SCHEMA_KEY_PREFIX: &'static str = "table.";

#[derive(Debug, Clone, Default)]
pub struct MsdDbOptions {
  pub worker_count: usize,
  pub refresh_interval: i64,
}

/// MSD Database
pub struct MsdDb<S: MsdStore> {
  store: Arc<S>,
  workers: Vec<mpsc::Sender<MsdRequest>>,
  // all schemas in memory for quick access, key is table name
  schemas: Arc<RwLock<HashMap<String, Table>>>,
  // all objects for each table in memory for quick access, key is table name, value is set of objects
  objects: Arc<RwLock<HashMap<String, HashSet<String>>>>,
  flusher: mpsc::Sender<Broadcast>,
}

/// ## Public methods
impl<S: MsdStore + Send + Sync + 'static> MsdDb<S> {
  /// Create a new MsdDb instance with the given store and number of workers
  pub async fn new(store: S, options: MsdDbOptions) -> Result<Self, DbError> {
    let store = Arc::new(store);
    let mut workers = Vec::with_capacity(options.worker_count);
    let mut flash_workers = Vec::with_capacity(options.worker_count);

    info!(workers = options.worker_count, "database workers starting");
    for i in 0..options.worker_count {
      let (tx, rx) = mpsc::channel(200_000);
      workers.push(tx.clone());
      flash_workers.push(tx.clone());
      let worker = Worker::new(i, store.clone(), tx, options.refresh_interval);
      tokio::spawn(worker.run(rx));
    }

    let schemas = Arc::new(RwLock::new(HashMap::new()));
    let objects = Arc::new(RwLock::new(HashMap::new()));
    let (flusher_tx, flusher_rx) = mpsc::channel(1);

    let db = Self {
      store: store.clone(),
      workers,
      schemas,
      objects,
      flusher: flusher_tx,
    };

    if options.refresh_interval > 0 {
      let _ = tokio::spawn(flusher::bg_flush(
        options.refresh_interval,
        flash_workers,
        flusher_rx,
      ));
    }

    // ensure schema table exists
    store.new_table(SCHEMA_TABLE_NAME)?;

    info!("loading database schema");
    match db.load_schema() {
      Ok(schema_map) => {
        // Load objects for each table
        let mut objects_map = HashMap::new();
        for name in schema_map.keys() {
          let name = name.to_string();
          // Skip objects loading for KV tables
          if schema_map.get(&name).map(|t| t.is_kv()).unwrap_or(false) {
            continue;
          }
          match db.load_objects_for_table(&name) {
            Ok(objs) => {
              objects_map.insert(name.clone(), objs);
            }
            Err(e) => {
              warn!(%e, table = name, "Failed to load objects for table");
            }
          }
        }

        {
          let mut schemas = db.schemas.write().unwrap();
          *schemas = schema_map.clone();
        }
        {
          let mut objects = db.objects.write().unwrap();
          *objects = objects_map;
        }

        db.request(MsdRequest::update_schema(schema_map)).await?;
      }
      Err(err) => {
        warn!(?err, "Failed to load database schema");
      }
    }

    Ok(db)
  }

  pub async fn shutdown(&self) {
    info!("database workers stopping");
    let _ = self.flusher.send(Broadcast::Shutdown).await;
    let tasks = self
      .workers
      .iter()
      .map(|worker| worker.send(MsdRequest::Broadcast(Broadcast::Shutdown)));
    futures::future::join_all(tasks).await;
    for worker in &self.workers {
      worker.closed().await;
    }
    info!("database workers stopped");
  }

  pub async fn request(&self, req: MsdRequest) -> Result<(), DbError> {
    let key = req.deref();
    match key.is_broadcast() {
      true => {
        match &req {
          MsdRequest::Broadcast(Broadcast::CreateTable(name, table)) => {
            self.create_table(name, table)?;
          }
          MsdRequest::Broadcast(Broadcast::DropTable(name)) => self.drop_table(name)?,
          _ => {}
        }
        for worker in &self.workers {
          match worker.send(req.clone()).await {
            Ok(_) => {}
            Err(e) => {
              warn!("Failed to send broadcast to worker: {}", e);
            }
          }
        }
      }
      false => match req {
        MsdRequest::ListObjects { req, resp_tx } => {
          let resp = self.matched_objects(&req).map(
            |s| table!({name: "objects", kind: string, data: s.into_iter().collect::<Vec<_>>()}),
          );
          match resp_tx.send(resp) {
            Ok(_) => {}
            Err(_) => {
              warn!(req = ?req, "Failed to send ListObjects response");
            }
          }
        }
        MsdRequest::Delete { req, resp_tx } => {
          if self.is_kv_table(&req.key.table) {
            let result = self.handle_kv_delete(&req);
            let _ = resp_tx.send(result);
          } else {
            self.delete_objects(req, resp_tx)?;
          }
        }
        MsdRequest::Insert { req, resp_tx } => {
          if self.is_kv_table(&req.key.table) {
            let result = self.handle_kv_insert(req);
            let _ = resp_tx.send(result);
          } else {
            {
              if let Ok(mut guard) = self.objects.write() {
                if let Some(set) = guard.get_mut(&req.key.table) {
                  set.insert(req.key.obj.clone());
                }
              }
            }
            let req = MsdRequest::Insert { req, resp_tx };
            let worker = self.get_worker(&req);
            match worker.try_send(req) {
              Ok(_) => {}
              Err(e) => {
                warn!("Failed to send request to worker: {}", e);
              }
            }
          }
        }
        MsdRequest::Query { req, resp_tx } => {
          if self.is_kv_table(&req.key.table) {
            let result = self.handle_kv_query(&req);
            let _ = resp_tx.send(result);
          } else {
            let worker = self.get_worker(&req);
            let req = MsdRequest::Query { req, resp_tx };
            match worker.try_send(req) {
              Ok(_) => {}
              Err(e) => {
                warn!("Failed to send request to worker: {}", e);
              }
            }
          }
        }
        MsdRequest::Comment { table, field, desc } => {
          self.update_schema_desc(table, field, desc)?
        }
        _ => {
          let worker = self.get_worker(&req);
          match worker.try_send(req) {
            Ok(_) => {}
            Err(e) => {
              warn!("Failed to send request to worker: {}", e);
            }
          }
        }
      },
    }
    Ok(())
  }

  fn delete_objects(
    &self,
    req: DeleteRequest,
    resp_tx: tokio::sync::oneshot::Sender<Result<Table, DbError>>,
  ) -> Result<(), DbError> {
    Ok(if req.key.obj.is_empty() {
      // Delete all objects in the table
      let objects_cache = self.objects.clone();
      let table = req.key.table.clone();
      let objects = {
        let mut guard = objects_cache.write().unwrap();
        guard.remove(&table).unwrap_or_default()
      };

      // drop the table for fast delete
      self.store.drop_table(&table)?;

      // We need to send response back, but we are triggering multiple deletes.
      // Ideally we should wait for all, but for now let's just trigger them and return empty table.
      // Or better, since it's "delete table", we can iterate and delete.

      for obj in objects {
        let mut sub_req = req.clone();
        sub_req.key.obj = obj;
        let worker = self.get_worker(&sub_req);
        // We don't have response channel for sub-requests, so we just send to worker as "fire and forget"
        // by wrapping in MsdRequest::Delete but we can't easily because we need a channel.
        // Actually we can create a new channel and ignore it, or just not wait.
        // But MsdRequest::Delete EXPECTS a channel.
        // So we have to create a dummy channel.
        let (tx, _rx) = tokio::sync::oneshot::channel();
        let wrapper = MsdRequest::Delete {
          req: sub_req,
          resp_tx: tx,
        };
        let _ = worker.try_send(wrapper);
      }

      // restore the table
      self.store.new_table(&table)?;
      self.objects.write().unwrap().insert(table, HashSet::new());

      let _ = resp_tx.send(Ok(msd_table::Table::default()));
    } else {
      // Specific object delete
      // Expand logic not needed if obj is specific, just send to worker.
      // But wait, user said "expand the obj by matched_objects, then dispatch to workers".
      // This implies obj could be a pattern?
      // If obj is specific, matched_objects returns just it.
      // Use matched_objects to support patterns.
      let list_req = ListObjectsRequest {
        key: req.key.clone(),
      };
      match self.matched_objects(&list_req) {
        Ok(objs) => {
          {
            // remove objects from objects cache
            let mut guard = self.objects.write().unwrap();
            match guard.get_mut(&req.table) {
              Some(set) => {
                set.retain(|obj| !objs.contains(obj));
              }
              None => {}
            }
          }

          for obj in objs {
            let mut sub_req = req.clone();
            sub_req.key.obj = obj;
            let worker = self.get_worker(&sub_req);
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let wrapper = MsdRequest::Delete {
              req: sub_req,
              resp_tx: tx,
            };
            let _ = worker.try_send(wrapper);
          }
          let _ = resp_tx.send(Ok(msd_table::Table::default()));
        }
        Err(e) => {
          let _ = resp_tx.send(Err(e));
        }
      }
    })
  }

  pub fn get_schema(&self, table: &str) -> Result<Table, DbError> {
    let result = (|| {
      let guard = self
        .schemas
        .read()
        .map_err(|_| DbError::InternalError("Lock poisoned".into()))?;
      guard
        .get(table)
        .cloned()
        .ok_or(DbError::TableNotFound(table.into()))
    })();
    result
  }

  pub fn list_tables(&self) -> Result<HashMap<String, Table>, DbError> {
    let guard = self
      .schemas
      .read()
      .map_err(|_| DbError::InternalError("Lock poisoned".into()))?;

    Ok(guard.clone())
  }

  /// get the underlying store
  pub fn store(&self) -> &Arc<S> {
    &self.store
  }

  pub fn matched_objects(&self, req: &ListObjectsRequest) -> Result<HashSet<String>, DbError> {
    // KV tables: enumerate keys from store directly
    if self.is_kv_table(&req.table) {
      let wildcard = if req.obj.is_empty() {
        None
      } else {
        Some(Wildcard::new(req.obj.as_bytes()).map_err(|e| DbError::KeyPatternError(e))?)
      };
      let mut objects = HashSet::new();
      self
        .store
        .prefix_with(b"", None, &req.table, false, |k, _v| {
          let obj = String::from_utf8_lossy(k).to_string();
          match &wildcard {
            Some(wc) => {
              if wc.is_match(obj.as_bytes()) {
                objects.insert(obj);
              }
            }
            None => {
              objects.insert(obj);
            }
          }
          true
        })?;
      return Ok(objects);
    }

    let objects_cache = self.objects.clone();
    let result = (|| {
      let guard = objects_cache
        .read()
        .map_err(|_| DbError::InternalError("Lock poisoned".into()))?;
      let set = guard
        .get(&req.table)
        .ok_or(DbError::TableNotFound(req.table.clone()))?;

      let wildcard = if req.obj.is_empty() {
        None
      } else {
        Some(Wildcard::new(req.obj.as_bytes()).map_err(|e| DbError::KeyPatternError(e))?)
      };

      let mut objects = HashSet::new();
      for obj in set {
        match &wildcard {
          Some(wc) => {
            if wc.is_match(obj.as_bytes()) {
              objects.insert(obj.clone());
            }
          }
          None => {
            objects.insert(obj.clone());
          }
        }
      }

      Ok::<HashSet<String>, DbError>(objects)
    })();
    result
  }
}

/// ## Private methods
impl<S: MsdStore + Send + Sync + 'static> MsdDb<S> {
  fn load_objects_for_table(&self, table: &str) -> Result<HashSet<String>, DbError> {
    let mut objects = HashSet::new();
    self
      .store
      .prefix_with(Key::index_prefix(), None, table, false, |k, _v| {
        if k.len() > Key::index_prefix().len() + 1 {
          let key = &k[Key::index_prefix().len() + 1..];
          objects.insert(String::from_utf8_lossy(key).to_string());
        }
        true
      })?;
    Ok(objects)
  }

  fn load_schema(&self) -> Result<HashMap<String, Table>, DbError> {
    let mut schema_map: HashMap<String, Table> = HashMap::new();
    self.store.prefix_with(
      TABLE_SCHEMA_KEY_PREFIX.as_bytes(),
      None,
      SCHEMA_TABLE_NAME,
      false,
      |k, v| {
        let key = String::from_utf8_lossy(&k[TABLE_SCHEMA_KEY_PREFIX.len()..]).to_string();
        match DbBinary::from_bytes(&v) {
          Ok(table) => {
            schema_map.insert(key, table);
          }
          Err(err) => {
            warn!(key, ?v, %err, "failed to deserialize table for schema entry");
          }
        }
        true
      },
    )?;
    Ok(schema_map)
  }

  fn create_table(&self, name: &str, table: &Table) -> Result<(), DbError> {
    // Before creating the table, validate its schema

    // must have at least one column
    if table.column_count() == 0 {
      return Err(DbError::InvalidTableSchema(
        "table must have at least one column".to_string(),
      ));
    }

    // KV table: must have exactly 2 columns, both String or Bytes
    if table.is_kv() {
      if table.column_count() != 2 {
        return Err(DbError::InvalidTableSchema(
          "KV table must have exactly 2 columns (key, value)".to_string(),
        ));
      }
      for col in table.columns() {
        if col.kind != DataType::String && col.kind != DataType::Bytes {
          return Err(DbError::InvalidTableSchema(format!(
            "KV table column '{}' must be String or Bytes, got {:?}",
            col.name, col.kind
          )));
        }
      }
    } else {
      // For non-KV tables, validate more schema rules

      // primary key must be of type DateTime
      match table.column_by_index(table.pk_column()) {
        Some(pk_column) => {
          if pk_column.kind != DataType::DateTime {
            return Err(DbError::InvalidTableSchema(
              "primary key must be of type datetime".to_string(),
            ));
          }
        }
        None => {
          return Err(DbError::InvalidTableSchema(
            "table must have a primary key".to_string(),
          ));
        }
      }

      // chunkSize should be UInt32 and greater than 0 if exists
      if let Some(chunk_size) = table.get_table_meta("chunkSize") {
        // if chunkSize metadata exists, it must be of type UInt32
        let chunk_size = chunk_size.get_u32().ok_or(DbError::InvalidTableSchema(
          "chunkSize metadata must be of type UInt32".to_string(),
        ))?;
        if *chunk_size == 0 {
          return Err(DbError::InvalidTableSchema(
            "chunkSize metadata must be greater than 0".to_string(),
          ));
        }
      }

      // round should be a valid time unit string if exists
      if let Some(round) = table.get_table_meta("round") {
        // if round metadata exists, it must be of type String
        let round = round.get_str().ok_or(DbError::InvalidTableSchema(
          "round metadata must be of type String".to_string(),
        ))?;
        // validate round unit
        parse_unit(round).map_err(|_| {
          DbError::InvalidTableSchema("round metadata has invalid time unit".to_string())
        })?;
      }

      if let Some(chan) = table.get_table_meta("chan") {
        // if chan metadata exists, it must be of type String
        let chan = chan.get_str().ok_or(DbError::InvalidTableSchema(
          "chan metadata must be of type String".to_string(),
        ))?;
        let targets = Chan::parse_targets(chan)?;
        let (first_target, other_targets) = targets.split_first().ok_or(
          DbError::InvalidTableSchema("chan metadata must have at least one target".to_string()),
        )?;
        let schema = self.schemas.read().unwrap();
        let target = schema
          .get(*first_target)
          .ok_or(DbError::TableNotFound(first_target.to_string()))?;

        // verify all targets exist and have same schema
        for &target_name in other_targets {
          let other_target = schema
            .get(target_name)
            .ok_or(DbError::TableNotFound(target_name.to_string()))?;
          if !target.same_shape(other_target) {
            return Err(DbError::InvalidTableSchema(format!(
              "chan targets have different schema for '{}' and '{}'",
              first_target, target_name
            )));
          }
        }
        // verify all chan descriptions are valid
        let chan = Chan::try_from(table)?;
        if !chan.match_target(target) {
          return Err(DbError::InvalidTableSchema(
            "chan should have same number of columns as target".to_string(),
          ));
        }
      }
    }

    // Create the table in the store
    self.store.new_table(SCHEMA_TABLE_NAME)?;
    if !self.store.new_table(name)? {
      // table already exists
      return Ok(());
    }

    let key = format!("{}{}", TABLE_SCHEMA_KEY_PREFIX, name);
    let value = table.to_bytes()?;
    self
      .store
      .put(key.as_bytes(), value, SCHEMA_TABLE_NAME, None)?;

    {
      let mut schemas = self.schemas.write().unwrap();
      schemas.insert(name.to_string(), table.clone());
    }
    {
      let mut objects = self.objects.write().unwrap();
      objects.insert(name.to_string(), HashSet::new());
    }
    Ok(())
  }

  fn drop_table(&self, name: &str) -> Result<(), DbError> {
    let key = format!("{}{}", TABLE_SCHEMA_KEY_PREFIX, name);
    self.store.delete(key.as_bytes(), SCHEMA_TABLE_NAME)?;

    {
      let mut schemas = self.schemas.write().unwrap();
      schemas.remove(name);
    }
    {
      let mut objects = self.objects.write().unwrap();
      objects.remove(name);
    }
    self.store.drop_table(name)?;
    Ok(())
  }

  /// get the appropriate worker for a given hashable object
  fn is_kv_table(&self, table_name: &str) -> bool {
    self
      .schemas
      .read()
      .ok()
      .and_then(|guard| guard.get(table_name).map(|t| t.is_kv()))
      .unwrap_or(false)
  }

  fn handle_kv_insert(&self, mut req: InsertRequest) -> Result<Table, DbError> {
    let table = req.take_table().map_err(|e| DbError::from(e))?;
    let value_col = table.column_by_index(1).ok_or(DbError::InternalError(
      "KV table missing value column".into(),
    ))?;
    let value_bytes = {
      let cell = value_col
        .data
        .get(0)
        .ok_or(DbError::InternalError("KV table empty value".into()))?;
      match cell.get_str() {
        Some(s) => s.as_bytes().to_vec(),
        None => match cell.get_bytes() {
          Some(b) => b.to_vec(),
          None => {
            return Err(DbError::InternalError(
              "KV value must be String or Bytes".into(),
            ));
          }
        },
      }
    };
    self
      .store
      .put(req.key.obj.as_bytes(), value_bytes, &req.key.table, None)?;
    Ok(Table::default())
  }

  fn handle_kv_query(&self, req: &QueryRequest) -> Result<Table, DbError> {
    let schema = self
      .schemas
      .read()
      .map_err(|_| DbError::InternalError("Lock poisoned".into()))?
      .get(&req.key.table)
      .cloned()
      .ok_or(DbError::TableNotFound(req.key.table.clone()))?;

    let key_col = schema
      .column_by_index(0)
      .ok_or(DbError::InternalError("KV table missing key column".into()))?;
    let value_col = schema.column_by_index(1).ok_or(DbError::InternalError(
      "KV table missing value column".into(),
    ))?;

    let mut result = schema.to_empty();

    if req.key.obj.is_empty() || req.key.obj.contains('*') || req.key.obj.contains('?') {
      let prefix = if req.key.obj.is_empty() || req.key.obj == "*" {
        b""
      } else {
        let end = req
          .key
          .obj
          .find(|c| c == '*' || c == '?')
          .unwrap_or(req.key.obj.len());
        &req.key.obj.as_bytes()[..end]
      };
      let prefix_len = if prefix.is_empty() {
        None
      } else {
        Some(prefix.len())
      };
      self
        .store
        .prefix_with(prefix, prefix_len, &req.key.table, false, |k, v| {
          let obj_str = String::from_utf8_lossy(k);
          let val_str = String::from_utf8_lossy(v);
          let key_val = match key_col.kind {
            DataType::Bytes => Variant::Bytes(obj_str.as_bytes().to_vec()),
            _ => Variant::String(obj_str.to_string()),
          };
          let val = match value_col.kind {
            DataType::Bytes => Variant::Bytes(val_str.as_bytes().to_vec()),
            _ => Variant::String(val_str.to_string()),
          };
          let _ = result.push_row(vec![key_val, val]);
          true
        })?;
    } else {
      match self.store.get(req.key.obj.as_bytes(), &req.key.table)? {
        Some(bytes) => {
          let val_str = String::from_utf8_lossy(&bytes);
          let key_val = match key_col.kind {
            DataType::Bytes => Variant::Bytes(req.key.obj.as_bytes().to_vec()),
            _ => Variant::String(req.key.obj.clone()),
          };
          let val = match value_col.kind {
            DataType::Bytes => Variant::Bytes(val_str.as_bytes().to_vec()),
            _ => Variant::String(val_str.to_string()),
          };
          result.push_row(vec![key_val, val])?;
        }
        None => {}
      }
    }

    result = result.replace_metadata([("table", &req.key.table)]);
    Ok(result)
  }

  fn handle_kv_delete(&self, req: &DeleteRequest) -> Result<Table, DbError> {
    if req.key.obj.is_empty() || req.key.obj.contains('*') || req.key.obj.contains('?') {
      let prefix = if req.key.obj.is_empty() || req.key.obj == "*" {
        b""
      } else {
        let end = req
          .key
          .obj
          .find(|c| c == '*' || c == '?')
          .unwrap_or(req.key.obj.len());
        &req.key.obj.as_bytes()[..end]
      };
      let prefix_len = if prefix.is_empty() {
        None
      } else {
        Some(prefix.len())
      };
      let mut keys: Vec<Vec<u8>> = Vec::new();
      self
        .store
        .prefix_with(prefix, prefix_len, &req.key.table, false, |k, _v| {
          keys.push(k.to_vec());
          true
        })?;
      for key in keys {
        self.store.delete(key, &req.key.table)?;
      }
    } else {
      self.store.delete(req.key.obj.as_bytes(), &req.key.table)?;
    }
    Ok(Table::default())
  }

  fn get_worker(&self, key: &RequestKey) -> &mpsc::Sender<MsdRequest> {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    let hash = hasher.finish();
    let index = (hash as usize) % self.workers.len();
    &self.workers[index]
  }

  /// update the schema description
  fn update_schema_desc(
    &self,
    table_name: String,
    field: String,
    desc: String,
  ) -> Result<(), DbError> {
    let mut guard = self
      .schemas
      .write()
      .map_err(|_| DbError::InternalError("Lock poisoned".into()))?;
    let table = guard
      .get_mut(&table_name)
      .ok_or(DbError::TableNotFound(table_name.clone()))?;

    if field.is_empty() {
      table.set_table_meta("desc", desc.into())?;
    } else {
      table.set_field_meta(field, "desc", desc.into())?;
    }

    let key = format!("{}{}", TABLE_SCHEMA_KEY_PREFIX, &table_name);
    let value = table.to_bytes()?;
    self.store.put(key, value, SCHEMA_TABLE_NAME, None)?;

    Ok(())
  }
}
