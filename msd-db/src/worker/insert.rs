use super::{MsdStore, Worker};
use crate::errors::DbError;
use crate::request::InsertRequest;

impl<S: MsdStore> Worker<S> {
  pub(super) fn handle_insert(&mut self, req: InsertRequest) -> Result<(), DbError> {
    let exist = self.ensure_cache_initialized(&req.key)?;
    if !exist {
      return self.on_insert_new(req);
    }

    Ok(())
  }

  fn on_insert_new(&mut self, req: InsertRequest) -> Result<(), DbError> {
    let table = self
      .schema
      .get(&req.key.table)
      .map(|t| t.to_empty())
      .ok_or_else(|| DbError::TableNotFound(req.key.table.clone()))?;

    let table = req.data.to_table(&table)?;
  }
}
