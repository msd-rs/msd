use msd_table::Table;
use tokio::sync::oneshot;

use crate::errors::DBError;

pub enum Request {
  Insert {
    table: String,
    obj: String,
    data: Table,
    resp: oneshot::Sender<Result<(), DBError>>,
  },
  Query {
    table: String,
    obj: String,
    // TODO: Add time range
    resp: oneshot::Sender<Result<Table, DBError>>,
  },
}
