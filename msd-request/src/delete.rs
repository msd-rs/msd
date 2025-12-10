use msd_table::Table;
use serde::{Deserialize, Serialize};

use crate::{DateRange, RequestKey};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
  #[serde(flatten)]
  pub key: RequestKey,
  pub date_range: DateRange,
}

pub type DeleteResponse = Table;
