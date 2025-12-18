use msd_request::SqlRequest;
use msd_table::now;
use serde::{Deserialize, Serialize};

const READ_ROLE: i64 = 1;
const WRITE_ROLE: i64 = 2;
const ADMIN_ROLE: i64 = 4;

#[derive(Serialize, Deserialize)]
pub struct Permission {
  /// expiration time in seconds
  exp: i64,
  /// bit mask of roles
  role: i64,
}

impl Permission {
  pub fn have_permission(&self, req: &SqlRequest) -> bool {
    let role = match req {
      SqlRequest::Query(_) => READ_ROLE,
      SqlRequest::CreateTable(_, _) => ADMIN_ROLE,
      SqlRequest::Insert(_) => WRITE_ROLE,
      SqlRequest::Delete(_) => ADMIN_ROLE,
      SqlRequest::Schema(_) => READ_ROLE,
    };
    self.check_permission(role)
  }

  pub fn have_read_permission(&self) -> bool {
    self.check_permission(READ_ROLE)
  }

  pub fn have_write_permission(&self) -> bool {
    self.check_permission(WRITE_ROLE) || self.check_permission(ADMIN_ROLE)
  }

  pub fn have_admin_permission(&self) -> bool {
    self.check_permission(ADMIN_ROLE)
  }

  fn check_permission(&self, want: i64) -> bool {
    let today = now() / 1_000_000;
    if today > self.exp {
      return false;
    }
    self.role & want > 0
  }
}
