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

  fn check_permission(&self, want: i64) -> bool {
    let today = now() / 1_000_000;
    if today > self.exp {
      return false;
    }
    self.role & want > 0
  }

  pub fn from_jwt(token: &str, secret: &str) -> anyhow::Result<Self> {
    let validation = jsonwebtoken::Validation::default();
    let key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
    let token_data = jsonwebtoken::decode::<Permission>(token, &key, &validation)?;
    Ok(token_data.claims)
  }

  pub fn check(
    headers: &axum::http::HeaderMap,
    request: &SqlRequest,
  ) -> Result<(), (axum::http::StatusCode, String)> {
    let auth_token = match &crate::app_config::app_config().command {
      crate::app_config::MsdCommands::Server(opts) => &opts.auth_token,
      _ => &None,
    };

    if let Some(auth_token) = auth_token {
      let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or((
          axum::http::StatusCode::UNAUTHORIZED,
          "Missing Authorization header".to_string(),
        ))?;

      let permission = Self::from_jwt(token, auth_token).map_err(|e| {
        (
          axum::http::StatusCode::UNAUTHORIZED,
          format!("Invalid token: {}", e),
        )
      })?;

      if !permission.have_permission(request) {
        return Err((
          axum::http::StatusCode::FORBIDDEN,
          "Permission denied".to_string(),
        ));
      }
    }
    Ok(())
  }

  pub fn check_write(
    headers: &axum::http::HeaderMap,
  ) -> Result<(), (axum::http::StatusCode, String)> {
    // Construct a dummy Insert request to check write permission
    let req = SqlRequest::Insert(msd_request::InsertRequest {
      key: msd_request::RequestKey::new("", ""),
      data: msd_request::InsertData::Table(msd_table::Table::default()),
    });
    Self::check(headers, &req)
  }
}
