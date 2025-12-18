use std::net::SocketAddr;

use anyhow::Result;
use msd_request::SqlRequest;
use serde::{Deserialize, Serialize};

use crate::app_config;

const READ_ROLE: i64 = 1;
const WRITE_ROLE: i64 = 2;
const ADMIN_ROLE: i64 = 4;

#[derive(Serialize, Deserialize)]
pub struct Permission {
  /// expiration time in seconds
  pub exp: i64,
  /// bit mask of roles
  pub role: i64,
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
    // exp had checked by jsonwebtoken's default validation
    self.role & want > 0
  }

  pub fn from_jwt(token: &str, secret: &str) -> anyhow::Result<Self> {
    let validation = jsonwebtoken::Validation::default();
    let key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
    let token_data = jsonwebtoken::decode::<Permission>(token, &key, &validation)?;
    Ok(token_data.claims)
  }

  pub fn to_jwt(&self, secret: &str) -> anyhow::Result<String> {
    let key = jsonwebtoken::EncodingKey::from_secret(secret.as_bytes());
    let token = jsonwebtoken::encode(&jsonwebtoken::Header::default(), self, &key)?;
    Ok(token)
  }

  pub fn check(
    headers: &axum::http::HeaderMap,
    remote_addr: &SocketAddr,
    request: &SqlRequest,
  ) -> Result<(), (axum::http::StatusCode, String)> {
    let server_options = match &app_config().command {
      app_config::MsdCommands::Server(opts) => opts,
      _ => {
        return Err((
          axum::http::StatusCode::FORBIDDEN,
          "Not a server command".to_string(),
        ));
      }
    };

    if let Some(auth_token) = server_options.auth_token.as_ref() {
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
      return Ok(());
    }
    if remote_addr.ip().is_loopback() {
      return Ok(());
    } else {
      let permission = Permission {
        exp: i64::MAX,
        role: server_options.default_permission,
      };
      if !permission.have_permission(request) {
        return Err((
          axum::http::StatusCode::FORBIDDEN,
          "Permission denied".to_string(),
        ));
      }
      return Ok(());
    }
  }

  pub fn check_write(
    headers: &axum::http::HeaderMap,
    remote_addr: &SocketAddr,
  ) -> Result<(), (axum::http::StatusCode, String)> {
    // Construct a dummy Insert request to check write permission
    let req = SqlRequest::Insert(msd_request::InsertRequest {
      key: msd_request::RequestKey::new("", ""),
      data: msd_request::InsertData::Table(msd_table::Table::default()),
    });
    Self::check(headers, remote_addr, &req)
  }
}

pub fn parse_roles(role_str: &str) -> Result<i64> {
  let mut role_mask = 0;
  for r in role_str.split(',') {
    match r.trim() {
      "read" => role_mask |= READ_ROLE,
      "write" => role_mask |= WRITE_ROLE,
      "admin" => role_mask |= ADMIN_ROLE,
      _ => anyhow::bail!("Invalid role: {}", r),
    }
  }
  if role_mask == 0 {
    anyhow::bail!("No valid roles provided");
  }
  Ok(role_mask)
}
