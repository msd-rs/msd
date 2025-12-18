use crate::app_config::TokenOptions;
use crate::server::handlers::permission::{ADMIN_ROLE, Permission, READ_ROLE, WRITE_ROLE};
use anyhow::Result;
use msd_table::now;

pub fn run(options: TokenOptions) -> Result<()> {
  let role_mask = parse_roles(&options.role)?;
  let now = now() / 1_000_000;
  let exp = now + options.exp as i64 * 24 * 3600;

  let permission = Permission {
    exp,
    role: role_mask,
  };

  let token = permission.to_jwt(&options.auth_token)?;
  println!("{}", token);

  Ok(())
}

fn parse_roles(role_str: &str) -> Result<i64> {
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
