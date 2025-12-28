// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::app_config::TokenOptions;
use crate::server::{Permission, parse_roles};
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
