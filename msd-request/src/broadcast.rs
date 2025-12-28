// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::collections::HashMap;

use msd_table::Table;

#[derive(Debug, Clone)]
pub enum Broadcast {
  UpdateSchema(HashMap<String, Table>),
  CreateTable(String, Table),
  DropTable(String),
  Shutdown,
}
