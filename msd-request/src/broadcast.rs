// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use std::collections::HashMap;

use msd_table::Table;
use smol_str::SmolStr;

#[derive(Debug, Clone)]
pub enum Broadcast {
  UpdateSchema(HashMap<SmolStr, Table>),
  CreateTable(SmolStr, Table),
  DropTable(SmolStr),
  Flush,
  Shutdown,
}
