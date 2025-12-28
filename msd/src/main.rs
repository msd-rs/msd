// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

mod app_config;
mod logging;
mod server;
mod shell;
mod token;

use anyhow::Result;
use msd_table::set_default_timezone;

use crate::app_config::{MsdCommands, app_config};

#[tokio::main]
async fn main() -> Result<()> {
  dotenvy::dotenv_override().ok();

  let main_options = app_config();
  let _logging_guards = logging::setup_logging();

  let pprof_guard = main_options.pprof.as_ref().and_then(|_| {
    pprof::ProfilerGuardBuilder::default()
      .frequency(1000)
      .blocklist(&["libc", "libgcc", "pthread", "vdso"])
      .build()
      .ok()
  });

  set_default_timezone(main_options.tz_offset.unwrap().whole_hours());

  match &main_options.command {
    MsdCommands::Server(options) => {
      server::run(options).await?;
    }
    MsdCommands::Shell(options) => {
      shell::run(options).await?;
    }
    MsdCommands::Token(options) => {
      token::run(options.clone())?;
    }
  }

  pprof_guard
    .and_then(|guard| guard.report().build().ok())
    .zip(main_options.pprof.as_ref())
    .map(|(report, file_name)| {
      let file_name = format!("{}.pprof.svg", file_name);
      let file = std::fs::File::create(file_name).unwrap();
      report.flamegraph(file).unwrap();
    });

  Ok(())
}
