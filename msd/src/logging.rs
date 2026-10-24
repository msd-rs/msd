// Copyright 2026 MSD-RS Project LiJia
// SPDX-License-Identifier: agpl-3.0-only

use crate::app_config;
use msd_table::RFC3399_DATETIME;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::time;

pub fn setup_logging() -> WorkerGuard {
  let options = app_config();
  let timer = time::OffsetTime::new(options.tz_offset.unwrap(), RFC3399_DATETIME);
  if let Some(log_dir) = options.log_dir.as_ref() {
    let mut builder = RollingFileAppender::builder()
      .rotation(Rotation::DAILY)
      .filename_prefix("msd.log");
    if let Some(max_days) = options.log_max_days {
      builder = builder.max_log_files(max_days);
    }
    let file_appender = builder
      .build(log_dir)
      .expect("failed to initialize rolling file appender");
    let (file_non_blocking, file_guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
      .with_env_filter(EnvFilter::from_default_env())
      .with_timer(timer)
      .with_ansi(false)
      .with_writer(file_non_blocking)
      .init();
    file_guard
  } else {
    let (stdout_appender, stdio_guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::fmt()
      .with_timer(timer)
      .with_env_filter(EnvFilter::from_default_env())
      .with_writer(stdout_appender)
      .init();
    stdio_guard
  }
}
