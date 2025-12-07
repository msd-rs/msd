mod app_config;
mod logging;
mod server;

use anyhow::Result;
use msd_table::set_default_timezone;
use tracing::info;

use crate::app_config::{MsdCommands, app_config};

#[tokio::main]
async fn main() -> Result<()> {
  dotenvy::dotenv_override().ok();

  let main_options = app_config();
  let _logging_guards = logging::setup_logging();

  set_default_timezone(main_options.tz_offset.unwrap().whole_hours());

  match &main_options.command {
    MsdCommands::Server(options) => {
      server::run(options).await?;
    }
  }

  Ok(())
}
