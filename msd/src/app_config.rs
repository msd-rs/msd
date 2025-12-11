use std::sync::OnceLock;

use clap::{Args, Parser};
use time::{UtcOffset, macros::format_description};

/// Msd is a high performance time-series database.
#[derive(Debug, Parser)]
#[command(version, about)]
pub struct MsdOptions {
  /// Path to the database directory
  #[arg(long = "db", default_value = "./msd_db", env = "MSD_DB_PATH")]
  pub db_path: String,

  /// Path to the log directory if provided, otherwise logs are output to only stdout/stderr
  #[arg(long = "log-dir", env = "MSD_LOG_PATH")]
  pub log_dir: Option<String>,

  #[arg(long = "tz", default_value = "", env = "MSD_TZ")]
  pub tz: String,

  #[arg(skip)]
  pub tz_offset: Option<time::UtcOffset>,

  #[command(subcommand)]
  pub command: MsdCommands,
}

pub fn app_config() -> &'static MsdOptions {
  static APP_CONFIG: OnceLock<MsdOptions> = OnceLock::new();
  APP_CONFIG.get_or_init(|| {
    let mut options = MsdOptions::parse();

    if options.tz.is_empty() {
      options.tz_offset = Some(UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC));
    } else {
      let format = format_description!("[offset_hour]");
      match UtcOffset::parse(&options.tz, &format) {
        Ok(offset) => {
          options.tz_offset = Some(offset);
        }
        Err(e) => {
          panic!("Failed to parse timezone offset '{}': {}", options.tz, e);
        }
      }
    }
    options
  })
}

#[derive(Debug, clap::Subcommand)]
pub enum MsdCommands {
  /// Start the Msd server
  Server(ServerOptions),
  /// Start a client shell
  Shell(ShellOptions),
}

#[derive(Debug, Args)]
pub struct ServerOptions {
  /// Listen address
  #[arg(
    short = 'l',
    long = "listen",
    default_value = "127.0.0.1:50510",
    env = "MSD_LISTEN_ADDR"
  )]
  pub listen_addr: String,

  /// Process id file, if provided, the server writes its pid to this file
  #[arg(short = 'p', long = "pid", env = "MSD_PID_FILE")]
  pub pid_file: Option<String>,

  /// Authentication token key
  #[arg(
    short = 'a',
    long = "auth-token",
    env = "MSD_AUTH_TOKEN",
    long_help = "Authentication token key. 
  If set, the server requires clients to provide this token for authentication. 
  If not set, no authentication is required."
  )]
  pub auth_token: Option<String>,

  /// worker threads
  #[arg(
    short = 'w',
    long = "workers",
    default_value_t = 8,
    env = "MSD_WORKERS"
  )]
  pub worker_threads: usize,
}

/// Shell options
#[derive(Debug, Clone, Args)]
pub struct ShellOptions {
  /// Optional command to run, will exit after the command is run, otherwise enter interactive mode
  pub command: Option<String>,

  /// Server URL
  #[arg(
    short = 's',
    long = "server",
    default_value = "http://127.0.0.1:50510",
    env = "MSD_SERVER_URL"
  )]
  pub server_url: String,

  /// Max table rows print in reactive mode, 0 for unlimited
  #[arg(short = 'r', long = "reactive-rows", default_value = "30")]
  pub reactive_rows: usize,
}
