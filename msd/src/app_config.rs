use std::sync::OnceLock;

use anyhow::Result;
use clap::{Args, Parser};
use time::{UtcOffset, macros::format_description};

use crate::server::parse_roles;

pub const MSD_USER_AGENT: &str = "msd-client";
pub const MSD_TABLE_FORMAT: &str = "application/x-msd-table-frame";

/// Get the global app config
pub fn app_config() -> &'static MsdOptions {
  static APP_CONFIG: OnceLock<MsdOptions> = OnceLock::new();
  APP_CONFIG.get_or_init(|| {
    let options = MsdOptions::parse();

    options
  })
}

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

  #[arg(long = "tz", default_value = "", env = "MSD_TZ", value_parser = parse_tz)]
  pub tz_offset: Option<UtcOffset>,

  #[arg(long = "pprof", default_value = "")]
  pub pprof: Option<String>,

  #[command(subcommand)]
  pub command: MsdCommands,
}

#[derive(Debug, clap::Subcommand)]
pub enum MsdCommands {
  /// Start the Msd server
  Server(ServerOptions),
  /// Start a client shell
  Shell(ShellOptions),
  /// Generate a JWT token
  Token(TokenOptions),
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

  /// Default public permission for no local request
  #[arg(short = 'P', long = "public-permission", default_value = "read", value_parser = parse_roles)]
  pub default_permission: i64,

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

  #[arg(short = 'v', long = "verbose", default_value_t = true)]
  pub verbose: bool,

  /// Whether to use binary protocol
  #[arg(short = 'b', long = "msd-binary", default_value_t = true)]
  pub msd_binary_protocol: bool,

  /// Which compression to use
  #[arg(short = 'c', long = "compression", default_value = "", value_parser = parse_compression)]
  pub compression: String,

  /// Output file for query results
  #[arg(short = 'o', long = "output")]
  pub output_file: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct TokenOptions {
  /// Authentication token key
  #[arg(short = 'a', long = "auth-token", env = "MSD_AUTH_TOKEN")]
  pub auth_token: String,

  /// Role: read/write/admin
  #[arg(short = 'r', long = "role", default_value = "read")]
  pub role: String,

  /// Expiration in days
  #[arg(short = 'e', long = "exp", default_value_t = 365)]
  pub exp: usize,
}

fn parse_tz(s: &str) -> Result<UtcOffset> {
  if s.is_empty() {
    return UtcOffset::current_local_offset()
      .map_err(|e| anyhow::anyhow!("Failed to get current local offset: {}", e));
  }

  let format = format_description!("[offset_hour]");
  match UtcOffset::parse(s, &format) {
    Ok(offset) => Ok(offset),
    Err(e) => Err(anyhow::anyhow!(
      "Failed to parse timezone offset '{}': {}",
      s,
      e
    )),
  }
}

fn parse_compression(s: &str) -> Result<String> {
  match s {
    "identity" | "none" | "" => Ok("identity".to_string()),
    "zstd" => Ok("zstd".to_string()),
    "gzip" => Ok("gzip".to_string()),
    "br" => Ok("br".to_string()),
    "deflate" => Ok("deflate".to_string()),
    _ => Err(anyhow::anyhow!("Invalid compression: {}", s)),
  }
}
