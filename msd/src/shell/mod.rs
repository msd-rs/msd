mod import;
mod query;
mod table_handler;

use crate::app_config::ShellOptions;
use anyhow::Result;
use msd_table::Table;

const IMPORT_COMMAND: &str = "\\import";
const EXIT_COMMANDS: [&str; 3] = ["\\exit", "\\quit", "\\q"];
const SET_SERVER_COMMAND: &str = "\\server";
const SET_REACTIVE_ROWS_COMMAND: &str = "\\rows";

pub async fn run(shell_options: &ShellOptions) -> Result<()> {
  let mut shell_options = shell_options.clone();
  match shell_options.command.as_ref() {
    Some(cmd) => {
      run_command(shell_options, cmd).await?;
    }
    None => {
      // TODO: start interactive shell, read each line from stdin and run it
      // until one of the exit commands is received.
      // special commands:
      // \server <url> - set server url
      // \rows <num> - set reactive rows
    }
  }

  Ok(())
}

async fn run_command(opts: &ShellOptions, cmd: &str) -> Result<()> {
  if cmd.starts_with(IMPORT_COMMAND) {
    let arg = cmd.trim_start_matches(IMPORT_COMMAND);
    let (table, file_path) = arg.split_once(' ').unwrap_or((arg, ""));
    let table = table.trim();
    let file_path = file_path.trim();
    return import::execute(opts, table, file_path).await;
  }
  Ok(())
}
