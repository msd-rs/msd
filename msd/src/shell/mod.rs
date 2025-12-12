mod import;
mod query;
mod table_handler;

use std::sync::OnceLock;

use crate::app_config::ShellOptions;
use anyhow::Result;
use rustyline::DefaultEditor;

const IMPORT_COMMAND: &str = "\\import";
const HELP_COMMAND: &str = "\\help";
const EXIT_COMMANDS: [&str; 3] = ["\\exit", "\\quit", "\\q"];
const SET_SERVER_COMMAND: &str = "\\server";
const SET_REACTIVE_ROWS_COMMAND: &str = "\\rows";

pub async fn run(shell_options: &ShellOptions) -> Result<()> {
  let mut shell_options = shell_options.clone();
  if let Some(cmd) = shell_options.command.clone() {
    run_command(&shell_options, &cmd).await?;
  } else {
    // interactive shell

    print_help();

    let mut rl = DefaultEditor::new()?;

    loop {
      let readline = rl.readline("> ");
      match readline {
        Ok(line) => {
          let line = line.trim();
          if line.is_empty() {
            continue;
          }

          let _ = rl.add_history_entry(line);

          if EXIT_COMMANDS.contains(&line) {
            break;
          }

          if line == HELP_COMMAND {
            print_help();
            continue;
          }

          if line.starts_with(SET_SERVER_COMMAND) {
            let url = line.trim_start_matches(SET_SERVER_COMMAND).trim();
            if !url.is_empty() {
              shell_options.server_url = url.to_string();
              println!("Server url set to: {}", shell_options.server_url);
            } else {
              println!("Current server url: {}", shell_options.server_url);
            }
            continue;
          }

          if line.starts_with(SET_REACTIVE_ROWS_COMMAND) {
            let rows = line.trim_start_matches(SET_REACTIVE_ROWS_COMMAND).trim();
            if let Ok(rows) = rows.parse::<usize>() {
              shell_options.reactive_rows = rows;
              println!("Reactive rows set to: {}", shell_options.reactive_rows);
            } else {
              println!("Current reactive rows: {}", shell_options.reactive_rows);
            }
            continue;
          }

          if let Err(e) = run_command(&shell_options, line).await {
            eprintln!("Error: {}", e);
          }
        }
        Err(rustyline::error::ReadlineError::Interrupted) => break,
        Err(rustyline::error::ReadlineError::Eof) => break,
        Err(e) => {
          eprintln!("Error reading line: {}", e);
          break;
        }
      }
    }
    eprintln!("Bye!");
  }

  Ok(())
}

fn get_client() -> &'static reqwest::Client {
  static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

  CLIENT.get_or_init(|| reqwest::Client::new())
}

async fn run_command(opts: &ShellOptions, cmd: &str) -> Result<()> {
  if cmd.starts_with(IMPORT_COMMAND) {
    let arg = cmd.trim_start_matches(IMPORT_COMMAND);
    let (table, file_path) = arg.split_once(' ').unwrap_or((arg, ""));
    let table = table.trim();
    let file_path = file_path.trim();
    if table.is_empty() || file_path.is_empty() {
      eprintln!("Usage: \\import <table> <file_path>");
      return Ok(());
    }
    return import::execute(opts, table, file_path).await;
  }

  // default query
  query::execute(opts, cmd).await
}

fn print_help() {
  println!("Input some SQL or commands");
  println!("Available commands:");
  println!("  \\server <url>    Set server url");
  println!("  \\rows <num>      Set reactive rows");
  println!("  \\import <table> <file_path>  Import csv file to table");
  println!("  \\help            Print this help message");
  println!("  \\exit | \\quit | \\q  Exit shell");
}
