mod import;
mod query;
mod table_handler;

use std::{env, path::PathBuf, sync::OnceLock};

use crate::app_config::ShellOptions;
use anyhow::Result;
use rustyline::{
  Completer, Editor, Helper, Highlighter, Hinter,
  error::ReadlineError,
  validate::{ValidationContext, ValidationResult, Validator},
};

const IMPORT_COMMAND: &str = "\\import";
const HELP_COMMAND: &str = "\\help";
const EXIT_COMMANDS: [&str; 3] = ["\\exit", "\\quit", "\\q"];
const SET_SERVER_COMMAND: &str = "\\server";
const SET_REACTIVE_ROWS_COMMAND: &str = "\\rows";

fn shell_history_file() -> PathBuf {
  match env::home_dir() {
    Some(mut path) => {
      #[cfg(target_os = "windows")]
      {
        use std::fs;

        path.push("AppData");
        path.push("Local");
        path.push("msd");
        fs::create_dir_all(&path).ok();
      }
      #[cfg(not(target_os = "windows"))]
      {
        path.push(".local");
        path.push("share");
        path.push("msd");
        std::fs::create_dir_all(&path).ok();
      }
      path.push(".msd_history");
      path
    }
    None => PathBuf::from(".msd_history"),
  }
}

#[derive(Completer, Helper, Highlighter, Hinter)]
struct InputValidator {}

impl Validator for InputValidator {
  fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult, ReadlineError> {
    use rustyline::validate::ValidationResult::{Incomplete, Valid};
    let input = ctx.input().trim();
    if input.starts_with('\\') {
      return Ok(Valid(None));
    }
    if !input.ends_with(';') {
      Ok(Incomplete)
    } else {
      Ok(Valid(None))
    }
  }
}

pub async fn run(shell_options: &ShellOptions) -> Result<()> {
  let mut shell_options = shell_options.clone();
  if let Some(cmd) = shell_options.command.clone() {
    let commands = if PathBuf::from(&cmd).is_file() {
      let content = std::fs::read_to_string(&cmd)?;
      content
        .split(';')
        .map_while(|s| {
          let s = s.trim();
          if s.is_empty() {
            None
          } else {
            Some(s.to_string())
          }
        })
        .collect::<Vec<_>>()
    } else {
      vec![cmd]
    };
    for cmd in commands {
      if cmd.trim().is_empty() {
        continue;
      }
      println!("> {}", cmd.trim());
      run_command(&shell_options, &cmd).await?;
    }
  } else {
    // interactive shell

    print_help();

    let mut rl = Editor::new()?;

    let h = InputValidator {};
    rl.set_helper(Some(h));

    rl.load_history(&shell_history_file()).ok();

    loop {
      let readline = rl.readline("> ");
      match readline {
        Ok(line) => {
          let _ = rl.add_history_entry(line.trim());
          let line = line.trim().trim_end_matches(';').trim();
          if line.is_empty() {
            continue;
          }

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
        Err(rustyline::error::ReadlineError::Interrupted) => {
          continue;
        }
        Err(rustyline::error::ReadlineError::Eof) => break,
        Err(e) => {
          eprintln!("Error reading line: {}", e);
          break;
        }
      }
    }
    rl.save_history(&shell_history_file()).ok();
    eprintln!("Bye!");
  }

  Ok(())
}

fn get_client(opts: &ShellOptions) -> &'static reqwest::Client {
  static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

  CLIENT.get_or_init(|| {
    let mut client = reqwest::ClientBuilder::new();
    if opts.msd_client {
      client = client.user_agent("msd-client").zstd(true)
    }
    client.build().unwrap()
  })
}

async fn run_command(opts: &ShellOptions, cmd: &str) -> Result<()> {
  if cmd.starts_with(IMPORT_COMMAND) {
    let arg = cmd.trim_start_matches(IMPORT_COMMAND).trim();
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
  println!("Input some SQL or commands, SQL should end with semicolon(;)");
  println!("Available commands:");
  println!("  \\server <url>    Set server url");
  println!("  \\rows <num>      Set reactive rows");
  println!("  \\import <table> <file_path>  Import csv file to table");
  println!("  \\help            Print this help message");
  println!("  \\exit | \\quit | \\q  Exit shell");
}
