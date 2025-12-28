use rmcp::{
  ServerHandler,
  handler::server::{tool::ToolRouter, wrapper::Parameters},
  model::{ServerCapabilities, ServerInfo},
  tool, tool_handler,
  transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
  },
};

use crate::server::DBState;
use anyhow::Result;

#[derive(Clone)]
pub struct MsdMcp {
  #[allow(dead_code)]
  db: DBState,
  tool_router: ToolRouter<MsdMcp>,
}

impl MsdMcp {
  pub fn new(db: DBState) -> Self {
    Self {
      db,
      tool_router: ToolRouter::new(),
    }
  }

  #[tool(description = "get usage guide for `msd`")]
  pub async fn usage(&self) -> Result<String> {
    Ok("".to_string())
  }

  #[tool(
    description = "list all tables and it's description, let you know what tables are available"
  )]
  pub async fn list_tables(&self) -> Result<Vec<String>> {
    let all = self.db.list_tables()?;
    let docs = if all.len() > 30 {
      all
        .iter()
        .map(|(name, table)| {
          let desc = table
            .get_table_meta("desc")
            .and_then(|v| v.get_str())
            .unwrap_or_default();
          format!("- `{}`: {}", name, desc)
        })
        .collect::<Vec<_>>()
    } else {
      all
        .iter()
        .map(|(name, table)| {
          let desc = table
            .get_table_meta("desc")
            .and_then(|v| v.get_str())
            .unwrap_or_default();
          let fields = table
            .columns()
            .iter()
            .map(|col| {
              let desc = col
                .get_metadata("desc")
                .and_then(|v| v.get_str())
                .unwrap_or_default();
              format!("  - `{}`: {}\n", col.name, desc)
            })
            .collect::<String>();

          format!("- `{}`: {}. Filed List:\n{}", name, desc, fields)
        })
        .collect::<Vec<_>>()
    };
    Ok(docs)
  }

  #[tool(description = "get table schema by names, you can provide multiple names")]
  pub async fn get_table(&self, params: Parameters<Vec<String>>) -> Result<String> {
    let all = self.db.list_tables()?;
    let table = all
      .iter()
      .filter(|(name, _)| params.0.contains(name))
      .map(|(name, table)| {
        let desc = table
          .get_table_meta("desc")
          .and_then(|v| v.get_str())
          .unwrap_or_default();
        let fields = table
          .columns()
          .iter()
          .map(|col| {
            let desc = col
              .get_metadata("desc")
              .and_then(|v| v.get_str())
              .unwrap_or_default();
            format!("  - `{}`: {}\n", col.name, desc)
          })
          .collect::<String>();

        format!("- `{}`: {}. Filed List:\n{}", name, desc, fields)
      })
      .collect::<String>();
    Ok(table)
  }
}

#[tool_handler]
impl ServerHandler for MsdMcp {
  fn get_info(&self) -> ServerInfo {
    ServerInfo {
      instructions: Some(
        "MSD tables schema list and description help you to understand the database schema".into(),
      ),
      capabilities: ServerCapabilities::builder().enable_tools().build(),
      ..Default::default()
    }
  }
}

pub fn mcp_service(db: DBState) -> StreamableHttpService<MsdMcp> {
  let service = StreamableHttpService::new(
    move || Ok(MsdMcp::new(db.clone())),
    LocalSessionManager::default().into(),
    StreamableHttpServerConfig::default(),
  );
  service
}
