use rmcp::{
  ErrorData as McpError, Json, RoleServer, ServerHandler,
  handler::server::{router::prompt::PromptRouter, tool::ToolRouter, wrapper::Parameters},
  model::{
    GetPromptRequestParam, GetPromptResult, Implementation, ListPromptsResult,
    PaginatedRequestParam, PromptMessage, PromptMessageRole, ServerCapabilities, ServerInfo,
  },
  prompt, prompt_handler, prompt_router,
  schemars::JsonSchema,
  service::RequestContext,
  tool, tool_handler, tool_router,
  transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
  },
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::server::DBState;

#[derive(Clone)]
pub struct MsdMcp {
  #[allow(dead_code)]
  db: DBState,
  tool_router: ToolRouter<MsdMcp>,
  prompt_router: PromptRouter<MsdMcp>,
}

impl MsdMcp {
  pub fn new(db: DBState) -> Self {
    Self {
      db,
      tool_router: Self::tool_router(),
      prompt_router: Self::prompt_router(),
    }
  }
}

#[tool_router]
impl MsdMcp {
  #[tool(description = "list all tables in database, let you know what tables are available")]
  pub async fn list_tables(&self) -> Result<Json<GetTableResult>, McpError> {
    let all = self
      .db
      .list_tables()
      .map_err(|e| McpError::internal_error(e.to_string(), None))?;
    let tables = if all.len() > 30 {
      all
        .iter()
        .map(|(name, table)| TableDefine::from(name, table, false))
        .collect::<Vec<_>>()
    } else {
      all
        .iter()
        .map(|(name, table)| TableDefine::from(name, table, true))
        .collect::<Vec<_>>()
    };
    Ok(Json(GetTableResult { tables }))
  }

  #[tool(description = "get table schema in database by names")]
  pub async fn get_table(
    &self,
    params: Parameters<GetTableParams>,
  ) -> Result<Json<GetTableResult>, McpError> {
    let all = self
      .db
      .list_tables()
      .map_err(|e| McpError::internal_error(e.to_string(), None))?;

    let names = params.0.get_tables();

    let tables = all
      .iter()
      .filter(|(name, _)| names.contains(name))
      .map(|(name, table)| TableDefine::from(name, table, true))
      .collect::<Vec<_>>();
    Ok(Json(GetTableResult { tables }))
  }
}

const MCP_SQL_GUIDE: &str = include_str!("mcp_sql_guide.md");
const MCP_PYTHON_GUIDE: &str = include_str!("mcp_python_guide.md");

#[prompt_router]
impl MsdMcp {
  #[prompt(description = "guide to write SQL queries for msd")]
  pub async fn sql_guide(&self) -> Result<Vec<PromptMessage>, McpError> {
    Ok(vec![PromptMessage::new_text(
      PromptMessageRole::User,
      MCP_SQL_GUIDE,
    )])
  }

  #[prompt(description = "guide to write python scripts for msd")]
  pub async fn python_guide(&self) -> Result<Vec<PromptMessage>, McpError> {
    Ok(vec![PromptMessage::new_text(
      PromptMessageRole::User,
      MCP_PYTHON_GUIDE,
    )])
  }
}

#[tool_handler]
#[prompt_handler]
impl ServerHandler for MsdMcp {
  fn get_info(&self) -> ServerInfo {
    ServerInfo {
      instructions: Some(
        "MSD tables schema list and description help you to understand the database schema".into(),
      ),
      capabilities: ServerCapabilities::builder().enable_tools().build(),
      server_info: Implementation {
        name: "msd".into(),
        title: Some("msd database information".into()),
        version: crate::server::VERSION.into(),
        icons: None,
        website_url: None,
      },
      ..Default::default()
    }
  }
}

pub fn mcp_service(
  db: DBState,
  cancellation_token: CancellationToken,
) -> StreamableHttpService<MsdMcp> {
  let service = StreamableHttpService::new(
    move || Ok(MsdMcp::new(db.clone())),
    LocalSessionManager::default().into(),
    StreamableHttpServerConfig {
      cancellation_token,
      ..Default::default()
    },
  );
  service
}

#[derive(Debug, Deserialize, JsonSchema)]
struct GetTableParams {
  names: Vec<String>,
}

impl GetTableParams {
  fn get_tables(self) -> Vec<String> {
    self.names
  }
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct TableDefine {
  name: String,
  desc: String,
  #[serde(skip_serializing_if = "Vec::is_empty")]
  columns: Vec<ColumnDefine>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct ColumnDefine {
  name: String,
  #[serde(rename = "type")]
  type_: String,
  desc: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct GetTableResult {
  tables: Vec<TableDefine>,
}

impl TableDefine {
  fn from(name: &String, table: &msd_table::Table, with_columns: bool) -> Self {
    let name = name.clone();
    let desc = table
      .get_table_meta("desc")
      .and_then(|v| v.get_str())
      .map(|s| s.to_string())
      .unwrap_or_default();

    if !with_columns {
      return Self {
        name,
        desc,
        columns: Vec::new(),
      };
    }
    let columns = if with_columns {
      let mut columns = table
        .columns()
        .iter()
        .map(|col| {
          let desc = col
            .get_metadata("desc")
            .and_then(|v| v.get_str())
            .unwrap_or_default();
          ColumnDefine {
            name: col.name.clone(),
            type_: col.kind.to_string(),
            desc: desc.to_string(),
          }
        })
        .collect::<Vec<_>>();
      columns.insert(
        0,
        ColumnDefine {
          name: "obj".to_string(),
          desc: "identifier of the object, alias of id, symbol, code, etc.".to_string(),
          type_: "string".to_string(),
        },
      );
      columns
    } else {
      Vec::new()
    };

    TableDefine {
      name,
      desc,
      columns,
    }
  }
}
