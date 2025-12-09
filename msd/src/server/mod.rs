mod handlers;

use std::sync::Arc;

use crate::app_config::ServerOptions;
use anyhow::Result;
use axum::{
  Router,
  routing::{post, put},
};
use msd_db::MsdDb;
use msd_store::RocksDbStore;
use tracing::info;

const VERSION: &str = env!("CARGO_PKG_VERSION");

type DBState = Arc<MsdDb<RocksDbStore>>;

pub async fn run(server_options: &ServerOptions) -> Result<()> {
  let app_options = super::app_config::app_config();

  info!("msd server version {}", VERSION);
  info!("Database path:     {}", app_options.db_path);
  info!("Listening on:      {}", server_options.listen_addr);
  info!("Worker threads:    {}", server_options.worker_threads);
  info!("Timezone offset:   {}", app_options.tz_offset.unwrap());
  info!(
    "Log directory:     {}",
    app_options.log_dir.as_deref().unwrap_or("stdout")
  );
  let pid = std::process::id();
  if let Some(pid_file) = server_options.pid_file.as_ref() {
    std::fs::write(pid_file, format!("{}", pid))?;
    info!("Wrote pid {} to {}", pid, pid_file);
  } else {
    info!("Process id:        {}", pid);
  }

  let db_path = app_options.db_path.clone();
  let listener = tokio::net::TcpListener::bind(server_options.listen_addr.as_str()).await?;

  let store = RocksDbStore::new(&db_path)?;
  let db = MsdDb::new(store, server_options.worker_threads).await?;

  let db = Arc::new(db);
  let app = Router::new()
    .route("/data", post(handlers::handle_data))
    .route("/table/{table_name}", put(handlers::handle_table))
    .with_state(db.clone());
  info!("msd server start");
  axum::serve(listener, app)
    .with_graceful_shutdown(shutdown_signal())
    .await?;
  db.shutdown().await;
  info!("msd server stopped");
  Ok(())
}

async fn shutdown_signal() {
  #[cfg(unix)]
  {
    let mut ctrlc = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
      .expect("Failed to install interrupt signal handler");
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
      .expect("Failed to install terminate signal handler");
    tokio::select! {
      _ = ctrlc.recv() => {},
      _ = terminate.recv() => {},
    }
  }
  #[cfg(not(unix))]
  {
    tokio::signal::ctrl_c()
      .await
      .expect("Failed to install Ctrl+C handler");
  }
  info!("shutdown signal received");
}
