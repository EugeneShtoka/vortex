mod auth;
mod config;
mod engine;
mod event;
mod gate;
mod listener;
mod ntfy;
mod scheduler;
mod server;
mod store;
mod template;

use std::sync::Arc;

use anyhow::Result;
use tokio::sync::broadcast;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env().add_directive(tracing::Level::INFO.into()))
        .init();

    let config_path = std::env::args().nth(1).unwrap_or_else(|| "vortex.toml".to_string());
    let config = config::load_config(&config_path)?;
    info!(config = config_path, workflows = config.workflows.len(), "Config loaded");

    let config = Arc::new(config);
    let (event_tx, _) = broadcast::channel::<event::Event>(256);

    scheduler::run(Arc::clone(&config), event_tx.clone()).await;

    for ntfy_cfg in config.inputs.ntfy.clone() {
        let cfg = Arc::clone(&config);
        let tx  = event_tx.clone();
        tokio::spawn(async move { ntfy::listen(ntfy_cfg, cfg, tx).await });
    }

    let uds_handle = {
        let cfg = Arc::clone(&config);
        let tx = event_tx.clone();
        tokio::spawn(listener::serve(cfg, tx))
    };

    let http_handle = {
        let cfg = Arc::clone(&config);
        let tx = event_tx.clone();
        tokio::spawn(async move {
            if cfg.server.network.as_ref().is_some_and(|n| n.enabled) {
                if let Err(e) = server::serve(cfg, tx).await {
                    tracing::error!("HTTP server error: {e:#}");
                }
            }
        })
    };

    let (uds_result, _) = tokio::try_join!(uds_handle, http_handle)?;
    uds_result?;
    Ok(())
}
