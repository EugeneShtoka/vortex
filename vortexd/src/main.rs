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
mod validator;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{broadcast, watch};
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

    let (config_tx, config_rx) = watch::channel(Arc::new(config));

    tokio::spawn(watch_config(config_path, config_tx));

    let (event_tx, _) = broadcast::channel::<event::Event>(256);

    scheduler::run(config_rx.clone(), event_tx.clone()).await;

    let ntfy_cfgs = config_rx.borrow().inputs.ntfy.clone();
    for ntfy_cfg in ntfy_cfgs {
        let rx = config_rx.clone();
        let tx = event_tx.clone();
        tokio::spawn(async move { ntfy::listen(ntfy_cfg, rx, tx).await });
    }

    let uds_handle = {
        let rx = config_rx.clone();
        let tx = event_tx.clone();
        tokio::spawn(listener::serve(rx, tx))
    };

    let http_handle = {
        let rx = config_rx.clone();
        let tx = event_tx.clone();
        tokio::spawn(async move {
            let enabled = rx.borrow().server.network.as_ref().is_some_and(|n| n.enabled);
            if enabled {
                if let Err(e) = server::serve(rx, tx).await {
                    tracing::error!("HTTP server error: {e:#}");
                }
            }
        })
    };

    let (uds_result, _) = tokio::try_join!(uds_handle, http_handle)?;
    uds_result?;
    Ok(())
}

async fn watch_config(path: String, tx: watch::Sender<Arc<config::Config>>) {
    let mut last_mtime = std::fs::metadata(&path).ok().and_then(|m| m.modified().ok());
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // consume the immediate first tick
    loop {
        interval.tick().await;
        let mtime = std::fs::metadata(&path).ok().and_then(|m| m.modified().ok());
        if mtime != last_mtime {
            last_mtime = mtime;
            match config::load_config(&path) {
                Ok(new_cfg) => {
                    let n = new_cfg.workflows.len();
                    if tx.send(Arc::new(new_cfg)).is_ok() {
                        info!(config = path, workflows = n, "Config reloaded");
                    }
                }
                Err(e) => tracing::warn!("Config reload failed: {e:#}"),
            }
        }
    }
}
