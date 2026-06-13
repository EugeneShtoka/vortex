mod app;
mod config;
mod graph;
mod ui;
mod ws;

use std::io;
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event as CrosstermEvent, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::sync::mpsc;

use app::{App, ConnectionStatus, RunDetailDto};
use config::TuiConfig;
use graph::{DependencyGraph, WorkflowConfigDto};
use ws::WsMsg;

#[derive(Parser)]
#[command(name = "vortex-tui", about = "Live observer for running vortexd instances")]
struct Cli {
    #[arg(long, help = "WebSocket URL — creates a single source (overrides vortex.toml)")]
    url: Option<String>,

    #[arg(long, help = "Bearer auth token (overrides vortex.toml)")]
    token: Option<String>,

    #[arg(long, default_value = "vortex.toml", help = "Path to vortex.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let toml = TuiConfig::load(Path::new(&cli.config))?;
    let cfg = TuiConfig::resolve(toml, cli.url, cli.token)?;

    // Build App with one SourceState per configured source
    let source_names: Vec<&str> = cfg.sources.iter().map(|s| s.name.as_str()).collect();
    let mut app = App::with_source_names(&source_names);

    // Pre-populate history for all sources in parallel
    let client = reqwest::Client::new();
    for (i, src) in cfg.sources.iter().enumerate() {
        let history_url = format!("{}/runs?limit={}", src.http_base, src.history_limit);
        if let Ok(resp) = client.get(&history_url)
            .header("Authorization", format!("Bearer {}", src.token))
            .send().await
        {
            if let Ok(runs) = resp.json::<Vec<serde_json::Value>>().await {
                for run_val in runs.into_iter().rev() {
                    let run_id = run_val["id"].as_str().unwrap_or("").to_string();
                    let detail_url = format!("{}/runs/{}", src.http_base, run_id);
                    if let Ok(dr) = client.get(&detail_url)
                        .header("Authorization", format!("Bearer {}", src.token))
                        .send().await
                    {
                        if let Ok(detail) = dr.json::<RunDetailDto>().await {
                            app.sources[i].apply_run_detail(detail);
                        }
                    }
                }
            }
        }
    }

    // Spawn a WebSocket task per source (with reconnect backoff)
    let (event_tx, mut event_rx) = mpsc::channel::<(usize, WsMsg)>(256);
    for (i, src) in cfg.sources.iter().enumerate() {
        let tx = event_tx.clone();
        let url = src.url.clone();
        let token = src.token.clone();
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                let err = ws::connect(&url, &token, tx.clone(), i).await.err()
                    .map(|e| format!("{e:#}"));
                let _ = tx.send((i, WsMsg::Disconnected(err))).await;
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        });
    }

    // Start TUI
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal, &mut event_rx, app, &cfg).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;

    result
}

async fn fetch_graph(http_base: &str, token: &str, workflow: &str) -> Option<DependencyGraph> {
    let url = format!("{http_base}/workflows/{workflow}/config");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .send().await.ok()?;
    let dto = resp.json::<WorkflowConfigDto>().await.ok()?;
    Some(DependencyGraph::from_config(dto))
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    event_rx: &mut mpsc::Receiver<(usize, WsMsg)>,
    mut app: App,
    cfg: &TuiConfig,
) -> Result<()> {
    let tick = Duration::from_millis(100);
    // Track last-known (active_source_idx, selected_run_idx) to detect changes
    let mut last_graph_key: Option<(usize, usize)> = None;

    // Load graph for initial selection of the active source
    if let Some((_, run)) = app.selected_run() {
        let src = &cfg.sources[app.active];
        let workflow = run.workflow.clone();
        if let Some(g) = fetch_graph(&src.http_base, &src.token, &workflow).await {
            app.set_graph(g);
        }
    }

    loop {
        terminal.draw(|f| ui::render(f, &app))?;

        // Drain all pending WS messages
        while let Ok((src_idx, msg)) = event_rx.try_recv() {
            match msg {
                WsMsg::Connected => {
                    if let Some(src) = app.sources.get_mut(src_idx) {
                        src.connection = ConnectionStatus::Connected;
                    }
                }
                WsMsg::AppEvent(event) => {
                    app.handle_sourced(src_idx, event);
                }
                WsMsg::Disconnected(err) => {
                    if let Some(src) = app.sources.get_mut(src_idx) {
                        src.connection = ConnectionStatus::Disconnected(err);
                    }
                }
            }
        }

        if event::poll(tick)? {
            if let CrosstermEvent::Key(key) = event::read()? {
                match (key.code, key.modifiers) {
                    (KeyCode::Char('q') | KeyCode::Char('Q'), _) => break,
                    (KeyCode::Char('g') | KeyCode::Char('G'), _) => app.toggle_graph(),
                    (KeyCode::Tab, KeyModifiers::SHIFT) => app.prev_source(),
                    (KeyCode::Tab, _) => app.next_source(),
                    (KeyCode::Down | KeyCode::Char('j'), _) => {
                        app.active_source_mut().show_graph = false;
                        app.select_next();
                    }
                    (KeyCode::Up | KeyCode::Char('k'), _) => {
                        app.active_source_mut().show_graph = false;
                        app.select_prev();
                    }
                    _ => {}
                }
            }
        }

        // Fetch workflow graph when (active source, selected run) changes
        let current_key = Some((app.active, app.active_source().selected));
        if last_graph_key != current_key {
            last_graph_key = current_key;
            if let Some((_, run)) = app.selected_run() {
                let workflow = run.workflow.clone();
                if !workflow.is_empty() {
                    let src = &cfg.sources[app.active];
                    if let Some(g) = fetch_graph(&src.http_base, &src.token, &workflow).await {
                        app.set_graph(g);
                    }
                }
            }
        }
    }

    Ok(())
}
