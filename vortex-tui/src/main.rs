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

use app::{App, ConnectionStatus, GlobalsEditState, LogEntry, RunDetailDto, TriggerSummaryDto, WorkflowIssueSummary};
use std::collections::HashMap;
use config::{TuiConfig, ViewMode};
use graph::{DependencyGraph, WorkflowConfigDto};
use ws::WsMsg;

#[derive(Parser)]
#[command(name = "vortex-tui", version, about = "Live observer for running vortexd instances")]
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

    // Apply layout config and default view mode per source
    for (i, src_cfg) in cfg.sources.iter().enumerate() {
        app.sources[i].layout = src_cfg.layout.clone();
        app.sources[i].set_view_mode(src_cfg.layout.default_mode.clone());
    }

    // Pre-populate history for all sources
    let client = reqwest::Client::new();
    for (i, src) in cfg.sources.iter().enumerate() {
        // Fetch run history
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
        // Fetch trigger history (runs must be loaded first for cross-referencing)
        let triggers_url = format!("{}/triggers?limit={}", src.http_base, src.history_limit);
        if let Ok(resp) = client.get(&triggers_url)
            .header("Authorization", format!("Bearer {}", src.token))
            .send().await
        {
            if let Ok(dtos) = resp.json::<Vec<TriggerSummaryDto>>().await {
                app.sources[i].apply_triggers(dtos);
            }
        }
        // Fetch workflow validation issues
        let workflows_url = format!("{}/workflows", src.http_base);
        if let Ok(resp) = client.get(&workflows_url)
            .header("Authorization", format!("Bearer {}", src.token))
            .send().await
        {
            if let Ok(summaries) = resp.json::<Vec<WorkflowIssueSummary>>().await {
                app.sources[i].apply_workflow_summaries(summaries);
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

async fn put_global(http_base: &str, token: &str, key: &str, value: &str) -> bool {
    let url = format!("{http_base}/globals/{key}");
    reqwest::Client::new()
        .put(&url)
        .header("Authorization", format!("Bearer {token}"))
        .body(value.to_string())
        .send().await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

async fn delete_global(http_base: &str, token: &str, key: &str) -> bool {
    let url = format!("{http_base}/globals/{key}");
    reqwest::Client::new()
        .delete(&url)
        .header("Authorization", format!("Bearer {token}"))
        .send().await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

async fn fetch_task_logs(http_base: &str, token: &str, run_id: &str, task_id: &str) -> Option<Vec<LogEntry>> {
    let url = format!("{http_base}/runs/{run_id}/tasks/{task_id}/logs");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .send().await.ok()?;
    if resp.status().is_success() {
        resp.json::<Vec<LogEntry>>().await.ok()
    } else {
        None
    }
}

async fn fetch_globals(http_base: &str, token: &str) -> Option<HashMap<String, String>> {
    let url = format!("{http_base}/globals");
    let resp = reqwest::Client::new()
        .get(&url)
        .header("Authorization", format!("Bearer {token}"))
        .send().await.ok()?;
    resp.json::<HashMap<String, String>>().await.ok()
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

    // Load graph for the initial selection and seed the tracking key
    let initial_workflow = app.active_source().workflow_names().into_iter().next();
    let mut last_graph_key: Option<(usize, Option<String>)> = Some((app.active, initial_workflow.clone()));
    if let Some(ref wf) = initial_workflow {
        let src = &cfg.sources[app.active];
        if let Some(g) = fetch_graph(&src.http_base, &src.token, wf).await {
            app.set_graph(g);
        }
    }

    // Track selected task to fetch logs lazily
    let mut last_task_key: Option<(usize, String, String)> = None; // (src_idx, run_id, task_id)

    loop {
        terminal.draw(|f| ui::render(f, &app))?;

        // Drain all pending WS messages; track runs needing globals snapshots
        let mut globals_pre_needed:  Vec<(usize, String)> = Vec::new();
        let mut globals_post_needed: Vec<(usize, String)> = Vec::new();
        while let Ok((src_idx, msg)) = event_rx.try_recv() {
            match msg {
                WsMsg::Connected => {
                    if let Some(src) = app.sources.get_mut(src_idx) {
                        src.connection = ConnectionStatus::Connected;
                    }
                }
                WsMsg::AppEvent(event) => {
                    use vortex_core::Event;
                    match &event {
                        Event::WorkflowStarted  { run_id, .. } => globals_pre_needed.push((src_idx, run_id.clone())),
                        Event::WorkflowFinished { run_id, .. } => globals_post_needed.push((src_idx, run_id.clone())),
                        _ => {}
                    }
                    app.handle_sourced(src_idx, event);
                }
                WsMsg::Disconnected(err) => {
                    if let Some(src) = app.sources.get_mut(src_idx) {
                        src.connection = ConnectionStatus::Disconnected(err);
                    }
                }
            }
        }

        // Fetch globals snapshots for runs that just started/finished
        for (src_idx, run_id) in globals_pre_needed {
            if let Some(src_cfg) = cfg.sources.get(src_idx) {
                if let Some(g) = fetch_globals(&src_cfg.http_base, &src_cfg.token).await {
                    if let Some(src) = app.sources.get_mut(src_idx) {
                        src.apply_globals_pre(&run_id, g);
                    }
                }
            }
        }
        for (src_idx, run_id) in globals_post_needed {
            if let Some(src_cfg) = cfg.sources.get(src_idx) {
                if let Some(g) = fetch_globals(&src_cfg.http_base, &src_cfg.token).await {
                    if let Some(src) = app.sources.get_mut(src_idx) {
                        src.apply_globals_post(&run_id, g);
                    }
                }
            }
        }

        if event::poll(tick)? {
            if let CrosstermEvent::Key(key) = event::read()? {
                use app::Focus;

                // Globals modal has exclusive key handling when open
                if app.active_source().show_globals {
                    let src_idx = app.active;
                    let src_cfg = &cfg.sources[src_idx];
                    let edit_state = app.active_source().globals_edit.clone();
                    match edit_state {
                        GlobalsEditState::None => match key.code {
                            KeyCode::Esc => app.close_globals(),
                            KeyCode::Char('q') => app.close_globals(),
                            KeyCode::Down | KeyCode::Char('j') => {
                                app.active_source_mut().globals_select_next();
                            }
                            KeyCode::Up | KeyCode::Char('k') => {
                                app.active_source_mut().globals_select_prev();
                            }
                            KeyCode::Char('e') => {
                                if let Some(key_name) = app.active_source().globals_selected_key() {
                                    let cur_val = app.active_source()
                                        .globals_current.get(&key_name).cloned().unwrap_or_default();
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::EditingValue { key: key_name, buf: cur_val };
                                }
                            }
                            KeyCode::Char('n') => {
                                app.active_source_mut().globals_edit =
                                    GlobalsEditState::AddingKey { key_buf: String::new() };
                            }
                            KeyCode::Char('d') => {
                                if let Some(key_name) = app.active_source().globals_selected_key() {
                                    delete_global(&src_cfg.http_base, &src_cfg.token, &key_name).await;
                                    app.active_source_mut().apply_globals_delete();
                                }
                            }
                            _ => {}
                        },
                        GlobalsEditState::EditingValue { key: ref k, ref buf } => {
                            let k = k.clone();
                            let mut buf = buf.clone();
                            match key.code {
                                KeyCode::Esc => {
                                    app.active_source_mut().globals_edit = GlobalsEditState::None;
                                }
                                KeyCode::Enter => {
                                    put_global(&src_cfg.http_base, &src_cfg.token, &k, &buf).await;
                                    app.active_source_mut().apply_globals_edit(k, buf);
                                }
                                KeyCode::Backspace => {
                                    buf.pop();
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::EditingValue { key: k, buf };
                                }
                                KeyCode::Char(c) => {
                                    buf.push(c);
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::EditingValue { key: k, buf };
                                }
                                _ => {}
                            }
                        }
                        GlobalsEditState::AddingKey { ref key_buf } => {
                            let mut key_buf = key_buf.clone();
                            match key.code {
                                KeyCode::Esc => {
                                    app.active_source_mut().globals_edit = GlobalsEditState::None;
                                }
                                KeyCode::Enter if !key_buf.is_empty() => {
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::AddingValue { key: key_buf, val_buf: String::new() };
                                }
                                KeyCode::Backspace => {
                                    key_buf.pop();
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::AddingKey { key_buf };
                                }
                                KeyCode::Char(c) => {
                                    key_buf.push(c);
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::AddingKey { key_buf };
                                }
                                _ => {}
                            }
                        }
                        GlobalsEditState::AddingValue { key: ref gname, ref val_buf } => {
                            let gkey = gname.clone();
                            let mut val_buf = val_buf.clone();
                            match key.code {
                                KeyCode::Esc => {
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::AddingKey { key_buf: gkey };
                                }
                                KeyCode::Enter => {
                                    put_global(&src_cfg.http_base, &src_cfg.token, &gkey, &val_buf).await;
                                    app.active_source_mut().apply_globals_edit(gkey, val_buf);
                                }
                                KeyCode::Backspace => {
                                    val_buf.pop();
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::AddingValue { key: gkey, val_buf };
                                }
                                KeyCode::Char(c) => {
                                    val_buf.push(c);
                                    app.active_source_mut().globals_edit =
                                        GlobalsEditState::AddingValue { key: gkey, val_buf };
                                }
                                _ => {}
                            }
                        }
                    }
                    continue;
                }

                let in_detail = app.active_source().focus == Focus::Detail;
                match (key.code, key.modifiers) {
                    (KeyCode::Char('q') | KeyCode::Char('Q'), _) => break,
                    (KeyCode::Char('t') | KeyCode::Char('T'), _) if !in_detail => {
                        app.set_view_mode(ViewMode::Triggers);
                    }
                    (KeyCode::Char('w') | KeyCode::Char('W'), _) if !in_detail => {
                        app.jump_to_workflow_view();
                    }
                    (KeyCode::Char('g'), _) if !in_detail => {
                        app.toggle_graph();
                    }
                    (KeyCode::Char('G'), _) if !in_detail => {
                        let src_cfg = &cfg.sources[app.active];
                        if let Some(globals) = fetch_globals(&src_cfg.http_base, &src_cfg.token).await {
                            app.open_globals(globals);
                        }
                    }
                    (KeyCode::Tab, KeyModifiers::SHIFT) => app.prev_source(),
                    (KeyCode::Tab, _) => app.next_source(),
                    (KeyCode::Down | KeyCode::Char('j'), _) => {
                        app.active_source_mut().show_graph = false;
                        app.navigate_down();
                    }
                    (KeyCode::Up | KeyCode::Char('k'), _) => {
                        app.active_source_mut().show_graph = false;
                        app.navigate_up();
                    }
                    (KeyCode::Right | KeyCode::Char('l'), _) => {
                        app.active_source_mut().show_graph = false;
                        app.focus_right();
                    }
                    (KeyCode::Left | KeyCode::Char('h'), _) => {
                        app.active_source_mut().show_graph = false;
                        app.focus_left();
                    }
                    (KeyCode::Enter, _) => {
                        app.active_source_mut().show_graph = false;
                        app.enter_pane();
                    }
                    (KeyCode::Char('['), _) => app.panels_narrower(),
                    (KeyCode::Char(']'), _) => app.panels_wider(),
                    (KeyCode::Esc, _) => {
                        if app.active_source().show_graph {
                            app.active_source_mut().show_graph = false;
                        } else {
                            app.escape_pane();
                        }
                    }
                    _ => {}
                }
            }
        }

        // Fetch task logs lazily when selected task changes
        let task_key: Option<(usize, String, String)> = {
            let src = app.active_source();
            src.selected_task_entry()
                .and_then(|(task_id, _)| src.selected_active_run().map(|(run_id, _)| (app.active, run_id.clone(), task_id.clone())))
        };
        if task_key != last_task_key {
            last_task_key = task_key.clone();
            if let Some((src_idx, ref run_id, ref task_id)) = task_key {
                let key = (run_id.clone(), task_id.clone());
                let already_cached = app.sources.get(src_idx)
                    .map(|s| s.task_logs.contains_key(&key))
                    .unwrap_or(false);
                if !already_cached {
                    if let Some(src_cfg) = cfg.sources.get(src_idx) {
                        if let Some(logs) = fetch_task_logs(&src_cfg.http_base, &src_cfg.token, run_id, task_id).await {
                            if let Some(src) = app.sources.get_mut(src_idx) {
                                src.apply_task_logs(run_id, task_id, logs);
                            }
                        }
                    }
                }
            }
        }

        // Fetch workflow graph when selection changes (mode-aware)
        let graph_key = {
            let src = app.active_source();
            let wf = match src.view_mode {
                ViewMode::Triggers => src.selected_trigger_entry()
                    .and_then(|t| t.workflow.clone()),
                ViewMode::Workflows => src.workflow_names()
                    .into_iter()
                    .nth(src.selected_workflow),
            };
            Some((app.active, wf))
        };
        if last_graph_key != graph_key {
            last_graph_key = graph_key.clone();
            if let Some((_, Some(wf))) = &graph_key {
                if !wf.is_empty() {
                    let src = &cfg.sources[app.active];
                    if let Some(g) = fetch_graph(&src.http_base, &src.token, wf).await {
                        app.set_graph(g);
                    }
                }
            }
        }
    }

    Ok(())
}
