use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, FromRequest, Path, Query, Request, State,
    },
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;
use tracing::error;
use vortex_core::TriggerStatus;

use tokio::sync::watch;

use crate::config::Config;
use crate::engine::Engine;
use crate::event::Event;
use crate::store::{Store, TaskRow, TriggerRow};
use crate::validator::{validate, Severity, ValidationIssue};

#[derive(Serialize)]
struct RunSummary {
    id: String,
    workflow: String,
    status: String,
    params: HashMap<String, String>,
    started_at: u64,
    finished_at: Option<u64>,
}

#[derive(Serialize)]
struct TaskSummary {
    task_id: String,
    status: String,
    exit_code: Option<i32>,
    stdout: Option<String>,
    stderr: Option<String>,
    started_at: Option<u64>,
    finished_at: Option<u64>,
    // Sprint 17 — task config fields (only set by get_run, not list_runs)
    #[serde(skip_serializing_if = "Option::is_none")]
    task_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_exec: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_when: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_abort_if: Option<String>,
}

#[derive(Serialize)]
struct RunDetail {
    #[serde(flatten)]
    summary: RunSummary,
    tasks: Vec<TaskSummary>,
}

#[derive(Serialize)]
struct TriggerSummary {
    id: String,
    workflow: String,
    status: String,
    params: HashMap<String, String>,
    source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    rejection_cause: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_addr: Option<String>,
    received_at: u64,
    finished_at: Option<u64>,
}

#[derive(Serialize)]
struct WorkflowConfigDto {
    name: String,
    tasks: Vec<TaskConfigDto>,
}

#[derive(Serialize)]
struct IssueCounts {
    errors:   usize,
    warnings: usize,
}

#[derive(Serialize)]
struct WorkflowSummaryDto {
    name:        String,
    issue_count: IssueCounts,
    issues:      Vec<ValidationIssueDto>,
}

#[derive(Serialize)]
struct ValidationIssueDto {
    severity: String,
    task_id:  Option<String>,
    code:     &'static str,
    message:  String,
}

impl From<ValidationIssue> for ValidationIssueDto {
    fn from(i: ValidationIssue) -> Self {
        Self {
            severity: match i.severity { Severity::Error => "error", Severity::Warning => "warning" }.to_string(),
            task_id:  i.task_id,
            code:     i.code,
            message:  i.message,
        }
    }
}

#[derive(Serialize)]
struct TaskConfigDto {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exec: Option<String>,
    when: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    depends_on: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct ListQuery {
    #[serde(default = "default_limit")]
    limit: usize,
    #[serde(default)]
    offset: usize,
}

fn default_limit() -> usize { 50 }

impl From<TaskRow> for TaskSummary {
    fn from(r: TaskRow) -> Self {
        Self {
            task_id: r.task_id,
            status: r.status,
            exit_code: r.exit_code,
            stdout: r.stdout,
            stderr: r.stderr,
            started_at: r.started_at,
            finished_at: r.finished_at,
            task_type: None,
            task_exec: None,
            task_when: None,
            task_abort_if: None,
        }
    }
}

impl From<TriggerRow> for TriggerSummary {
    fn from(t: TriggerRow) -> Self {
        let params = serde_json::from_str(&t.params).unwrap_or_default();
        Self {
            id: t.id, workflow: t.workflow, status: t.status, params,
            source: t.source, rejection_cause: t.rejection_cause,
            remote_addr: t.remote_addr,
            received_at: t.received_at, finished_at: t.finished_at,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: watch::Receiver<Arc<Config>>,
    pub event_tx: broadcast::Sender<Event>,
    pub auth_token: String,
}

pub fn build_router(state: AppState) -> Router {
    // /trigger/{workflow} does auth inside the handler so it can emit TriggerReceived
    // before checking the token and TriggerRejected if auth fails.
    //
    // /ws uses a route-level middleware because WebSocketUpgrade is an axum extractor
    // that runs before the handler body — returning 401 from inside the handler arrives
    // too late; the extractor has already emitted a 426.
    let authed = axum::middleware::from_fn_with_state(state.clone(), require_auth);
    Router::new()
        .route("/trigger/{workflow}", post(trigger_workflow))
        // /execute/{workflow} is unauthenticated — security comes from bind address
        // (127.0.0.1 only), same pattern as the Unix socket listener.
        .route("/execute/{workflow}", post(execute_workflow))
        .route("/ws",   get(websocket_handler).layer(authed.clone()))
        .route("/runs", get(list_runs).layer(authed.clone()))
        .route("/runs/{run_id}", get(get_run).layer(authed.clone()))
        .route("/globals", get(get_globals).layer(authed.clone()))
        .route("/globals/{key}", put(put_global).delete(delete_global).layer(authed.clone()))
        .route("/triggers", get(list_triggers).layer(authed.clone()))
        .route("/triggers/{trigger_id}", get(get_trigger).layer(authed.clone()))
        .route("/workflows",              get(list_workflows).layer(authed.clone()))
        .route("/workflows/{name}",      get(get_workflow).layer(authed.clone()))
        .route("/workflows/{name}/config", get(get_workflow_config).layer(authed.clone()))
        .route("/workflows/{name}/logs", get(get_workflow_logs).layer(authed.clone()))
        .route("/runs/{run_id}/tasks/{task_id}/logs", get(get_task_logs).layer(authed))
        .with_state(state)
}

fn parse_params(value: serde_json::Value) -> Result<HashMap<String, String>, Response> {
    match value {
        serde_json::Value::Object(map) => Ok(map.into_iter()
            .map(|(k, v)| (k, match v {
                serde_json::Value::String(s) => s,
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b)   => b.to_string(),
                other                        => other.to_string(),
            }))
            .collect()),
        _ => Err((StatusCode::BAD_REQUEST, "body must be a JSON object").into_response()),
    }
}

async fn require_auth(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if check_auth(request.headers(), &state.auth_token) {
        next.run(request).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

pub async fn serve(config_rx: watch::Receiver<Arc<Config>>, event_tx: broadcast::Sender<Event>) -> Result<()> {
    let config = config_rx.borrow().clone();
    let network = config
        .server
        .network
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Network config is required to run the HTTP server"))?;
    if !network.enabled {
        return Ok(());
    }
    let token = crate::auth::resolve_token(&network.auth_method, &network.auth_key)?;
    let bind = network.bind.clone();
    drop(config);
    let state = AppState { config: config_rx, event_tx, auth_token: token };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "HTTP server listening");
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>()).await?;
    Ok(())
}

/// Extract and validate the bearer token from request headers.
fn check_auth(headers: &HeaderMap, expected: &str) -> bool {
    headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|t| t == expected)
}

async fn trigger_workflow(
    State(state): State<AppState>,
    Path(workflow): Path<String>,
    headers: HeaderMap,
    request: axum::extract::Request,
) -> Response {
    let remote_addr = request.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.to_string());
    let body = match axum::Json::<serde_json::Value>::from_request(request, &state).await {
        Ok(axum::Json(v)) => v,
        Err(e) => return e.into_response(),
    };
    let params = match parse_params(body) { Ok(p) => p, Err(e) => return e };
    let run_id = uuid::Uuid::new_v4().to_string();
    let received_at = vortex_core::now_ms();
    let params_json = serde_json::to_string(&params).unwrap_or_else(|_| "{}".into());

    let config = state.config.borrow().clone();
    let store = Store::open(&config.server.db_path).ok();
    if let Some(ref s) = store {
        if let Err(e) = s.insert_trigger(&run_id, &workflow, &params_json, "http", remote_addr.as_deref(), received_at) {
            error!("Failed to insert trigger: {e:#}");
        }
    }

    state.event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(), workflow: workflow.clone(), params: params.clone(),
    }).ok();

    if !check_auth(&headers, &state.auth_token) {
        if let Some(ref s) = store {
            let _ = s.update_trigger_status(&run_id, TriggerStatus::Rejected, Some("unauthorized"), Some(vortex_core::now_ms()));
        }
        state.event_tx.send(Event::TriggerRejected { run_id, reason: "unauthorized".into() }).ok();
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Some(workflow_config) = config.workflows.get(&workflow).cloned() else {
        if let Some(ref s) = store {
            let _ = s.update_trigger_status(&run_id, TriggerStatus::Rejected, Some("workflow_not_found"), Some(vortex_core::now_ms()));
        }
        state.event_tx.send(Event::TriggerRejected { run_id, reason: "unknown_workflow".into() }).ok();
        return (StatusCode::NOT_FOUND, Json(json!({"error": "unknown workflow"}))).into_response();
    };

    if let Some(ref s) = store {
        let _ = s.update_trigger_status(&run_id, TriggerStatus::Accepted, None, None);
    }

    spawn_workflow(workflow_config, config.server.db_path.clone(), state.event_tx.clone(), workflow.clone(), run_id.clone(), params.clone());

    state.event_tx.send(Event::TriggerAccepted {
        run_id: run_id.clone(), workflow: workflow.clone(), params,
    }).ok();

    (StatusCode::ACCEPTED, Json(json!({"run_id": run_id, "workflow": workflow}))).into_response()
}

fn spawn_workflow(
    config: crate::config::WorkflowConfig,
    db_path: String,
    event_tx: broadcast::Sender<Event>,
    workflow: String,
    run_id: String,
    params: HashMap<String, String>,
) {
    tokio::spawn(async move {
        let engine = Engine::new(config, &db_path)
            .with_events(event_tx)
            .with_run_id(run_id)
            .with_params(params);
        if let Err(e) = engine.run(&workflow).await {
            error!("Workflow '{workflow}' run failed: {e:#}");
        }
    });
}

async fn list_runs(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.list_runs(q.limit, q.offset) {
        Ok(rows) => {
            let summaries: Vec<RunSummary> = rows.into_iter().map(|r| {
                let params = serde_json::from_str(&r.params).unwrap_or_default();
                RunSummary { id: r.id, workflow: r.workflow, status: r.status, params, started_at: r.started_at, finished_at: r.finished_at }
            }).collect();
            Json(summaries).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.get_run(&run_id) {
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Ok(Some(detail)) => {
            let params = serde_json::from_str(&detail.run.params).unwrap_or_default();
            let wf_config = config.workflows.get(&detail.run.workflow);
            let summary = RunSummary {
                id: detail.run.id, workflow: detail.run.workflow,
                status: detail.run.status, params,
                started_at: detail.run.started_at, finished_at: detail.run.finished_at,
            };
            let tasks = detail.tasks.into_iter().map(|row| {
                let mut ts = TaskSummary::from(row);
                if let Some(cfg) = wf_config.and_then(|wf| wf.tasks.iter().find(|t| t.id == ts.task_id)) {
                    use crate::config::TaskKind;
                    ts.task_type = Some(match &cfg.kind {
                        TaskKind::Shell { .. }    => "shell",
                        TaskKind::Http  { .. }    => "http",
                        TaskKind::Email { .. }    => "email",
                        TaskKind::Sleep { .. }    => "sleep",
                        TaskKind::StoreSet { .. } => "store_set",
                        TaskKind::Peer  { .. }    => "peer",
                        TaskKind::Spawn { .. }    => "spawn",
                        TaskKind::Response { .. } => "response",
                        TaskKind::Condition { .. }=> "condition",
                        TaskKind::Eval { .. }     => "eval",
                        TaskKind::ForEach { .. }  => "foreach",
                    }.to_string());
                    ts.task_exec = Some(match &cfg.kind {
                        TaskKind::Shell { exec }         => exec.clone(),
                        TaskKind::Http  { url, .. }      => url.clone(),
                        TaskKind::Email { to, .. }       => to.clone(),
                        TaskKind::Sleep { duration }     => duration.clone(),
                        TaskKind::Peer  { trigger, .. }  => trigger.clone(),
                        TaskKind::Spawn { exe, .. }      => exe.clone(),
                        TaskKind::Response { template }  => template.clone(),
                        TaskKind::Condition { expr }     => expr.clone(),
                        TaskKind::Eval { expr }          => expr.clone(),
                        TaskKind::ForEach { items, .. }  => items.clone(),
                        TaskKind::StoreSet { .. }        => String::new(),
                    });
                    ts.task_when     = cfg.when.clone();
                    ts.task_abort_if = cfg.abort_if.clone();
                }
                ts
            }).collect();
            Json(RunDetail { summary, tasks }).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn list_triggers(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.list_triggers(q.limit, q.offset) {
        Ok(rows) => Json(rows.into_iter().map(TriggerSummary::from).collect::<Vec<_>>()).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_trigger(
    State(state): State<AppState>,
    Path(trigger_id): Path<String>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.get_trigger(&trigger_id) {
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Ok(Some(t)) => Json(TriggerSummary::from(t)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_globals(
    State(state): State<AppState>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.get_all() {
        Ok(map) => Json(map).into_response(),
        Err(e)  => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn put_global(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: String,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.set(&key, body.trim()) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn delete_global(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.delete(&key) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_workflow_config(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    let config = state.config.borrow().clone();
    match config.workflows.get(&name) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(wf) => {
            let dto = WorkflowConfigDto {
                name: name.clone(),
                tasks: wf.tasks.iter().map(|t| TaskConfigDto {
                    id: t.id.clone(),
                    exec: if let crate::config::TaskKind::Shell { exec } = &t.kind { Some(exec.clone()) } else { None },
                    when: t.when.clone(),
                    depends_on: t.depends_on.clone(),
                }).collect(),
            };
            Json(dto).into_response()
        }
    }
}

fn make_workflow_summary(name: String, issues: Vec<ValidationIssue>) -> WorkflowSummaryDto {
    let errors   = issues.iter().filter(|i| i.severity == Severity::Error).count();
    let warnings = issues.iter().filter(|i| i.severity == Severity::Warning).count();
    WorkflowSummaryDto {
        name,
        issue_count: IssueCounts { errors, warnings },
        issues: issues.into_iter().map(ValidationIssueDto::from).collect(),
    }
}

async fn list_workflows(State(state): State<AppState>) -> Response {
    let config = state.config.borrow().clone();
    let summaries: Vec<WorkflowSummaryDto> = config.workflows.iter()
        .map(|(name, wf)| make_workflow_summary(name.clone(), validate(wf)))
        .collect();
    Json(summaries).into_response()
}

async fn get_workflow(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    let config = state.config.borrow().clone();
    match config.workflows.get(&name) {
        None     => StatusCode::NOT_FOUND.into_response(),
        Some(wf) => Json(make_workflow_summary(name, validate(wf))).into_response(),
    }
}

#[derive(Serialize)]
struct LogEntryDto {
    run_id:    String,
    task_id:   String,
    stream:    String,
    line:      String,
    logged_at: u64,
}

impl From<crate::store::LogRow> for LogEntryDto {
    fn from(r: crate::store::LogRow) -> Self {
        Self { run_id: r.run_id, task_id: r.task_id, stream: r.stream, line: r.line, logged_at: r.logged_at }
    }
}

async fn get_task_logs(
    State(state): State<AppState>,
    Path((run_id, task_id)): Path<(String, String)>,
) -> Response {
    let config = state.config.borrow().clone();
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.get_task_logs(&run_id, &task_id) {
        Ok(rows) => {
            if rows.is_empty() {
                // distinguish "run not found" from "no logs yet"
                match store.get_run(&run_id) {
                    Ok(None) => StatusCode::NOT_FOUND.into_response(),
                    _ => Json(Vec::<LogEntryDto>::new()).into_response(),
                }
            } else {
                Json(rows.into_iter().map(LogEntryDto::from).collect::<Vec<_>>()).into_response()
            }
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_workflow_logs(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<ListQuery>,
) -> Response {
    let config = state.config.borrow().clone();
    if !config.workflows.contains_key(&name) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let store = match Store::open(&config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.get_workflow_logs(&name, q.limit, q.offset) {
        Ok(rows) => Json(rows.into_iter().map(LogEntryDto::from).collect::<Vec<_>>()).into_response(),
        Err(e)   => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> Response {
    // Subscribe before sending 101 so no events are missed after the client connects
    let rx = state.event_tx.subscribe();
    ws.on_upgrade(move |socket| handle_ws(socket, rx))
}

async fn handle_ws(mut socket: WebSocket, mut rx: broadcast::Receiver<Event>) {
    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(e) => {
                        let Ok(json) = serde_json::to_string(&e) else { continue };
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("WS receiver lagged {n} messages");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        let _ = socket.send(Message::Pong(data)).await;
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Synchronous execution endpoint — runs the named workflow and returns the last
/// task's stdout as the HTTP response body. No auth required (security provided
/// by the bind address; callers must use localhost or a trusted network).
///
/// The JSON request body fields are injected as trigger params, accessible
/// both via `{{trigger.<key>}}` templates and the `VORTEX_TRIGGER_PARAMS` env var.
async fn execute_workflow(
    State(state): State<AppState>,
    Path(workflow): Path<String>,
    request: axum::extract::Request,
) -> Response {
    let remote_addr = request.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.to_string());
    let body = match axum::Json::<serde_json::Value>::from_request(request, &state).await {
        Ok(axum::Json(v)) => v,
        Err(e) => return e.into_response(),
    };
    let params = match parse_params(body) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let run_id = uuid::Uuid::new_v4().to_string();
    let received_at = vortex_core::now_ms();
    let params_json = serde_json::to_string(&params).unwrap_or_else(|_| "{}".into());

    let config = state.config.borrow().clone();
    let store = Store::open(&config.server.db_path).ok();
    if let Some(ref s) = store {
        if let Err(e) = s.insert_trigger(&run_id, &workflow, &params_json, "http", remote_addr.as_deref(), received_at) {
            error!("Failed to insert trigger: {e:#}");
        }
    }

    let Some(workflow_config) = config.workflows.get(&workflow).cloned() else {
        if let Some(ref s) = store {
            let _ = s.update_trigger_status(&run_id, TriggerStatus::Rejected, Some("workflow_not_found"), Some(vortex_core::now_ms()));
        }
        return StatusCode::NOT_FOUND.into_response();
    };

    if let Some(ref s) = store {
        let _ = s.update_trigger_status(&run_id, TriggerStatus::Accepted, None, None);
    }

    // Emit events so TUI observers see this run
    state.event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(), workflow: workflow.clone(), params: params.clone(),
    }).ok();
    state.event_tx.send(Event::TriggerAccepted {
        run_id: run_id.clone(), workflow: workflow.clone(), params: params.clone(),
    }).ok();

    let engine = Engine::new(workflow_config, &config.server.db_path)
        .with_events(state.event_tx.clone())
        .with_run_id(run_id)
        .with_params(params);

    match engine.run(&workflow).await {
        Err(e) => {
            error!("process/{workflow}: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Ok(results) => {
            let output = results.iter()
                .filter(|r| r.is_success())
                .last()
                .map(|r| r.stdout.trim().to_string())
                .unwrap_or_default();
            (
                StatusCode::OK,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                output,
            ).into_response()
        }
    }
}

// ──────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use tokio::sync::watch;
    use tower::ServiceExt;

    fn test_db_path() -> String {
        std::env::temp_dir()
            .join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()))
            .to_str().unwrap().to_string()
    }

    fn make_state() -> AppState {
        let (tx, _) = broadcast::channel(32);
        let config = Config {
            server: crate::config::ServerConfig {
                unix_socket: "/tmp/test.sock".into(),
                network: None,
                db_path: test_db_path(),
            },
            workflows: {
                let mut m = std::collections::HashMap::new();
                m.insert(
                    "test-workflow".into(),
                    crate::config::WorkflowConfig {
                        tasks: vec![crate::config::TaskConfig {
                            id: "step".into(),
                            kind: crate::config::TaskKind::Shell { exec: "echo hi".into() },
                            when: None,
                            depends_on: None,
                            response_template: None,
                            abort_if: None,
                        }],
                        cron: None,
                        correlation_id: None,
                        status_eval: None, log_retention: None,
                    },
                );
                m
            },
            inputs: Default::default(),
            email: None,
        };
        let (_, config_rx) = watch::channel(Arc::new(config));
        AppState { config: config_rx, event_tx: tx, auth_token: "secret".into() }
    }

    async fn post_trigger(app: Router, token: &str, workflow: &str, body: &str) -> axum::response::Response {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/trigger/{workflow}"))
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    async fn post_execute(app: Router, workflow: &str, body: &str) -> axum::response::Response {
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/execute/{workflow}"))
                .header("Content-Type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap()
    }

    // --- auth ---

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let app = build_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/trigger/test-workflow")
                    .header("Content-Type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_token_returns_401() {
        let resp = post_trigger(build_router(make_state()), "wrong", "test-workflow", "{}").await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // --- POST /trigger/{workflow} ---

    #[tokio::test]
    async fn known_workflow_returns_202_with_run_id() {
        let resp = post_trigger(build_router(make_state()), "secret", "test-workflow", "{}").await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["workflow"], "test-workflow");
        assert!(json["run_id"].as_str().is_some_and(|s| !s.is_empty()));
    }

    #[tokio::test]
    async fn unknown_workflow_returns_404() {
        let resp = post_trigger(build_router(make_state()), "secret", "ghost", "{}").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn malformed_json_returns_4xx() {
        let resp = post_trigger(build_router(make_state()), "secret", "test-workflow", "not json").await;
        assert!(resp.status().is_client_error());
    }

    // --- trigger event emission ---

    async fn collect_events(
        mut rx: broadcast::Receiver<Event>,
        count: usize,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        while events.len() < count {
            if let Ok(e) = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                async { rx.recv().await },
            )
            .await
            {
                if let Ok(e) = e {
                    events.push(e);
                }
            } else {
                break;
            }
        }
        events
    }

    #[tokio::test]
    async fn trigger_received_then_accepted_on_valid_request() {
        let state = make_state();
        let rx = state.event_tx.subscribe();
        let app = build_router(state);
        post_trigger(app, "secret", "test-workflow", "{}").await;
        let events = collect_events(rx, 2).await;
        assert!(events.iter().any(|e| matches!(e, Event::TriggerReceived { .. })));
        assert!(events.iter().any(|e| matches!(e, Event::TriggerAccepted { .. })));
    }

    #[tokio::test]
    async fn trigger_received_then_rejected_on_bad_auth() {
        let state = make_state();
        let rx = state.event_tx.subscribe();
        let app = build_router(state);
        post_trigger(app, "wrong", "test-workflow", "{}").await;
        let events = collect_events(rx, 2).await;
        assert!(events.iter().any(|e| matches!(e, Event::TriggerReceived { .. })));
        assert!(events.iter().any(|e| matches!(
            e,
            Event::TriggerRejected { reason, .. } if reason == "unauthorized"
        )));
    }

    #[tokio::test]
    async fn trigger_rejected_on_unknown_workflow() {
        let state = make_state();
        let rx = state.event_tx.subscribe();
        let app = build_router(state);
        post_trigger(app, "secret", "ghost", "{}").await;
        let events = collect_events(rx, 2).await;
        assert!(events.iter().any(|e| matches!(
            e,
            Event::TriggerRejected { reason, .. } if reason == "unknown_workflow"
        )));
    }

    // --- trigger persistence in DB ---

    #[tokio::test]
    async fn trigger_accepted_persisted_in_db() {
        let state = make_state();
        let db = state.config.borrow().server.db_path.clone();
        post_trigger(build_router(state), "secret", "test-workflow", "{}").await;
        let store = Store::open(&db).unwrap();
        let triggers = store.list_triggers(10, 0).unwrap();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].workflow, "test-workflow");
        assert_eq!(triggers[0].source, "http");
        assert_eq!(triggers[0].status, "accepted");
        assert!(triggers[0].rejection_cause.is_none());
    }

    #[tokio::test]
    async fn trigger_rejected_bad_auth_persisted_in_db() {
        let state = make_state();
        let db = state.config.borrow().server.db_path.clone();
        post_trigger(build_router(state), "wrong", "test-workflow", "{}").await;
        let store = Store::open(&db).unwrap();
        let triggers = store.list_triggers(10, 0).unwrap();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].status, "rejected");
        assert_eq!(triggers[0].rejection_cause.as_deref(), Some("unauthorized"));
    }

    #[tokio::test]
    async fn trigger_rejected_unknown_workflow_persisted_in_db() {
        let state = make_state();
        let db = state.config.borrow().server.db_path.clone();
        post_trigger(build_router(state), "secret", "ghost", "{}").await;
        let store = Store::open(&db).unwrap();
        let triggers = store.list_triggers(10, 0).unwrap();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].status, "rejected");
        assert_eq!(triggers[0].rejection_cause.as_deref(), Some("workflow_not_found"));
    }

    // --- GET /ws ---

    #[tokio::test]
    async fn ws_without_auth_returns_401() {
        let app = build_router(make_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/ws")
                    .header("Connection", "Upgrade")
                    .header("Upgrade", "websocket")
                    .header("Sec-WebSocket-Version", "13")
                    .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn ws_forwards_broadcast_events_to_client() {
        use futures_util::StreamExt;
        use tokio::net::TcpListener;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;

        let (tx, _) = broadcast::channel::<Event>(64);
        let tx_emit = tx.clone();
        let mut state = make_state();
        state.event_tx = tx;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, build_router(state).into_make_service_with_connect_info::<SocketAddr>()).await.unwrap()
        });

        let mut req = format!("ws://127.0.0.1:{}/ws", addr.port())
            .into_client_request()
            .unwrap();
        req.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
            "Bearer secret".parse().unwrap(),
        );
        let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();

        tx_emit
            .send(Event::TriggerReceived { run_id: "r1".into(), workflow: "deploy".into(), params: HashMap::new() })
            .unwrap();
        tx_emit
            .send(Event::WorkflowFinished {
                run_id: "r1".into(),
                workflow: "deploy".into(),
                success: true,
                timestamp: 0,
            })
            .unwrap();

        let m1 = ws.next().await.unwrap().unwrap();
        let m2 = ws.next().await.unwrap().unwrap();
        let e1: Event = serde_json::from_str(m1.to_text().unwrap()).unwrap();
        let e2: Event = serde_json::from_str(m2.to_text().unwrap()).unwrap();

        assert!(matches!(e1, Event::TriggerReceived { .. }));
        assert!(matches!(e2, Event::WorkflowFinished { success: true, .. }));
    }

    // --- trigger params ---

    #[tokio::test]
    async fn trigger_with_params_emits_params_in_event() {
        let state = make_state();
        let rx = state.event_tx.subscribe();
        let app = build_router(state);
        post_trigger(app, "secret", "test-workflow", r#"{"key":"val"}"#).await;
        let events = collect_events(rx, 2).await;
        assert!(events.iter().any(|e| matches!(
            e,
            Event::TriggerReceived { params, .. } if params.get("key").map(String::as_str) == Some("val")
        )));
    }

    #[tokio::test]
    async fn trigger_without_params_defaults_to_empty() {
        let state = make_state();
        let rx = state.event_tx.subscribe();
        let app = build_router(state);
        post_trigger(app, "secret", "test-workflow", "{}").await;
        let events = collect_events(rx, 2).await;
        assert!(events.iter().any(|e| matches!(
            e,
            Event::TriggerReceived { params, .. } if params.is_empty()
        )));
    }

    // --- GET /runs ---

    fn make_state_with_db(db_path: &str) -> AppState {
        let (tx, _) = broadcast::channel(32);
        let config = Config {
            server: crate::config::ServerConfig {
                unix_socket: "/tmp/test.sock".into(),
                network: None,
                db_path: db_path.into(),
            },
            workflows: {
                let mut m = std::collections::HashMap::new();
                m.insert("wf".into(), crate::config::WorkflowConfig {
                    tasks: vec![crate::config::TaskConfig {
                        id: "step".into(), kind: crate::config::TaskKind::Shell { exec: "echo hi".into() }, when: None, depends_on: None, response_template: None, abort_if: None,
                    }],
                    cron: None,
                    correlation_id: None,
                    status_eval: None, log_retention: None,
                });
                m
            },
            inputs: Default::default(),
            email: None,
        };
        let (_, config_rx) = watch::channel(Arc::new(config));
        AppState { config: config_rx, event_tx: tx, auth_token: "secret".into() }
    }

    #[tokio::test]
    async fn get_runs_returns_empty_list_initially() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json, serde_json::json!([]));
    }

    #[tokio::test]
    async fn get_runs_returns_persisted_runs() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let store = Store::open(&db).unwrap();
        store.insert_run("r1", "deploy", "{}", 1000).unwrap();
        store.finish_run("r1", true, 2000).unwrap();

        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["id"], "r1");
        assert_eq!(json[0]["status"], "success");
    }

    #[tokio::test]
    async fn get_runs_requires_auth() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder().method("GET").uri("/runs")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn get_run_by_id_returns_detail() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let store = Store::open(&db).unwrap();
        store.insert_run("r1", "deploy", "{}", 1000).unwrap();
        store.upsert_task("r1", "pull", vortex_core::TaskStatus::Success, Some(0), Some("ok\n"), Some(""), Some(1000), Some(1200)).unwrap();
        store.finish_run("r1", true, 2000).unwrap();

        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs/r1")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], "r1");
        assert_eq!(json["tasks"][0]["task_id"], "pull");
        assert_eq!(json["tasks"][0]["stdout"], "ok\n");
    }

    #[tokio::test]
    async fn get_run_by_id_returns_404_for_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs/nope")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // --- GET /triggers ---

    #[tokio::test]
    async fn get_triggers_returns_empty_list_initially() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/triggers")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json, serde_json::json!([]));
    }

    #[tokio::test]
    async fn get_triggers_returns_persisted_triggers() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let store = Store::open(&db).unwrap();
        store.insert_trigger("t1", "deploy", "{}", "http", None, 1000).unwrap();
        store.update_trigger_status("t1", TriggerStatus::Accepted, None, None).unwrap();

        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/triggers")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["id"], "t1");
        assert_eq!(json[0]["status"], "accepted");
        assert_eq!(json[0]["source"], "http");
    }

    #[tokio::test]
    async fn get_triggers_requires_auth() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder().method("GET").uri("/triggers")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn get_trigger_by_id_returns_detail() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let store = Store::open(&db).unwrap();
        store.insert_trigger("t1", "deploy", r#"{"key":"val"}"#, "http", Some("1.2.3.4:5678"), 1000).unwrap();
        store.update_trigger_status("t1", TriggerStatus::Rejected, Some("unauthorized"), Some(1050)).unwrap();

        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/triggers/t1")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["id"], "t1");
        assert_eq!(json["status"], "rejected");
        assert_eq!(json["rejection_cause"], "unauthorized");
        assert_eq!(json["remote_addr"], "1.2.3.4:5678");
        assert_eq!(json["params"]["key"], "val");
    }

    #[tokio::test]
    async fn get_trigger_by_id_returns_404_for_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let app = build_router(make_state_with_db(&db));
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/triggers/nope")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // --- GET /workflows/{name}/config ---

    #[tokio::test]
    async fn get_workflow_config_returns_tasks() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/test-workflow/config")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "test-workflow");
        assert_eq!(json["tasks"][0]["id"], "step");
        assert_eq!(json["tasks"][0]["exec"], "echo hi");
        assert!(json["tasks"][0]["when"].is_null());
    }

    #[tokio::test]
    async fn get_workflow_config_returns_404_for_unknown() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/ghost/config")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_workflow_config_requires_auth() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/test-workflow/config")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // --- POST /execute/{workflow} ---

    fn make_state_with_workflow(id: &str, exec: &str) -> AppState {
        let (tx, _) = broadcast::channel(32);
        let config = Config {
            server: crate::config::ServerConfig {
                unix_socket: "/tmp/test.sock".into(),
                network: None,
                db_path: test_db_path(),
            },
            workflows: {
                let mut m = std::collections::HashMap::new();
                m.insert(id.into(), crate::config::WorkflowConfig {
                    tasks: vec![crate::config::TaskConfig {
                        id: "step".into(), kind: crate::config::TaskKind::Shell { exec: exec.into() }, when: None, depends_on: None, response_template: None, abort_if: None,
                    }],
                    cron: None,
                    correlation_id: None,
                    status_eval: None, log_retention: None,
                });
                m
            },
            inputs: Default::default(),
            email: None,
        };
        let (_, config_rx) = watch::channel(Arc::new(config));
        AppState { config: config_rx, event_tx: tx, auth_token: "secret".into() }
    }

    #[tokio::test]
    async fn execute_workflow_returns_task_stdout() {
        let state = make_state_with_workflow("echo-wf", r#"printf '{"result":"ok"}'"#);
        let resp = post_execute(build_router(state), "echo-wf", "{}").await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        assert_eq!(body.as_ref(), br#"{"result":"ok"}"#);
    }

    #[tokio::test]
    async fn execute_workflow_returns_404_for_unknown_workflow() {
        let resp = post_execute(build_router(make_state()), "ghost", "{}").await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn execute_workflow_does_not_require_auth() {
        let state = make_state_with_workflow("wf", "printf '{}'");
        let resp = post_execute(build_router(state), "wf", "{}").await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn execute_workflow_injects_trigger_params_via_env() {
        let state = make_state_with_workflow("wf", "printf '%s' \"$VORTEX_TRIGGER_PARAMS\"");
        let resp = post_execute(build_router(state), "wf", r#"{"msg":"hello"}"#).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["msg"], "hello");
    }

    #[tokio::test]
    async fn execute_workflow_emits_trigger_events() {
        let state = make_state_with_workflow("wf", "printf '{}'");
        let rx = state.event_tx.subscribe();
        let app = build_router(state);
        post_execute(app, "wf", "{}").await;
        let events = collect_events(rx, 2).await;
        assert!(events.iter().any(|e| matches!(e, Event::TriggerReceived { .. })));
        assert!(events.iter().any(|e| matches!(e, Event::TriggerAccepted { .. })));
    }

    #[tokio::test]
    async fn execute_workflow_returns_bad_request_for_non_object_body() {
        let resp = post_execute(build_router(make_state()), "test-workflow", r#"["not","an","object"]"#).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // --- GET /globals ---

    #[tokio::test]
    async fn get_globals_returns_empty_map_initially() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/globals")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let map: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(map.is_object());
        assert_eq!(map.as_object().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn get_globals_requires_auth() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/globals")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // --- GET /runs/{id} with task config fields ---

    // --- GET /workflows ---

    #[tokio::test]
    async fn list_workflows_returns_all_workflows() {
        let state = make_state_with_workflow("my-wf", "echo hi");
        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["name"], "my-wf");
        assert!(arr[0]["issue_count"].is_object());
        assert!(arr[0]["issues"].is_array());
    }

    #[tokio::test]
    async fn list_workflows_clean_workflow_has_zero_counts() {
        let state = make_state_with_workflow("clean-wf", "echo hi");
        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json[0]["issue_count"]["errors"], 0);
        assert_eq!(json[0]["issue_count"]["warnings"], 0);
        assert_eq!(json[0]["issues"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn list_workflows_requires_auth() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // --- GET /workflows/{name} ---

    #[tokio::test]
    async fn get_workflow_returns_issues_for_known_workflow() {
        let state = make_state_with_workflow("wf", "echo hi");
        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/wf")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["name"], "wf");
        assert!(json["issues"].is_array());
        assert!(json["issue_count"].is_object());
    }

    #[tokio::test]
    async fn get_workflow_returns_404_for_unknown() {
        let state = make_state_with_workflow("wf", "echo hi");
        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/ghost")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_workflow_reports_missing_dep_issue() {
        let (tx, _) = broadcast::channel(32);
        let config = Config {
            server: crate::config::ServerConfig {
                unix_socket: "/tmp/test.sock".into(),
                network: None,
                db_path: test_db_path(),
            },
            workflows: {
                let mut m = std::collections::HashMap::new();
                m.insert("broken-wf".into(), crate::config::WorkflowConfig {
                    tasks: vec![crate::config::TaskConfig {
                        id: "step".into(),
                        kind: crate::config::TaskKind::Shell { exec: "true".into() },
                        when: None,
                        depends_on: Some(vec!["ghost".into()]),
                        response_template: None,
                        abort_if: None,
                    }],
                    cron: None,
                    correlation_id: None,
                    status_eval: None, log_retention: None,
                });
                m
            },
            inputs: Default::default(),
            email: None,
        };
        let (_, config_rx) = watch::channel(Arc::new(config));
        let state = AppState { config: config_rx, event_tx: tx, auth_token: "secret".into() };
        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/broken-wf")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["issue_count"]["errors"], 1);
        assert_eq!(json["issues"][0]["code"], "missing_dep");
    }

    #[tokio::test]
    async fn get_run_includes_task_config_fields_when_workflow_known() {
        let state = make_state_with_workflow("wf", "echo hello");
        let store = Store::open(&state.config.borrow().server.db_path).unwrap();
        store.insert_run("r1", "wf", "{}", 1000).unwrap();
        store.upsert_task("r1", "step", vortex_core::TaskStatus::Success, Some(0), Some("hello\n"), Some(""), Some(1000), Some(2000)).unwrap();
        store.finish_run("r1", true, 2000).unwrap();

        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs/r1")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["tasks"][0]["task_type"], "shell");
        assert_eq!(json["tasks"][0]["task_exec"], "echo hello");
    }

    // --- GET /runs/{id}/tasks/{task_id}/logs ---

    #[tokio::test]
    async fn get_task_logs_returns_lines() {
        let state = make_state_with_workflow("wf", "echo hi");
        let store = Store::open(&state.config.borrow().server.db_path).unwrap();
        store.insert_run("r1", "wf", "{}", 1000).unwrap();
        store.insert_task_log("r1", "step", "stdout", "hello", 1500, None).unwrap();
        store.insert_task_log("r1", "step", "stdout", "world", 1600, None).unwrap();

        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs/r1/tasks/step/logs")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["line"], "hello");
        assert_eq!(arr[1]["line"], "world");
        assert_eq!(arr[0]["stream"], "stdout");
    }

    #[tokio::test]
    async fn get_task_logs_returns_404_for_unknown_run() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/runs/ghost/tasks/step/logs")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // --- GET /workflows/{name}/logs ---

    #[tokio::test]
    async fn get_workflow_logs_returns_lines_across_runs() {
        let state = make_state_with_workflow("wf", "echo hi");
        let store = Store::open(&state.config.borrow().server.db_path).unwrap();
        store.insert_run("r1", "wf", "{}", 1000).unwrap();
        store.insert_run("r2", "wf", "{}", 2000).unwrap();
        store.insert_task_log("r1", "step", "stdout", "from r1", 1500, None).unwrap();
        store.insert_task_log("r2", "step", "stdout", "from r2", 2500, None).unwrap();

        let app = build_router(state);
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/wf/logs")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn get_workflow_logs_returns_404_for_unknown_workflow() {
        let app = build_router(make_state());
        let resp = app.oneshot(
            Request::builder()
                .method("GET").uri("/workflows/ghost/logs")
                .header("Authorization", "Bearer secret")
                .body(Body::empty()).unwrap(),
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
