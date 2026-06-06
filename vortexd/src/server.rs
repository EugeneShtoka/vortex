use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, Query, Request, State,
    },
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;
use tracing::error;

use crate::config::Config;
use crate::engine::Engine;
use crate::event::Event;
use crate::store::{Store, TaskRow};

#[derive(Serialize)]
struct RunSummary {
    id: String,
    workflow: String,
    status: String,
    rejection: Option<String>,
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
}

#[derive(Serialize)]
struct RunDetail {
    #[serde(flatten)]
    summary: RunSummary,
    tasks: Vec<TaskSummary>,
}

#[derive(Serialize)]
struct WorkflowConfigDto {
    name: String,
    tasks: Vec<TaskConfigDto>,
}

#[derive(Serialize)]
struct TaskConfigDto {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    exec: Option<String>,
    when: Option<String>,
}

#[derive(Deserialize)]
struct ListRunsQuery {
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
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
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
        .route("/workflows/{name}/config", get(get_workflow_config).layer(authed))
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

pub async fn serve(config: Arc<Config>, event_tx: broadcast::Sender<Event>) -> Result<()> {
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
    let state = AppState { config, event_tx, auth_token: token };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(%bind, "HTTP server listening");
    axum::serve(listener, app).await?;
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
    Json(body): Json<serde_json::Value>,
) -> Response {
    let params = match parse_params(body) { Ok(p) => p, Err(e) => return e };
    let run_id = uuid::Uuid::new_v4().to_string();

    state.event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(), workflow: workflow.clone(), params: params.clone(),
    }).ok();

    if !check_auth(&headers, &state.auth_token) {
        state.event_tx.send(Event::TriggerRejected { run_id, reason: "unauthorized".into() }).ok();
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Some(workflow_config) = state.config.workflows.get(&workflow).cloned() else {
        state.event_tx.send(Event::TriggerRejected { run_id, reason: "unknown_workflow".into() }).ok();
        return (StatusCode::NOT_FOUND, Json(json!({"error": "unknown workflow"}))).into_response();
    };

    spawn_workflow(workflow_config, state.config.server.db_path.clone(), state.event_tx.clone(), workflow.clone(), run_id.clone(), params.clone());

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
    Query(q): Query<ListRunsQuery>,
) -> Response {
    let store = match Store::open(&state.config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.list_runs(q.limit, q.offset) {
        Ok(rows) => {
            let summaries: Vec<RunSummary> = rows.into_iter().map(|r| {
                let params = serde_json::from_str(&r.params).unwrap_or_default();
                RunSummary {
                    id: r.id, workflow: r.workflow, status: r.status,
                    rejection: r.rejection, params,
                    started_at: r.started_at, finished_at: r.finished_at,
                }
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
    let store = match Store::open(&state.config.server.db_path) {
        Ok(s) => s,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    match store.get_run(&run_id) {
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Ok(Some(detail)) => {
            let params = serde_json::from_str(&detail.run.params).unwrap_or_default();
            let summary = RunSummary {
                id: detail.run.id, workflow: detail.run.workflow,
                status: detail.run.status, rejection: detail.run.rejection, params,
                started_at: detail.run.started_at, finished_at: detail.run.finished_at,
            };
            Json(RunDetail { summary, tasks: detail.tasks.into_iter().map(Into::into).collect() }).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn get_workflow_config(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
    match state.config.workflows.get(&name) {
        None => StatusCode::NOT_FOUND.into_response(),
        Some(wf) => {
            let dto = WorkflowConfigDto {
                name: name.clone(),
                tasks: wf.tasks.iter().map(|t| TaskConfigDto {
                    id: t.id.clone(),
                    exec: if let crate::config::TaskKind::Shell { exec } = &t.kind { Some(exec.clone()) } else { None },
                    when: t.when.clone(),
                }).collect(),
            };
            Json(dto).into_response()
        }
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
    Json(body): Json<serde_json::Value>,
) -> Response {
    let params = match parse_params(body) {
        Ok(p) => p,
        Err(e) => return e,
    };

    let Some(workflow_config) = state.config.workflows.get(&workflow).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let run_id = uuid::Uuid::new_v4().to_string();

    // Emit events so TUI observers see this run
    state.event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(), workflow: workflow.clone(), params: params.clone(),
    }).ok();
    state.event_tx.send(Event::TriggerAccepted {
        run_id: run_id.clone(), workflow: workflow.clone(), params: params.clone(),
    }).ok();

    let engine = Engine::new(workflow_config, &state.config.server.db_path)
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
                .filter(|r| r.success)
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
                            response_template: None,
                        }],
                        cron: None,
                        correlation_id: None,
                    },
                );
                m
            },
            inputs: Default::default(),
            email: None,
        };
        AppState { config: Arc::new(config), event_tx: tx, auth_token: "secret".into() }
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
        tokio::spawn(async move { axum::serve(listener, build_router(state)).await.unwrap() });

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
                        id: "step".into(), kind: crate::config::TaskKind::Shell { exec: "echo hi".into() }, when: None, response_template: None,
                    }],
                    cron: None,
                    correlation_id: None,
                });
                m
            },
            inputs: Default::default(),
            email: None,
        };
        AppState { config: Arc::new(config), event_tx: tx, auth_token: "secret".into() }
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
        store.upsert_task("r1", "pull", "success", Some(0), Some("ok\n"), Some(""), Some(1000), Some(1200)).unwrap();
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
                        id: "step".into(), kind: crate::config::TaskKind::Shell { exec: exec.into() }, when: None, response_template: None,
                    }],
                    cron: None,
                    correlation_id: None,
                });
                m
            },
            inputs: Default::default(),
            email: None,
        };
        AppState { config: Arc::new(config), event_tx: tx, auth_token: "secret".into() }
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
}
