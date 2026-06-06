use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde::Deserialize;
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::engine::{Engine, TaskResult};
use crate::event::Event;
use crate::template;

#[derive(Debug, Deserialize)]
struct TriggerRequest {
    workflow: String,
    #[serde(default)]
    params: HashMap<String, String>,
    id: Option<String>,
}

pub async fn serve(config: Arc<Config>, event_tx: broadcast::Sender<Event>) -> Result<()> {
    let socket_path = &config.server.unix_socket;

    if std::path::Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    info!(socket = socket_path, "Listening on Unix socket");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let config = Arc::clone(&config);
                let tx = event_tx.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, config, tx).await {
                        error!("Connection error: {e:#}");
                    }
                });
            }
            Err(e) => error!("Accept error: {e}"),
        }
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    config: Arc<Config>,
    event_tx: broadcast::Sender<Event>,
) -> Result<()> {
    let (read_half, mut write_half) = stream.split();
    let mut reader = BufReader::new(read_half);

    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<TriggerRequest>(line) {
            Err(e) => {
                warn!("Malformed request: {e}");
                json!({ "status": "error", "message": format!("Invalid JSON: {e}") })
            }
            Ok(req) => handle_request(req, &config, &event_tx).await,
        };

        let mut payload = serde_json::to_string(&response)?;
        payload.push('\n');
        write_half.write_all(payload.as_bytes()).await?;
    }

    Ok(())
}

async fn handle_request(
    req: TriggerRequest,
    config: &Arc<Config>,
    event_tx: &broadcast::Sender<Event>,
) -> serde_json::Value {
    let run_id = uuid::Uuid::new_v4().to_string();

    // Inject top-level request `id` into params so correlation_id templates and
    // fallback chain can reference it as `{{trigger.id}}` / `params.get("id")`.
    let mut params = req.params;
    if let Some(ref rid) = req.id {
        params.entry("id".into()).or_insert_with(|| rid.clone());
    }

    info!(workflow = %req.workflow, "Trigger received on UDS");
    event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(),
        workflow: req.workflow.clone(),
        params: params.clone(),
    }).ok();

    let Some(workflow_config) = config.workflows.get(&req.workflow) else {
        warn!(workflow = %req.workflow, "Unknown workflow");
        event_tx.send(Event::TriggerRejected { run_id, reason: "unknown_workflow".into() }).ok();
        let cid = correlation_id_fallback(&params);
        return json!({ "id": cid, "status": "error", "message": format!("unknown workflow: {}", req.workflow) });
    };

    let correlation_id = compute_correlation_id(workflow_config, &params);

    event_tx.send(Event::TriggerAccepted {
        run_id: run_id.clone(),
        workflow: req.workflow.clone(),
        params: params.clone(),
    }).ok();

    execute_workflow(&req.workflow, workflow_config.clone(), params, &config.server.db_path, run_id, correlation_id, event_tx).await
}

async fn execute_workflow(
    workflow: &str,
    workflow_config: crate::config::WorkflowConfig,
    params: HashMap<String, String>,
    db_path: &str,
    run_id: String,
    correlation_id: String,
    event_tx: &broadcast::Sender<Event>,
) -> serde_json::Value {
    let engine = Engine::new(workflow_config, db_path)
        .with_events(event_tx.clone())
        .with_run_id(run_id)
        .with_params(params)
        .with_correlation_id(correlation_id.clone());

    match engine.run(workflow).await {
        Ok(results) => {
            match workflow_response(&results) {
                Some(mut val) => {
                    if let Some(obj) = val.as_object_mut() {
                        obj.entry("id").or_insert_with(|| json!(correlation_id));
                    }
                    val
                }
                None => json!({ "id": correlation_id }),
            }
        }
        Err(e) => {
            error!(workflow = workflow, "Workflow execution error: {e:#}");
            json!({ "id": correlation_id, "status": "error", "message": e.to_string() })
        }
    }
}

/// Returns the rendered response from the last successful task that has either
/// a `response_template` (rendered into `result.response`) or is a `Response`
/// task kind (stdout = rendered template). Returns None if no task produced a
/// response — callers fall through to their default behavior.
fn workflow_response(results: &[TaskResult]) -> Option<serde_json::Value> {
    results.iter()
        .filter(|r| r.success && r.response.is_some())
        .last()
        .and_then(|r| {
            let s = r.response.as_deref()?;
            serde_json::from_str(s.trim()).ok()
        })
}

/// Computes the correlation ID for a workflow run:
/// 1. Render `workflow_config.correlation_id` template if set
/// 2. Else fall back via `correlation_id_fallback`
fn compute_correlation_id(
    wf: &crate::config::WorkflowConfig,
    params: &HashMap<String, String>,
) -> String {
    if let Some(tmpl) = &wf.correlation_id {
        if let Ok(rendered) = template::render(tmpl, &HashMap::new(), &HashMap::new(), params, "") {
            let trimmed = rendered.trim().to_string();
            if !trimmed.is_empty() {
                return trimmed;
            }
        }
    }
    correlation_id_fallback(params)
}

/// Fallback chain: trigger.correlation_id → trigger.id → UUID
fn correlation_id_fallback(params: &HashMap<String, String>) -> String {
    if let Some(v) = params.get("correlation_id").filter(|v| !v.is_empty()) {
        return v.clone();
    }
    if let Some(v) = params.get("id").filter(|v| !v.is_empty()) {
        return v.clone();
    }
    uuid::Uuid::new_v4().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ServerConfig, TaskConfig, TaskKind, WorkflowConfig};
    use tempfile::NamedTempFile;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixStream;

    fn test_db_path() -> String {
        std::env::temp_dir()
            .join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()))
            .to_str().unwrap().to_string()
    }

    fn make_config(socket_path: &str) -> Arc<Config> {
        let mut workflows = std::collections::HashMap::new();
        workflows.insert(
            "greet".into(),
            WorkflowConfig {
                tasks: vec![TaskConfig {
                    id: "say".into(),
                    kind: TaskKind::Shell { exec: "echo hello={{trigger.name}}".into() },
                    when: None,
                    response_template: None,
                }],
                cron: None,
                correlation_id: None,
            },
        );
        Arc::new(Config {
            server: ServerConfig {
                unix_socket: socket_path.into(),
                network: None,
                db_path: test_db_path(),
            },
            workflows,
            inputs: Default::default(),
            email: None,
        })
    }

    async fn send_and_recv(socket_path: &str, msg: &str) -> String {
        let mut stream = UnixStream::connect(socket_path).await.unwrap();
        stream.write_all(format!("{msg}\n").as_bytes()).await.unwrap();
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        line
    }

    #[tokio::test]
    async fn uds_trigger_with_params_runs_workflow() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let config = make_config(&socket_path);
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(
            &socket_path,
            r#"{"workflow":"greet","params":{"name":"world"}}"#,
        ).await;

        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        // No response_template configured — response is just {"id":"..."}
        assert!(v["id"].is_string());
        assert!(v["status"].is_null()); // no status when no response template
    }

    #[tokio::test]
    async fn uds_trigger_without_params_ok() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let config = make_config(&socket_path);
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(&socket_path, r#"{"workflow":"greet"}"#).await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert!(v["id"].is_string());
    }

    #[tokio::test]
    async fn uds_response_includes_response_template_output() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let mut workflows = std::collections::HashMap::new();
        workflows.insert("output-wf".into(), WorkflowConfig {
            tasks: vec![TaskConfig {
                id: "step".into(),
                kind: TaskKind::Shell { exec: r#"true"#.into() },
                when: None,
                response_template: Some(r#"{"id":"{{correlation_id}}","status":"ok","val":"hello"}"#.into()),
            }],
            cron: None,
            correlation_id: None,
        });
        let config = Arc::new(Config {
            server: ServerConfig { unix_socket: socket_path.clone(), network: None, db_path: test_db_path() },
            workflows,
            inputs: Default::default(),
            email: None,
        });
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(&socket_path, r#"{"workflow":"output-wf","params":{"id":"req-1"}}"#).await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["val"], "hello");
        assert_eq!(v["id"], "req-1");
    }

    #[tokio::test]
    async fn uds_response_task_produces_output() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let mut workflows = std::collections::HashMap::new();
        workflows.insert("reply-wf".into(), WorkflowConfig {
            tasks: vec![TaskConfig {
                id: "r".into(),
                kind: TaskKind::Response {
                    template: r#"{"id":"{{correlation_id}}","status":"ok","msg":"{{trigger.text}}"}"#.into(),
                },
                when: None,
                response_template: None,
            }],
            cron: None,
            correlation_id: None,
        });
        let config = Arc::new(Config {
            server: ServerConfig { unix_socket: socket_path.clone(), network: None, db_path: test_db_path() },
            workflows,
            inputs: Default::default(),
            email: None,
        });
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(
            &socket_path,
            r#"{"workflow":"reply-wf","params":{"id":"req-7","text":"hi there"}}"#,
        ).await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["msg"], "hi there");
        assert_eq!(v["id"], "req-7");
    }

    #[tokio::test]
    async fn uds_correlation_id_echoed_via_trigger_id_fallback() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let config = make_config(&socket_path);
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(
            &socket_path,
            r#"{"workflow":"greet","params":{"name":"world"},"id":"req-42"}"#,
        ).await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["id"], "req-42");
    }

    #[tokio::test]
    async fn uds_multiple_requests_on_one_connection() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let config = make_config(&socket_path);
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut stream = UnixStream::connect(&socket_path).await.unwrap();
        stream.write_all(b"{\"workflow\":\"greet\",\"id\":\"a\"}\n").await.unwrap();
        stream.write_all(b"{\"workflow\":\"greet\",\"id\":\"b\"}\n").await.unwrap();

        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();

        reader.read_line(&mut line).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(v["id"], "a");

        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(v["id"], "b");
    }

    #[tokio::test]
    async fn uds_unknown_workflow_returns_error_status() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let config = make_config(&socket_path);
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(&socket_path, r#"{"workflow":"no-such-workflow","id":"x"}"#).await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["status"], "error");
        assert_eq!(v["id"], "x");
    }
}
