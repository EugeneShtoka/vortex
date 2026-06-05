use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::engine::{Engine, TaskResult};
use crate::event::Event;

#[derive(Debug, Deserialize)]
struct TriggerRequest {
    workflow: String,
    #[serde(default)]
    params: HashMap<String, String>,
    id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Response {
    Ok { id: Option<String>, run_id: String, tasks_run: usize, output: Option<serde_json::Value> },
    Error { id: Option<String>, message: String },
    UnknownWorkflow { id: Option<String>, workflow: String },
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
            break; // client disconnected
        }
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<TriggerRequest>(line) {
            Err(e) => {
                warn!("Malformed request: {e}");
                Response::Error { id: None, message: format!("Invalid JSON: {e}") }
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
) -> Response {
    let id = req.id.clone();
    let run_id = uuid::Uuid::new_v4().to_string();

    info!(workflow = %req.workflow, "Trigger received on UDS");
    event_tx.send(Event::TriggerReceived {
        run_id: run_id.clone(),
        workflow: req.workflow.clone(),
        params: req.params.clone(),
    }).ok();

    let Some(workflow_config) = config.workflows.get(&req.workflow) else {
        warn!(workflow = %req.workflow, "Unknown workflow");
        event_tx.send(Event::TriggerRejected { run_id, reason: "unknown_workflow".into() }).ok();
        return Response::UnknownWorkflow { id, workflow: req.workflow };
    };

    // UDS is secured by filesystem permissions — no bearer auth needed
    event_tx.send(Event::TriggerAccepted {
        run_id: run_id.clone(),
        workflow: req.workflow.clone(),
        params: req.params.clone(),
    }).ok();

    execute_workflow(&req.workflow, workflow_config.clone(), req.params, &config.server.db_path, run_id, id, event_tx).await
}

async fn execute_workflow(
    workflow: &str,
    workflow_config: crate::config::WorkflowConfig,
    params: HashMap<String, String>,
    db_path: &str,
    run_id: String,
    id: Option<String>,
    event_tx: &broadcast::Sender<Event>,
) -> Response {
    let engine = Engine::new(workflow_config, db_path)
        .with_events(event_tx.clone())
        .with_run_id(run_id.clone())
        .with_params(params);

    match engine.run(workflow).await {
        Ok(results) => Response::Ok { id, run_id, tasks_run: results.len(), output: last_output(&results) },
        Err(e) => Response::Error { id, message: format!("{e:#}") },
    }
}

fn last_output(results: &[TaskResult]) -> Option<serde_json::Value> {
    results.iter()
        .filter(|r| r.success)
        .last()
        .map(|r| {
            let trimmed = r.stdout.trim();
            serde_json::from_str(trimmed)
                .unwrap_or_else(|_| serde_json::Value::String(trimmed.to_string()))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ServerConfig, TaskConfig, WorkflowConfig};
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
                    kind: crate::config::TaskKind::Shell { exec: "echo hello={{trigger.name}}".into() },
                    when: None,
                }],
                cron: None,
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
    async fn uds_trigger_with_params_injects_into_task() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp); // release so listener can bind

        let (tx, _) = broadcast::channel(32);
        let config = make_config(&socket_path);
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(
            &socket_path,
            r#"{"workflow":"greet","params":{"name":"world"}}"#,
        )
        .await;

        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["status"], "ok");
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
        assert_eq!(v["status"], "ok");
    }

    #[tokio::test]
    async fn uds_response_includes_output() {
        let tmp = NamedTempFile::new().unwrap();
        let socket_path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let (tx, _) = broadcast::channel(32);
        let mut workflows = std::collections::HashMap::new();
        workflows.insert("output-wf".into(), crate::config::WorkflowConfig {
            tasks: vec![crate::config::TaskConfig {
                id: "step".into(),
                kind: crate::config::TaskKind::Shell { exec: r#"printf '{"hello":"world"}'"#.into() },
                when: None,
            }],
            cron: None,
        });
        let config = std::sync::Arc::new(crate::config::Config {
            server: crate::config::ServerConfig {
                unix_socket: socket_path.clone(),
                network: None,
                db_path: test_db_path(),
            },
            workflows,
            inputs: Default::default(),
            email: None,
        });
        tokio::spawn(serve(config, tx));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = send_and_recv(&socket_path, r#"{"workflow":"output-wf"}"#).await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["output"], serde_json::json!({"hello": "world"}));
    }

    #[tokio::test]
    async fn uds_response_echoes_id() {
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
        )
        .await;
        let v: serde_json::Value = serde_json::from_str(resp.trim()).unwrap();
        assert_eq!(v["status"], "ok");
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
        assert_eq!(v["status"], "ok");
        assert_eq!(v["id"], "a");

        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let v: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["id"], "b");
    }
}
