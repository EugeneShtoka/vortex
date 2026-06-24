use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::{broadcast, watch};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::{Config, SseListenerConfig};
use crate::engine::Engine;
use crate::event::Event;

pub async fn listen(cfg: SseListenerConfig, config_rx: watch::Receiver<Arc<Config>>, event_tx: broadcast::Sender<Event>) {
    loop {
        if let Err(e) = stream(&cfg, &config_rx, &event_tx).await {
            error!(url = %cfg.url, "sse listener error: {e:#}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn stream(cfg: &SseListenerConfig, config_rx: &watch::Receiver<Arc<Config>>, event_tx: &broadcast::Sender<Event>) -> Result<()> {
    let mut req = Client::new().get(&cfg.url);
    if let Some(token) = resolve_token(cfg)? {
        req = req.header("Authorization", format!("Bearer {token}"));
    }
    info!(url = %cfg.url, "sse: subscribing");
    let mut body_stream = req.send().await?.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = body_stream.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(pos) = buf.find('\n') {
            let line = buf[..pos].trim().to_string();
            buf.drain(..=pos);
            if !line.is_empty() {
                handle_line(&line, cfg, config_rx, event_tx).await;
            }
        }
    }
    Ok(())
}

async fn handle_line(line: &str, cfg: &SseListenerConfig, config_rx: &watch::Receiver<Arc<Config>>, event_tx: &broadcast::Sender<Event>) {
    let json: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => { warn!(url = %cfg.url, "sse: failed to parse line: {e}"); return; }
    };
    if !should_process(cfg, &json) { return; }

    let config = config_rx.borrow().clone();
    let Some(wf) = config.workflows.get(&cfg.workflow) else {
        warn!(workflow = %cfg.workflow, "sse: workflow not found");
        return;
    };
    let run_id = Uuid::new_v4().to_string();
    let params = extract_params(cfg, &json);
    let _ = event_tx.send(Event::TriggerReceived { run_id: run_id.clone(), workflow: cfg.workflow.clone(), params: params.clone() });
    let _ = event_tx.send(Event::TriggerAccepted { run_id: run_id.clone(), workflow: cfg.workflow.clone(), params: params.clone() });

    let engine = Engine::new(wf.clone(), &config.server.db_path)
        .with_events(event_tx.clone())
        .with_run_id(run_id)
        .with_params(params);
    if let Err(e) = engine.run(&cfg.workflow).await {
        error!(workflow = %cfg.workflow, "sse: workflow run failed: {e:#}");
    }
}

pub fn should_process(cfg: &SseListenerConfig, json: &Value) -> bool {
    match &cfg.event_filter {
        Some(filter) => json.get("event").and_then(Value::as_str) == Some(filter.as_str()),
        None => true,
    }
}

pub fn extract_params(cfg: &SseListenerConfig, json: &Value) -> HashMap<String, String> {
    let mut p: HashMap<String, String> = cfg.params.clone();
    for field in &cfg.fields {
        if let Some(val) = json.get(field) {
            let s = match val {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            p.insert(field.clone(), s);
        }
    }
    p
}

fn resolve_token(cfg: &SseListenerConfig) -> Result<Option<String>> {
    match (&cfg.auth_method, &cfg.auth_key) {
        (Some(method), Some(key)) => Ok(Some(crate::auth::resolve_token(method, key)?)),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(fields: &[&str], event_filter: Option<&str>) -> SseListenerConfig {
        SseListenerConfig {
            url:          "https://example.com/stream".into(),
            workflow:     "wf".into(),
            auth_method:  None,
            auth_key:     None,
            fields:       fields.iter().map(|s| s.to_string()).collect(),
            event_filter: event_filter.map(str::to_string),
            params:       HashMap::new(),
        }
    }

    #[test]
    fn extract_params_promotes_declared_fields() {
        let c = cfg(&["message", "title", "priority"], None);
        let json = serde_json::json!({
            "event": "message", "message": "hello", "title": "Alert",
            "priority": 3, "tags": ["a","b"], "topic": "t"
        });
        let p = extract_params(&c, &json);
        assert_eq!(p["message"], "hello");
        assert_eq!(p["title"], "Alert");
        assert_eq!(p["priority"], "3");
        assert!(!p.contains_key("tags"));
        assert!(!p.contains_key("topic"));
    }

    #[test]
    fn extract_params_missing_field_skipped() {
        let c = cfg(&["message", "missing_key"], None);
        let json = serde_json::json!({"event": "message", "message": "hi"});
        let p = extract_params(&c, &json);
        assert_eq!(p["message"], "hi");
        assert!(!p.contains_key("missing_key"));
    }

    #[test]
    fn extract_params_array_field_serialized_as_json() {
        let c = cfg(&["tags"], None);
        let json = serde_json::json!({"tags": ["a", "b"]});
        let p = extract_params(&c, &json);
        assert_eq!(p["tags"], "[\"a\",\"b\"]");
    }

    #[test]
    fn event_filter_accepts_matching() {
        let c = cfg(&[], Some("message"));
        let json = serde_json::json!({"event": "message"});
        assert!(should_process(&c, &json));
    }

    #[test]
    fn event_filter_rejects_non_matching() {
        let c = cfg(&[], Some("message"));
        let json = serde_json::json!({"event": "keepalive"});
        assert!(!should_process(&c, &json));
    }

    #[test]
    fn event_filter_none_accepts_all() {
        let c = cfg(&[], None);
        assert!(should_process(&c, &serde_json::json!({"event": "anything"})));
        assert!(should_process(&c, &serde_json::json!({})));
    }

    #[test]
    fn static_params_merged() {
        let mut c = cfg(&["message"], None);
        c.params.insert("source".into(), "ntfy".into());
        let json = serde_json::json!({"message": "hi"});
        let p = extract_params(&c, &json);
        assert_eq!(p["source"], "ntfy");
        assert_eq!(p["message"], "hi");
    }
}
