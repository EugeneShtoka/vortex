use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use futures_util::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::{Config, NtfyListenerConfig};
use crate::engine::Engine;
use crate::event::Event;

#[derive(Debug, Deserialize)]
struct NtfyMessage {
    #[serde(default)]
    event:    String,
    #[serde(default)]
    topic:    String,
    #[serde(default)]
    message:  String,
    #[serde(default)]
    title:    String,
    #[serde(default)]
    priority: u8,
    #[serde(default)]
    tags:     Vec<String>,
}

pub async fn listen(cfg: NtfyListenerConfig, config: Arc<Config>, event_tx: broadcast::Sender<Event>) {
    loop {
        if let Err(e) = stream(&cfg, &config, &event_tx).await {
            error!(topic = %cfg.topic, "ntfy listener error: {e:#}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn stream(cfg: &NtfyListenerConfig, config: &Arc<Config>, event_tx: &broadcast::Sender<Event>) -> Result<()> {
    let url = format!("{}/{}/json", cfg.server, cfg.topic);
    let mut req = Client::new().get(&url);
    if let Some(token) = resolve_token(cfg)? {
        req = req.header("Authorization", format!("Bearer {token}"));
    }
    info!(topic = %cfg.topic, "ntfy: subscribing");
    let mut body_stream = req.send().await?.bytes_stream();
    let mut buf = String::new();

    while let Some(chunk) = body_stream.next().await {
        buf.push_str(&String::from_utf8_lossy(&chunk?));
        while let Some(pos) = buf.find('\n') {
            let line = buf[..pos].trim().to_string();
            buf.drain(..=pos);
            if !line.is_empty() {
                handle_line(&line, cfg, config, event_tx).await;
            }
        }
    }
    Ok(())
}

async fn handle_line(line: &str, cfg: &NtfyListenerConfig, config: &Arc<Config>, event_tx: &broadcast::Sender<Event>) {
    let msg: NtfyMessage = match serde_json::from_str(line) {
        Ok(m) => m,
        Err(e) => { warn!(topic = %cfg.topic, "ntfy: failed to parse message: {e}"); return; }
    };
    if msg.event != "message" { return; }

    let Some(wf) = config.workflows.get(&cfg.workflow) else {
        warn!(workflow = %cfg.workflow, "ntfy: workflow not found");
        return;
    };
    let run_id = Uuid::new_v4().to_string();
    let params = message_to_params(&msg);
    let _ = event_tx.send(Event::TriggerReceived { run_id: run_id.clone(), workflow: cfg.workflow.clone(), params: params.clone() });
    let _ = event_tx.send(Event::TriggerAccepted { run_id: run_id.clone(), workflow: cfg.workflow.clone(), params: params.clone() });

    let engine = Engine::new(wf.clone(), &config.server.db_path)
        .with_events(event_tx.clone())
        .with_run_id(run_id)
        .with_params(params);
    if let Err(e) = engine.run(&cfg.workflow).await {
        error!(workflow = %cfg.workflow, "ntfy: workflow run failed: {e:#}");
    }
}

fn resolve_token(cfg: &NtfyListenerConfig) -> Result<Option<String>> {
    match (&cfg.auth_method, &cfg.auth_key) {
        (Some(method), Some(key)) => Ok(Some(crate::auth::resolve_token(method, key)?)),
        _ => Ok(None),
    }
}

pub fn message_to_params(msg: &NtfyMessage) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("message".into(),  msg.message.clone());
    p.insert("title".into(),    msg.title.clone());
    p.insert("priority".into(), msg.priority.to_string());
    p.insert("tags".into(),     msg.tags.join(","));
    p.insert("topic".into(),    msg.topic.clone());
    p
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(message: &str, title: &str, priority: u8, tags: Vec<&str>) -> NtfyMessage {
        NtfyMessage {
            event: "message".into(),
            topic: "test".into(),
            message: message.into(),
            title: title.into(),
            priority,
            tags: tags.into_iter().map(str::to_string).collect(),
        }
    }

    #[test]
    fn message_to_params_extracts_all_fields() {
        let p = message_to_params(&msg("hello", "Alert", 3, vec!["tag1", "tag2"]));
        assert_eq!(p["message"],  "hello");
        assert_eq!(p["title"],    "Alert");
        assert_eq!(p["priority"], "3");
        assert_eq!(p["tags"],     "tag1,tag2");
    }

    #[test]
    fn message_to_params_empty_tags_joins_empty() {
        let p = message_to_params(&msg("hi", "", 0, vec![]));
        assert_eq!(p["tags"], "");
    }
}
