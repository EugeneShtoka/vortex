# Plan: Generalize ntfy listener → generic `[[inputs.sse]]`

## Goal

Replace the hardcoded ntfy SSE subscriber (`ntfy.rs`, `NtfyListenerConfig`) with a
protocol-agnostic `[[inputs.sse]]` input. No new dependencies. All behaviour preserved
for the ntfy use-case via config.

---

## Phase 0: Allowed APIs (reference only)

All patterns already exist in the codebase — no new crates needed.

| Need | Existing pattern | Location |
|---|---|---|
| HTTP streaming | `reqwest::Client::get().send().await?.bytes_stream()` | `ntfy.rs:43-49` |
| Newline-delimited JSON parse | `serde_json::from_str(line)` | `ntfy.rs:65` |
| Auth token resolution | `crate::auth::resolve_token(method, key)` | `ntfy.rs:93` |
| Broadcast trigger events | `event_tx.send(Event::TriggerReceived { … })` | `ntfy.rs:79-80` |
| Engine dispatch | `Engine::new(wf, db).with_events(tx).with_run_id(id).with_params(p).run(wf_name)` | `ntfy.rs:82-88` |
| Config deserialization | `#[serde(default)]`, `#[serde(rename_all = "snake_case")]` | `config.rs` |
| Watch channel forwarding | `config_rx: watch::Receiver<Arc<Config>>` | `main.rs:41-45` |

Anti-patterns to avoid:
- Do NOT construct `{server}/{topic}/json` (ntfy-specific — gone)
- Do NOT hardcode field names (`message`, `title`, `priority`, `tags`, `topic`)
- Do NOT add new crate dependencies

---

## Phase 1: Config — `SseListenerConfig`

**What to implement**

In `vortexd/src/config.rs`:

1. Write the test **first**:

```rust
#[test]
fn parses_sse_input_config() {
    let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[[inputs.sse]]
url      = "https://ntfy.example.com/alerts/json"
workflow = "handle_alert"
fields   = ["message", "title", "topic"]

[[inputs.sse]]
url          = "https://other.example.com/events"
workflow     = "handle_other"
event_filter = "update"
auth_method  = "env"
auth_key     = "SSE_TOKEN"
fields       = ["data", "id"]
[inputs.sse.params]  # this is the second array entry — use [[inputs.sse]] + [inputs.sse.params]
source = "other"
"#;
    let cfg = parse_config(toml).unwrap();
    assert_eq!(cfg.inputs.sse.len(), 2);
    let first = &cfg.inputs.sse[0];
    assert_eq!(first.url, "https://ntfy.example.com/alerts/json");
    assert_eq!(first.workflow, "handle_alert");
    assert_eq!(first.fields, vec!["message", "title", "topic"]);
    assert!(first.event_filter.is_none());
    let second = &cfg.inputs.sse[1];
    assert_eq!(second.event_filter.as_deref(), Some("update"));
    assert_eq!(second.params["source"], "other");
}

#[test]
fn sse_input_empty_by_default() {
    let cfg = parse_config(SAMPLE).unwrap();
    assert!(cfg.inputs.sse.is_empty());
}
```

2. Add the struct (replacing `NtfyListenerConfig`):

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct SseListenerConfig {
    pub url:          String,
    pub workflow:     String,
    pub auth_method:  Option<String>,
    pub auth_key:     Option<String>,
    /// Top-level JSON keys to promote to trigger params.
    #[serde(default)]
    pub fields:       Vec<String>,
    /// If set, only process lines where `.event == event_filter`.
    pub event_filter: Option<String>,
    /// Static extra params merged into every trigger.
    #[serde(default)]
    pub params:       HashMap<String, String>,
}
```

3. Update `InputsConfig`:

```rust
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InputsConfig {
    #[serde(default)]
    pub sse: Vec<SseListenerConfig>,
}
```

4. Delete `NtfyListenerConfig` struct and the `parses_ntfy_input_config` test.

**Verification**
- `cargo test -p vortexd config` passes (including the two new tests)
- `grep -r NtfyListenerConfig vortexd/src/` returns nothing
- `grep -r "inputs.ntfy" vortexd/src/` returns nothing

---

## Phase 2: `sse.rs` — generic SSE listener

**What to implement**

Create `vortexd/src/sse.rs`. Model structure on `ntfy.rs` exactly; replace ntfy-specific parts.

1. Write tests **first** (unit tests, no network):

```rust
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
        assert!(!p.contains_key("tags"));   // not declared in fields
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
```

2. Implement the module:

```rust
// vortexd/src/sse.rs
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
```

**Verification**
- `cargo test -p vortexd sse` passes (all 7 unit tests)
- `grep -n "ntfy" vortexd/src/sse.rs` returns nothing

---

## Phase 3: Wire `sse.rs` into `main.rs`, delete `ntfy.rs`

**What to implement**

In `vortexd/src/main.rs`:

1. Replace `mod ntfy;` with `mod sse;`
2. Replace the listener-spawning block:

```rust
// Before:
let ntfy_cfgs = config_rx.borrow().inputs.ntfy.clone();
for ntfy_cfg in ntfy_cfgs {
    let rx = config_rx.clone();
    let tx = event_tx.clone();
    tokio::spawn(async move { ntfy::listen(ntfy_cfg, rx, tx).await });
}

// After:
let sse_cfgs = config_rx.borrow().inputs.sse.clone();
for sse_cfg in sse_cfgs {
    let rx = config_rx.clone();
    let tx = event_tx.clone();
    tokio::spawn(async move { sse::listen(sse_cfg, rx, tx).await });
}
```

3. Delete `vortexd/src/ntfy.rs`.

**Verification**
- `cargo build -p vortexd` succeeds (zero errors)
- `cargo test -p vortexd` passes
- `ls vortexd/src/ntfy.rs` returns "no such file"
- `grep -r "ntfy" vortexd/src/` returns nothing

---

## Phase 4: VPS migration (manual)

After deploying the new binary to the VPS, update `/etc/nixos/orchestrator.nix`.

**Change in the embedded workflow TOML** (inside the `vortexd` service config):

```toml
# Before:
[[inputs.ntfy]]
server      = "https://ntfy.example.com"
topic       = "my-topic"
workflow    = "mx-message"
auth_method = "env"
auth_key    = "NTFY_TOKEN"
[inputs.ntfy.params]
foo = "bar"

# After:
[[inputs.sse]]
url         = "https://ntfy.example.com/my-topic/json"
workflow    = "mx-message"
event_filter = "message"
fields      = ["message", "title", "priority", "tags", "topic"]
auth_method = "env"
auth_key    = "NTFY_TOKEN"
[inputs.sse.params]
foo = "bar"
```

Key differences:
- `url` = `{server}/{topic}/json` (combine the two old fields)
- `event_filter = "message"` (replaces the hardcoded `msg.event != "message"` check)
- `fields` lists the ntfy fields that were previously hardcoded in `message_to_params()`
- `[inputs.sse.params]` replaces `[inputs.ntfy.params]`

Then rebuild + switch:
```bash
# push new commit, update rev+hash in orchestrator.nix (standard deploy procedure)
ssh vps "sudo nixos-rebuild switch"
```
