use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub workflows: HashMap<String, WorkflowConfig>,
    #[serde(default)]
    pub inputs: InputsConfig,
    pub email: Option<EmailConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub unix_socket: String,
    pub network: Option<NetworkConfig>,
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

fn default_db_path() -> String {
    "./vortex.db".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetworkConfig {
    pub enabled: bool,
    pub bind: String,
    pub auth_method: String,
    pub auth_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowConfig {
    pub tasks: Vec<TaskConfig>,
    #[serde(default)]
    pub cron: Option<String>,
    /// Handlebars template evaluated against trigger params to determine the
    /// correlation ID included in the response. Falls back to trigger.correlation_id
    /// → trigger.id → UUID if omitted or if the rendered value is empty.
    pub correlation_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskConfig {
    pub id: String,
    #[serde(flatten)]
    pub kind: TaskKind,
    /// CEL expression evaluated at runtime. Has access to `tasks.<id>.{success,stdout,
    /// stderr,exit_code}`, `trigger.<key>`, `env.<KEY>` (JSON-parsed), `globals.<key>`,
    /// `correlation_id`, and bare task-ID booleans for backward compat.
    pub when: Option<String>,
    /// Explicit ordering dependencies. Tasks listed here are guaranteed to run before
    /// this task regardless of `when`. When omitted, deps are inferred from task-ID
    /// tokens in `when` for backward compat.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Handlebars template rendered after the task succeeds. The rendered string
    /// becomes the workflow response instead of raw stdout. Has access to all prior
    /// task results (including this task's own stdout via `{{tasks.<id>.stdout}}`),
    /// trigger params, globals, env vars, and `{{correlation_id}}`.
    pub response_template: Option<String>,
}

/// Task type dispatched by the engine. Tagged: `type = "shell"` etc. in TOML.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskKind {
    Shell   { exec: String },
    Http    { url: String, #[serde(default = "default_method")] method: String, #[serde(default)] headers: HashMap<String, String>, body: Option<String> },
    Email   { to: String, subject: String, body: String, cc: Option<String> },
    Sleep   { duration: String },
    StoreSet { set: HashMap<String, String> },
    Peer    { vortex: String, trigger: String, #[serde(default)] params: HashMap<String, String> },
    /// Spawns a binary directly (no shell). Trigger params JSON is piped to stdin and set as
    /// VORTEX_TRIGGER_PARAMS. Each element of `args` is passed as a separate argv entry —
    /// no quoting or escaping needed.
    Spawn   { exe: String, #[serde(default)] args: Vec<String> },
    /// Renders a Handlebars template and returns the result as the workflow response.
    /// No subprocess is spawned. Has access to all prior task results, trigger params,
    /// globals, env vars, and `{{correlation_id}}`. Use as the last task in a workflow
    /// to explicitly shape the reply to the caller.
    Response { template: String },
    /// Evaluates a CEL expression and exposes the result as task success/failure.
    /// exit_code 0 = true, 1 = false, 2 = evaluation error.
    /// Has access to the same context as `when` gates: tasks.*, trigger.*, env.*, globals.*.
    Condition { expr: String },
    /// Evaluates a CEL expression and returns the result as stdout.
    /// Success/failure is determined by truthiness: non-empty string, true, non-zero int,
    /// non-empty list/map succeed; false, empty string, 0, empty list/map, null fail.
    /// Evaluation errors (including index-out-of-bounds) also fail.
    /// Supersedes `condition` — bool expressions work identically; use this when the
    /// result value is needed downstream via `{{tasks.<id>.stdout}}` or `output`.
    Eval { expr: String },
}

fn default_method() -> String {
    "GET".to_string()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct InputsConfig {
    #[serde(default)]
    pub ntfy: Vec<NtfyListenerConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NtfyListenerConfig {
    pub server: String,
    pub topic: String,
    pub workflow: String,
    pub auth_method: Option<String>,
    pub auth_key: Option<String>,
    #[serde(default)]
    pub params: HashMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EmailConfig {
    pub smtp_host: String,
    #[serde(default = "default_smtp_port")]
    pub smtp_port: u16,
    pub from: String,
    pub auth_method: String,
    pub auth_key: String,
}

fn default_smtp_port() -> u16 {
    587
}

pub fn load_config(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {path}"))?;
    parse_config(&content).with_context(|| format!("Failed to parse config file: {path}"))
}

pub fn parse_config(toml: &str) -> Result<Config> {
    Ok(toml::from_str(toml)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
[server]
unix_socket = "/tmp/vortex.sock"

[server.network]
enabled = true
bind = "0.0.0.0:9000"
auth_method = "env"
auth_key = "VORTEX_TOKEN"

[workflows.deploy]
tasks = [
  { id = "pull_code",   type = "shell", exec = "git pull" },
  { id = "build",       type = "shell", exec = "cargo build", when = "pull_code" },
  { id = "notify_fail", type = "shell", exec = "echo failed",  when = "NOT build" },
]
"#;

    #[test]
    fn parses_server_config() {
        let cfg = parse_config(SAMPLE).unwrap();
        assert_eq!(cfg.server.unix_socket, "/tmp/vortex.sock");
        let net = cfg.server.network.unwrap();
        assert!(net.enabled);
        assert_eq!(net.bind, "0.0.0.0:9000");
        assert_eq!(net.auth_method, "env");
        assert_eq!(net.auth_key, "VORTEX_TOKEN");
    }

    #[test]
    fn parses_workflows_and_tasks() {
        let cfg = parse_config(SAMPLE).unwrap();
        let deploy = cfg.workflows.get("deploy").unwrap();
        assert_eq!(deploy.tasks.len(), 3);
        assert_eq!(deploy.tasks[0].id, "pull_code");
        assert_eq!(deploy.tasks[1].when.as_deref(), Some("pull_code"));
        assert_eq!(deploy.tasks[2].when.as_deref(), Some("NOT build"));
    }

    #[test]
    fn shell_task_kind_parsed() {
        let cfg = parse_config(SAMPLE).unwrap();
        let t = &cfg.workflows["deploy"].tasks[0];
        assert!(matches!(t.kind, TaskKind::Shell { .. }));
    }

    #[test]
    fn db_path_defaults_when_omitted() {
        let cfg = parse_config(SAMPLE).unwrap();
        assert_eq!(cfg.server.db_path, "./vortex.db");
    }

    #[test]
    fn db_path_can_be_overridden() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"
db_path = "/var/lib/vortex/state.db"

[workflows]
"#;
        let cfg = parse_config(toml).unwrap();
        assert_eq!(cfg.server.db_path, "/var/lib/vortex/state.db");
    }

    #[test]
    fn no_network_is_optional() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows]
"#;
        let cfg = parse_config(toml).unwrap();
        assert!(cfg.server.network.is_none());
        assert!(cfg.workflows.is_empty());
    }

    #[test]
    fn invalid_toml_returns_error() {
        assert!(parse_config("not = valid = toml [[[").is_err());
    }

    #[test]
    fn hyphenated_workflow_name_parses() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.sync-and-build]
tasks = [
  { id = "pull", type = "shell", exec = "git pull" },
]
"#;
        let cfg = parse_config(toml).unwrap();
        assert!(cfg.workflows.contains_key("sync-and-build"));
    }

    const TASK_TYPES_SAMPLE: &str = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.test]
tasks = [
  { id = "shell_task",  type = "shell",     exec = "echo hi" },
  { id = "http_task",   type = "http",      url = "https://example.com/api", method = "POST", body = "{}" },
  { id = "sleep_task",  type = "sleep",     duration = "100ms" },
  { id = "email_task",  type = "email",     to = "a@b.com", subject = "Hi", body = "body" },
  { id = "store_set",   type = "store_set", set = { version = "1.0" } },
  { id = "eval_task",   type = "eval",      expr = "trigger.x == \"y\"" },
]
"#;

    #[test]
    fn parses_all_task_kinds() {
        let cfg = parse_config(TASK_TYPES_SAMPLE).unwrap();
        let tasks = &cfg.workflows["test"].tasks;
        assert_eq!(tasks.len(), 6);
        assert!(matches!(tasks[0].kind, TaskKind::Shell { .. }));
        assert!(matches!(tasks[1].kind, TaskKind::Http  { .. }));
        assert!(matches!(tasks[2].kind, TaskKind::Sleep { .. }));
        assert!(matches!(tasks[3].kind, TaskKind::Email  { .. }));
        assert!(matches!(tasks[4].kind, TaskKind::StoreSet { .. }));
        assert!(matches!(tasks[5].kind, TaskKind::Eval { .. }));
    }

    #[test]
    fn http_task_defaults_method_to_get() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"
[workflows.w]
tasks = [{ id = "t", type = "http", url = "https://example.com" }]
"#;
        let cfg = parse_config(toml).unwrap();
        if let TaskKind::Http { method, .. } = &cfg.workflows["w"].tasks[0].kind {
            assert_eq!(method, "GET");
        } else {
            panic!("expected Http kind");
        }
    }

    #[test]
    fn parses_ntfy_input_config() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[[inputs.ntfy]]
server   = "https://ntfy.sh"
topic    = "alerts"
workflow = "handle_alert"
"#;
        let cfg = parse_config(toml).unwrap();
        assert_eq!(cfg.inputs.ntfy.len(), 1);
        assert_eq!(cfg.inputs.ntfy[0].topic, "alerts");
        assert_eq!(cfg.inputs.ntfy[0].workflow, "handle_alert");
    }

    #[test]
    fn parses_email_config() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[email]
smtp_host   = "smtp.example.com"
from        = "bot@example.com"
auth_method = "env"
auth_key    = "SMTP_PASS"
"#;
        let cfg = parse_config(toml).unwrap();
        let email = cfg.email.unwrap();
        assert_eq!(email.smtp_host, "smtp.example.com");
        assert_eq!(email.smtp_port, 587);
    }

    #[test]
    fn parses_cron_field() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.backup]
cron  = "0 2 * * *"
tasks = [{ id = "run", type = "shell", exec = "backup.sh" }]
"#;
        let cfg = parse_config(toml).unwrap();
        assert_eq!(cfg.workflows["backup"].cron.as_deref(), Some("0 2 * * *"));
    }

    #[test]
    fn cron_is_optional_on_workflow() {
        let cfg = parse_config(SAMPLE).unwrap();
        assert!(cfg.workflows["deploy"].cron.is_none());
    }

    #[test]
    fn parses_spawn_task_with_args() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.w]
tasks = [
  { id = "filter", type = "spawn", exe = "jx-match", args = ["-e", "Sender contains \"@whatsapp\""] },
]
"#;
        let cfg = parse_config(toml).unwrap();
        let task = &cfg.workflows["w"].tasks[0];
        assert_eq!(task.id, "filter");
        if let TaskKind::Spawn { exe, args } = &task.kind {
            assert_eq!(exe, "jx-match");
            assert_eq!(args, &["-e", "Sender contains \"@whatsapp\""]);
        } else {
            panic!("expected Spawn kind");
        }
    }

    #[test]
    fn parses_spawn_task_without_args() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.w]
tasks = [{ id = "t", type = "spawn", exe = "true" }]
"#;
        let cfg = parse_config(toml).unwrap();
        if let TaskKind::Spawn { exe, args } = &cfg.workflows["w"].tasks[0].kind {
            assert_eq!(exe, "true");
            assert!(args.is_empty());
        } else {
            panic!("expected Spawn kind");
        }
    }

    #[test]
    fn parses_response_task() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.w]
tasks = [{ id = "reply", type = "response", template = "{{json trigger.text}}" }]
"#;
        let cfg = parse_config(toml).unwrap();
        let task = &cfg.workflows["w"].tasks[0];
        assert_eq!(task.id, "reply");
        if let TaskKind::Response { template } = &task.kind {
            assert_eq!(template, "{{json trigger.text}}");
        } else {
            panic!("expected Response kind");
        }
    }

    #[test]
    fn parses_response_template_field_on_task() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.w]
tasks = [{ id = "check", type = "spawn", exe = "spam.sh", response_template = "{\"status\":\"drop\"}" }]
"#;
        let cfg = parse_config(toml).unwrap();
        let task = &cfg.workflows["w"].tasks[0];
        assert_eq!(task.response_template.as_deref(), Some("{\"status\":\"drop\"}"));
    }

    #[test]
    fn parses_condition_task() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"
[workflows.w]
tasks = [{ id = "check", type = "condition", expr = "trigger.sender == \"@alice:server\"" }]
"#;
        let cfg = parse_config(toml).unwrap();
        if let TaskKind::Condition { expr } = &cfg.workflows["w"].tasks[0].kind {
            assert_eq!(expr, "trigger.sender == \"@alice:server\"");
        } else {
            panic!("expected Condition kind");
        }
    }

    #[test]
    fn parses_eval_task() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"
[workflows.w]
tasks = [{ id = "find", type = "eval", expr = "env.SPACES.filter(s, s.id == trigger.room).map(s, s.name)[0]" }]
"#;
        let cfg = parse_config(toml).unwrap();
        if let TaskKind::Eval { expr } = &cfg.workflows["w"].tasks[0].kind {
            assert!(expr.contains("SPACES"));
        } else {
            panic!("expected Eval kind");
        }
    }

    #[test]
    fn parses_correlation_id_on_workflow() {
        let toml = r#"
[server]
unix_socket = "/tmp/v.sock"

[workflows.w]
correlation_id = "{{trigger.event_id}}"
tasks = [{ id = "t", type = "shell", exec = "true" }]
"#;
        let cfg = parse_config(toml).unwrap();
        assert_eq!(cfg.workflows["w"].correlation_id.as_deref(), Some("{{trigger.event_id}}"));
    }

    #[test]
    fn correlation_id_is_optional() {
        let cfg = parse_config(SAMPLE).unwrap();
        assert!(cfg.workflows["deploy"].correlation_id.is_none());
    }
}
