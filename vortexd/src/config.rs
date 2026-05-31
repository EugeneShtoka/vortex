use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    #[serde(default)]
    pub workflows: HashMap<String, WorkflowConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub unix_socket: String,
    pub network: Option<NetworkConfig>,
    /// Path to the SQLite state database. Defaults to `./vortex.db`.
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
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskConfig {
    pub id: String,
    pub exec: String,
    pub when: Option<String>,
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
  { id = "pull_code", exec = "git pull" },
  { id = "build",       exec = "cargo build", when = "pull_code" },
  { id = "notify_fail", exec = "echo failed",  when = "NOT build" },
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
  { id = "pull", exec = "git pull" },
]
"#;
        let cfg = parse_config(toml).unwrap();
        assert!(cfg.workflows.contains_key("sync-and-build"));
    }
}
