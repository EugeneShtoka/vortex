use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

const DEFAULT_HISTORY_LIMIT: usize = 10;

#[derive(Deserialize, Default)]
struct TomlFile {
    tui: Option<TomlTui>,
}

#[derive(Deserialize, Default)]
pub(crate) struct TomlTui {
    url:           Option<String>,
    token:         Option<String>,
    history_limit: Option<usize>,
    sources:       Option<Vec<TomlSource>>,
}

#[derive(Deserialize)]
struct TomlSource {
    name:          String,
    url:           String,
    token:         String,
    history_limit: Option<usize>,
}

pub struct SourceConfig {
    pub name:          String,
    pub url:           String,
    pub token:         String,
    pub history_limit: usize,
    pub http_base:     String,
}

impl SourceConfig {
    fn new(name: impl Into<String>, url: impl Into<String>, token: impl Into<String>, history_limit: usize) -> Self {
        let url = url.into();
        let http_base = ws_to_http(&url);
        Self { name: name.into(), url, token: token.into(), history_limit, http_base }
    }
}

pub struct TuiConfig {
    pub sources: Vec<SourceConfig>,
}

impl TuiConfig {
    /// Load from a TOML file. Returns an error only if the file exists but is unparseable.
    pub fn load(path: &Path) -> Result<TomlTui> {
        if !path.exists() {
            return Ok(TomlTui::default());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()))?;
        let file: TomlFile = toml::from_str(&text)
            .with_context(|| format!("parsing {}", path.display()))?;
        Ok(file.tui.unwrap_or_default())
    }

    /// Build final config. CLI --url creates a single source (overrides [[tui.sources]]).
    pub fn resolve(
        file: TomlTui,
        cli_url: Option<String>,
        cli_token: Option<String>,
    ) -> Result<Self> {
        // CLI --url: single source, overrides any [[tui.sources]] in TOML
        if let Some(url) = cli_url {
            let token = cli_token.or(file.token)
                .context("--token is required (or set token in vortex.toml)")?;
            let history_limit = file.history_limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
            return Ok(Self { sources: vec![SourceConfig::new("default", url, token, history_limit)] });
        }

        // [[tui.sources]] array
        if let Some(toml_sources) = file.sources.filter(|s| !s.is_empty()) {
            let global_limit = file.history_limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
            let sources = toml_sources.into_iter()
                .map(|s| SourceConfig::new(s.name, s.url, s.token, s.history_limit.unwrap_or(global_limit)))
                .collect();
            return Ok(Self { sources });
        }

        // Legacy single-source: [tui] url + token
        let url = file.url.context("--url is required (or set [tui] url / [[tui.sources]] in vortex.toml)")?;
        let token = cli_token.or(file.token)
            .context("--token is required (or set [tui] token in vortex.toml)")?;
        let history_limit = file.history_limit.unwrap_or(DEFAULT_HISTORY_LIMIT);
        Ok(Self { sources: vec![SourceConfig::new("default", url, token, history_limit)] })
    }
}

fn ws_to_http(url: &str) -> String {
    url.replacen("wss://", "https://", 1)
       .replacen("ws://",  "http://",  1)
       .trim_end_matches("/ws")
       .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_toml(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    // --- legacy single-source (backward compat) ---

    #[test]
    fn missing_file_returns_defaults() {
        let cfg = TuiConfig::resolve(
            TuiConfig::load(Path::new("/nonexistent/vortex.toml")).unwrap(),
            Some("ws://localhost:9000/ws".into()),
            Some("tok".into()),
        ).unwrap();
        assert_eq!(cfg.sources.len(), 1);
        assert_eq!(cfg.sources[0].history_limit, DEFAULT_HISTORY_LIMIT);
    }

    #[test]
    fn history_limit_read_from_toml() {
        let f = write_toml("[tui]\nhistory_limit = 25\n");
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, Some("ws://h/ws".into()), Some("t".into())).unwrap();
        assert_eq!(cfg.sources[0].history_limit, 25);
    }

    #[test]
    fn cli_url_overrides_toml() {
        let f = write_toml("[tui]\nurl = \"ws://from-file/ws\"\ntoken = \"t\"\n");
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, Some("ws://from-cli/ws".into()), None).unwrap();
        assert_eq!(cfg.sources[0].url, "ws://from-cli/ws");
    }

    #[test]
    fn token_falls_back_to_toml() {
        let f = write_toml("[tui]\nurl = \"ws://h/ws\"\ntoken = \"fromfile\"\n");
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        assert_eq!(cfg.sources[0].token, "fromfile");
    }

    #[test]
    fn missing_url_and_token_returns_error() {
        let cfg = TuiConfig::resolve(TomlTui::default(), None, None);
        assert!(cfg.is_err());
    }

    #[test]
    fn ws_url_converted_to_http_base() {
        let f = write_toml("[tui]\nurl = \"ws://localhost:9000/ws\"\ntoken = \"t\"\n");
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        assert_eq!(cfg.sources[0].http_base, "http://localhost:9000");
    }

    #[test]
    fn wss_url_converted_to_https_base() {
        let cfg = TuiConfig::resolve(
            TomlTui::default(),
            Some("wss://example.com/ws".into()),
            Some("t".into()),
        ).unwrap();
        assert_eq!(cfg.sources[0].http_base, "https://example.com");
    }

    // --- multi-source: [[tui.sources]] ---

    #[test]
    fn sources_array_creates_multiple_sources() {
        let f = write_toml(r#"
[[tui.sources]]
name = "local"
url = "ws://localhost:9000/ws"
token = "tok1"

[[tui.sources]]
name = "prod"
url = "wss://prod.example.com/ws"
token = "tok2"
"#);
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        assert_eq!(cfg.sources.len(), 2);
        assert_eq!(cfg.sources[0].name, "local");
        assert_eq!(cfg.sources[0].url, "ws://localhost:9000/ws");
        assert_eq!(cfg.sources[0].token, "tok1");
        assert_eq!(cfg.sources[1].name, "prod");
        assert_eq!(cfg.sources[1].url, "wss://prod.example.com/ws");
        assert_eq!(cfg.sources[1].token, "tok2");
    }

    #[test]
    fn sources_array_derives_http_base_per_source() {
        let f = write_toml(r#"
[[tui.sources]]
name = "local"
url = "ws://localhost:9000/ws"
token = "t"

[[tui.sources]]
name = "prod"
url = "wss://prod.example.com/ws"
token = "t"
"#);
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        assert_eq!(cfg.sources[0].http_base, "http://localhost:9000");
        assert_eq!(cfg.sources[1].http_base, "https://prod.example.com");
    }

    #[test]
    fn global_history_limit_applies_to_sources() {
        let f = write_toml(r#"
[tui]
history_limit = 20

[[tui.sources]]
name = "local"
url = "ws://localhost:9000/ws"
token = "t"

[[tui.sources]]
name = "prod"
url = "ws://prod:9000/ws"
token = "t"
"#);
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        assert_eq!(cfg.sources[0].history_limit, 20);
        assert_eq!(cfg.sources[1].history_limit, 20);
    }

    #[test]
    fn per_source_history_limit_overrides_global() {
        let f = write_toml(r#"
[tui]
history_limit = 20

[[tui.sources]]
name = "local"
url = "ws://localhost:9000/ws"
token = "t"
history_limit = 5
"#);
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        assert_eq!(cfg.sources[0].history_limit, 5);
    }

    #[test]
    fn cli_url_overrides_sources_array() {
        let f = write_toml(r#"
[[tui.sources]]
name = "local"
url = "ws://from-toml/ws"
token = "toml-tok"
"#);
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, Some("ws://from-cli/ws".into()), Some("cli-tok".into())).unwrap();
        assert_eq!(cfg.sources.len(), 1);
        assert_eq!(cfg.sources[0].url, "ws://from-cli/ws");
        assert_eq!(cfg.sources[0].token, "cli-tok");
    }

    #[test]
    fn sources_array_declaration_order_preserved() {
        let f = write_toml(r#"
[[tui.sources]]
name = "a"
url = "ws://a/ws"
token = "t"

[[tui.sources]]
name = "b"
url = "ws://b/ws"
token = "t"

[[tui.sources]]
name = "c"
url = "ws://c/ws"
token = "t"
"#);
        let toml = TuiConfig::load(f.path()).unwrap();
        let cfg = TuiConfig::resolve(toml, None, None).unwrap();
        let names: Vec<&str> = cfg.sources.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }
}
