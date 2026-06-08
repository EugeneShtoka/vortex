use std::collections::HashMap;

use anyhow::{bail, Result};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use lettre::transport::smtp::authentication::Credentials;
use reqwest::Client;
use tokio::process::Command;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::config::{EmailConfig, TaskConfig, TaskKind, WorkflowConfig};
use crate::event::Event;
use crate::gate;
use crate::store::Store;
use crate::template;

// ── result types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub id:        String,
    pub stdout:    String,
    pub stderr:    String,
    pub exit_code: i32,
    pub success:   bool,
    pub output:    Option<serde_json::Value>,
    pub status:    Option<u16>,
    /// Rendered response_template (or Response task output). When set on the last
    /// successful task that has one, this becomes the workflow's response to the caller.
    pub response:  Option<String>,
}

struct TaskOutcome {
    stdout:    String,
    stderr:    String,
    exit_code: i32,
    success:   bool,
    output:    Option<serde_json::Value>,
    status:    Option<u16>,
}

// ── engine ────────────────────────────────────────────────────────────────────

pub struct Engine {
    config:         WorkflowConfig,
    db_path:        String,
    event_tx:       Option<broadcast::Sender<Event>>,
    run_id:         Option<String>,
    trigger_params: HashMap<String, String>,
    email_config:   Option<EmailConfig>,
    correlation_id: String,
}

impl Engine {
    pub fn new(config: WorkflowConfig, db_path: &str) -> Self {
        Self { config, db_path: db_path.to_string(), event_tx: None, run_id: None, trigger_params: HashMap::new(), email_config: None, correlation_id: String::new() }
    }

    pub fn with_events(mut self, tx: broadcast::Sender<Event>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    pub fn with_correlation_id(mut self, id: String) -> Self {
        self.correlation_id = id;
        self
    }

    pub fn with_run_id(mut self, id: String) -> Self {
        self.run_id = Some(id);
        self
    }

    pub fn with_params(mut self, params: HashMap<String, String>) -> Self {
        self.trigger_params = params;
        self
    }

    pub fn with_email_config(mut self, cfg: EmailConfig) -> Self {
        self.email_config = Some(cfg);
        self
    }

    fn emit(&self, event: Event) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(event);
        }
    }

    pub async fn run(&self, workflow_name: &str) -> Result<Vec<TaskResult>> {
        let run_id = self.run_id.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let ordered = self.topological_sort()?;

        info!(workflow = workflow_name, tasks = ordered.len(), "Starting workflow run");
        let started_at = vortex_core::now_ms();
        self.emit(Event::WorkflowStarted { run_id: run_id.clone(), workflow: workflow_name.to_string(), timestamp: started_at });

        let store = Store::open(&self.db_path)?;
        let mut globals = store.get_all()?;
        store.insert_run(&run_id, workflow_name, &serde_json::to_string(&self.trigger_params).unwrap_or_else(|_| "{}".into()), started_at)?;

        let all_ids: Vec<&str> = self.config.tasks.iter().map(|t| t.id.as_str()).collect();
        let mut results: HashMap<String, TaskResult> = HashMap::new();
        let mut all_results = Vec::new();

        let mut response_task_count = 0usize;
        for task in &ordered {
            if task.response_template.is_some() || matches!(task.kind, TaskKind::Response { .. }) {
                response_task_count += 1;
            }
        }
        if response_task_count > 1 {
            error!(workflow = workflow_name, count = response_task_count, "Multiple response tasks defined — only the last successful one will be used");
        }

        for task in &ordered {
            if !self.gate_allows(task, &results, &all_ids, &globals)? {
                warn!(task = %task.id, "Skipped (gate not met)");
                let ts = vortex_core::now_ms();
                store.upsert_task(&run_id, &task.id, "skipped", None, None, None, Some(ts), Some(ts))?;
                self.emit(Event::TaskSkipped { run_id: run_id.clone(), task: task.id.clone(), timestamp: ts });
                continue;
            }

            let mut result = self.run_task(task, &run_id, &results, &globals).await?;

            if result.success {
                if matches!(task.kind, TaskKind::Response { .. }) {
                    // Response task: stdout IS the rendered template, promote to response
                    result.response = Some(result.stdout.clone());
                } else if let Some(tmpl) = &task.response_template {
                    let mut rmap = results.clone();
                    rmap.insert(result.id.clone(), result.clone());
                    match template::render(tmpl, &rmap, &globals, &self.trigger_params, &self.correlation_id) {
                        Ok(rendered) => result.response = Some(rendered),
                        Err(e) => error!(task = %task.id, "response_template render error: {e:#}"),
                    }
                }
            }

            if result.success {
                if let TaskKind::StoreSet { set } = &task.kind {
                    if let Ok(rendered) = render_map(set, &results, &globals, &self.trigger_params, &self.correlation_id) {
                        globals.extend(rendered);
                    }
                }
            }

            results.insert(result.id.clone(), result.clone());
            all_results.push(result);
        }

        let overall_success = all_results.iter().all(|r| r.success);
        let finished_at = vortex_core::now_ms();
        store.finish_run(&run_id, overall_success, finished_at)?;
        self.emit(Event::WorkflowFinished {
            run_id,
            workflow: workflow_name.to_string(),
            success: overall_success,
            timestamp: finished_at,
        });

        Ok(all_results)
    }

    async fn run_task(
        &self,
        task: &TaskConfig,
        run_id: &str,
        results: &HashMap<String, TaskResult>,
        globals: &HashMap<String, String>,
    ) -> Result<TaskResult> {
        let started_at = vortex_core::now_ms();
        let store = Store::open(&self.db_path)?;
        store.upsert_task(run_id, &task.id, "running", None, None, None, Some(started_at), None)?;
        self.emit(Event::TaskStarted { run_id: run_id.to_string(), task: task.id.clone(), timestamp: started_at });

        let outcome = self.dispatch_task(task, results, globals).await.unwrap_or_else(|e| {
            error!(task = %task.id, "Task failed: {e:#}");
            TaskOutcome { stdout: String::new(), stderr: e.to_string(), exit_code: -1, success: false, output: None, status: None }
        });

        let finished_at = vortex_core::now_ms();
        if !outcome.stdout.trim().is_empty() { info!(task = %task.id, "stdout: {}", outcome.stdout.trim_end()); }
        if !outcome.stderr.trim().is_empty() { error!(task = %task.id, "stderr: {}", outcome.stderr.trim_end()); }
        if outcome.success { info!(task = %task.id, "Finished OK") } else { warn!(task = %task.id, "Finished with error") }

        let status_str = if outcome.success { "success" } else { "failure" };
        store.upsert_task(run_id, &task.id, status_str, Some(outcome.exit_code),
            Some(&outcome.stdout), Some(&outcome.stderr), Some(started_at), Some(finished_at))?;
        self.emit(Event::TaskFinished {
            run_id: run_id.to_string(), task: task.id.clone(),
            success: outcome.success, exit_code: outcome.exit_code,
            stdout: outcome.stdout.clone(), stderr: outcome.stderr.clone(),
            timestamp: finished_at,
        });

        Ok(TaskResult { id: task.id.clone(), stdout: outcome.stdout, stderr: outcome.stderr,
            exit_code: outcome.exit_code, success: outcome.success, output: outcome.output, status: outcome.status, response: None })
    }

    async fn dispatch_task(
        &self,
        task: &TaskConfig,
        results: &HashMap<String, TaskResult>,
        globals: &HashMap<String, String>,
    ) -> Result<TaskOutcome> {
        let cid = &self.correlation_id;
        match &task.kind {
            TaskKind::Shell { exec } => {
                let cmd = template::render(exec, results, globals, &self.trigger_params, cid)?;
                execute_shell(&task.id, &cmd, &self.trigger_params).await
            }
            TaskKind::Http { url, method, headers, body } => {
                let url  = template::render(url, results, globals, &self.trigger_params, cid)?;
                let body = body.as_ref().map(|b| template::render(b, results, globals, &self.trigger_params, cid)).transpose()?;
                let hdrs = render_map(headers, results, globals, &self.trigger_params, cid)?;
                execute_http(method, &url, &hdrs, body.as_deref()).await
            }
            TaskKind::Notify { server, topic, message, title, priority, tags, token } => {
                let msg   = template::render(message, results, globals, &self.trigger_params, cid)?;
                let title = title.as_ref().map(|t| template::render(t, results, globals, &self.trigger_params, cid)).transpose()?;
                let tok   = token.as_ref().map(|t| template::render(t, results, globals, &self.trigger_params, cid)).transpose()?;
                let srv   = server.as_deref().unwrap_or("https://ntfy.sh");
                execute_notify(srv, topic, &msg, title.as_deref(), priority.as_deref(), tags.as_deref(), tok.as_deref()).await
            }
            TaskKind::Email { to, subject, body, cc } => {
                let subject = template::render(subject, results, globals, &self.trigger_params, cid)?;
                let body    = template::render(body, results, globals, &self.trigger_params, cid)?;
                let cfg = self.email_config.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Email task requires [email] config section"))?;
                execute_email(to, &subject, &body, cc.as_deref(), cfg).await
            }
            TaskKind::Sleep { duration } => execute_sleep(duration).await,
            TaskKind::StoreSet { set } => {
                let rendered = render_map(set, results, globals, &self.trigger_params, cid)?;
                execute_store_set(rendered, &self.db_path).await
            }
            TaskKind::Peer { .. } => bail!("Peer tasks not yet implemented (Sprint 14)"),
            TaskKind::Spawn { exe, args } => {
                execute_spawn(&task.id, exe, args, &self.trigger_params).await
            }
            TaskKind::Response { template } => {
                let rendered = template::render(template, results, globals, &self.trigger_params, cid)?;
                Ok(TaskOutcome {
                    stdout: rendered, stderr: String::new(),
                    exit_code: 0, success: true, output: None, status: None,
                })
            }
            TaskKind::Condition { expr } => {
                let all_ids: Vec<&str> = results.keys().map(String::as_str).collect();
                match gate::evaluate(expr, results, &all_ids, &self.trigger_params, globals, cid) {
                    Ok(true)  => Ok(TaskOutcome { stdout: "true".into(),  stderr: String::new(), exit_code: 0, success: true,  output: None, status: None }),
                    Ok(false) => Ok(TaskOutcome { stdout: "false".into(), stderr: String::new(), exit_code: 1, success: false, output: None, status: None }),
                    Err(e)    => Ok(TaskOutcome { stdout: String::new(),  stderr: e.to_string(), exit_code: 2, success: false, output: None, status: None }),
                }
            }
        }
    }

    fn gate_allows(
        &self,
        task: &TaskConfig,
        results: &HashMap<String, TaskResult>,
        all_ids: &[&str],
        globals: &HashMap<String, String>,
    ) -> Result<bool> {
        let Some(expr) = &task.when else { return Ok(true) };
        gate::evaluate(expr, results, all_ids, &self.trigger_params, globals, &self.correlation_id)
    }

    /// Kahn's topological sort. Deps come from `depends_on` when set; otherwise
    /// inferred by scanning `when` for tokens that match task IDs (backward compat).
    fn topological_sort(&self) -> Result<Vec<TaskConfig>> {
        let tasks = &self.config.tasks;
        let task_ids: HashMap<&str, usize> =
            tasks.iter().enumerate().map(|(i, t)| (t.id.as_str(), i)).collect();

        let mut deps: Vec<Vec<usize>> = vec![vec![]; tasks.len()];
        for (i, task) in tasks.iter().enumerate() {
            let mut seen = std::collections::HashSet::new();
            if let Some(explicit) = &task.depends_on {
                for dep_id in explicit {
                    match task_ids.get(dep_id.as_str()) {
                        Some(&j) if seen.insert(j) => deps[i].push(j),
                        Some(_) => {}
                        None => bail!("Task '{}' depends_on unknown task '{dep_id}'", task.id),
                    }
                }
            } else if let Some(expr) = &task.when {
                for token in expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-') {
                    if token.is_empty() || matches!(token, "AND" | "OR" | "NOT") {
                        continue;
                    }
                    if let Some(&j) = task_ids.get(token) {
                        if seen.insert(j) { deps[i].push(j); }
                    }
                }
            }
        }

        let mut rev: Vec<Vec<usize>> = vec![vec![]; tasks.len()];
        for (i, task_deps) in deps.iter().enumerate() {
            for &j in task_deps { rev[j].push(i); }
        }

        let mut in_degree: Vec<usize> = deps.iter().map(|d| d.len()).collect();
        let mut queue: Vec<usize> = in_degree.iter().enumerate()
            .filter(|(_, &d)| d == 0).map(|(i, _)| i).collect();

        let mut ordered = Vec::with_capacity(tasks.len());
        while !queue.is_empty() {
            queue.sort_unstable();
            let cur = queue.remove(0);
            ordered.push(tasks[cur].clone());
            for &next in &rev[cur] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 { queue.push(next); }
            }
        }

        if ordered.len() != tasks.len() {
            bail!("Circular dependency detected in task graph");
        }
        Ok(ordered)
    }
}

// ── task executors ────────────────────────────────────────────────────────────

async fn execute_spawn(
    task_id: &str,
    exe: &str,
    args: &[String],
    trigger_params: &HashMap<String, String>,
) -> Result<TaskOutcome> {
    use tokio::io::AsyncWriteExt;

    let params_json = serde_json::to_string(trigger_params).unwrap_or_else(|_| "{}".into());
    info!(task = %task_id, exe = %exe, "Running spawn task");

    let mut child = Command::new(exe)
        .args(args)
        .env("VORTEX_TRIGGER_PARAMS", &params_json)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("failed to spawn {exe}: {e}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(params_json.as_bytes()).await?;
    }

    let output = child.wait_with_output().await?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    let exit_code = output.status.code().unwrap_or(-1);
    Ok(TaskOutcome { stdout, stderr, exit_code, success: output.status.success(), output: None, status: None })
}

async fn execute_shell(task_id: &str, exec: &str, trigger_params: &HashMap<String, String>) -> Result<TaskOutcome> {
    info!(task = %task_id, exec = %exec, "Running shell task");
    let params_json = serde_json::to_string(trigger_params).unwrap_or_else(|_| "{}".into());
    let output = Command::new("/bin/sh").arg("-c").arg(exec)
        .env("VORTEX_TRIGGER_PARAMS", &params_json)
        .output().await?;
    let stdout    = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr    = String::from_utf8_lossy(&output.stderr).into_owned();
    let exit_code = output.status.code().unwrap_or(-1);
    Ok(TaskOutcome { stdout, stderr, exit_code, success: output.status.success(), output: None, status: None })
}

async fn execute_http(method: &str, url: &str, headers: &HashMap<String, String>, body: Option<&str>) -> Result<TaskOutcome> {
    let method = method.to_uppercase().parse::<reqwest::Method>()
        .map_err(|_| anyhow::anyhow!("Invalid HTTP method: {method}"))?;
    let mut req = Client::new().request(method, url);
    for (k, v) in headers { req = req.header(k.as_str(), v.as_str()); }
    if let Some(b) = body { req = req.body(b.to_string()); }
    let resp      = req.send().await?;
    let http_status = resp.status();
    let body_text = resp.text().await.unwrap_or_default();
    let parsed    = serde_json::from_str(&body_text).ok();
    Ok(TaskOutcome {
        stdout: body_text, stderr: String::new(),
        exit_code: http_status.as_u16() as i32, success: http_status.is_success(),
        output: parsed, status: Some(http_status.as_u16()),
    })
}

async fn execute_notify(server: &str, topic: &str, message: &str, title: Option<&str>, priority: Option<&str>, tags: Option<&str>, token: Option<&str>) -> Result<TaskOutcome> {
    let mut req = Client::new().post(format!("{server}/{topic}")).body(message.to_string());
    if let Some(t) = title    { req = req.header("Title",         t); }
    if let Some(p) = priority { req = req.header("Priority",      p); }
    if let Some(t) = tags     { req = req.header("Tags",          t); }
    if let Some(t) = token    { req = req.header("Authorization", format!("Bearer {t}")); }
    let resp   = req.send().await?;
    let status = resp.status();
    let body   = resp.text().await.unwrap_or_default();
    Ok(TaskOutcome {
        stdout: body, stderr: String::new(),
        exit_code: status.as_u16() as i32, success: status.is_success(),
        output: None, status: Some(status.as_u16()),
    })
}

async fn execute_email(to: &str, subject: &str, body: &str, cc: Option<&str>, cfg: &EmailConfig) -> Result<TaskOutcome> {
    let password = crate::auth::resolve_token(&cfg.auth_method, &cfg.auth_key)?;
    let creds    = Credentials::new(cfg.from.clone(), password);
    let mut builder = Message::builder()
        .from(cfg.from.parse()?)
        .to(to.parse()?)
        .subject(subject);
    if let Some(addr) = cc { builder = builder.cc(addr.parse()?); }
    let email = builder.body(body.to_string())?;
    let mailer: AsyncSmtpTransport<Tokio1Executor> =
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&cfg.smtp_host)?
            .port(cfg.smtp_port)
            .credentials(creds)
            .build();
    mailer.send(email).await?;
    Ok(TaskOutcome { stdout: String::new(), stderr: String::new(), exit_code: 0, success: true, output: None, status: None })
}

async fn execute_sleep(duration: &str) -> Result<TaskOutcome> {
    let d = parse_duration(duration)?;
    tokio::time::sleep(d).await;
    Ok(TaskOutcome { stdout: String::new(), stderr: String::new(), exit_code: 0, success: true, output: None, status: None })
}

async fn execute_store_set(set: HashMap<String, String>, db_path: &str) -> Result<TaskOutcome> {
    let store = Store::open(db_path)?;
    for (k, v) in &set { store.set(k, v)?; }
    Ok(TaskOutcome { stdout: String::new(), stderr: String::new(), exit_code: 0, success: true, output: None, status: None })
}


fn render_map(map: &HashMap<String, String>, results: &HashMap<String, TaskResult>, globals: &HashMap<String, String>, params: &HashMap<String, String>, cid: &str) -> Result<HashMap<String, String>> {
    map.iter().map(|(k, v)| template::render(v, results, globals, params, cid).map(|r| (k.clone(), r))).collect()
}

fn parse_duration(s: &str) -> Result<std::time::Duration> {
    if let Some(n) = s.strip_suffix("ms") {
        return Ok(std::time::Duration::from_millis(n.trim().parse()?));
    }
    if let Some(n) = s.strip_suffix('s') {
        return Ok(std::time::Duration::from_secs(n.trim().parse()?));
    }
    if let Some(n) = s.strip_suffix('m') {
        return Ok(std::time::Duration::from_secs(n.trim().parse::<u64>()? * 60));
    }
    bail!("Invalid duration '{s}': expected format like '5s', '100ms', '2m'")
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TaskKind, WorkflowConfig, TaskConfig};
    use crate::event::Event;

    fn task(id: &str, exec: &str, when: Option<&str>) -> TaskConfig {
        TaskConfig { id: id.into(), kind: TaskKind::Shell { exec: exec.into() }, when: when.map(str::to_string), depends_on: None, response_template: None }
    }

    fn workflow(tasks: Vec<TaskConfig>) -> WorkflowConfig {
        WorkflowConfig { tasks, cron: None, correlation_id: None }
    }

    fn engine(tasks: Vec<TaskConfig>) -> Engine {
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        Engine::new(workflow(tasks), path.to_str().unwrap())
    }

    fn tr(id: &str, success: bool) -> TaskResult {
        TaskResult { id: id.into(), stdout: String::new(), stderr: String::new(), exit_code: if success { 0 } else { 1 }, success, output: None, status: None, response: None }
    }

    // --- topological sort ---

    #[test]
    fn topo_sort_linear_chain() {
        let e = engine(vec![task("a", "echo a", None), task("b", "echo b", Some("a")), task("c", "echo c", Some("b"))]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        assert_eq!(order, ["a", "b", "c"]);
    }

    #[test]
    fn topo_sort_parallel_roots() {
        let e = engine(vec![task("a", "echo a", None), task("b", "echo b", None), task("c", "echo c", Some("a"))]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        assert!(order.iter().position(|x| x == "a").unwrap() < order.iter().position(|x| x == "c").unwrap());
        assert!(order.iter().position(|x| x == "b").unwrap() < order.iter().position(|x| x == "c").unwrap());
    }

    #[test]
    fn topo_sort_not_gate_still_creates_dep_edge() {
        let e = engine(vec![task("a", "echo a", None), task("b", "echo b", Some("NOT a"))]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        assert_eq!(order, ["a", "b"]);
    }

    #[test]
    fn topo_sort_detects_cycle() {
        let e = engine(vec![task("a", "echo a", Some("b")), task("b", "echo b", Some("a"))]);
        assert!(e.topological_sort().is_err());
    }

    #[test]
    fn topo_sort_and_gate_orders_both_deps() {
        let e = engine(vec![task("a", "echo a", None), task("c", "echo c", Some("a AND b")), task("b", "echo b", None)]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        let pos_c = order.iter().position(|x| x == "c").unwrap();
        assert!(order.iter().position(|x| x == "a").unwrap() < pos_c);
        assert!(order.iter().position(|x| x == "b").unwrap() < pos_c);
    }

    #[test]
    fn topo_sort_or_gate_orders_both_deps() {
        let e = engine(vec![task("a", "echo a", None), task("c", "echo c", Some("a OR b")), task("b", "echo b", None)]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        let pos_c = order.iter().position(|x| x == "c").unwrap();
        assert!(order.iter().position(|x| x == "a").unwrap() < pos_c);
        assert!(order.iter().position(|x| x == "b").unwrap() < pos_c);
    }

    #[test]
    fn topo_sort_complex_expression_orders_all_deps() {
        let e = engine(vec![task("a", "echo a", None), task("d", "echo d", Some("(a AND b) OR c")), task("b", "echo b", None), task("c", "echo c", None)]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        let pos_d = order.iter().position(|x| x == "d").unwrap();
        assert!(order.iter().position(|x| x == "a").unwrap() < pos_d);
        assert!(order.iter().position(|x| x == "b").unwrap() < pos_d);
        assert!(order.iter().position(|x| x == "c").unwrap() < pos_d);
    }

    // --- gate ---

    #[test]
    fn gate_none_always_runs() {
        let e = engine(vec![]);
        let t = task("x", "echo", None);
        assert!(e.gate_allows(&t, &HashMap::new(), &[], &HashMap::new()).unwrap());
    }

    #[test]
    fn gate_positive_dep_runs_if_success() {
        let e = engine(vec![]);
        let t = task("x", "echo", Some("a"));
        let ok   = HashMap::from([("a".into(), tr("a", true))]);
        let fail = HashMap::from([("a".into(), tr("a", false))]);
        assert!( e.gate_allows(&t, &ok,             &["a"], &HashMap::new()).unwrap());
        assert!(!e.gate_allows(&t, &fail,            &["a"], &HashMap::new()).unwrap());
        assert!(!e.gate_allows(&t, &HashMap::new(),  &["a"], &HashMap::new()).unwrap());
    }

    #[test]
    fn gate_and_expression() {
        let e = engine(vec![]);
        let t    = task("x", "echo", Some("a AND b"));
        let both = HashMap::from([("a".into(), tr("a", true)), ("b".into(), tr("b", true))]);
        let one  = HashMap::from([("a".into(), tr("a", true)), ("b".into(), tr("b", false))]);
        assert!( e.gate_allows(&t, &both, &["a", "b"], &HashMap::new()).unwrap());
        assert!(!e.gate_allows(&t, &one,  &["a", "b"], &HashMap::new()).unwrap());
    }

    // --- integration: full async run ---

    #[tokio::test]
    async fn run_linear_success_chain() {
        let e = engine(vec![task("step1", "echo hello", None), task("step2", "echo world", Some("step1"))]);
        let results = e.run("test-workflow").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].success);
        assert!(results[1].success);
        assert!(results[0].stdout.contains("hello"));
        assert!(results[1].stdout.contains("world"));
    }

    #[tokio::test]
    async fn run_skips_positive_gate_when_dep_fails() {
        let e = engine(vec![
            task("fail_step", "exit 1", None),
            task("skip_me",   "echo should_not_run", Some("fail_step")),
            task("run_me",    "echo recovery",       Some("NOT fail_step")),
        ]);
        let results = e.run("test-workflow").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "fail_step");
        assert!(!results[0].success);
        assert_eq!(results[1].id, "run_me");
        assert!(results[1].success);
    }

    #[tokio::test]
    async fn run_injects_stdout_into_next_task() {
        let e = engine(vec![
            task("producer", "echo artifact.tar", None),
            task("consumer", "echo got={{tasks.producer.stdout}}", Some("producer")),
        ]);
        let results = e.run("test-workflow").await.unwrap();
        assert!(results[1].stdout.contains("got=artifact.tar"));
    }

    #[tokio::test]
    async fn run_and_gate_skips_if_either_fails() {
        let e = engine(vec![task("a", "echo a", None), task("b", "exit 1", None), task("c", "echo c", Some("a AND b"))]);
        let results = e.run("test-workflow").await.unwrap();
        assert!(!results.iter().any(|r| r.id == "c"));
    }

    // --- events ---

    #[tokio::test]
    async fn run_emits_lifecycle_events() {
        let (tx, mut rx) = broadcast::channel(32);
        let e = engine(vec![task("step", "echo hi", None)]).with_events(tx).with_run_id("run-1".into());
        e.run("test-workflow").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowStarted { run_id, .. } if run_id == "run-1")));
        assert!(events.iter().any(|e| matches!(e, Event::TaskStarted    { task, .. } if task == "step")));
        assert!(events.iter().any(|e| matches!(e, Event::TaskFinished   { task, success: true, .. } if task == "step")));
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: true, .. })));
    }

    #[tokio::test]
    async fn run_emits_task_skipped_when_gate_fails() {
        let (tx, mut rx) = broadcast::channel(32);
        let e = engine(vec![task("fail", "exit 1", None), task("skip", "echo nope", Some("fail"))]).with_events(tx).with_run_id("run-2".into());
        e.run("test-workflow").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::TaskSkipped { task, .. } if task == "skip")));
    }

    // --- trigger params ---

    #[tokio::test]
    async fn run_injects_trigger_params_into_task() {
        let e = engine(vec![task("greet", "echo {{trigger.name}}", None)])
            .with_params(HashMap::from([("name".into(), "vortex".into())]));
        let results = e.run("test-workflow").await.unwrap();
        assert!(results[0].stdout.contains("vortex"));
    }

    #[tokio::test]
    async fn run_missing_trigger_param_renders_empty() {
        let e = engine(vec![task("greet", "echo x{{trigger.missing}}y", None)]);
        let results = e.run("test-workflow").await.unwrap();
        assert!(results[0].stdout.contains("xy"));
    }

    // --- store persistence ---

    #[tokio::test]
    async fn run_persists_to_store() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let e2 = Engine::new(workflow(vec![task("a", "echo a", None), task("b", "echo b", Some("a"))]), &db)
            .with_run_id("hist-2".into());
        e2.run("wf").await.unwrap();
        let s = Store::open(&db).unwrap();
        let run = s.get_run("hist-2").unwrap().unwrap();
        assert_eq!(run.run.workflow, "wf");
        assert_eq!(run.run.status, "success");
        assert!(run.run.finished_at.is_some());
        assert_eq!(run.tasks.len(), 2);
        assert!(run.tasks.iter().all(|t| t.status == "success"));
    }

    #[tokio::test]
    async fn run_persists_skipped_task() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let e = Engine::new(workflow(vec![task("fail", "exit 1", None), task("skip", "echo nope", Some("fail"))]), &db)
            .with_run_id("hist-3".into());
        e.run("wf").await.unwrap();
        let s = Store::open(&db).unwrap();
        let run = s.get_run("hist-3").unwrap().unwrap();
        assert_eq!(run.run.status, "failure");
        let skip_task = run.tasks.iter().find(|t| t.task_id == "skip").unwrap();
        assert_eq!(skip_task.status, "skipped");
    }

    // --- CEL gate expressions ---

    #[tokio::test]
    async fn run_cel_gate_routes_by_exit_code() {
        let e = engine(vec![
            task("build", "exit 42", None),
            task("on_42", "echo matched",     Some("tasks.build.exit_code == 42")),
            task("on_0",  "echo not_matched", Some("tasks.build.exit_code == 0")),
        ]);
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "on_42" && r.success));
        assert!(!results.iter().any(|r| r.id == "on_0"));
    }

    #[tokio::test]
    async fn run_cel_gate_compares_stdout_string() {
        let e = engine(vec![
            task("step",   "echo hello", None),
            task("match",  "echo yes",   Some("tasks.step.stdout == \"hello\"")),
            task("nomatch","echo no",    Some("tasks.step.stdout == \"other\"")),
        ]);
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "match" && r.success));
        assert!(!results.iter().any(|r| r.id == "nomatch"));
    }

    #[tokio::test]
    async fn run_cel_gate_trigger_field() {
        let e = engine(vec![
            task("notify", "echo ok", Some("trigger.event_id == \"\"")),
        ]).with_params(HashMap::from([("event_id".into(), "".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "notify" && r.success));
    }

    #[tokio::test]
    async fn run_cel_gate_trigger_field_blocks_when_nonempty() {
        let e = engine(vec![
            task("notify", "echo ok", Some("trigger.event_id == \"\"")),
        ]).with_params(HashMap::from([("event_id".into(), "$abc:server".into())]));
        let results = e.run("test").await.unwrap();
        assert!(!results.iter().any(|r| r.id == "notify"));
    }

    // --- Sprint 13: new task types ---

    #[tokio::test]
    async fn sleep_task_runs_successfully() {
        let e = engine(vec![TaskConfig {
            id: "wait".into(),
            kind: TaskKind::Sleep { duration: "10ms".into() },
            when: None, depends_on: None, response_template: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
    }

    #[tokio::test]
    async fn store_set_updates_globals_within_same_run() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let wf = workflow(vec![
            TaskConfig { id: "save".into(), kind: TaskKind::StoreSet { set: [("mykey".into(), "hello".into())].into() }, when: None, depends_on: None, response_template: None },
            TaskConfig { id: "use".into(),  kind: TaskKind::Shell { exec: "echo {{globals.mykey}}".into() }, when: Some("save".into()), depends_on: None, response_template: None },
        ]);
        let e = Engine::new(wf, &db);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[1].success);
        assert!(results[1].stdout.contains("hello"));
    }

    #[test]
    fn parse_duration_parses_common_formats() {
        assert_eq!(parse_duration("100ms").unwrap(), std::time::Duration::from_millis(100));
        assert_eq!(parse_duration("5s").unwrap(),    std::time::Duration::from_secs(5));
        assert_eq!(parse_duration("2m").unwrap(),    std::time::Duration::from_secs(120));
    }

    #[test]
    fn parse_duration_rejects_invalid() {
        assert!(parse_duration("not-a-duration").is_err());
    }

    // --- Spawn task ---

    #[tokio::test]
    async fn spawn_task_captures_stdout() {
        let e = engine(vec![TaskConfig {
            id: "greet".into(),
            kind: TaskKind::Spawn { exe: "echo".into(), args: vec!["hello".into(), "world".into()] },
            when: None, depends_on: None, response_template: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert!(results[0].stdout.contains("hello world"));
    }

    #[tokio::test]
    async fn spawn_task_exit_zero_is_success() {
        let e = engine(vec![TaskConfig {
            id: "ok".into(),
            kind: TaskKind::Spawn { exe: "true".into(), args: vec![] },
            when: None, depends_on: None, response_template: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].success);
        assert_eq!(results[0].exit_code, 0);
    }

    #[tokio::test]
    async fn spawn_task_nonzero_exit_is_failure() {
        let e = engine(vec![TaskConfig {
            id: "fail".into(),
            kind: TaskKind::Spawn { exe: "false".into(), args: vec![] },
            when: None, depends_on: None, response_template: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert!(!results[0].success);
        assert_ne!(results[0].exit_code, 0);
    }

    #[tokio::test]
    async fn spawn_task_reads_trigger_params_from_stdin() {
        // `cat` echoes stdin to stdout — trigger params JSON should appear
        let e = engine(vec![TaskConfig {
            id: "echo_params".into(),
            kind: TaskKind::Spawn { exe: "cat".into(), args: vec![] },
            when: None, depends_on: None, response_template: None,
        }]).with_params(HashMap::from([("Body".into(), "hello".into()), ("Sender".into(), "@user".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results[0].success);
        let out: serde_json::Value = serde_json::from_str(&results[0].stdout).unwrap();
        assert_eq!(out["Body"], "hello");
        assert_eq!(out["Sender"], "@user");
    }

    #[tokio::test]
    async fn spawn_task_gates_on_exit_code() {
        let e = engine(vec![
            TaskConfig { id: "filter".into(), kind: TaskKind::Spawn { exe: "false".into(), args: vec![] }, when: None, depends_on: None, response_template: None },
            TaskConfig { id: "action".into(), kind: TaskKind::Shell { exec: "echo done".into() }, when: Some("filter".into()), depends_on: None, response_template: None },
        ]);
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "filter" && !r.success));
        assert!(!results.iter().any(|r| r.id == "action"));
    }

    // --- Response task kind ---

    #[tokio::test]
    async fn response_task_renders_template() {
        let e = engine(vec![
            task("hello", "echo world", None),
            TaskConfig {
                id: "reply".into(),
                kind: TaskKind::Response { template: "got={{tasks.hello.stdout}}".into() },
                when: Some("hello".into()),
                depends_on: None,
                response_template: None,
            },
        ]);
        let results = e.run("test").await.unwrap();
        let r = results.iter().find(|r| r.id == "reply").unwrap();
        assert!(r.success);
        assert!(r.stdout.contains("got=world"));
    }

    #[tokio::test]
    async fn response_task_uses_trigger_params() {
        let e = engine(vec![TaskConfig {
            id: "r".into(),
            kind: TaskKind::Response { template: "msg={{trigger.text}}".into() },
            when: None,
            depends_on: None,
            response_template: None,
        }]).with_params(HashMap::from([("text".into(), "hello".into())]));
        let results = e.run("test").await.unwrap();
        assert_eq!(results[0].stdout.trim(), "msg=hello");
    }

    #[tokio::test]
    async fn response_task_uses_correlation_id() {
        let e = engine(vec![TaskConfig {
            id: "r".into(),
            kind: TaskKind::Response { template: "id={{correlation_id}}".into() },
            when: None,
            depends_on: None,
            response_template: None,
        }]).with_correlation_id("req-99".into());
        let results = e.run("test").await.unwrap();
        assert_eq!(results[0].stdout.trim(), "id=req-99");
    }

    // --- response_template field on task ---

    #[tokio::test]
    async fn response_template_rendered_after_task_succeeds() {
        let e = engine(vec![TaskConfig {
            id: "t".into(),
            kind: TaskKind::Shell { exec: "echo raw".into() },
            when: None,
            depends_on: None,
            response_template: Some("wrapped={{tasks.t.stdout}}".into()),
        }]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].success);
        assert!(results[0].response.as_deref().unwrap_or("").contains("wrapped=raw"));
    }

    #[tokio::test]
    async fn response_template_not_set_when_task_fails() {
        let e = engine(vec![TaskConfig {
            id: "t".into(),
            kind: TaskKind::Shell { exec: "exit 1".into() },
            when: None,
            depends_on: None,
            response_template: Some("should_not_appear".into()),
        }]);
        let results = e.run("test").await.unwrap();
        assert!(!results[0].success);
        assert!(results[0].response.is_none());
    }

    #[tokio::test]
    async fn response_template_can_reference_own_stdout() {
        let e = engine(vec![TaskConfig {
            id: "t".into(),
            kind: TaskKind::Shell { exec: "echo hello".into() },
            when: None,
            depends_on: None,
            response_template: Some(r#"{"out":"{{tasks.t.stdout}}"}"#.into()),
        }]);
        let results = e.run("test").await.unwrap();
        let resp = results[0].response.as_deref().unwrap();
        assert!(resp.contains("hello"));
    }

    // --- Condition task ---

    fn condition(id: &str, expr: &str) -> TaskConfig {
        TaskConfig { id: id.into(), kind: TaskKind::Condition { expr: expr.into() }, when: None, depends_on: None, response_template: None }
    }

    #[tokio::test]
    async fn condition_true_expr_succeeds_with_exit_code_0() {
        let e = engine(vec![condition("c", "true")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].success);
        assert_eq!(results[0].exit_code, 0);
        assert_eq!(results[0].stdout.trim(), "true");
    }

    #[tokio::test]
    async fn condition_false_expr_fails_with_exit_code_1() {
        let e = engine(vec![condition("c", "false")]);
        let results = e.run("test").await.unwrap();
        assert!(!results[0].success);
        assert_eq!(results[0].exit_code, 1);
        assert_eq!(results[0].stdout.trim(), "false");
    }

    #[tokio::test]
    async fn condition_non_bool_expr_fails_with_exit_code_2() {
        // "42" is valid CEL but returns Int, not Bool → gate returns Err → exit_code 2
        let e = engine(vec![condition("c", "42")]);
        let results = e.run("test").await.unwrap();
        assert!(!results[0].success);
        assert_eq!(results[0].exit_code, 2);
        assert!(!results[0].stderr.is_empty());
    }

    #[tokio::test]
    async fn condition_reads_trigger_param() {
        let e = engine(vec![condition("c", "trigger.x == \"yes\"")])
            .with_params(HashMap::from([("x".into(), "yes".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results[0].success);
    }

    #[tokio::test]
    async fn condition_gates_downstream_tasks() {
        let e = engine(vec![
            condition("is_even", "trigger.n == \"2\""),
            task("on_even",  "echo even", Some("is_even")),
            task("on_other", "echo other", Some("NOT is_even")),
        ]).with_params(HashMap::from([("n".into(), "2".into())]));
        let results = e.run("test").await.unwrap();
        assert!( results.iter().any(|r| r.id == "on_even"  && r.success));
        assert!(!results.iter().any(|r| r.id == "on_other"));
    }
}
