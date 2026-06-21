use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::{bail, Result};
use cel_interpreter::Value as CelValue;
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
use vortex_core::{TaskStatus, TriggerStatus};

// ── log retention helper ──────────────────────────────────────────────────────

/// Compute the expiry timestamp for task log lines from `log_retention`.
/// Returns `None` when logging is disabled (`log_retention = 0`).
/// Returns `Some(None)` when logs should be kept forever (`log_retention = -1`).
/// Returns `Some(Some(ts))` with an absolute expiry timestamp otherwise
/// (default 7 days when `log_retention` is unset).
pub fn log_expiry(log_retention: Option<i32>, now_ms: u64) -> Option<Option<u64>> {
    match log_retention {
        Some(0)  => None,
        Some(-1) => Some(None),
        Some(n)  => Some(Some(now_ms + n as u64 * 86_400_000)),
        None     => Some(Some(now_ms + 7 * 86_400_000)),
    }
}

// ── result types ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub id:          String,
    pub stdout:      String,
    pub stderr:      String,
    pub exit_code:   i32,
    pub status:      TaskStatus,
    pub output:      Option<serde_json::Value>,
    pub http_status: Option<u16>,
    /// Rendered response_template (or Response task output). When set on the last
    /// successful task that has one, this becomes the workflow's response to the caller.
    pub response:    Option<String>,
}

impl TaskResult {
    pub fn is_success(&self) -> bool { self.status.is_success() }
    pub fn is_failed(&self)  -> bool { self.status.is_failed()  }
    pub fn is_skipped(&self) -> bool { self.status.is_skipped() }
}

struct TaskOutcome {
    stdout:      String,
    stderr:      String,
    exit_code:   i32,
    success:     bool,
    output:      Option<serde_json::Value>,
    http_status: Option<u16>,
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
        if let Err(e) = store.update_trigger_status(&run_id, TriggerStatus::Running, None, None) {
            warn!("Failed to update trigger status to running: {e:#}");
        }

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
                self.emit(Event::TaskSkipped { run_id: run_id.clone(), task: task.id.clone(), timestamp: ts });
                store.upsert_task(&run_id, &task.id, TaskStatus::Skipped, None, None, None, Some(ts), Some(ts))?;
                all_results.push(TaskResult {
                    id: task.id.clone(), stdout: String::new(), stderr: String::new(),
                    exit_code: -1, status: TaskStatus::Skipped,
                    output: None, http_status: None, response: None,
                });
                continue;
            }

            let mut result = self.run_task(task, &run_id, &results, &globals).await?;

            if result.is_success() {
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

            if result.is_success() {
                if let TaskKind::StoreSet { set } = &task.kind {
                    if let Ok(rendered) = render_map(set, &results, &globals, &self.trigger_params, &self.correlation_id) {
                        globals.extend(rendered);
                    }
                }
            }

            results.insert(result.id.clone(), result.clone());

            let should_abort = if let Some(expr) = &task.abort_if {
                let all_ids_here: Vec<&str> = results.keys().map(String::as_str).collect();
                match gate::task_result_to_cel(&result).and_then(|self_val| {
                    gate::evaluate_with_extras(expr, &results, &all_ids_here, &self.trigger_params, &globals, &self.correlation_id, &[("self", self_val)])
                }) {
                    Ok(true)  => { info!(task = %task.id, "abort_if triggered — stopping workflow early"); true }
                    Ok(false) => false,
                    Err(e)    => { warn!(task = %task.id, "abort_if eval error: {e:#}"); false }
                }
            } else { false };

            all_results.push(result);
            if should_abort { break; }
        }

        let overall_success = if let Some(expr) = &self.config.status_eval {
            match gate::evaluate(expr, &results, &all_ids, &self.trigger_params, &globals, &self.correlation_id) {
                Ok(b)  => b,
                Err(e) => { warn!("status_eval error: {e:#}"); false }
            }
        } else {
            all_results.iter().filter(|r| !r.is_skipped()).all(|r| r.is_success())
        };
        let finished_at = vortex_core::now_ms();
        store.finish_run(&run_id, overall_success, finished_at)?;
        if let Err(e) = store.update_trigger_status(&run_id, TriggerStatus::Finished, None, Some(finished_at)) {
            warn!("Failed to update trigger status to finished: {e:#}");
        }
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
        store.upsert_task(run_id, &task.id, TaskStatus::Running, None, None, None, Some(started_at), None)?;
        self.emit(Event::TaskStarted { run_id: run_id.to_string(), task: task.id.clone(), timestamp: started_at });

        let outcome = self.dispatch_task(task, results, globals).await.unwrap_or_else(|e| {
            error!(task = %task.id, "Task failed: {e:#}");
            TaskOutcome { stdout: String::new(), stderr: e.to_string(), exit_code: -1, success: false, output: None, http_status: None }
        });

        let finished_at = vortex_core::now_ms();
        if !outcome.stdout.trim().is_empty() { info!(task = %task.id, "stdout: {}", outcome.stdout.trim_end()); }
        if !outcome.stderr.trim().is_empty() { error!(task = %task.id, "stderr: {}", outcome.stderr.trim_end()); }
        if outcome.success { info!(task = %task.id, "Finished OK") } else { warn!(task = %task.id, "Finished with error") }

        if let Some(expires_at) = log_expiry(self.config.log_retention, finished_at) {
            for line in outcome.stdout.lines().filter(|l| !l.is_empty()) {
                let _ = store.insert_task_log(run_id, &task.id, "stdout", line, finished_at, expires_at);
            }
            for line in outcome.stderr.lines().filter(|l| !l.is_empty()) {
                let _ = store.insert_task_log(run_id, &task.id, "stderr", line, finished_at, expires_at);
            }
        }

        let task_status = if outcome.success { TaskStatus::Success } else { TaskStatus::Failed };
        store.upsert_task(run_id, &task.id, task_status, Some(outcome.exit_code),
            Some(&outcome.stdout), Some(&outcome.stderr), Some(started_at), Some(finished_at))?;
        self.emit(Event::TaskFinished {
            run_id: run_id.to_string(), task: task.id.clone(),
            success: outcome.success, exit_code: outcome.exit_code,
            stdout: outcome.stdout.clone(), stderr: outcome.stderr.clone(),
            timestamp: finished_at,
        });

        Ok(TaskResult {
            id: task.id.clone(), stdout: outcome.stdout, stderr: outcome.stderr,
            exit_code: outcome.exit_code,
            status: if outcome.success { TaskStatus::Success } else { TaskStatus::Failed },
            output: outcome.output, http_status: outcome.http_status, response: None,
        })
    }

    async fn dispatch_task(
        &self,
        task: &TaskConfig,
        results: &HashMap<String, TaskResult>,
        globals: &HashMap<String, String>,
    ) -> Result<TaskOutcome> {
        self.dispatch_task_inner(task, results, globals, None).await
    }

    fn dispatch_task_inner<'a>(
        &'a self,
        task: &'a TaskConfig,
        results: &'a HashMap<String, TaskResult>,
        globals: &'a HashMap<String, String>,
        item: Option<&'a serde_json::Value>,
    ) -> Pin<Box<dyn Future<Output = Result<TaskOutcome>> + Send + 'a>> {
        Box::pin(async move {
        let cid = &self.correlation_id;
        let render = |tmpl: &str| -> Result<String> {
            match item {
                Some(it) => template::render_with_item(tmpl, results, globals, &self.trigger_params, cid, it),
                None     => template::render(tmpl, results, globals, &self.trigger_params, cid),
            }
        };
        match &task.kind {
            TaskKind::Shell { exec } => {
                let cmd = render(exec)?;
                execute_shell(&task.id, &cmd, &self.trigger_params).await
            }
            TaskKind::Http { url, method, headers, body } => {
                let url  = render(url)?;
                let body = body.as_ref().map(|b| render(b)).transpose()?;
                let hdrs: HashMap<String, String> = headers.iter()
                    .map(|(k, v)| render(v).map(|v| (k.clone(), v)))
                    .collect::<Result<_>>()?;
                execute_http(method, &url, &hdrs, body.as_deref()).await
            }
            TaskKind::Email { to, subject, body, cc } => {
                let subject = render(subject)?;
                let body    = render(body)?;
                let cfg = self.email_config.as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Email task requires [email] config section"))?;
                execute_email(to, &subject, &body, cc.as_deref(), cfg).await
            }
            TaskKind::Sleep { duration } => execute_sleep(duration).await,
            TaskKind::StoreSet { set } => {
                let rendered: HashMap<String, String> = set.iter()
                    .map(|(k, v)| render(v).map(|v| (k.clone(), v)))
                    .collect::<Result<_>>()?;
                execute_store_set(rendered, &self.db_path).await
            }
            TaskKind::Peer { vortex, trigger, params } => {
                let socket  = render(vortex)?;
                let wf      = render(trigger)?;
                let params: HashMap<String, String> = params.iter()
                    .map(|(k, v)| render(v).map(|v| (k.clone(), v)))
                    .collect::<Result<_>>()?;
                execute_peer(&task.id, &socket, &wf, &params).await
            }
            TaskKind::Spawn { exe, args } => {
                let exe  = render(exe)?;
                let args = args.iter().map(|a| render(a)).collect::<Result<Vec<_>>>()?;
                execute_spawn(&task.id, &exe, &args, &self.trigger_params).await
            }
            TaskKind::Response { template } => {
                let rendered = render(template)?;
                Ok(TaskOutcome {
                    stdout: rendered, stderr: String::new(),
                    exit_code: 0, success: true, output: None, http_status: None,
                })
            }
            TaskKind::Condition { expr } => {
                let all_ids: Vec<&str> = results.keys().map(String::as_str).collect();
                let extras: Vec<(&str, cel_interpreter::Value)> = item
                    .and_then(|it| cel_interpreter::to_value(it).ok())
                    .map(|v| vec![("item", v)])
                    .unwrap_or_default();
                match gate::evaluate_with_extras(expr, results, &all_ids, &self.trigger_params, globals, cid, &extras) {
                    Ok(true)  => Ok(TaskOutcome { stdout: "true".into(),  stderr: String::new(), exit_code: 0, success: true,  output: None, http_status: None }),
                    Ok(false) => Ok(TaskOutcome { stdout: "false".into(), stderr: String::new(), exit_code: 1, success: false, output: None, http_status: None }),
                    Err(e)    => Ok(TaskOutcome { stdout: String::new(),  stderr: e.to_string(), exit_code: 2, success: false, output: None, http_status: None }),
                }
            }
            TaskKind::Eval { expr } => {
                let all_ids: Vec<&str> = results.keys().map(String::as_str).collect();
                let extras: Vec<(&str, cel_interpreter::Value)> = item
                    .and_then(|it| cel_interpreter::to_value(it).ok())
                    .map(|v| vec![("item", v)])
                    .unwrap_or_default();
                match gate::evaluate_value_with_extras(expr, results, &all_ids, &self.trigger_params, globals, cid, &extras) {
                    Ok(value) => Ok(cel_to_outcome(value)),
                    Err(e)    => Ok(TaskOutcome { stdout: String::new(), stderr: e.to_string(), exit_code: 2, success: false, output: None, http_status: None }),
                }
            }
            TaskKind::ForEach { items, tasks, initial, accumulate } => {
                self.run_foreach(items, tasks, initial, accumulate, globals, &self.correlation_id.clone()).await
            }
        }
        })
    }

    async fn run_foreach(
        &self,
        items_expr: &str,
        inner_tasks: &[TaskConfig],
        initial_expr: &str,
        accumulate_expr: &str,
        globals: &HashMap<String, String>,
        cid: &str,
    ) -> Result<TaskOutcome> {
        // Evaluate `items` to get the iteration list
        let empty_results = HashMap::new();
        let all_ids: Vec<&str> = vec![];
        let items_val = gate::evaluate_value(items_expr, &empty_results, &all_ids, &self.trigger_params, globals, cid)
            .map_err(|e| anyhow::anyhow!("foreach items eval error: {e}"))?;
        let CelValue::List(items) = items_val else {
            bail!("foreach `items` expression must evaluate to a list");
        };

        // Evaluate `initial` to seed the accumulator
        let mut acc = gate::evaluate_value(initial_expr, &empty_results, &all_ids, &self.trigger_params, globals, cid)
            .map_err(|e| anyhow::anyhow!("foreach initial eval error: {e}"))?;

        for item in items.iter() {
            // Run inner pipeline with `item` injected
            let inner_results = self.run_foreach_iteration(item, inner_tasks, globals, cid).await?;

            // Evaluate accumulate with `acc` and `item` in context
            let all_inner_ids: Vec<&str> = inner_results.keys().map(String::as_str).collect();
            let new_acc = gate::evaluate_value_with_extras(
                accumulate_expr, &inner_results, &all_inner_ids,
                &self.trigger_params, globals, cid,
                &[("acc", acc.clone()), ("item", item.clone())],
            ).map_err(|e| anyhow::anyhow!("foreach accumulate eval error: {e}"))?;
            acc = new_acc;
        }

        Ok(cel_to_outcome(acc))
    }

    async fn run_foreach_iteration(
        &self,
        item: &CelValue,
        inner_tasks: &[TaskConfig],
        globals: &HashMap<String, String>,
        cid: &str,
    ) -> Result<HashMap<String, TaskResult>> {
        let item_json = cel_to_json(item);
        let cel_item  = item.clone();
        let mut results: HashMap<String, TaskResult> = HashMap::new();
        let all_ids: Vec<&str> = inner_tasks.iter().map(|t| t.id.as_str()).collect();

        for task in inner_tasks {
            let allowed = if let Some(expr) = &task.when {
                gate::evaluate_with_extras(expr, &results, &all_ids, &self.trigger_params, globals, cid,
                    &[("item", cel_item.clone())])?
            } else { true };
            if !allowed {
                results.insert(task.id.clone(), TaskResult {
                    id: task.id.clone(), stdout: String::new(), stderr: String::new(),
                    exit_code: -1, status: TaskStatus::Skipped,
                    output: None, http_status: None, response: None,
                });
                continue;
            }
            let outcome = self.dispatch_task_inner(task, &results, globals, Some(&item_json)).await
                .unwrap_or_else(|e| TaskOutcome {
                    stdout: String::new(), stderr: e.to_string(),
                    exit_code: -1, success: false, output: None, http_status: None,
                });
            let success = outcome.success;
            results.insert(task.id.clone(), TaskResult {
                id: task.id.clone(),
                stdout: outcome.stdout, stderr: outcome.stderr,
                exit_code: outcome.exit_code,
                status: if outcome.success { TaskStatus::Success } else { TaskStatus::Failed },
                output: outcome.output, http_status: outcome.http_status, response: None,
            });
            if !success {
                bail!("foreach inner task '{}' failed", task.id);
            }
        }
        Ok(results)
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
    Ok(TaskOutcome { stdout, stderr, exit_code, success: output.status.success(), output: None, http_status: None })
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
    Ok(TaskOutcome { stdout, stderr, exit_code, success: output.status.success(), output: None, http_status: None })
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
        output: parsed, http_status: Some(http_status.as_u16()),
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
    Ok(TaskOutcome { stdout: String::new(), stderr: String::new(), exit_code: 0, success: true, output: None, http_status: None })
}

async fn execute_sleep(duration: &str) -> Result<TaskOutcome> {
    let d = parse_duration(duration)?;
    tokio::time::sleep(d).await;
    Ok(TaskOutcome { stdout: String::new(), stderr: String::new(), exit_code: 0, success: true, output: None, http_status: None })
}

async fn execute_store_set(set: HashMap<String, String>, db_path: &str) -> Result<TaskOutcome> {
    let store = Store::open(db_path)?;
    for (k, v) in &set { store.set(k, v)?; }
    Ok(TaskOutcome { stdout: String::new(), stderr: String::new(), exit_code: 0, success: true, output: None, http_status: None })
}

async fn execute_peer(
    task_id: &str,
    socket_path: &str,
    workflow: &str,
    params: &HashMap<String, String>,
) -> Result<TaskOutcome> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    info!(task = %task_id, peer = %socket_path, workflow = %workflow, "Running peer task");

    let mut stream = UnixStream::connect(socket_path).await
        .map_err(|e| anyhow::anyhow!("peer task: failed to connect to {socket_path}: {e}"))?;

    let request = serde_json::json!({"workflow": workflow, "params": params});
    let mut payload = serde_json::to_string(&request)?;
    payload.push('\n');

    let (read_half, mut write_half) = stream.split();
    write_half.write_all(payload.as_bytes()).await?;

    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await?;
    let line = line.trim().to_string();

    if line.is_empty() {
        bail!("peer task: no response from {socket_path}");
    }

    let resp: serde_json::Value = serde_json::from_str(&line)
        .map_err(|e| anyhow::anyhow!("peer task: invalid JSON response: {e}"))?;

    let is_error = resp.get("status").and_then(|s| s.as_str()) == Some("error");
    let stderr = if is_error {
        resp.get("message").and_then(|m| m.as_str()).unwrap_or("peer trigger failed").to_string()
    } else {
        String::new()
    };

    Ok(TaskOutcome {
        stdout: line,
        stderr,
        exit_code: if is_error { 1 } else { 0 },
        success: !is_error,
        output: Some(resp),
        http_status: None,
    })
}


fn cel_to_outcome(value: CelValue) -> TaskOutcome {
    let success = cel_is_truthy(&value);
    let exit_code = if success { 0 } else { 1 };
    match value {
        CelValue::String(s) => {
            let s = s.as_ref().clone();
            TaskOutcome { stdout: s, stderr: String::new(), exit_code, success, output: None, http_status: None }
        }
        CelValue::Bool(b) => {
            TaskOutcome { stdout: if b { "true" } else { "false" }.into(), stderr: String::new(), exit_code, success, output: Some(serde_json::Value::Bool(b)), http_status: None }
        }
        other => {
            let json = cel_to_json(&other);
            let stdout = json.to_string();
            TaskOutcome { stdout, stderr: String::new(), exit_code, success, output: Some(json), http_status: None }
        }
    }
}

fn cel_is_truthy(val: &CelValue) -> bool {
    match val {
        CelValue::Bool(b)   => *b,
        CelValue::String(s) => !s.is_empty(),
        CelValue::Int(i)    => *i != 0,
        CelValue::UInt(u)   => *u != 0,
        CelValue::Float(f)  => *f != 0.0,
        CelValue::Null      => false,
        CelValue::List(l)   => !l.is_empty(),
        CelValue::Map(m)    => !m.map.is_empty(),
        CelValue::Bytes(b)  => !b.is_empty(),
        _                   => true,
    }
}

fn cel_to_json(val: &CelValue) -> serde_json::Value {
    match val {
        CelValue::Bool(b)   => serde_json::Value::Bool(*b),
        CelValue::Int(i)    => serde_json::json!(i),
        CelValue::UInt(u)   => serde_json::json!(u),
        CelValue::Float(f)  => serde_json::json!(f),
        CelValue::String(s) => serde_json::Value::String(s.as_ref().clone()),
        CelValue::Null      => serde_json::Value::Null,
        CelValue::List(l)   => serde_json::Value::Array(l.iter().map(cel_to_json).collect()),
        CelValue::Map(m)    => serde_json::Value::Object(
            m.map.iter().map(|(k, v)| (k.to_string(), cel_to_json(v))).collect()
        ),
        CelValue::Bytes(b)  => serde_json::Value::String(
            b.iter().map(|byte| format!("{byte:02x}")).collect()
        ),
        other => serde_json::Value::String(format!("{other:?}")),
    }
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
        TaskConfig { id: id.into(), kind: TaskKind::Shell { exec: exec.into() }, when: when.map(str::to_string), depends_on: None, response_template: None, abort_if: None }
    }

    fn workflow(tasks: Vec<TaskConfig>) -> WorkflowConfig {
        WorkflowConfig { tasks, cron: None, correlation_id: None, status_eval: None, log_retention: None }
    }

    fn engine(tasks: Vec<TaskConfig>) -> Engine {
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        Engine::new(workflow(tasks), path.to_str().unwrap())
    }

    fn tr(id: &str, success: bool) -> TaskResult {
        TaskResult {
            id: id.into(), stdout: String::new(), stderr: String::new(),
            exit_code: if success { 0 } else { 1 },
            status: if success { TaskStatus::Success } else { TaskStatus::Failed },
            output: None, http_status: None, response: None,
        }
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
        assert!(results[0].is_success());
        assert!(results[1].is_success());
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
        assert_eq!(results.len(), 3);
        assert!(results.iter().any(|r| r.id == "fail_step" && r.is_failed()));
        assert!(results.iter().any(|r| r.id == "skip_me"   && r.is_skipped()));
        assert!(results.iter().any(|r| r.id == "run_me"    && r.is_success()));
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
        assert!(results.iter().any(|r| r.id == "c" && r.is_skipped()));
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
        assert_eq!(run.run.status, "failed");
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
        assert!(results.iter().any(|r| r.id == "on_42" && r.is_success()));
        assert!(results.iter().any(|r| r.id == "on_0"  && r.is_skipped()));
    }

    #[tokio::test]
    async fn run_cel_gate_compares_stdout_string() {
        let e = engine(vec![
            task("step",   "echo hello", None),
            task("match",  "echo yes",   Some("tasks.step.stdout == \"hello\"")),
            task("nomatch","echo no",    Some("tasks.step.stdout == \"other\"")),
        ]);
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "match"   && r.is_success()));
        assert!(results.iter().any(|r| r.id == "nomatch" && r.is_skipped()));
    }

    #[tokio::test]
    async fn run_cel_gate_trigger_field() {
        let e = engine(vec![
            task("notify", "echo ok", Some("trigger.event_id == \"\"")),
        ]).with_params(HashMap::from([("event_id".into(), "".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "notify" && r.is_success()));
    }

    #[tokio::test]
    async fn run_cel_gate_trigger_field_blocks_when_nonempty() {
        let e = engine(vec![
            task("notify", "echo ok", Some("trigger.event_id == \"\"")),
        ]).with_params(HashMap::from([("event_id".into(), "$abc:server".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "notify" && r.is_skipped()));
    }

    // --- Sprint 13: new task types ---

    #[tokio::test]
    async fn sleep_task_runs_successfully() {
        let e = engine(vec![TaskConfig {
            id: "wait".into(),
            kind: TaskKind::Sleep { duration: "10ms".into() },
            when: None, depends_on: None, response_template: None, abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());
    }

    #[tokio::test]
    async fn store_set_updates_globals_within_same_run() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("v.db").to_string_lossy().into_owned();
        let wf = workflow(vec![
            TaskConfig { id: "save".into(), kind: TaskKind::StoreSet { set: [("mykey".into(), "hello".into())].into() }, when: None, depends_on: None, response_template: None, abort_if: None },
            TaskConfig { id: "use".into(),  kind: TaskKind::Shell { exec: "echo {{globals.mykey}}".into() }, when: Some("save".into()), depends_on: None, response_template: None, abort_if: None },
        ]);
        let e = Engine::new(wf, &db);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[1].is_success());
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
            when: None, depends_on: None, response_template: None, abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_success());
        assert!(results[0].stdout.contains("hello world"));
    }

    #[tokio::test]
    async fn spawn_task_exit_zero_is_success() {
        let e = engine(vec![TaskConfig {
            id: "ok".into(),
            kind: TaskKind::Spawn { exe: "true".into(), args: vec![] },
            when: None, depends_on: None, response_template: None, abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
        assert_eq!(results[0].exit_code, 0);
    }

    #[tokio::test]
    async fn spawn_task_nonzero_exit_is_failure() {
        let e = engine(vec![TaskConfig {
            id: "fail".into(),
            kind: TaskKind::Spawn { exe: "false".into(), args: vec![] },
            when: None, depends_on: None, response_template: None, abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
        assert_ne!(results[0].exit_code, 0);
    }

    #[tokio::test]
    async fn spawn_task_reads_trigger_params_from_stdin() {
        // `cat` echoes stdin to stdout — trigger params JSON should appear
        let e = engine(vec![TaskConfig {
            id: "echo_params".into(),
            kind: TaskKind::Spawn { exe: "cat".into(), args: vec![] },
            when: None, depends_on: None, response_template: None, abort_if: None,
        }]).with_params(HashMap::from([("Body".into(), "hello".into()), ("Sender".into(), "@user".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
        let out: serde_json::Value = serde_json::from_str(&results[0].stdout).unwrap();
        assert_eq!(out["Body"], "hello");
        assert_eq!(out["Sender"], "@user");
    }

    #[tokio::test]
    async fn spawn_task_gates_on_exit_code() {
        let e = engine(vec![
            TaskConfig { id: "filter".into(), kind: TaskKind::Spawn { exe: "false".into(), args: vec![] }, when: None, depends_on: None, response_template: None, abort_if: None },
            TaskConfig { id: "action".into(), kind: TaskKind::Shell { exec: "echo done".into() }, when: Some("filter".into()), depends_on: None, response_template: None, abort_if: None },
        ]);
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "filter" && r.is_failed()));
        assert!(results.iter().any(|r| r.id == "action" && r.is_skipped()));
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
                abort_if: None,
            },
        ]);
        let results = e.run("test").await.unwrap();
        let r = results.iter().find(|r| r.id == "reply").unwrap();
        assert!(r.is_success());
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
            abort_if: None,
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
            abort_if: None,
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
            abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
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
            abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
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
            abort_if: None,
        }]);
        let results = e.run("test").await.unwrap();
        let resp = results[0].response.as_deref().unwrap();
        assert!(resp.contains("hello"));
    }

    // --- abort_if ---

    #[tokio::test]
    async fn abort_if_stops_workflow_on_success() {
        let mut check = task("check", "true", None);
        check.abort_if = Some("self.success".into());
        let e = engine(vec![check, task("work", "echo should_not_run", Some("check"))]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 1, "only check ran");
        assert!(results[0].is_success());
        assert_eq!(results[0].id, "check");
    }

    #[tokio::test]
    async fn abort_if_false_does_not_stop_workflow() {
        let mut check = task("check", "false", None);
        check.abort_if = Some("self.success".into());
        // check fails → abort_if = false → work is gated on "check" which failed → skipped
        let e = engine(vec![check, task("work", "echo done", Some("check"))]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results.iter().any(|r| r.id == "check" && r.is_failed()));
        assert!(results.iter().any(|r| r.id == "work"  && r.is_skipped()));
    }

    #[tokio::test]
    async fn abort_if_can_reference_stdout() {
        let mut probe = task("probe", "echo stop", None);
        probe.abort_if = Some(r#"self.stdout == "stop""#.into());
        let e = engine(vec![probe, task("next", "echo after", None)]);
        let results = e.run("test").await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "probe");
    }

    // --- Eval task ---

    fn eval(id: &str, expr: &str) -> TaskConfig {
        TaskConfig { id: id.into(), kind: TaskKind::Eval { expr: expr.into() }, when: None, depends_on: None, response_template: None, abort_if: None }
    }

    #[tokio::test]
    async fn eval_string_result_is_stdout() {
        let e = engine(vec![eval("find", "\"friends\"")])
            .with_params(HashMap::new());
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
        assert_eq!(results[0].exit_code, 0);
        assert_eq!(results[0].stdout.trim(), "friends");
        assert!(results[0].output.is_none());
    }

    #[tokio::test]
    async fn eval_bool_true_succeeds() {
        let e = engine(vec![eval("c", "true")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
        assert_eq!(results[0].exit_code, 0);
        assert_eq!(results[0].stdout.trim(), "true");
    }

    #[tokio::test]
    async fn eval_bool_false_fails() {
        let e = engine(vec![eval("c", "false")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
        assert_eq!(results[0].exit_code, 1);
        assert_eq!(results[0].stdout.trim(), "false");
    }

    #[tokio::test]
    async fn eval_empty_string_fails() {
        let e = engine(vec![eval("c", "\"\"")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
        assert_eq!(results[0].exit_code, 1);
    }

    #[tokio::test]
    async fn eval_error_fails_with_exit_2() {
        // invalid CEL → compile error
        let e = engine(vec![eval("c", "this is not valid $$$ cel")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
        assert_eq!(results[0].exit_code, 2);
        assert!(!results[0].stderr.is_empty());
    }

    #[tokio::test]
    async fn eval_reads_trigger_param() {
        let e = engine(vec![eval("find", "trigger.space")])
            .with_params(HashMap::from([("space".into(), "friends".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
        assert_eq!(results[0].stdout.trim(), "friends");
    }

    #[tokio::test]
    async fn eval_gates_downstream_tasks() {
        let e = engine(vec![
            eval("find_space", "trigger.room == \"!abc:server\" ? \"friends\" : \"\""),
            task("notify", "echo notified", Some("find_space")),
            task("skip",   "echo skipped",  Some("NOT find_space")),
        ]).with_params(HashMap::from([("room".into(), "!abc:server".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "find_space" && r.is_success() && r.stdout.trim() == "friends"));
        assert!(results.iter().any(|r| r.id == "notify" && r.is_success()));
        assert!(results.iter().any(|r| r.id == "skip"   && r.is_skipped()));
    }

    #[tokio::test]
    async fn eval_stdout_usable_in_downstream_template() {
        let e = engine(vec![
            eval("find_space", "\"friends\""),
            task("use", "echo topic={{tasks.find_space.stdout}}", Some("find_space")),
        ]);
        let results = e.run("test").await.unwrap();
        assert!(results[1].stdout.contains("topic=friends"));
    }

    // --- Condition task ---

    fn condition(id: &str, expr: &str) -> TaskConfig {
        TaskConfig { id: id.into(), kind: TaskKind::Condition { expr: expr.into() }, when: None, depends_on: None, response_template: None, abort_if: None }
    }

    #[tokio::test]
    async fn condition_true_expr_succeeds_with_exit_code_0() {
        let e = engine(vec![condition("c", "true")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
        assert_eq!(results[0].exit_code, 0);
        assert_eq!(results[0].stdout.trim(), "true");
    }

    #[tokio::test]
    async fn condition_false_expr_fails_with_exit_code_1() {
        let e = engine(vec![condition("c", "false")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
        assert_eq!(results[0].exit_code, 1);
        assert_eq!(results[0].stdout.trim(), "false");
    }

    #[tokio::test]
    async fn condition_non_bool_expr_fails_with_exit_code_2() {
        // "42" is valid CEL but returns Int, not Bool → gate returns Err → exit_code 2
        let e = engine(vec![condition("c", "42")]);
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_failed());
        assert_eq!(results[0].exit_code, 2);
        assert!(!results[0].stderr.is_empty());
    }

    #[tokio::test]
    async fn condition_reads_trigger_param() {
        let e = engine(vec![condition("c", "trigger.x == \"yes\"")])
            .with_params(HashMap::from([("x".into(), "yes".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results[0].is_success());
    }

    #[tokio::test]
    async fn condition_gates_downstream_tasks() {
        let e = engine(vec![
            condition("is_even", "trigger.n == \"2\""),
            task("on_even",  "echo even", Some("is_even")),
            task("on_other", "echo other", Some("NOT is_even")),
        ]).with_params(HashMap::from([("n".into(), "2".into())]));
        let results = e.run("test").await.unwrap();
        assert!(results.iter().any(|r| r.id == "on_even"  && r.is_success()));
        assert!(results.iter().any(|r| r.id == "on_other" && r.is_skipped()));
    }

    // --- ForEach task ---

    fn foreach_task(id: &str, items: &str, inner: Vec<TaskConfig>, initial: &str, accumulate: &str) -> TaskConfig {
        TaskConfig {
            id: id.into(),
            kind: TaskKind::ForEach {
                items: items.into(),
                tasks: inner,
                initial: initial.into(),
                accumulate: accumulate.into(),
            },
            when: None, depends_on: None, response_template: None, abort_if: None,
        }
    }

    #[tokio::test]
    async fn foreach_accumulates_over_list() {
        std::env::set_var("VORTEX_TEST_ITEMS", r#"["a","b","c"]"#);
        let inner = vec![
            TaskConfig { id: "echo".into(), kind: TaskKind::Shell { exec: "echo {{item}}".into() },
                when: None, depends_on: None, response_template: None, abort_if: None },
        ];
        let e = engine(vec![
            foreach_task("loop", "env.VORTEX_TEST_ITEMS", inner, "[]",
                "acc + [tasks.echo.stdout]"),
        ]);
        let results = e.run("test").await.unwrap();
        std::env::remove_var("VORTEX_TEST_ITEMS");
        let r = results.iter().find(|r| r.id == "loop").unwrap();
        assert!(r.is_success());
        let out: serde_json::Value = serde_json::from_str(&r.stdout).unwrap();
        assert_eq!(out.as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn foreach_item_field_available_in_template() {
        std::env::set_var("VORTEX_TEST_SPACES", r#"[{"id":"s1","name":"friends"},{"id":"s2","name":"work"}]"#);
        let inner = vec![
            TaskConfig { id: "label".into(), kind: TaskKind::Shell { exec: "echo {{item.name}}".into() },
                when: None, depends_on: None, response_template: None, abort_if: None },
        ];
        let e = engine(vec![
            foreach_task("loop", "env.VORTEX_TEST_SPACES", inner, "{}",
                r#"merge(acc, {tasks.label.stdout: item.name})"#),
        ]);
        let results = e.run("test").await.unwrap();
        std::env::remove_var("VORTEX_TEST_SPACES");
        let r = results.iter().find(|r| r.id == "loop").unwrap();
        assert!(r.is_success());
    }

    #[tokio::test]
    async fn foreach_empty_list_returns_initial() {
        std::env::set_var("VORTEX_TEST_EMPTY", "[]");
        let inner = vec![
            TaskConfig { id: "t".into(), kind: TaskKind::Shell { exec: "echo hi".into() },
                when: None, depends_on: None, response_template: None, abort_if: None },
        ];
        let e = engine(vec![
            foreach_task("loop", "env.VORTEX_TEST_EMPTY", inner, "\"done\"", "acc"),
        ]);
        let results = e.run("test").await.unwrap();
        std::env::remove_var("VORTEX_TEST_EMPTY");
        let r = results.iter().find(|r| r.id == "loop").unwrap();
        assert!(r.is_success());
        assert_eq!(r.stdout.trim(), "done");
    }

    #[tokio::test]
    async fn foreach_inner_task_failure_fails_foreach() {
        std::env::set_var("VORTEX_TEST_ONE", r#"["x"]"#);
        let inner = vec![
            TaskConfig { id: "fail".into(), kind: TaskKind::Shell { exec: "exit 1".into() },
                when: None, depends_on: None, response_template: None, abort_if: None },
        ];
        let e = engine(vec![
            foreach_task("loop", "env.VORTEX_TEST_ONE", inner, "{}", "acc"),
        ]);
        let results = e.run("test").await.unwrap();
        std::env::remove_var("VORTEX_TEST_ONE");
        let r = results.iter().find(|r| r.id == "loop").unwrap();
        assert!(r.is_failed());
    }

    #[tokio::test]
    async fn nested_foreach_accumulates() {
        // outer iterates [1,2], inner iterates ["a","b"] — collects 4 combos
        std::env::set_var("VORTEX_TEST_OUTER", r#"["1","2"]"#);
        std::env::set_var("VORTEX_TEST_INNER", r#"["a","b"]"#);
        let inner_inner = vec![
            TaskConfig { id: "combo".into(), kind: TaskKind::Shell { exec: "echo {{item}}".into() },
                when: None, depends_on: None, response_template: None, abort_if: None },
        ];
        let inner = vec![
            foreach_task("inner_loop", "env.VORTEX_TEST_INNER", inner_inner, "[]",
                "merge(acc, [tasks.combo.stdout])"),
        ];
        let e = engine(vec![
            foreach_task("outer_loop", "env.VORTEX_TEST_OUTER", inner, "[]",
                "merge(acc, tasks.inner_loop.output)"),
        ]);
        let results = e.run("test").await.unwrap();
        std::env::remove_var("VORTEX_TEST_OUTER");
        std::env::remove_var("VORTEX_TEST_INNER");
        let r = results.iter().find(|r| r.id == "outer_loop").unwrap();
        assert!(r.is_success());
        let out: serde_json::Value = serde_json::from_str(&r.stdout).unwrap();
        assert_eq!(out.as_array().unwrap().len(), 4);
    }

    // --- status_eval ---

    fn workflow_with_status_eval(tasks: Vec<TaskConfig>, expr: &str) -> WorkflowConfig {
        WorkflowConfig { tasks, cron: None, correlation_id: None, status_eval: Some(expr.into()), log_retention: None }
    }

    #[tokio::test]
    async fn status_eval_can_override_failure_to_success() {
        // build fails, but status_eval says success = tasks.check.success (a different task)
        let e = {
            let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
            Engine::new(
                workflow_with_status_eval(
                    vec![task("check", "true", None), task("build", "exit 1", Some("check"))],
                    "tasks.check.success",
                ),
                path.to_str().unwrap(),
            )
        };
        let results = e.run("test").await.unwrap();
        // build failed, but overall run should be success because check succeeded
        assert!(results.iter().any(|r| r.id == "build" && r.is_failed()));
        let store_path = e.db_path.clone();
        let _s = Store::open(&store_path).unwrap();
        // run_id is unknown here; verify via overall results only
        // overall_success = true because status_eval = tasks.check.success = true
        // We verify by checking the WorkflowFinished event via a channel
        let (tx, mut rx) = broadcast::channel(32);
        let path2 = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        let e2 = Engine::new(
            workflow_with_status_eval(
                vec![task("check", "true", None), task("build", "exit 1", Some("check"))],
                "tasks.check.success",
            ),
            path2.to_str().unwrap(),
        ).with_events(tx).with_run_id("se-1".into());
        e2.run("test").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: true, .. })));
    }

    #[tokio::test]
    async fn status_eval_can_override_success_to_failure() {
        let (tx, mut rx) = broadcast::channel(32);
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        let e = Engine::new(
            workflow_with_status_eval(vec![task("ok", "true", None)], "false"),
            path.to_str().unwrap(),
        ).with_events(tx).with_run_id("se-2".into());
        e.run("test").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: false, .. })));
    }

    #[tokio::test]
    async fn status_eval_error_means_failure() {
        let (tx, mut rx) = broadcast::channel(32);
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        let e = Engine::new(
            workflow_with_status_eval(vec![task("ok", "true", None)], "this is $$$ not valid cel"),
            path.to_str().unwrap(),
        ).with_events(tx).with_run_id("se-3".into());
        e.run("test").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: false, .. })));
    }

    #[tokio::test]
    async fn status_eval_absent_ignores_skipped_tasks() {
        // fail_step fails → skip_me gets skipped → overall should still be failure (fail_step failed)
        let e = engine(vec![
            task("fail_step", "exit 1", None),
            task("skip_me", "echo hi", Some("fail_step")),
        ]);
        let (tx, mut rx) = broadcast::channel(32);
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        let e2 = Engine::new(workflow(vec![
            task("fail_step", "exit 1", None),
            task("skip_me", "echo hi", Some("fail_step")),
        ]), path.to_str().unwrap()).with_events(tx).with_run_id("se-4".into());
        e2.run("test").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: false, .. })));
        drop(e);
    }

    #[tokio::test]
    async fn status_eval_absent_all_pass_is_success() {
        let (tx, mut rx) = broadcast::channel(32);
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        let e = Engine::new(workflow(vec![task("ok", "true", None)]), path.to_str().unwrap())
            .with_events(tx).with_run_id("se-5".into());
        e.run("test").await.unwrap();
        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() { events.push(ev); }
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: true, .. })));
    }

    // --- task log writing ---

    fn engine_with_retention(tasks: Vec<TaskConfig>, log_retention: Option<i32>) -> (Engine, String) {
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        let db = path.to_str().unwrap().to_string();
        let mut wf = workflow(tasks);
        wf.log_retention = log_retention;
        (Engine::new(wf, &db).with_run_id("log-run".into()), db)
    }

    #[tokio::test]
    async fn task_logs_written_after_shell_task() {
        let (e, db) = engine_with_retention(vec![task("greet", "echo hello world", None)], None);
        e.run("wf").await.unwrap();
        let store = Store::open(&db).unwrap();
        let logs = store.get_task_logs("log-run", "greet").unwrap();
        assert!(!logs.is_empty());
        assert!(logs.iter().any(|l| l.line.contains("hello world")));
        assert!(logs.iter().all(|l| l.stream == "stdout"));
    }

    #[tokio::test]
    async fn task_logs_not_written_when_retention_zero() {
        let (e, db) = engine_with_retention(vec![task("greet", "echo hi", None)], Some(0));
        e.run("wf").await.unwrap();
        let store = Store::open(&db).unwrap();
        let logs = store.get_task_logs("log-run", "greet").unwrap();
        assert!(logs.is_empty());
    }

    #[tokio::test]
    async fn task_logs_expiry_set_from_retention_days() {
        let (e, db) = engine_with_retention(vec![task("step", "echo hi", None)], Some(3));
        let before = vortex_core::now_ms();
        e.run("wf").await.unwrap();
        let after = vortex_core::now_ms();
        let store = Store::open(&db).unwrap();
        let logs = store.get_task_logs("log-run", "step").unwrap();
        assert!(!logs.is_empty());
        // expires_at is set; query raw to check
        let exp: Option<i64> = store.conn.query_row(
            "SELECT expires_at FROM task_logs WHERE run_id = 'log-run' AND task_id = 'step' LIMIT 1",
            [], |r| r.get(0),
        ).unwrap();
        let exp = exp.unwrap() as u64;
        let expected_min = before + 3 * 86_400_000;
        let expected_max = after  + 3 * 86_400_000;
        assert!(exp >= expected_min && exp <= expected_max);
    }

    #[tokio::test]
    async fn task_logs_no_expiry_when_retention_minus_one() {
        let (e, db) = engine_with_retention(vec![task("step", "echo hi", None)], Some(-1));
        e.run("wf").await.unwrap();
        let store = Store::open(&db).unwrap();
        let exp: Option<i64> = store.conn.query_row(
            "SELECT expires_at FROM task_logs WHERE run_id = 'log-run' AND task_id = 'step' LIMIT 1",
            [], |r| r.get(0),
        ).unwrap();
        assert!(exp.is_none(), "expires_at should be NULL for log_retention = -1");
    }

    // --- log_expiry helper ---

    #[test]
    fn log_expiry_none_retention_gives_7_day_default() {
        let now = 1_000_000u64;
        let exp = super::log_expiry(None, now);
        assert_eq!(exp, Some(Some(now + 7 * 86_400_000)));
    }

    #[test]
    fn log_expiry_zero_disables_logging() {
        assert_eq!(super::log_expiry(Some(0), 1000), None);
    }

    #[test]
    fn log_expiry_minus_one_keeps_forever() {
        assert_eq!(super::log_expiry(Some(-1), 1000), Some(None));
    }

    #[test]
    fn log_expiry_n_days_computes_correctly() {
        let now = 0u64;
        assert_eq!(super::log_expiry(Some(14), now), Some(Some(14 * 86_400_000)));
    }

    // --- peer tasks ---

    fn peer_task(id: &str, vortex: &str, trigger: &str, params: HashMap<String, String>) -> TaskConfig {
        TaskConfig {
            id: id.into(),
            kind: TaskKind::Peer { vortex: vortex.into(), trigger: trigger.into(), params },
            when: None, depends_on: None, response_template: None, abort_if: None,
        }
    }

    /// Spawn a one-shot mock Unix socket listener that reads one line and writes `response`.
    /// Returns the socket path. The listener task runs in the background.
    async fn mock_peer(response: &'static str) -> String {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::UnixListener;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        drop(tmp); // release the file so we can bind the socket

        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            write.write_all(format!("{response}\n").as_bytes()).await.unwrap();
        });

        path
    }

    /// Like `mock_peer` but captures the received request for inspection.
    async fn mock_peer_capture(response: &'static str) -> (String, tokio::sync::oneshot::Receiver<String>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::UnixListener;

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        drop(tmp);

        let listener = UnixListener::bind(&path).unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, mut write) = stream.into_split();
            let mut reader = BufReader::new(read);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let _ = tx.send(line.trim().to_string());
            write.write_all(format!("{response}\n").as_bytes()).await.unwrap();
        });

        (path, rx)
    }

    #[tokio::test]
    async fn peer_task_succeeds_on_ok_response() {
        let path = mock_peer(r#"{"id":"run-42"}"#).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let e = engine(vec![peer_task("call", &path, "greet", HashMap::new())]);
        let results = e.run("test-wf").await.unwrap();
        assert!(results[0].is_success());
        assert!(results[0].stdout.contains("run-42"));
        assert_eq!(results[0].output.as_ref().and_then(|v| v.get("id")).and_then(|v| v.as_str()), Some("run-42"));
    }

    #[tokio::test]
    async fn peer_task_fails_on_error_response() {
        let path = mock_peer(r#"{"id":"x","status":"error","message":"unknown workflow: no-such"}"#).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let e = engine(vec![peer_task("call", &path, "no-such", HashMap::new())]);
        let results = e.run("test-wf").await.unwrap();
        assert!(results[0].is_failed());
        assert_eq!(results[0].exit_code, 1);
        assert!(results[0].stderr.contains("unknown workflow"));
    }

    #[tokio::test]
    async fn peer_task_sends_workflow_and_params() {
        let (path, rx) = mock_peer_capture(r#"{"id":"r1"}"#).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let params = HashMap::from([("env".into(), "prod".into()), ("branch".into(), "main".into())]);
        let e = engine(vec![peer_task("call", &path, "deploy", params)]);
        e.run("test-wf").await.unwrap();

        let raw = rx.await.unwrap();
        let req: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(req["workflow"], "deploy");
        assert_eq!(req["params"]["env"], "prod");
        assert_eq!(req["params"]["branch"], "main");
    }

    #[tokio::test]
    async fn peer_task_templates_are_rendered_in_vortex_and_trigger() {
        let (path, rx) = mock_peer_capture(r#"{"id":"r1"}"#).await;
        let path_tmpl = format!("{path}"); // used verbatim, but trigger uses a template
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // trigger field uses a trigger param template
        let e = engine(vec![peer_task("call", &path_tmpl, "deploy-{{trigger.env}}", HashMap::new())])
            .with_params(HashMap::from([("env".into(), "staging".into())]));
        e.run("test-wf").await.unwrap();

        let raw = rx.await.unwrap();
        let req: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(req["workflow"], "deploy-staging");
    }

    #[tokio::test]
    async fn peer_task_params_values_are_rendered() {
        let (path, rx) = mock_peer_capture(r#"{"id":"r1"}"#).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let params = HashMap::from([("target".into(), "{{trigger.host}}".into())]);
        let e = engine(vec![peer_task("call", &path, "ping", params)])
            .with_params(HashMap::from([("host".into(), "db.prod".into())]));
        e.run("test-wf").await.unwrap();

        let raw = rx.await.unwrap();
        let req: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(req["params"]["target"], "db.prod");
    }

    #[tokio::test]
    async fn peer_task_gates_downstream_on_failure() {
        let path = mock_peer(r#"{"id":"x","status":"error","message":"nope"}"#).await;
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let e = engine(vec![
            peer_task("call", &path, "deploy", HashMap::new()),
            task("on_success", "echo should_skip", Some("call")),
            task("on_failure", "echo ran",          Some("NOT call")),
        ]);
        let results = e.run("test-wf").await.unwrap();
        assert!(results.iter().any(|r| r.id == "on_success" && r.is_skipped()));
        assert!(results.iter().any(|r| r.id == "on_failure" && r.is_success()));
    }

    #[tokio::test]
    async fn peer_task_fails_when_socket_not_found() {
        let e = engine(vec![peer_task("call", "/tmp/no-such-vortex.sock", "deploy", HashMap::new())]);
        let results = e.run("test-wf").await.unwrap();
        assert!(results[0].is_failed());
    }
}
