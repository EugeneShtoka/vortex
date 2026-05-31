use std::collections::HashMap;

use anyhow::{bail, Result};
use tokio::process::Command;
use tracing::{error, info, warn};

use tokio::sync::broadcast;

use crate::config::{WorkflowConfig, TaskConfig};
use crate::event::Event;
use crate::gate;
use crate::store::Store;
use crate::template;

#[derive(Debug, Clone)]
pub struct TaskResult {
    pub id: String,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub success: bool,
}

pub struct Engine {
    config: WorkflowConfig,
    db_path: String,
    event_tx: Option<broadcast::Sender<Event>>,
    run_id: Option<String>,
    trigger_params: HashMap<String, String>,
}

impl Engine {
    pub fn new(config: WorkflowConfig, db_path: &str) -> Self {
        Self { config, db_path: db_path.to_string(), event_tx: None, run_id: None, trigger_params: HashMap::new() }
    }

    pub fn with_events(mut self, tx: broadcast::Sender<Event>) -> Self {
        self.event_tx = Some(tx);
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
        let globals = store.get_all()?;
        store.insert_run(&run_id, workflow_name, &serde_json::to_string(&self.trigger_params).unwrap_or_else(|_| "{}".into()), started_at)?;

        let all_ids: Vec<&str> = self.config.tasks.iter().map(|t| t.id.as_str()).collect();
        let mut results: HashMap<String, TaskResult> = HashMap::new();
        let mut all_results = Vec::new();

        for task in &ordered {
            if !self.gate_allows(task, &results, &all_ids)? {
                warn!(task = %task.id, "Skipped (gate not met)");
                let ts = vortex_core::now_ms();
                store.upsert_task(&run_id, &task.id, "skipped", None, None, None, Some(ts), Some(ts))?;
                self.emit(Event::TaskSkipped { run_id: run_id.clone(), task: task.id.clone(), timestamp: ts });
                continue;
            }

            let result = self.run_task(task, &run_id, &results, &globals).await?;
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
        let exec = template::render(&task.exec, results, globals, &self.trigger_params)?;
        info!(task = %task.id, exec = %exec, "Running task");

        let started_at = vortex_core::now_ms();
        let store = Store::open(&self.db_path)?;
        store.upsert_task(run_id, &task.id, "running", None, None, None, Some(started_at), None)?;
        self.emit(Event::TaskStarted { run_id: run_id.to_string(), task: task.id.clone(), timestamp: started_at });

        let params_json = serde_json::to_string(&self.trigger_params).unwrap_or_else(|_| "{}".into());
        let output = Command::new("/bin/sh")
            .arg("-c")
            .arg(&exec)
            .env("VORTEX_TRIGGER_PARAMS", &params_json)
            .output().await?;

        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let exit_code = output.status.code().unwrap_or(-1);
        let success = output.status.success();

        if !stdout.trim().is_empty() { info!(task = %task.id, "stdout: {}", stdout.trim_end()); }
        if !stderr.trim().is_empty() { error!(task = %task.id, "stderr: {}", stderr.trim_end()); }
        if success { info!(task = %task.id, "Finished OK") } else { warn!(task = %task.id, exit_code, "Finished with error") }

        let finished_at = vortex_core::now_ms();
        store.upsert_task(run_id, &task.id, if success { "success" } else { "failure" },
            Some(exit_code), Some(&stdout), Some(&stderr), Some(started_at), Some(finished_at))?;
        self.emit(Event::TaskFinished {
            run_id: run_id.to_string(), task: task.id.clone(),
            success, exit_code,
            stdout: stdout.clone(), stderr: stderr.clone(),
            timestamp: finished_at,
        });

        Ok(TaskResult { id: task.id.clone(), stdout, stderr, exit_code, success })
    }

    fn gate_allows(
        &self,
        task: &TaskConfig,
        results: &HashMap<String, TaskResult>,
        all_ids: &[&str],
    ) -> Result<bool> {
        match &task.when {
            None => Ok(true),
            Some(expr) => gate::evaluate(expr, results, all_ids),
        }
    }

    /// Kahn's topological sort. All task IDs referenced in a `when` expression
    /// become dependency edges, so compound gates like `"a AND b"` correctly
    /// order all mentioned tasks before the dependent one.
    fn topological_sort(&self) -> Result<Vec<TaskConfig>> {
        let tasks = &self.config.tasks;
        let task_ids: HashMap<&str, usize> =
            tasks.iter().enumerate().map(|(i, t)| (t.id.as_str(), i)).collect();

        let mut deps: Vec<Vec<usize>> = vec![vec![]; tasks.len()];
        for (i, task) in tasks.iter().enumerate() {
            if let Some(expr) = &task.when {
                let mut seen = std::collections::HashSet::new();
                for token in expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-') {
                    if token.is_empty() || matches!(token, "AND" | "OR" | "NOT") {
                        continue;
                    }
                    if let Some(&j) = task_ids.get(token) {
                        if seen.insert(j) {
                            deps[i].push(j);
                        }
                    }
                }
            }
        }

        let mut rev: Vec<Vec<usize>> = vec![vec![]; tasks.len()];
        for (i, task_deps) in deps.iter().enumerate() {
            for &j in task_deps {
                rev[j].push(i);
            }
        }

        let mut in_degree: Vec<usize> = deps.iter().map(|d| d.len()).collect();
        let mut queue: Vec<usize> = in_degree
            .iter()
            .enumerate()
            .filter(|(_, &d)| d == 0)
            .map(|(i, _)| i)
            .collect();

        let mut ordered = Vec::with_capacity(tasks.len());
        while !queue.is_empty() {
            queue.sort_unstable();
            let cur = queue.remove(0);
            ordered.push(tasks[cur].clone());
            for &next in &rev[cur] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    queue.push(next);
                }
            }
        }

        if ordered.len() != tasks.len() {
            bail!("Circular dependency detected in task graph");
        }

        Ok(ordered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{WorkflowConfig, TaskConfig};
    use crate::event::Event;

    fn task(id: &str, exec: &str, when: Option<&str>) -> TaskConfig {
        TaskConfig { id: id.into(), exec: exec.into(), when: when.map(str::to_string) }
    }

    fn workflow(tasks: Vec<TaskConfig>) -> WorkflowConfig {
        WorkflowConfig { tasks }
    }

    fn engine(tasks: Vec<TaskConfig>) -> Engine {
        let path = std::env::temp_dir().join(format!("vortex-test-{}.db", uuid::Uuid::new_v4()));
        Engine::new(workflow(tasks), path.to_str().unwrap())
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
        // c is listed before b in config — without full dep extraction, c could run before b
        let e = engine(vec![
            task("a", "echo a", None),
            task("c", "echo c", Some("a AND b")),
            task("b", "echo b", None),
        ]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        let pos_c = order.iter().position(|x| x == "c").unwrap();
        assert!(order.iter().position(|x| x == "a").unwrap() < pos_c);
        assert!(order.iter().position(|x| x == "b").unwrap() < pos_c);
    }

    #[test]
    fn topo_sort_or_gate_orders_both_deps() {
        let e = engine(vec![
            task("a", "echo a", None),
            task("c", "echo c", Some("a OR b")),
            task("b", "echo b", None),
        ]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        let pos_c = order.iter().position(|x| x == "c").unwrap();
        assert!(order.iter().position(|x| x == "a").unwrap() < pos_c);
        assert!(order.iter().position(|x| x == "b").unwrap() < pos_c);
    }

    #[test]
    fn topo_sort_complex_expression_orders_all_deps() {
        // d listed second, depends on a, b, c all out-of-order
        let e = engine(vec![
            task("a", "echo a", None),
            task("d", "echo d", Some("(a AND b) OR c")),
            task("b", "echo b", None),
            task("c", "echo c", None),
        ]);
        let order: Vec<_> = e.topological_sort().unwrap().iter().map(|t| t.id.clone()).collect();
        let pos_d = order.iter().position(|x| x == "d").unwrap();
        assert!(order.iter().position(|x| x == "a").unwrap() < pos_d);
        assert!(order.iter().position(|x| x == "b").unwrap() < pos_d);
        assert!(order.iter().position(|x| x == "c").unwrap() < pos_d);
    }

    // --- gate integration (Sprint 2: full boolean via evalexpr) ---

    #[test]
    fn gate_none_always_runs() {
        let e = engine(vec![]);
        let t = task("x", "echo", None);
        assert!(e.gate_allows(&t, &HashMap::new(), &[]).unwrap());
    }

    #[test]
    fn gate_positive_dep_runs_if_success() {
        let e = engine(vec![]);
        let t = task("x", "echo", Some("a"));
        let ok = HashMap::from([(
            "a".into(),
            TaskResult { id: "a".into(), stdout: String::new(), stderr: String::new(), exit_code: 0, success: true },
        )]);
        let fail = HashMap::from([(
            "a".into(),
            TaskResult { id: "a".into(), stdout: String::new(), stderr: String::new(), exit_code: 1, success: false },
        )]);
        assert!(e.gate_allows(&t, &ok, &["a"]).unwrap());
        assert!(!e.gate_allows(&t, &fail, &["a"]).unwrap());
        assert!(!e.gate_allows(&t, &HashMap::new(), &["a"]).unwrap());
    }

    #[test]
    fn gate_and_expression() {
        let e = engine(vec![]);
        let t = task("x", "echo", Some("a AND b"));
        let both = HashMap::from([
            ("a".into(), TaskResult { id: "a".into(), stdout: String::new(), stderr: String::new(), exit_code: 0, success: true }),
            ("b".into(), TaskResult { id: "b".into(), stdout: String::new(), stderr: String::new(), exit_code: 0, success: true }),
        ]);
        let one_fail = HashMap::from([
            ("a".into(), TaskResult { id: "a".into(), stdout: String::new(), stderr: String::new(), exit_code: 0, success: true }),
            ("b".into(), TaskResult { id: "b".into(), stdout: String::new(), stderr: String::new(), exit_code: 1, success: false }),
        ]);
        assert!(e.gate_allows(&t, &both, &["a", "b"]).unwrap());
        assert!(!e.gate_allows(&t, &one_fail, &["a", "b"]).unwrap());
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
            task("skip_me", "echo should_not_run", Some("fail_step")),
            task("run_me", "echo recovery", Some("NOT fail_step")),
        ]);
        let results = e.run("test-workflow").await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, "fail_step");
        assert!(!results[0].success);
        assert_eq!(results[1].id, "run_me");
        assert!(results[1].success);
        assert!(results[1].stdout.contains("recovery"));
    }

    #[tokio::test]
    async fn run_injects_stdout_into_next_task() {
        let e = engine(vec![
            task("producer", "echo artifact.tar", None),
            task("consumer", "echo got={{tasks.producer.stdout}}", Some("producer")),
        ]);
        let results = e.run("test-workflow").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[1].stdout.contains("got=artifact.tar"));
    }

    #[tokio::test]
    async fn run_and_gate_skips_if_either_fails() {
        let e = engine(vec![
            task("a", "echo a", None),
            task("b", "exit 1", None),
            task("c", "echo c", Some("a AND b")),
        ]);
        let results = e.run("test-workflow").await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(!results.iter().any(|r| r.id == "c"));
    }

    // --- Sprint 3: event emission ---

    #[tokio::test]
    async fn run_emits_lifecycle_events() {
        use tokio::sync::broadcast;
        let (tx, mut rx) = broadcast::channel(32);
        let e = engine(vec![task("step", "echo hi", None)])
            .with_events(tx)
            .with_run_id("run-1".into());
        e.run("test-workflow").await.unwrap();

        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(events.iter().any(|e| matches!(e, Event::WorkflowStarted { run_id, .. } if run_id == "run-1")));
        assert!(events.iter().any(|e| matches!(e, Event::TaskStarted    { task, .. } if task == "step")));
        assert!(events.iter().any(|e| matches!(e, Event::TaskFinished   { task, success: true, .. } if task == "step")));
        assert!(events.iter().any(|e| matches!(e, Event::WorkflowFinished { success: true, .. })));
    }

    #[tokio::test]
    async fn run_emits_task_skipped_when_gate_fails() {
        use tokio::sync::broadcast;
        let (tx, mut rx) = broadcast::channel(32);
        let e = engine(vec![
            task("fail", "exit 1", None),
            task("skip", "echo nope", Some("fail")),
        ])
        .with_events(tx)
        .with_run_id("run-2".into());
        e.run("test-workflow").await.unwrap();

        let mut events = vec![];
        while let Ok(ev) = rx.try_recv() {
            events.push(ev);
        }

        assert!(events.iter().any(|e| matches!(e, Event::TaskSkipped { task, .. } if task == "skip")));
    }

    // --- Sprint 4: trigger params ---

    #[tokio::test]
    async fn run_injects_trigger_params_into_task() {
        let e = engine(vec![task("greet", "echo {{trigger.name}}", None)])
            .with_params(HashMap::from([("name".into(), "vortex".into())]));
        let results = e.run("test-workflow").await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].stdout.contains("vortex"));
    }

    #[tokio::test]
    async fn run_missing_trigger_param_renders_empty() {
        let e = engine(vec![task("greet", "echo x{{trigger.missing}}y", None)]);
        let results = e.run("test-workflow").await.unwrap();
        assert!(results[0].stdout.contains("xy"));
    }

    // --- Sprint 6: history persistence ---

    #[tokio::test]
    async fn run_persists_to_store() {
        let e = engine(vec![
            task("step1", "echo hello", None),
            task("step2", "echo world", Some("step1")),
        ]).with_run_id("hist-1".into());
        e.run("test-workflow").await.unwrap();

        let store = crate::store::Store::open(":memory:").unwrap();
        // Engine opens its own :memory: store — verify via a fresh engine run that
        // writes to a file-based store we can then inspect.
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
        let e = Engine::new(
            workflow(vec![task("fail", "exit 1", None), task("skip", "echo nope", Some("fail"))]),
            &db,
        ).with_run_id("hist-3".into());
        e.run("wf").await.unwrap();

        let s = Store::open(&db).unwrap();
        let run = s.get_run("hist-3").unwrap().unwrap();
        assert_eq!(run.run.status, "failure");
        let skip_task = run.tasks.iter().find(|t| t.task_id == "skip").unwrap();
        assert_eq!(skip_task.status, "skipped");
    }
}
