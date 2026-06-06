use std::time::Instant;

use indexmap::IndexMap;
use serde::Deserialize;
use vortex_core::Event;

use crate::graph::DependencyGraph;

/// REST response shapes from GET /runs and GET /runs/{id}
#[derive(Debug, Clone, Deserialize)]
pub struct RunSummary {
    pub id: String,
    pub workflow: String,
    pub status: String,
    pub rejection: Option<String>,
    pub started_at: u64,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskSummary {
    pub task_id: String,
    pub status: String,
    pub exit_code: Option<i32>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
    pub started_at: Option<u64>,
    pub finished_at: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunDetailDto {
    #[serde(flatten)]
    pub summary: RunSummary,
    pub tasks: Vec<TaskSummary>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum RunStatus {
    Running,
    Finished(bool),
    Rejected(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TaskStatus {
    Running,
    Finished { success: bool, exit_code: i32, stdout: String, stderr: String },
    Skipped,
}

#[derive(Debug, Clone)]
pub struct RunState {
    pub workflow: String,
    pub status: RunStatus,
    pub tasks: IndexMap<String, TaskStatus>,
    pub started_at: Instant,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected(Option<String>),
}

/// Per-daemon state: runs, selection, graph modal, connection status.
pub struct SourceState {
    pub name: String,
    pub connection: ConnectionStatus,
    pub runs: IndexMap<String, RunState>,
    pub selected: usize,
    pub graph: Option<DependencyGraph>,
    pub show_graph: bool,
}

impl SourceState {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            connection: ConnectionStatus::Connecting,
            runs: IndexMap::new(),
            selected: 0,
            graph: None,
            show_graph: false,
        }
    }

    pub fn handle(&mut self, event: Event) {
        match event {
            Event::WorkflowStarted { run_id, workflow, timestamp } => {
                self.runs.insert(run_id, RunState {
                    workflow,
                    status: RunStatus::Running,
                    tasks: IndexMap::new(),
                    started_at: Instant::now(),
                    started_at_ms: timestamp,
                    finished_at_ms: None,
                });
            }
            Event::TriggerRejected { run_id, reason } => {
                self.runs.insert(run_id, RunState {
                    workflow: String::new(),
                    status: RunStatus::Rejected(reason),
                    tasks: IndexMap::new(),
                    started_at: Instant::now(),
                    started_at_ms: 0,
                    finished_at_ms: None,
                });
            }
            Event::TaskStarted { run_id, task, .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.tasks.insert(task, TaskStatus::Running);
                }
            }
            Event::TaskFinished { run_id, task, success, exit_code, stdout, stderr, .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.tasks.insert(task, TaskStatus::Finished { success, exit_code, stdout, stderr });
                }
            }
            Event::TaskSkipped { run_id, task, .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.tasks.insert(task, TaskStatus::Skipped);
                }
            }
            Event::WorkflowFinished { run_id, success, timestamp, .. } => {
                if let Some(run) = self.runs.get_mut(&run_id) {
                    run.status = RunStatus::Finished(success);
                    run.finished_at_ms = Some(timestamp);
                }
            }
            _ => {}
        }
    }

    /// Populate a run from a REST API response. Upserts — existing live runs are updated.
    pub fn apply_run_detail(&mut self, detail: RunDetailDto) {
        let status = match detail.summary.status.as_str() {
            "success"  => RunStatus::Finished(true),
            "failure"  => RunStatus::Finished(false),
            "rejected" => RunStatus::Rejected(detail.summary.rejection.clone().unwrap_or_default()),
            _          => RunStatus::Running,
        };
        let tasks = detail.tasks.iter().map(|t| {
            let ts = match t.status.as_str() {
                "success" => TaskStatus::Finished {
                    success: true,
                    exit_code: t.exit_code.unwrap_or(0),
                    stdout: t.stdout.clone().unwrap_or_default(),
                    stderr: t.stderr.clone().unwrap_or_default(),
                },
                "failure" => TaskStatus::Finished {
                    success: false,
                    exit_code: t.exit_code.unwrap_or(1),
                    stdout: t.stdout.clone().unwrap_or_default(),
                    stderr: t.stderr.clone().unwrap_or_default(),
                },
                "skipped" => TaskStatus::Skipped,
                _         => TaskStatus::Running,
            };
            (t.task_id.clone(), ts)
        }).collect();

        self.runs.insert(detail.summary.id.clone(), RunState {
            workflow: detail.summary.workflow,
            status,
            tasks,
            started_at: Instant::now(),
            started_at_ms: detail.summary.started_at,
            finished_at_ms: detail.summary.finished_at,
        });
    }

    pub fn set_graph(&mut self, graph: DependencyGraph) {
        self.graph = Some(graph);
        self.show_graph = false;
    }

    pub fn toggle_graph(&mut self) {
        if self.graph.is_some() {
            self.show_graph = !self.show_graph;
        }
    }

    pub fn select_next(&mut self) {
        if !self.runs.is_empty() {
            self.selected = (self.selected + 1).min(self.runs.len() - 1);
        }
    }

    pub fn select_prev(&mut self) {
        self.selected = self.selected.saturating_sub(1);
    }

    pub fn selected_run(&self) -> Option<(&String, &RunState)> {
        self.runs.get_index(self.selected)
    }
}

/// Top-level TUI state owning all per-daemon sources and the active tab index.
pub struct App {
    pub sources: Vec<SourceState>,
    pub active: usize,
}

impl App {
    /// Creates a single anonymous source (used in tests and single-source mode).
    pub fn new() -> Self {
        Self { sources: vec![SourceState::new("")], active: 0 }
    }

    pub fn with_source_names(names: &[&str]) -> Self {
        Self {
            sources: names.iter().map(|n| SourceState::new(*n)).collect(),
            active: 0,
        }
    }

    pub fn active_source(&self) -> &SourceState {
        &self.sources[self.active]
    }

    pub fn active_source_mut(&mut self) -> &mut SourceState {
        &mut self.sources[self.active]
    }

    pub fn next_source(&mut self) {
        self.active = (self.active + 1).min(self.sources.len().saturating_sub(1));
    }

    pub fn prev_source(&mut self) {
        self.active = self.active.saturating_sub(1);
    }

    // --- delegate to active source (keeps call-sites unchanged) ---

    pub fn handle(&mut self, event: Event) {
        self.active_source_mut().handle(event);
    }

    /// Route an event from a specific source index (used by the multi-source WS loop).
    pub fn handle_sourced(&mut self, source_idx: usize, event: Event) {
        if let Some(src) = self.sources.get_mut(source_idx) {
            src.handle(event);
        }
    }

    pub fn select_next(&mut self) { self.active_source_mut().select_next(); }
    pub fn select_prev(&mut self) { self.active_source_mut().select_prev(); }
    pub fn selected_run(&self) -> Option<(&String, &RunState)> { self.active_source().selected_run() }
    pub fn set_graph(&mut self, graph: DependencyGraph) { self.active_source_mut().set_graph(graph); }
    pub fn toggle_graph(&mut self) { self.active_source_mut().toggle_graph(); }
    pub fn apply_run_detail(&mut self, detail: RunDetailDto) { self.active_source_mut().apply_run_detail(detail); }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn started(run_id: &str, workflow: &str) -> Event {
        Event::WorkflowStarted { run_id: run_id.into(), workflow: workflow.into(), timestamp: 0 }
    }

    fn task_started(run_id: &str, task: &str) -> Event {
        Event::TaskStarted { run_id: run_id.into(), task: task.into(), timestamp: 0 }
    }

    fn task_finished(run_id: &str, task: &str, success: bool) -> Event {
        Event::TaskFinished {
            run_id: run_id.into(),
            task: task.into(),
            success,
            exit_code: if success { 0 } else { 1 },
            stdout: String::new(),
            stderr: String::new(),
            timestamp: 0,
        }
    }

    fn task_skipped(run_id: &str, task: &str) -> Event {
        Event::TaskSkipped { run_id: run_id.into(), task: task.into(), timestamp: 0 }
    }

    fn workflow_finished(run_id: &str, workflow: &str, success: bool) -> Event {
        Event::WorkflowFinished { run_id: run_id.into(), workflow: workflow.into(), success, timestamp: 0 }
    }

    fn rejected(run_id: &str, reason: &str) -> Event {
        Event::TriggerRejected { run_id: run_id.into(), reason: reason.into() }
    }

    #[test]
    fn run_added_on_workflow_started() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        assert_eq!(app.active_source().runs.len(), 1);
        let (id, run) = app.active_source().runs.get_index(0).unwrap();
        assert_eq!(id, "r1");
        assert_eq!(run.workflow, "deploy");
        assert_eq!(run.status, RunStatus::Running);
    }

    #[test]
    fn task_status_updated_on_task_started() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(task_started("r1", "pull"));
        let run = app.active_source().runs.get("r1").unwrap();
        assert_eq!(run.tasks.get("pull"), Some(&TaskStatus::Running));
    }

    #[test]
    fn task_status_updated_on_task_finished() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(task_started("r1", "build"));
        app.handle(task_finished("r1", "build", true));
        let run = app.active_source().runs.get("r1").unwrap();
        assert!(matches!(
            run.tasks.get("build"),
            Some(TaskStatus::Finished { success: true, .. })
        ));
    }

    #[test]
    fn task_status_updated_on_task_skipped() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(task_skipped("r1", "notify"));
        let run = app.active_source().runs.get("r1").unwrap();
        assert_eq!(run.tasks.get("notify"), Some(&TaskStatus::Skipped));
    }

    #[test]
    fn run_status_updated_on_workflow_finished() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(workflow_finished("r1", "deploy", true));
        assert_eq!(app.active_source().runs.get("r1").unwrap().status, RunStatus::Finished(true));
    }

    #[test]
    fn run_marked_rejected_on_trigger_rejected() {
        let mut app = App::new();
        app.handle(rejected("r1", "unauthorized"));
        assert_eq!(
            app.active_source().runs.get("r1").unwrap().status,
            RunStatus::Rejected("unauthorized".into())
        );
    }

    #[test]
    fn multiple_concurrent_runs_tracked_independently() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(started("r2", "build"));
        app.handle(task_started("r1", "pull"));
        app.handle(task_started("r2", "compile"));
        app.handle(workflow_finished("r1", "deploy", true));

        assert_eq!(app.active_source().runs.get("r1").unwrap().status, RunStatus::Finished(true));
        assert_eq!(app.active_source().runs.get("r2").unwrap().status, RunStatus::Running);
        assert!(app.active_source().runs.get("r2").unwrap().tasks.contains_key("compile"));
    }

    #[test]
    fn select_next_and_prev_clamp_to_bounds() {
        let mut app = App::new();
        app.handle(started("r1", "a"));
        app.handle(started("r2", "b"));
        app.handle(started("r3", "c"));

        assert_eq!(app.active_source().selected, 0);
        app.select_next();
        assert_eq!(app.active_source().selected, 1);
        app.select_next();
        assert_eq!(app.active_source().selected, 2);
        app.select_next(); // at end — clamp
        assert_eq!(app.active_source().selected, 2);
        app.select_prev();
        assert_eq!(app.active_source().selected, 1);
        app.select_prev();
        app.select_prev(); // at start — clamp
        assert_eq!(app.active_source().selected, 0);
    }

    #[test]
    fn task_stdout_captured_in_finished_status() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(Event::TaskFinished {
            run_id: "r1".into(),
            task: "build".into(),
            success: true,
            exit_code: 0,
            stdout: "artifact.tar\n".into(),
            stderr: String::new(),
            timestamp: 0,
        });
        let run = app.active_source().runs.get("r1").unwrap();
        if let Some(TaskStatus::Finished { stdout, .. }) = run.tasks.get("build") {
            assert_eq!(stdout, "artifact.tar\n");
        } else {
            panic!("expected Finished status");
        }
    }

    // --- Sprint 6: apply_run_detail + timestamps ---

    fn make_detail(id: &str, workflow: &str, status: &str, started_at: u64, finished_at: Option<u64>) -> RunDetailDto {
        RunDetailDto {
            summary: RunSummary {
                id: id.into(), workflow: workflow.into(), status: status.into(),
                rejection: None, started_at, finished_at,
            },
            tasks: vec![],
        }
    }

    #[test]
    fn apply_run_detail_populates_finished_run() {
        let mut app = App::new();
        app.apply_run_detail(make_detail("r1", "deploy", "success", 1000, Some(2000)));
        let run = app.active_source().runs.get("r1").unwrap();
        assert_eq!(run.workflow, "deploy");
        assert_eq!(run.status, RunStatus::Finished(true));
        assert_eq!(run.started_at_ms, 1000);
        assert_eq!(run.finished_at_ms, Some(2000));
    }

    #[test]
    fn apply_run_detail_populates_rejected_run() {
        let detail = RunDetailDto {
            summary: RunSummary {
                id: "r1".into(), workflow: String::new(), status: "rejected".into(),
                rejection: Some("unauthorized".into()), started_at: 1000, finished_at: Some(1000),
            },
            tasks: vec![],
        };
        let mut app = App::new();
        app.apply_run_detail(detail);
        assert_eq!(app.active_source().runs.get("r1").unwrap().status, RunStatus::Rejected("unauthorized".into()));
    }

    #[test]
    fn apply_run_detail_populates_task_statuses() {
        let detail = RunDetailDto {
            summary: RunSummary {
                id: "r1".into(), workflow: "wf".into(), status: "success".into(),
                rejection: None, started_at: 0, finished_at: Some(1),
            },
            tasks: vec![
                TaskSummary { task_id: "pull".into(), status: "success".into(), exit_code: Some(0), stdout: Some("ok\n".into()), stderr: Some(String::new()), started_at: Some(0), finished_at: Some(1) },
                TaskSummary { task_id: "notify".into(), status: "skipped".into(), exit_code: None, stdout: None, stderr: None, started_at: None, finished_at: None },
            ],
        };
        let mut app = App::new();
        app.apply_run_detail(detail);
        let run = app.active_source().runs.get("r1").unwrap();
        assert!(matches!(run.tasks.get("pull"), Some(TaskStatus::Finished { success: true, .. })));
        assert_eq!(run.tasks.get("notify"), Some(&TaskStatus::Skipped));
    }

    #[test]
    fn apply_run_detail_upserts_live_run() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        assert_eq!(app.active_source().runs.get("r1").unwrap().status, RunStatus::Running);
        app.apply_run_detail(make_detail("r1", "deploy", "success", 1000, Some(2000)));
        assert_eq!(app.active_source().runs.get("r1").unwrap().status, RunStatus::Finished(true));
        assert_eq!(app.active_source().runs.len(), 1);
    }

    // --- Sprint 7: graph state ---

    fn make_graph(workflow: &str) -> DependencyGraph {
        use crate::graph::{WorkflowConfigDto, TaskConfigDto};
        DependencyGraph::from_config(WorkflowConfigDto {
            name: workflow.into(),
            tasks: vec![
                TaskConfigDto { id: "a".into(), exec: None, when: None },
                TaskConfigDto { id: "b".into(), exec: None, when: Some("a".into()) },
            ],
        })
    }

    #[test]
    fn set_graph_stores_graph_and_hides_modal() {
        let mut app = App::new();
        app.active_source_mut().show_graph = true;
        app.set_graph(make_graph("deploy"));
        assert!(app.active_source().graph.is_some());
        assert_eq!(app.active_source().graph.as_ref().unwrap().workflow, "deploy");
        assert!(!app.active_source().show_graph, "set_graph should reset show_graph to false");
    }

    #[test]
    fn toggle_graph_shows_and_hides_modal() {
        let mut app = App::new();
        assert!(!app.active_source().show_graph);
        app.toggle_graph(); // no graph yet — stays false
        assert!(!app.active_source().show_graph);
        app.set_graph(make_graph("deploy"));
        app.toggle_graph();
        assert!(app.active_source().show_graph);
        app.toggle_graph();
        assert!(!app.active_source().show_graph);
    }

    #[test]
    fn finished_at_ms_set_from_workflow_finished_event() {
        let mut app = App::new();
        app.handle(Event::WorkflowStarted { run_id: "r1".into(), workflow: "wf".into(), timestamp: 1000 });
        app.handle(Event::WorkflowFinished { run_id: "r1".into(), workflow: "wf".into(), success: true, timestamp: 2000 });
        let run = app.active_source().runs.get("r1").unwrap();
        assert_eq!(run.started_at_ms, 1000);
        assert_eq!(run.finished_at_ms, Some(2000));
    }

    // --- Sprint 8: multi-source ---

    #[test]
    fn app_with_multiple_sources_has_correct_names() {
        let app = App::with_source_names(&["local", "prod", "staging"]);
        assert_eq!(app.sources.len(), 3);
        assert_eq!(app.sources[0].name, "local");
        assert_eq!(app.sources[1].name, "prod");
        assert_eq!(app.sources[2].name, "staging");
    }

    #[test]
    fn handle_sourced_routes_event_to_correct_source() {
        let mut app = App::with_source_names(&["local", "prod"]);
        app.handle_sourced(1, started("r1", "deploy"));
        assert_eq!(app.sources[0].runs.len(), 0, "source 0 should not receive event");
        assert_eq!(app.sources[1].runs.len(), 1, "source 1 should receive event");
    }

    #[test]
    fn handle_sourced_ignores_out_of_bounds_index() {
        let mut app = App::with_source_names(&["local"]);
        // Should not panic
        app.handle_sourced(99, started("r1", "deploy"));
        assert_eq!(app.sources[0].runs.len(), 0);
    }

    #[test]
    fn next_source_increments_active() {
        let mut app = App::with_source_names(&["a", "b", "c"]);
        assert_eq!(app.active, 0);
        app.next_source();
        assert_eq!(app.active, 1);
        app.next_source();
        assert_eq!(app.active, 2);
    }

    #[test]
    fn next_source_clamps_at_last() {
        let mut app = App::with_source_names(&["a", "b"]);
        app.next_source();
        assert_eq!(app.active, 1);
        app.next_source(); // already at last
        assert_eq!(app.active, 1);
    }

    #[test]
    fn prev_source_decrements_active() {
        let mut app = App::with_source_names(&["a", "b", "c"]);
        app.active = 2;
        app.prev_source();
        assert_eq!(app.active, 1);
        app.prev_source();
        assert_eq!(app.active, 0);
    }

    #[test]
    fn prev_source_clamps_at_zero() {
        let mut app = App::with_source_names(&["a", "b"]);
        app.prev_source(); // already at 0
        assert_eq!(app.active, 0);
    }

    #[test]
    fn active_source_reflects_active_index() {
        let mut app = App::with_source_names(&["local", "prod"]);
        app.handle_sourced(0, started("local-run", "deploy"));
        app.handle_sourced(1, started("prod-run", "build"));

        app.active = 0;
        assert!(app.active_source().runs.contains_key("local-run"));
        assert!(!app.active_source().runs.contains_key("prod-run"));

        app.active = 1;
        assert!(app.active_source().runs.contains_key("prod-run"));
        assert!(!app.active_source().runs.contains_key("local-run"));
    }

    #[test]
    fn sources_start_with_connecting_status() {
        let app = App::with_source_names(&["local", "prod"]);
        assert_eq!(app.sources[0].connection, ConnectionStatus::Connecting);
        assert_eq!(app.sources[1].connection, ConnectionStatus::Connecting);
    }

    #[test]
    fn each_source_has_independent_selection() {
        let mut app = App::with_source_names(&["local", "prod"]);
        app.handle_sourced(0, started("r1", "a"));
        app.handle_sourced(0, started("r2", "b"));
        app.handle_sourced(1, started("r3", "c"));

        app.active = 0;
        app.select_next();
        assert_eq!(app.active_source().selected, 1);

        app.active = 1;
        assert_eq!(app.active_source().selected, 0, "source 1 selection should be independent");
    }
}
