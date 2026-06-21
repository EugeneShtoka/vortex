use std::collections::HashMap;
use std::time::Instant;

use indexmap::IndexMap;
use serde::Deserialize;
use vortex_core::Event;

use crate::config::{TuiLayout, ViewMode};
use crate::graph::DependencyGraph;

/// REST response shape from GET /workflows
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowIssueSummary {
    pub name:        String,
    pub issue_count: WorkflowIssueCounts,
    pub issues:      Vec<WorkflowIssue>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowIssueCounts {
    pub errors:   usize,
    pub warnings: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowIssue {
    pub severity: String,
    pub task_id:  Option<String>,
    pub code:     String,
    pub message:  String,
}

/// REST response shape from GET /runs/{id}/tasks/{task_id}/logs
#[derive(Debug, Clone, Deserialize)]
pub struct LogEntry {
    pub run_id:    String,
    pub task_id:   String,
    pub stream:    String,
    pub line:      String,
    pub logged_at: u64,
}

/// REST response shapes from GET /runs and GET /runs/{id}
#[derive(Debug, Clone, Deserialize)]
pub struct RunSummary {
    pub id: String,
    pub workflow: String,
    pub status: String,
    #[serde(default)]
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
    // Sprint 17 — task config fields, populated by GET /runs/{id}
    #[serde(default)]
    pub task_type: Option<String>,
    #[serde(default)]
    pub task_exec: Option<String>,
    #[serde(default)]
    pub task_when: Option<String>,
    #[serde(default)]
    pub task_abort_if: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GlobalsDiffEntry {
    Changed { key: String, before: String, after: String },
    Added   { key: String, value: String },
    Removed { key: String, value: String },
}

pub fn diff_globals(
    pre: &HashMap<String, String>,
    post: &HashMap<String, String>,
) -> Vec<GlobalsDiffEntry> {
    let mut diff = Vec::new();
    for (key, after) in post {
        match pre.get(key) {
            Some(before) if before != after => diff.push(GlobalsDiffEntry::Changed {
                key: key.clone(), before: before.clone(), after: after.clone(),
            }),
            None => diff.push(GlobalsDiffEntry::Added { key: key.clone(), value: after.clone() }),
            _ => {}
        }
    }
    for key in pre.keys() {
        if !post.contains_key(key) {
            diff.push(GlobalsDiffEntry::Removed { key: key.clone(), value: pre[key].clone() });
        }
    }
    diff.sort_by(|a, b| {
        let ka = match a {
            GlobalsDiffEntry::Changed { key, .. }
            | GlobalsDiffEntry::Added   { key, .. }
            | GlobalsDiffEntry::Removed { key, .. } => key,
        };
        let kb = match b {
            GlobalsDiffEntry::Changed { key, .. }
            | GlobalsDiffEntry::Added   { key, .. }
            | GlobalsDiffEntry::Removed { key, .. } => key,
        };
        ka.cmp(kb)
    });
    diff
}

#[derive(Debug, Clone, Deserialize)]
pub struct RunDetailDto {
    #[serde(flatten)]
    pub summary: RunSummary,
    pub tasks: Vec<TaskSummary>,
}

/// Deserialization shape matching GET /triggers response.
#[derive(Debug, Clone, Deserialize)]
pub struct TriggerSummaryDto {
    pub id:              String,
    pub workflow:        String,
    pub status:          String,
    pub source:          String,
    pub rejection_cause: Option<String>,
    pub received_at:     u64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TriggerEntryStatus {
    Running,
    Finished(bool),
    Rejected(String),
}

#[derive(Debug, Clone)]
pub struct TriggerEntry {
    pub id:          String,
    pub workflow:    Option<String>,
    pub run_id:      Option<String>,
    pub source:      String,
    pub status:      TriggerEntryStatus,
    pub received_at: u64,
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

#[derive(Debug, Clone, PartialEq)]
pub enum Focus {
    TriggerList,
    WorkflowList,
    Runs,
    Tasks,
    Detail,
}

/// Per-daemon state: runs, selection, graph modal, connection status.
pub struct SourceState {
    pub name: String,
    pub connection: ConnectionStatus,
    pub runs: IndexMap<String, RunState>,
    pub triggers: IndexMap<String, TriggerEntry>,
    pub selected: usize,
    pub selected_workflow: usize,
    pub selected_trigger: usize,
    pub selected_task: usize,
    pub task_scroll: usize,
    pub focus: Focus,
    pub view_mode: ViewMode,
    pub layout: TuiLayout,
    pub graph: Option<DependencyGraph>,
    pub show_graph: bool,
    pub globals_pre:  HashMap<String, HashMap<String, String>>,
    pub globals_post: HashMap<String, HashMap<String, String>>,
    /// task config fields cached from GET /runs/{id} — run_id → task_id → TaskSummary
    pub task_summaries: HashMap<String, HashMap<String, TaskSummary>>,
    /// validation issues per workflow, populated from GET /workflows on connect
    pub workflow_issues: IndexMap<String, WorkflowIssueSummary>,
    /// task log lines fetched lazily — (run_id, task_id) → lines
    pub task_logs: HashMap<(String, String), Vec<LogEntry>>,
}

impl SourceState {
    pub fn new(name: impl Into<String>) -> Self {
        let layout = TuiLayout::default();
        let view_mode = layout.default_mode.clone();
        Self {
            name: name.into(),
            connection: ConnectionStatus::Connecting,
            runs: IndexMap::new(),
            triggers: IndexMap::new(),
            selected: 0,
            selected_workflow: 0,
            selected_trigger: 0,
            selected_task: 0,
            task_scroll: 0,
            focus: Focus::WorkflowList,
            view_mode,
            layout,
            graph: None,
            show_graph: false,
            globals_pre:  HashMap::new(),
            globals_post: HashMap::new(),
            task_summaries: HashMap::new(),
            workflow_issues: IndexMap::new(),
            task_logs: HashMap::new(),
        }
    }

    pub fn set_view_mode(&mut self, mode: ViewMode) {
        self.focus = match &mode {
            ViewMode::Triggers  => Focus::TriggerList,
            ViewMode::Workflows => Focus::WorkflowList,
        };
        self.view_mode = mode;
    }

    fn list_focus(&self) -> Focus {
        match self.view_mode {
            ViewMode::Triggers  => Focus::TriggerList,
            ViewMode::Workflows => Focus::WorkflowList,
        }
    }

    fn clamp_focus_to_panels(&mut self) {
        let panels = self.layout.panels;
        let new_focus = match self.focus {
            Focus::Tasks if panels < 3 => {
                Some(if panels >= 2 { Focus::Runs } else { self.list_focus() })
            }
            Focus::Runs if panels < 2 => Some(self.list_focus()),
            _ => None,
        };
        if let Some(f) = new_focus {
            self.focus = f;
        }
    }

    pub fn handle(&mut self, event: Event) {
        match event {
            Event::WorkflowStarted { run_id, workflow, timestamp } => {
                self.runs.insert(run_id.clone(), RunState {
                    workflow: workflow.clone(),
                    status: RunStatus::Running,
                    tasks: IndexMap::new(),
                    started_at: Instant::now(),
                    started_at_ms: timestamp,
                    finished_at_ms: None,
                });
                let entry = self.triggers.entry(run_id.clone()).or_insert_with(|| TriggerEntry {
                    id: run_id.clone(),
                    workflow: Some(workflow.clone()),
                    run_id: Some(run_id.clone()),
                    source: String::new(),
                    status: TriggerEntryStatus::Running,
                    received_at: timestamp,
                });
                entry.workflow = Some(workflow);
                entry.run_id = Some(run_id);
                entry.status = TriggerEntryStatus::Running;
            }
            Event::TriggerRejected { run_id, reason } => {
                self.runs.insert(run_id.clone(), RunState {
                    workflow: String::new(),
                    status: RunStatus::Rejected(reason.clone()),
                    tasks: IndexMap::new(),
                    started_at: Instant::now(),
                    started_at_ms: 0,
                    finished_at_ms: None,
                });
                let new_status = TriggerEntryStatus::Rejected(reason.clone());
                self.triggers.entry(run_id.clone()).or_insert_with(|| TriggerEntry {
                    id: run_id,
                    workflow: None,
                    run_id: None,
                    source: String::new(),
                    status: TriggerEntryStatus::Rejected(reason),
                    received_at: 0,
                }).status = new_status;
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
                if let Some(entry) = self.triggers.get_mut(&run_id) {
                    entry.status = TriggerEntryStatus::Finished(success);
                }
            }
            _ => {}
        }
        self.clamp_selections();
    }

    /// Populate a run from a REST API response. Upserts — existing live runs are updated.
    pub fn apply_run_detail(&mut self, detail: RunDetailDto) {
        let status = match detail.summary.status.as_str() {
            "success"  => RunStatus::Finished(true),
            "failed"   => RunStatus::Finished(false),
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
                "failed"  => TaskStatus::Finished {
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

        // Cache task summaries (for config fields in the detail panel)
        let summaries: HashMap<String, TaskSummary> = detail.tasks.iter()
            .map(|t| (t.task_id.clone(), t.clone()))
            .collect();
        self.task_summaries.insert(detail.summary.id.clone(), summaries);

        self.runs.insert(detail.summary.id.clone(), RunState {
            workflow: detail.summary.workflow,
            status,
            tasks,
            started_at: Instant::now(),
            started_at_ms: detail.summary.started_at,
            finished_at_ms: detail.summary.finished_at,
        });
        self.clamp_selections();
    }

    pub fn run_task_summary(&self, run_id: &str, task_id: &str) -> Option<&TaskSummary> {
        self.task_summaries.get(run_id)?.get(task_id)
    }

    /// Populate trigger entries from a REST API response (GET /triggers).
    pub fn apply_triggers(&mut self, dtos: Vec<TriggerSummaryDto>) {
        for t in dtos {
            let status = match t.status.as_str() {
                "rejected" => TriggerEntryStatus::Rejected(
                    t.rejection_cause.clone().unwrap_or_default()
                ),
                "finished" => {
                    let success = self.runs.get(&t.id)
                        .map(|r| matches!(r.status, RunStatus::Finished(true)))
                        .unwrap_or(false);
                    TriggerEntryStatus::Finished(success)
                }
                _ => TriggerEntryStatus::Running,
            };
            let run_id = if t.status != "rejected" { Some(t.id.clone()) } else { None };
            let workflow = if t.workflow.is_empty() { None } else { Some(t.workflow) };
            self.triggers.entry(t.id.clone()).or_insert(TriggerEntry {
                id: t.id,
                workflow,
                run_id,
                source: t.source,
                status,
                received_at: t.received_at,
            });
        }
        self.clamp_selections();
    }

    pub fn apply_workflow_summaries(&mut self, summaries: Vec<WorkflowIssueSummary>) {
        for s in summaries {
            self.workflow_issues.insert(s.name.clone(), s);
        }
    }

    pub fn apply_task_logs(&mut self, run_id: &str, task_id: &str, logs: Vec<LogEntry>) {
        self.task_logs.insert((run_id.to_string(), task_id.to_string()), logs);
    }

    /// Triggers sorted newest-first.
    pub fn apply_globals_pre(&mut self, run_id: &str, globals: HashMap<String, String>) {
        self.globals_pre.insert(run_id.to_string(), globals);
    }

    pub fn apply_globals_post(&mut self, run_id: &str, globals: HashMap<String, String>) {
        self.globals_post.insert(run_id.to_string(), globals);
    }

    pub fn sorted_triggers(&self) -> Vec<&TriggerEntry> {
        let mut triggers: Vec<&TriggerEntry> = self.triggers.values().collect();
        triggers.sort_by(|a, b| b.received_at.cmp(&a.received_at));
        triggers
    }

    pub fn selected_trigger_entry(&self) -> Option<&TriggerEntry> {
        self.sorted_triggers().into_iter().nth(self.selected_trigger)
    }

    /// Runs linked to the currently selected trigger (0 or 1).
    pub fn runs_for_selected_trigger(&self) -> Vec<(&String, &RunState)> {
        let entry = self.sorted_triggers().into_iter().nth(self.selected_trigger);
        let Some(entry) = entry else { return vec![] };
        let Some(run_id) = &entry.run_id else { return vec![] };
        self.runs.get_key_value(run_id)
            .map(|(k, v)| vec![(k, v)])
            .unwrap_or_default()
    }

    /// Runs visible in the Runs column based on current view mode.
    pub fn active_runs(&self) -> Vec<(&String, &RunState)> {
        match self.view_mode {
            ViewMode::Triggers  => self.runs_for_selected_trigger(),
            ViewMode::Workflows => self.runs_for_selected_workflow(),
        }
    }

    /// Selected run within the active view (mode-aware).
    pub fn selected_active_run(&self) -> Option<(&String, &RunState)> {
        self.active_runs().into_iter().nth(self.selected)
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

    pub fn sorted_runs(&self) -> Vec<(&String, &RunState)> {
        let mut runs: Vec<_> = self.runs.iter().collect();
        runs.sort_by(|a, b| b.1.started_at_ms.cmp(&a.1.started_at_ms));
        runs
    }

    pub fn selected_run(&self) -> Option<(&String, &RunState)> {
        self.sorted_runs().into_iter().nth(self.selected)
    }

    /// Deduplicated workflow names, ordered by most-recent run first.
    pub fn workflow_names(&self) -> Vec<String> {
        let mut runs: Vec<_> = self.runs.values().collect();
        runs.sort_by(|a, b| b.started_at_ms.cmp(&a.started_at_ms));
        let mut seen = std::collections::HashSet::new();
        let mut names = vec![];
        for run in runs {
            if !run.workflow.is_empty() && seen.insert(run.workflow.clone()) {
                names.push(run.workflow.clone());
            }
        }
        names
    }

    /// Runs filtered to the currently selected workflow, newest first.
    pub fn runs_for_selected_workflow(&self) -> Vec<(&String, &RunState)> {
        let names = self.workflow_names();
        let workflow = names.get(self.selected_workflow);
        let mut runs: Vec<_> = self.runs.iter()
            .filter(|(_, r)| workflow.map(|w| w == &r.workflow).unwrap_or(false))
            .collect();
        runs.sort_by(|a, b| b.1.started_at_ms.cmp(&a.1.started_at_ms));
        runs
    }

    /// Selected run within the filtered workflow list.
    pub fn selected_run_in_workflow(&self) -> Option<(&String, &RunState)> {
        self.runs_for_selected_workflow().into_iter().nth(self.selected)
    }

    /// The task currently highlighted in the tasks pane.
    pub fn selected_task_entry(&self) -> Option<(&String, &TaskStatus)> {
        let (_, run) = self.selected_active_run()?;
        run.tasks.iter().nth(self.selected_task)
    }

    /// Clamp all selection indices so they stay in-bounds after data changes.
    pub fn clamp_selections(&mut self) {
        // Trigger selection
        let trigger_count = self.triggers.len();
        if trigger_count > 0 {
            self.selected_trigger = self.selected_trigger.min(trigger_count - 1);
        } else {
            self.selected_trigger = 0;
        }

        // Workflow selection
        let names = self.workflow_names();
        let wf_count = names.len();
        if wf_count > 0 {
            self.selected_workflow = self.selected_workflow.min(wf_count - 1);
        } else {
            self.selected_workflow = 0;
        }

        // Run selection (mode-aware; active_runs is safe now that trigger/workflow are clamped)
        let run_count = self.active_runs().len();
        if run_count > 0 {
            self.selected = self.selected.min(run_count - 1);
        } else {
            self.selected = 0;
        }

        // Task selection
        let task_count = self.selected_active_run()
            .map(|(_, r)| r.tasks.len())
            .unwrap_or(0);
        if task_count > 0 {
            self.selected_task = self.selected_task.min(task_count - 1);
        } else {
            self.selected_task = 0;
        }
    }

    pub fn select_next_workflow(&mut self) {
        let count = self.workflow_names().len();
        if count > 0 {
            self.selected_workflow = (self.selected_workflow + 1).min(count - 1);
            self.selected = 0;
            self.selected_task = 0;
            self.task_scroll = 0;
        }
    }

    pub fn select_prev_workflow(&mut self) {
        self.selected_workflow = self.selected_workflow.saturating_sub(1);
        self.selected = 0;
        self.selected_task = 0;
        self.task_scroll = 0;
    }

    pub fn select_next_run_in_workflow(&mut self) {
        let count = self.runs_for_selected_workflow().len();
        if count > 0 {
            self.selected = (self.selected + 1).min(count - 1);
            self.selected_task = 0;
            self.task_scroll = 0;
        }
    }

    pub fn select_prev_run_in_workflow(&mut self) {
        self.selected = self.selected.saturating_sub(1);
        self.selected_task = 0;
        self.task_scroll = 0;
    }

    pub fn select_next_task(&mut self) {
        let count = self.selected_active_run()
            .map(|(_, r)| r.tasks.len())
            .unwrap_or(0);
        if count > 0 {
            self.selected_task = (self.selected_task + 1).min(count - 1);
            self.task_scroll = 0;
        }
    }

    pub fn select_prev_task(&mut self) {
        self.selected_task = self.selected_task.saturating_sub(1);
        self.task_scroll = 0;
    }

    pub fn select_next_trigger(&mut self) {
        let count = self.triggers.len();
        if count > 0 {
            self.selected_trigger = (self.selected_trigger + 1).min(count - 1);
            self.selected = 0;
            self.selected_task = 0;
            self.task_scroll = 0;
        }
    }

    pub fn select_prev_trigger(&mut self) {
        self.selected_trigger = self.selected_trigger.saturating_sub(1);
        self.selected = 0;
        self.selected_task = 0;
        self.task_scroll = 0;
    }

    pub fn select_next_active_run(&mut self) {
        let count = self.active_runs().len();
        if count > 0 {
            self.selected = (self.selected + 1).min(count - 1);
            self.selected_task = 0;
            self.task_scroll = 0;
        }
    }

    pub fn select_prev_active_run(&mut self) {
        self.selected = self.selected.saturating_sub(1);
        self.selected_task = 0;
        self.task_scroll = 0;
    }

    pub fn scroll_task_down(&mut self) {
        self.task_scroll += 1;
    }

    pub fn scroll_task_up(&mut self) {
        self.task_scroll = self.task_scroll.saturating_sub(1);
    }

    pub fn navigate_down(&mut self) {
        match self.focus {
            Focus::TriggerList  => self.select_next_trigger(),
            Focus::WorkflowList => self.select_next_workflow(),
            Focus::Runs         => self.select_next_active_run(),
            Focus::Tasks        => self.select_next_task(),
            Focus::Detail       => self.scroll_task_down(),
        }
    }

    pub fn navigate_up(&mut self) {
        match self.focus {
            Focus::TriggerList  => self.select_prev_trigger(),
            Focus::WorkflowList => self.select_prev_workflow(),
            Focus::Runs         => self.select_prev_active_run(),
            Focus::Tasks        => self.select_prev_task(),
            Focus::Detail       => self.scroll_task_up(),
        }
    }

    pub fn focus_right(&mut self) {
        self.focus = match self.focus {
            Focus::TriggerList | Focus::WorkflowList => Focus::Runs,
            Focus::Runs                              => Focus::Tasks,
            Focus::Tasks | Focus::Detail             => Focus::Detail,
        };
    }

    pub fn focus_left(&mut self) {
        self.task_scroll = 0;
        self.focus = match (&self.focus, &self.view_mode) {
            (Focus::Detail, _)                 => Focus::Tasks,
            (Focus::Tasks, _)                  => Focus::Runs,
            (Focus::Runs, ViewMode::Triggers)  => Focus::TriggerList,
            (Focus::Runs, ViewMode::Workflows) => Focus::WorkflowList,
            (Focus::TriggerList, _)            => Focus::TriggerList,
            (Focus::WorkflowList, _)           => Focus::WorkflowList,
        };
    }

    pub fn enter_pane(&mut self) {
        match self.focus {
            Focus::TriggerList | Focus::WorkflowList => { self.focus = Focus::Runs; }
            Focus::Runs => { self.focus = Focus::Tasks; }
            Focus::Tasks => {
                if self.selected_task_entry().is_some() {
                    self.task_scroll = 0;
                    self.focus = Focus::Detail;
                }
            }
            Focus::Detail => {}
        }
    }

    pub fn escape_pane(&mut self) {
        match (&self.focus, &self.view_mode) {
            (Focus::Detail, _)                 => { self.task_scroll = 0; self.focus = Focus::Tasks; }
            (Focus::Tasks, _)                  => { self.focus = Focus::Runs; }
            (Focus::Runs, ViewMode::Triggers)  => { self.focus = Focus::TriggerList; }
            (Focus::Runs, ViewMode::Workflows) => { self.focus = Focus::WorkflowList; }
            (Focus::TriggerList, _)            => {}
            (Focus::WorkflowList, _)           => {}
        }
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

    pub fn navigate_up(&mut self)   { self.active_source_mut().navigate_up(); }
    pub fn navigate_down(&mut self) { self.active_source_mut().navigate_down(); }
    pub fn focus_left(&mut self)    { self.active_source_mut().focus_left(); }
    pub fn focus_right(&mut self)   { self.active_source_mut().focus_right(); }
    pub fn enter_pane(&mut self)    { self.active_source_mut().enter_pane(); }
    pub fn escape_pane(&mut self)   { self.active_source_mut().escape_pane(); }
    pub fn set_view_mode(&mut self, mode: ViewMode) { self.active_source_mut().set_view_mode(mode); }

    /// Switch to Workflow view. When called from Trigger view, also selects the
    /// workflow linked to the currently selected trigger entry.
    pub fn jump_to_workflow_view(&mut self) {
        let src = self.active_source_mut();
        let linked = matches!(src.view_mode, ViewMode::Triggers)
            .then(|| src.selected_trigger_entry().and_then(|t| t.workflow.clone()))
            .flatten();
        src.set_view_mode(ViewMode::Workflows);
        if let Some(wf_name) = linked {
            let names = src.workflow_names();
            if let Some(idx) = names.iter().position(|n| n == &wf_name) {
                src.selected_workflow = idx;
                src.selected = 0;
            }
        }
    }

    pub fn panels_wider(&mut self) {
        let src = self.active_source_mut();
        src.layout.panels = (src.layout.panels + 1).min(3);
    }

    pub fn panels_narrower(&mut self) {
        let src = self.active_source_mut();
        if src.layout.panels > 1 {
            src.layout.panels -= 1;
            src.clamp_focus_to_panels();
        }
    }
    pub fn apply_triggers(&mut self, dtos: Vec<TriggerSummaryDto>) { self.active_source_mut().apply_triggers(dtos); }
    pub fn apply_workflow_summaries(&mut self, summaries: Vec<WorkflowIssueSummary>) { self.active_source_mut().apply_workflow_summaries(summaries); }
    pub fn apply_task_logs(&mut self, run_id: &str, task_id: &str, logs: Vec<LogEntry>) { self.active_source_mut().apply_task_logs(run_id, task_id, logs); }
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
                TaskSummary { task_id: "pull".into(), status: "success".into(), exit_code: Some(0), stdout: Some("ok\n".into()), stderr: Some(String::new()), started_at: Some(0), finished_at: Some(1), task_type: None, task_exec: None, task_when: None, task_abort_if: None },
                TaskSummary { task_id: "notify".into(), status: "skipped".into(), exit_code: None, stdout: None, stderr: None, started_at: None, finished_at: None, task_type: None, task_exec: None, task_when: None, task_abort_if: None },
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

    // --- Sprint 13: multi-pane navigation ---

    #[test]
    fn workflow_names_deduplicates_across_runs() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(started("r2", "deploy"));
        app.handle(started("r3", "build"));
        let names = app.active_source().workflow_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"deploy".to_string()));
        assert!(names.contains(&"build".to_string()));
    }

    #[test]
    fn workflow_names_excludes_rejected_runs() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(rejected("r2", "gate failed")); // empty workflow
        let names = app.active_source().workflow_names();
        assert_eq!(names, vec!["deploy"]);
    }

    #[test]
    fn runs_for_selected_workflow_filters_by_selected_workflow() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(started("r2", "build"));
        app.handle(started("r3", "deploy"));

        let src = app.active_source_mut();
        src.selected_workflow = 0; // first workflow (newest-first)
        let runs = src.runs_for_selected_workflow();
        // All runs in the selected workflow should have the same workflow name
        let first_wf = &runs[0].1.workflow.clone();
        assert!(runs.iter().all(|(_, r)| &r.workflow == first_wf));
    }

    #[test]
    fn navigate_down_in_workflows_focus_changes_selected_workflow() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(started("r2", "build"));
        {
            let src = app.active_source_mut();
            src.focus = Focus::WorkflowList;
            src.selected_workflow = 0;
        }
        app.active_source_mut().navigate_down();
        assert_eq!(app.active_source().selected_workflow, 1);
    }

    #[test]
    fn navigate_up_in_workflows_focus_clamps_at_zero() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        {
            let src = app.active_source_mut();
            src.focus = Focus::WorkflowList;
            src.selected_workflow = 0;
        }
        app.active_source_mut().navigate_up();
        assert_eq!(app.active_source().selected_workflow, 0);
    }

    #[test]
    fn enter_pane_advances_focus_from_workflows_to_runs() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.active_source_mut().focus = Focus::WorkflowList;
        app.active_source_mut().enter_pane();
        assert_eq!(app.active_source().focus, Focus::Runs);
    }

    #[test]
    fn enter_pane_advances_focus_from_runs_to_tasks() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.active_source_mut().focus = Focus::Runs;
        app.active_source_mut().enter_pane();
        assert_eq!(app.active_source().focus, Focus::Tasks);
    }

    #[test]
    fn enter_pane_opens_task_detail_when_task_has_output() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(Event::TaskFinished {
            run_id: "r1".into(), task: "build".into(), success: true,
            exit_code: 0, stdout: "done\n".into(), stderr: String::new(), timestamp: 0,
        });
        app.active_source_mut().focus = Focus::Tasks;
        app.active_source_mut().enter_pane();
        assert_eq!(app.active_source().focus, Focus::Detail);
    }

    #[test]
    fn escape_pane_retreats_focus_from_detail_to_tasks() {
        let mut app = App::new();
        app.active_source_mut().focus = Focus::Detail;
        app.active_source_mut().escape_pane();
        assert_eq!(app.active_source().focus, Focus::Tasks);
    }

    #[test]
    fn escape_pane_retreats_focus_from_runs_to_workflows() {
        let mut app = App::new();
        app.active_source_mut().focus = Focus::Runs;
        app.active_source_mut().escape_pane();
        assert_eq!(app.active_source().focus, Focus::WorkflowList);
    }

    #[test]
    fn focus_left_and_right_cycle_panes() {
        let mut app = App::new();
        let src = app.active_source_mut();
        src.focus = Focus::WorkflowList;
        src.focus_right();
        assert_eq!(src.focus, Focus::Runs);
        src.focus_right();
        assert_eq!(src.focus, Focus::Tasks);
        src.focus_right();
        assert_eq!(src.focus, Focus::Detail);
        src.focus_left();
        assert_eq!(src.focus, Focus::Tasks);
        src.focus_left();
        assert_eq!(src.focus, Focus::Runs);
        src.focus_left();
        assert_eq!(src.focus, Focus::WorkflowList);
        src.focus_left(); // already at leftmost
        assert_eq!(src.focus, Focus::WorkflowList);
    }

    #[test]
    fn clamp_selections_prevents_out_of_bounds_after_workflow_removed() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(started("r2", "build"));
        {
            let src = app.active_source_mut();
            src.selected_workflow = 1; // "build"
        }
        // Manually remove runs to simulate data shrink, then clamp
        app.active_source_mut().runs.clear();
        app.active_source_mut().clamp_selections();
        assert_eq!(app.active_source().selected_workflow, 0);
        assert_eq!(app.active_source().selected, 0);
    }

    #[test]
    fn task_scroll_resets_on_workflow_change() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(started("r2", "build"));
        {
            let src = app.active_source_mut();
            src.task_scroll = 5;
            src.focus = Focus::WorkflowList;
        }
        app.active_source_mut().navigate_down();
        assert_eq!(app.active_source().task_scroll, 0);
    }

    // --- Sprint 16: trigger view mode ---

    #[test]
    fn trigger_entry_inserted_on_trigger_rejected() {
        let mut app = App::new();
        app.handle(rejected("r1", "unauthorized"));
        let src = app.active_source();
        let triggers = src.sorted_triggers();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].id, "r1");
        assert_eq!(triggers[0].status, TriggerEntryStatus::Rejected("unauthorized".into()));
        assert!(triggers[0].run_id.is_none());
    }

    #[test]
    fn trigger_entry_upserted_on_workflow_started() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        let src = app.active_source();
        let triggers = src.sorted_triggers();
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].id, "r1");
        assert_eq!(triggers[0].workflow, Some("deploy".into()));
        assert_eq!(triggers[0].run_id, Some("r1".into()));
        assert_eq!(triggers[0].status, TriggerEntryStatus::Running);
    }

    #[test]
    fn trigger_entry_status_updated_on_workflow_finished() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        app.handle(workflow_finished("r1", "deploy", true));
        let triggers = app.active_source().sorted_triggers();
        assert_eq!(triggers[0].status, TriggerEntryStatus::Finished(true));
    }

    #[test]
    fn runs_for_selected_trigger_returns_linked_run() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        let src = app.active_source_mut();
        src.selected_trigger = 0;
        let runs = src.runs_for_selected_trigger();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].0, "r1");
    }

    #[test]
    fn sorted_triggers_newest_first() {
        let mut app = App::new();
        app.active_source_mut().triggers.insert("t1".into(), TriggerEntry {
            id: "t1".into(), workflow: None, run_id: None, source: String::new(),
            status: TriggerEntryStatus::Running, received_at: 100,
        });
        app.active_source_mut().triggers.insert("t2".into(), TriggerEntry {
            id: "t2".into(), workflow: None, run_id: None, source: String::new(),
            status: TriggerEntryStatus::Running, received_at: 200,
        });
        let sorted = app.active_source().sorted_triggers();
        assert_eq!(sorted[0].id, "t2");
        assert_eq!(sorted[1].id, "t1");
    }

    #[test]
    fn view_mode_starts_at_workflows_by_default() {
        let app = App::new();
        assert_eq!(app.active_source().view_mode, ViewMode::Workflows);
        assert_eq!(app.active_source().focus, Focus::WorkflowList);
    }

    #[test]
    fn focus_advances_through_trigger_hierarchy() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        let src = app.active_source_mut();
        src.set_view_mode(ViewMode::Triggers);
        assert_eq!(src.focus, Focus::TriggerList);
        src.enter_pane();
        assert_eq!(src.focus, Focus::Runs);
        src.enter_pane();
        assert_eq!(src.focus, Focus::Tasks);
        src.escape_pane();
        assert_eq!(src.focus, Focus::Runs);
        src.escape_pane();
        assert_eq!(src.focus, Focus::TriggerList);
    }

    #[test]
    fn focus_advances_through_workflow_hierarchy() {
        let mut app = App::new();
        app.handle(started("r1", "deploy"));
        let src = app.active_source_mut();
        src.set_view_mode(ViewMode::Workflows);
        assert_eq!(src.focus, Focus::WorkflowList);
        src.enter_pane();
        assert_eq!(src.focus, Focus::Runs);
        src.enter_pane();
        assert_eq!(src.focus, Focus::Tasks);
        src.escape_pane();
        assert_eq!(src.focus, Focus::Runs);
        src.escape_pane();
        assert_eq!(src.focus, Focus::WorkflowList);
    }

    #[test]
    fn apply_triggers_populates_from_rest_response() {
        let mut app = App::new();
        let dtos = vec![
            TriggerSummaryDto {
                id: "t1".into(), workflow: "deploy".into(), status: "running".into(),
                source: "http".into(), rejection_cause: None, received_at: 1000,
            },
            TriggerSummaryDto {
                id: "t2".into(), workflow: String::new(), status: "rejected".into(),
                source: "http".into(), rejection_cause: Some("unauthorized".into()), received_at: 2000,
            },
        ];
        app.active_source_mut().apply_triggers(dtos);
        let src = app.active_source();
        assert_eq!(src.triggers.len(), 2);
        assert_eq!(src.triggers.get("t1").unwrap().run_id, Some("t1".into()));
        let t2 = src.triggers.get("t2").unwrap();
        assert!(t2.run_id.is_none());
        assert!(matches!(t2.status, TriggerEntryStatus::Rejected(_)));
    }

    // --- Sprint 17: detail panel + globals diff ---

    #[test]
    fn diff_globals_detects_changed_key() {
        let mut pre = HashMap::new();
        pre.insert("counter".to_string(), "1".to_string());
        let mut post = HashMap::new();
        post.insert("counter".to_string(), "2".to_string());
        let diff = diff_globals(&pre, &post);
        assert_eq!(diff.len(), 1);
        assert!(matches!(&diff[0], GlobalsDiffEntry::Changed { key, before, after }
            if key == "counter" && before == "1" && after == "2"));
    }

    #[test]
    fn diff_globals_detects_added_key() {
        let pre = HashMap::new();
        let mut post = HashMap::new();
        post.insert("new_key".to_string(), "val".to_string());
        let diff = diff_globals(&pre, &post);
        assert_eq!(diff.len(), 1);
        assert!(matches!(&diff[0], GlobalsDiffEntry::Added { key, value }
            if key == "new_key" && value == "val"));
    }

    #[test]
    fn diff_globals_detects_removed_key() {
        let mut pre = HashMap::new();
        pre.insert("gone".to_string(), "was_here".to_string());
        let post = HashMap::new();
        let diff = diff_globals(&pre, &post);
        assert_eq!(diff.len(), 1);
        assert!(matches!(&diff[0], GlobalsDiffEntry::Removed { key, value }
            if key == "gone" && value == "was_here"));
    }

    #[test]
    fn diff_globals_empty_when_no_change() {
        let mut pre = HashMap::new();
        pre.insert("same".to_string(), "value".to_string());
        let post = pre.clone();
        assert!(diff_globals(&pre, &post).is_empty());
    }

    #[test]
    fn diff_globals_sorted_by_key() {
        let mut pre = HashMap::new();
        pre.insert("z".to_string(), "1".to_string());
        pre.insert("a".to_string(), "1".to_string());
        let mut post = HashMap::new();
        post.insert("z".to_string(), "2".to_string());
        post.insert("a".to_string(), "2".to_string());
        let diff = diff_globals(&pre, &post);
        assert_eq!(diff.len(), 2);
        let key0 = match &diff[0] { GlobalsDiffEntry::Changed { key, .. } => key, _ => panic!() };
        let key1 = match &diff[1] { GlobalsDiffEntry::Changed { key, .. } => key, _ => panic!() };
        assert!(key0 < key1);
    }

    #[test]
    fn apply_globals_pre_stores_snapshot() {
        let mut src = SourceState::new("test");
        let mut g = HashMap::new();
        g.insert("k".to_string(), "v".to_string());
        src.apply_globals_pre("r1", g.clone());
        assert_eq!(src.globals_pre.get("r1"), Some(&g));
        assert!(src.globals_post.is_empty());
    }

    #[test]
    fn apply_globals_post_stores_snapshot() {
        let mut src = SourceState::new("test");
        let mut g = HashMap::new();
        g.insert("k".to_string(), "v2".to_string());
        src.apply_globals_post("r1", g.clone());
        assert_eq!(src.globals_post.get("r1"), Some(&g));
        assert!(src.globals_pre.is_empty());
    }

    #[test]
    fn detail_focus_reachable_from_tasks() {
        let mut src = SourceState::new("test");
        src.handle(Event::WorkflowStarted { run_id: "r1".into(), workflow: "wf".into(), timestamp: 0 });
        src.handle(Event::TaskFinished {
            run_id: "r1".into(), task: "t1".into(), success: true,
            exit_code: 0, stdout: String::new(), stderr: String::new(), timestamp: 0,
        });
        src.focus = Focus::Tasks;
        src.focus_right();
        assert_eq!(src.focus, Focus::Detail);
    }

    #[test]
    fn detail_focus_left_returns_to_tasks() {
        let mut src = SourceState::new("test");
        src.focus = Focus::Detail;
        src.focus_left();
        assert_eq!(src.focus, Focus::Tasks);
    }

    #[test]
    fn escape_from_detail_resets_scroll_and_returns_to_tasks() {
        let mut src = SourceState::new("test");
        src.focus = Focus::Detail;
        src.task_scroll = 7;
        src.escape_pane();
        assert_eq!(src.focus, Focus::Tasks);
        assert_eq!(src.task_scroll, 0);
    }

    // --- panel toggle ---

    #[test]
    fn panels_wider_increments_up_to_3() {
        let mut app = App::new();
        app.active_source_mut().layout.panels = 1;
        app.panels_wider();
        assert_eq!(app.active_source().layout.panels, 2);
        app.panels_wider();
        assert_eq!(app.active_source().layout.panels, 3);
        app.panels_wider(); // no-op at max
        assert_eq!(app.active_source().layout.panels, 3);
    }

    #[test]
    fn panels_narrower_decrements_down_to_1() {
        let mut app = App::new();
        app.active_source_mut().layout.panels = 3;
        app.panels_narrower();
        assert_eq!(app.active_source().layout.panels, 2);
        app.panels_narrower();
        assert_eq!(app.active_source().layout.panels, 1);
        app.panels_narrower(); // no-op at min
        assert_eq!(app.active_source().layout.panels, 1);
    }

    #[test]
    fn panels_narrower_clamps_tasks_focus_to_runs() {
        let mut app = App::new();
        app.active_source_mut().layout.panels = 3;
        app.active_source_mut().focus = Focus::Tasks;
        app.panels_narrower(); // 3 → 2
        assert_eq!(app.active_source().layout.panels, 2);
        assert_eq!(app.active_source().focus, Focus::Runs);
    }

    #[test]
    fn panels_narrower_clamps_runs_focus_to_list() {
        let mut app = App::new();
        app.active_source_mut().layout.panels = 2;
        app.active_source_mut().focus = Focus::Runs;
        app.panels_narrower(); // 2 → 1
        assert_eq!(app.active_source().layout.panels, 1);
        assert_eq!(app.active_source().focus, Focus::WorkflowList);
    }

    #[test]
    fn panels_narrower_keeps_detail_focus() {
        let mut app = App::new();
        app.active_source_mut().layout.panels = 3;
        app.active_source_mut().focus = Focus::Detail;
        app.panels_narrower(); // 3 → 2; Detail always visible
        assert_eq!(app.active_source().focus, Focus::Detail);
    }

    #[test]
    fn panels_narrower_trigger_list_focus_clamps_to_trigger_list() {
        let mut app = App::new();
        app.active_source_mut().layout.panels = 2;
        app.active_source_mut().set_view_mode(crate::config::ViewMode::Triggers);
        app.active_source_mut().focus = Focus::Runs;
        app.panels_narrower(); // 2 → 1
        assert_eq!(app.active_source().focus, Focus::TriggerList);
    }

    // --- T→W cross-navigation ---

    fn trigger_dto(id: &str, workflow: &str, received_at: u64) -> TriggerSummaryDto {
        TriggerSummaryDto { id: id.into(), workflow: workflow.into(), status: "accepted".into(), source: "http".into(), rejection_cause: None, received_at }
    }

    #[test]
    fn jump_to_workflow_view_selects_linked_workflow() {
        let mut app = App::new();
        let src = app.active_source_mut();
        // Two runs: "build" (ts=1, older), "deploy" (ts=2, newer) → workflow_names() = ["deploy","build"]
        src.handle(started("r1", "build"));
        src.handle(started("r2", "deploy"));
        // Trigger "t1" for "build" — received_at=999 ensures it sorts first among all trigger entries
        src.apply_triggers(vec![trigger_dto("t1", "build", 999)]);
        src.set_view_mode(ViewMode::Triggers);
        // sorted_triggers[0] = "t1" (received_at 999 > r2's 0 > r1's 0)

        app.jump_to_workflow_view();

        let src = app.active_source();
        assert_eq!(src.view_mode, ViewMode::Workflows);
        assert_eq!(src.focus, Focus::WorkflowList);
        let names = src.workflow_names();
        let selected = names.get(src.selected_workflow).map(String::as_str);
        assert_eq!(selected, Some("build"));
    }

    #[test]
    fn jump_to_workflow_view_noop_when_workflow_not_in_runs() {
        let mut app = App::new();
        // "ghost" trigger has no run → workflow_names() is empty → no index to jump to
        app.active_source_mut().apply_triggers(vec![trigger_dto("t1", "ghost", 0)]);
        app.active_source_mut().set_view_mode(ViewMode::Triggers);

        app.jump_to_workflow_view(); // should not panic

        let src = app.active_source();
        assert_eq!(src.view_mode, ViewMode::Workflows);
        assert_eq!(src.focus, Focus::WorkflowList);
        assert_eq!(src.selected_workflow, 0); // unchanged
    }

    #[test]
    fn jump_to_workflow_view_from_workflow_mode_does_not_jump() {
        let mut app = App::new();
        let src = app.active_source_mut();
        src.handle(started("r1", "build"));
        src.handle(started("r2", "deploy"));
        src.set_view_mode(ViewMode::Workflows);
        src.selected_workflow = 1; // "build" selected

        app.jump_to_workflow_view(); // W while already in Workflow mode

        // selected_workflow must not change — no trigger context to jump from
        assert_eq!(app.active_source().selected_workflow, 1);
    }
}
