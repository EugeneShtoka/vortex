use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs},
    Frame,
};

use crate::app::{App, ConnectionStatus, Focus, GlobalsDiffEntry, RunStatus, SourceState, TaskStatus, TriggerEntry, TriggerEntryStatus, diff_globals};
use crate::config::ViewMode;
use crate::graph::TaskNode;

pub fn render(f: &mut Frame, app: &App) {
    let area = f.area();

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // tab bar
            Constraint::Min(0),    // main content
            Constraint::Length(1), // status bar
        ])
        .split(area);

    render_tabs(f, app, outer[0]);

    let src = app.active_source();
    if let ConnectionStatus::Disconnected(err) = &src.connection {
        render_disconnected(f, err.as_deref(), outer[1]);
    } else {
        let l = &src.layout;
        let w = &l.widths;
        let panels = l.panels as usize;
        let mut ratios: Vec<u32> = Vec::new();
        if panels >= 1 { ratios.push(w.workflows as u32); }
        if panels >= 2 { ratios.push(w.runs as u32); }
        if panels >= 3 { ratios.push(w.tasks as u32); }
        ratios.push(w.detail as u32);
        let total: u32 = ratios.iter().sum();
        let constraints: Vec<Constraint> = ratios.iter()
            .map(|r| Constraint::Percentage((*r * 100 / total) as u16))
            .collect();

        let panes = Layout::default()
            .direction(Direction::Horizontal)
            .constraints(constraints)
            .split(outer[1]);

        // panes layout: [col1, runs?, tasks?, detail]
        // detail is always panes[panels] (last slot)
        render_col1(f, src, panes[0]);
        if panels >= 2 { render_runs_pane(f, src, panes[1]); }
        if panels >= 3 { render_tasks_pane(f, src, panes[2]); }
        render_detail(f, src, panes[panels]);
    }
    render_statusbar(f, src, outer[2]);

    if src.show_graph {
        render_graph_modal(f, src, area);
    }
}

fn render_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = app.sources.iter().map(|src| {
        let (indicator, color) = match &src.connection {
            ConnectionStatus::Connected       => ("●", Color::Green),
            ConnectionStatus::Connecting      => ("◌", Color::Yellow),
            ConnectionStatus::Disconnected(_) => ("○", Color::Red),
        };
        Line::from(vec![
            Span::raw(src.name.clone()),
            Span::raw(" "),
            Span::styled(indicator, Style::default().fg(color)),
        ])
    }).collect();

    let tabs = Tabs::new(titles)
        .select(app.active)
        .style(Style::default().fg(Color::DarkGray))
        .highlight_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD))
        .divider(Span::raw("  "));

    f.render_widget(tabs, area);
}

fn render_disconnected(f: &mut Frame, err: Option<&str>, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Disconnected ");
    let inner = block.inner(area);
    f.render_widget(block, area);
    let mut lines = vec![
        Line::from(Span::styled(" disconnected", Style::default().fg(Color::Red))),
    ];
    if let Some(msg) = err {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            format!(" {msg}"),
            Style::default().fg(Color::DarkGray),
        )));
    }
    f.render_widget(Paragraph::new(lines), inner);
}

fn format_age(started_at_ms: u64) -> String {
    if started_at_ms == 0 { return String::new(); }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let secs = now_ms.saturating_sub(started_at_ms) / 1000;
    if secs < 60         { format!("{}s ago", secs) }
    else if secs < 3600  { format!("{}m ago", secs / 60) }
    else if secs < 86400 { format!("{}h ago", secs / 3600) }
    else                 { format!("{}d ago", secs / 86400) }
}

fn format_duration_ms(started_at_ms: u64, finished_at_ms: Option<u64>) -> String {
    match finished_at_ms {
        Some(fin) if started_at_ms > 0 => {
            let ms = fin.saturating_sub(started_at_ms);
            format!("{:.1}s", ms as f64 / 1000.0)
        }
        _ => String::new(),
    }
}

/// Health badge derived from validation issues: ✗ errors > ⚠ warnings > ✓ clean.
fn workflow_health_badge(src: &SourceState, workflow: &str) -> Option<(&'static str, Color)> {
    let issues = src.workflow_issues.get(workflow)?;
    if issues.issue_count.errors > 0 {
        Some(("✗", Color::Red))
    } else if issues.issue_count.warnings > 0 {
        Some(("⚠", Color::Yellow))
    } else {
        Some(("✓", Color::Green))
    }
}

/// Worst-case status badge for all runs of a workflow: running > failure > success > rejected.
fn workflow_status_badge(src: &SourceState, workflow: &str) -> (&'static str, Color) {
    let has_running = src.runs.values()
        .any(|r| r.workflow == workflow && r.status == RunStatus::Running);
    let has_failure = src.runs.values()
        .any(|r| r.workflow == workflow && matches!(r.status, RunStatus::Finished(false)));
    let has_success = src.runs.values()
        .any(|r| r.workflow == workflow && matches!(r.status, RunStatus::Finished(true)));

    if has_running      { ("●", Color::Yellow) }
    else if has_failure { ("✗", Color::Red) }
    else if has_success { ("✓", Color::Green) }
    else                { ("○", Color::DarkGray) }
}

fn pane_border_style(active: bool) -> Style {
    if active {
        Style::default().fg(Color::Cyan)
    } else {
        Style::default().fg(Color::DarkGray)
    }
}

fn render_col1(f: &mut Frame, src: &SourceState, area: Rect) {
    match src.view_mode {
        ViewMode::Triggers  => render_trigger_list(f, src, area),
        ViewMode::Workflows => render_workflows(f, src, area),
    }
}

fn render_trigger_list(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::TriggerList;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Triggers ")
        .border_style(pane_border_style(is_active));

    let triggers = src.sorted_triggers();
    if triggers.is_empty() {
        f.render_widget(
            Paragraph::new(Span::styled(" no triggers", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    }

    let items: Vec<ListItem> = triggers.iter().map(|t| {
        let (symbol, color) = match &t.status {
            TriggerEntryStatus::Running        => ("●", Color::Yellow),
            TriggerEntryStatus::Finished(true) => ("✓", Color::Green),
            TriggerEntryStatus::Finished(false) => ("✗", Color::Red),
            TriggerEntryStatus::Rejected(_)    => ("⊘", Color::Red),
        };
        let label = match &t.status {
            TriggerEntryStatus::Rejected(reason) => {
                Span::styled(reason.clone(), Style::default().fg(Color::DarkGray))
            }
            _ => Span::raw(t.workflow.clone().unwrap_or_default()),
        };
        let source_badge = if t.source.is_empty() {
            String::new()
        } else {
            format!("  [{}]", t.source)
        };
        let age = format_age(t.received_at);
        ListItem::new(Line::from(vec![
            Span::styled(format!("{symbol} "), Style::default().fg(color)),
            label,
            Span::styled(format!("{source_badge}  {age}"), Style::default().fg(Color::DarkGray)),
        ]))
    }).collect();

    let mut state = ListState::default();
    state.select(Some(src.selected_trigger));

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(list, area, &mut state);
}

fn render_workflows(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::WorkflowList;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Workflows ")
        .border_style(pane_border_style(is_active));

    let names = src.workflow_names();

    if names.is_empty() {
        f.render_widget(
            Paragraph::new(Span::styled(" no runs", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    }

    let items: Vec<ListItem> = names.iter().map(|name| {
        let (symbol, color) = workflow_status_badge(src, name);
        let mut spans = vec![
            Span::styled(format!("{symbol} "), Style::default().fg(color)),
            Span::raw(name.clone()),
        ];
        if let Some((health, health_color)) = workflow_health_badge(src, name) {
            spans.push(Span::styled(format!("  {health}"), Style::default().fg(health_color)));
        }
        ListItem::new(Line::from(spans))
    }).collect();

    let mut state = ListState::default();
    state.select(Some(src.selected_workflow));

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(list, area, &mut state);
}

fn render_runs_pane(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::Runs;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Runs ")
        .border_style(pane_border_style(is_active));

    let runs = src.runs_for_selected_workflow();

    if runs.is_empty() {
        f.render_widget(
            Paragraph::new(Span::styled(" no runs", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    }

    let items: Vec<ListItem> = runs.iter().map(|(_, run)| {
        let (symbol, color) = match &run.status {
            RunStatus::Running        => ("●", Color::Yellow),
            RunStatus::Finished(true) => ("✓", Color::Green),
            RunStatus::Finished(false) => ("✗", Color::Red),
            RunStatus::Rejected(_)    => ("⊘", Color::Red),
        };
        let dur = format_duration_ms(run.started_at_ms, run.finished_at_ms);
        let age = format_age(run.started_at_ms);
        let label = if dur.is_empty() && age.is_empty() {
            String::new()
        } else if dur.is_empty() {
            format!("  {age}")
        } else if age.is_empty() {
            format!("  {dur}")
        } else {
            format!("  {dur}  {age}")
        };
        ListItem::new(Line::from(vec![
            Span::styled(format!("{symbol} "), Style::default().fg(color)),
            Span::styled(label, Style::default().fg(Color::DarkGray)),
        ]))
    }).collect();

    let mut state = ListState::default();
    state.select(Some(src.selected));

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(list, area, &mut state);
}

fn render_tasks_pane(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::Tasks;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Tasks ")
        .border_style(pane_border_style(is_active));

    let Some((_, run)) = src.selected_run_in_workflow() else {
        f.render_widget(block, area);
        return;
    };

    let tasks: Vec<_> = run.tasks.iter().collect();
    let items: Vec<ListItem> = tasks.iter().map(|(task_id, status)| {
        let (symbol, color) = match status {
            TaskStatus::Running                         => ("▶", Color::Yellow),
            TaskStatus::Finished { success: true, .. }  => ("✓", Color::Green),
            TaskStatus::Finished { success: false, .. } => ("✗", Color::Red),
            TaskStatus::Skipped                         => ("─", Color::DarkGray),
        };
        let has_output = matches!(
            status,
            TaskStatus::Finished { stdout, stderr, .. } if !stdout.is_empty() || !stderr.is_empty()
        );
        let hint = if has_output { " ›" } else { "" };
        ListItem::new(Line::from(vec![
            Span::styled(format!("{symbol} "), Style::default().fg(color)),
            Span::raw(task_id.to_string()),
            Span::styled(hint, Style::default().fg(Color::DarkGray)),
        ]))
    }).collect();

    let mut state = ListState::default();
    if !tasks.is_empty() {
        state.select(Some(src.selected_task));
    }

    let list = List::new(items)
        .block(block)
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(list, area, &mut state);
}

fn render_detail(f: &mut Frame, src: &SourceState, area: Rect) {
    match &src.focus {
        Focus::TriggerList => render_trigger_detail(f, src, area),
        Focus::WorkflowList => render_workflow_detail(f, src, area),
        Focus::Runs => render_run_detail(f, src, area),
        Focus::Tasks | Focus::Detail => render_task_detail(f, src, area),
    }
}

fn render_trigger_detail(f: &mut Frame, src: &SourceState, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Trigger ")
        .border_style(pane_border_style(src.focus == Focus::TriggerList));

    let Some(entry) = src.selected_trigger_entry() else {
        f.render_widget(
            Paragraph::new(Span::styled(" no trigger selected", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    };

    let mut lines: Vec<Line> = Vec::new();

    let short_id = if entry.id.len() > 8 { &entry.id[entry.id.len()-8..] } else { &entry.id };
    lines.push(Line::from(vec![
        Span::styled("id:      ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("…{short_id}")),
    ]));
    lines.push(Line::from(vec![
        Span::styled("source:  ", Style::default().fg(Color::DarkGray)),
        Span::raw(entry.source.clone()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("age:     ", Style::default().fg(Color::DarkGray)),
        Span::raw(format_age(entry.received_at)),
    ]));

    let (status_str, status_color) = trigger_status_display(entry);
    lines.push(Line::from(vec![
        Span::styled("status:  ", Style::default().fg(Color::DarkGray)),
        Span::styled(status_str, Style::default().fg(status_color)),
    ]));

    if let TriggerEntryStatus::Rejected(reason) = &entry.status {
        lines.push(Line::from(vec![
            Span::styled("reason:  ", Style::default().fg(Color::DarkGray)),
            Span::styled(reason.clone(), Style::default().fg(Color::Red)),
        ]));
    }

    if let Some(wf) = &entry.workflow {
        lines.push(Line::from(vec![
            Span::styled("workflow:", Style::default().fg(Color::DarkGray)),
            Span::raw(format!(" {wf}")),
        ]));
    }

    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(lines), inner);
}

fn trigger_status_display(entry: &TriggerEntry) -> (String, Color) {
    match &entry.status {
        TriggerEntryStatus::Running          => ("● running".into(),  Color::Yellow),
        TriggerEntryStatus::Finished(true)   => ("✓ success".into(),  Color::Green),
        TriggerEntryStatus::Finished(false)  => ("✗ failed".into(),   Color::Red),
        TriggerEntryStatus::Rejected(reason) => (format!("⊘ {reason}"), Color::Red),
    }
}

fn render_workflow_detail(f: &mut Frame, src: &SourceState, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Workflow ")
        .border_style(pane_border_style(src.focus == Focus::WorkflowList));

    let names = src.workflow_names();
    let Some(name) = names.get(src.selected_workflow) else {
        f.render_widget(
            Paragraph::new(Span::styled(" no workflow selected", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    };

    let runs: Vec<_> = src.runs_for_selected_workflow();
    let total = runs.len();
    let successes = runs.iter().filter(|(_, r)| r.status == RunStatus::Finished(true)).count();
    let last_age = runs.first()
        .and_then(|(_, r)| r.finished_at_ms)
        .map(format_age)
        .unwrap_or_else(|| "—".into());

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("workflow: ", Style::default().fg(Color::DarkGray)),
        Span::styled(name.clone(), Style::default().add_modifier(Modifier::BOLD)),
    ]));
    lines.push(Line::from(vec![
        Span::styled("runs:     ", Style::default().fg(Color::DarkGray)),
        Span::raw(total.to_string()),
    ]));
    lines.push(Line::from(vec![
        Span::styled("success:  ", Style::default().fg(Color::DarkGray)),
        Span::raw(format!("{successes}/{total}")),
    ]));
    lines.push(Line::from(vec![
        Span::styled("last run: ", Style::default().fg(Color::DarkGray)),
        Span::raw(last_age),
    ]));
    lines.push(Line::from(""));
    if let Some(summary) = src.workflow_issues.get(name) {
        if summary.issues.is_empty() {
            lines.push(Line::from(Span::styled("✓ no validation issues", Style::default().fg(Color::Green))));
        } else {
            lines.push(Line::from(Span::styled("validation issues:", Style::default().fg(Color::DarkGray))));
            for issue in &summary.issues {
                let (icon, color) = if issue.severity == "error" {
                    ("✗", Color::Red)
                } else {
                    ("⚠", Color::Yellow)
                };
                let task_ctx = issue.task_id.as_deref()
                    .map(|id| format!(" [{id}]"))
                    .unwrap_or_default();
                lines.push(Line::from(vec![
                    Span::styled(format!("{icon} "), Style::default().fg(color)),
                    Span::styled(issue.code.clone(), Style::default().add_modifier(Modifier::BOLD)),
                    Span::styled(task_ctx, Style::default().fg(Color::DarkGray)),
                    Span::raw(format!(": {}", issue.message)),
                ]));
            }
        }
    } else {
        lines.push(Line::from(Span::styled("no validation issues detected", Style::default().fg(Color::DarkGray))));
    }

    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(lines), inner);
}

fn render_run_detail(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::Runs;
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Run ")
        .border_style(pane_border_style(is_active));

    let Some((run_id, run)) = src.selected_active_run() else {
        f.render_widget(
            Paragraph::new(Span::styled(" no run selected", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    };

    let passed  = run.tasks.values().filter(|s| matches!(s, TaskStatus::Finished { success: true,  .. })).count();
    let failed  = run.tasks.values().filter(|s| matches!(s, TaskStatus::Finished { success: false, .. })).count();
    let skipped = run.tasks.values().filter(|s| matches!(s, TaskStatus::Skipped)).count();

    let duration = run.finished_at_ms
        .map(|end| format!("{:.1}s", (end.saturating_sub(run.started_at_ms)) as f64 / 1000.0))
        .unwrap_or_else(|| "running…".into());

    let mut lines: Vec<Line> = Vec::new();
    lines.push(Line::from(vec![
        Span::styled("duration: ", Style::default().fg(Color::DarkGray)),
        Span::raw(duration),
    ]));
    lines.push(Line::from(vec![
        Span::styled("tasks:    ", Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{passed} passed"), Style::default().fg(Color::Green)),
        Span::raw("  "),
        Span::styled(format!("{skipped} skipped"), Style::default().fg(Color::DarkGray)),
        Span::raw("  "),
        Span::styled(format!("{failed} failed"), Style::default().fg(if failed > 0 { Color::Red } else { Color::DarkGray })),
    ]));

    // Globals diff if available
    let pre  = src.globals_pre.get(run_id);
    let post = src.globals_post.get(run_id);
    if let (Some(pre), Some(post)) = (pre, post) {
        let diff = diff_globals(pre, post);
        lines.push(Line::from(""));
        if diff.is_empty() {
            lines.push(Line::from(Span::styled("globals: no changes", Style::default().fg(Color::DarkGray))));
        } else {
            lines.push(Line::from(Span::styled("globals:", Style::default().fg(Color::DarkGray))));
            for entry in &diff {
                match entry {
                    GlobalsDiffEntry::Changed { key, before, after } => {
                        lines.push(Line::from(vec![
                            Span::styled(format!("  {key}  "), Style::default().fg(Color::DarkGray)),
                            Span::styled(before.clone(), Style::default().fg(Color::DarkGray)),
                            Span::styled(" → ", Style::default().fg(Color::DarkGray)),
                            Span::styled(after.clone(), Style::default().fg(Color::White)),
                        ]));
                    }
                    GlobalsDiffEntry::Added { key, value } => {
                        lines.push(Line::from(Span::styled(
                            format!("  + {key}  {value}"),
                            Style::default().fg(Color::Green),
                        )));
                    }
                    GlobalsDiffEntry::Removed { key, value } => {
                        lines.push(Line::from(Span::styled(
                            format!("  - {key}  {value}"),
                            Style::default().fg(Color::Red),
                        )));
                    }
                }
            }
        }
    }

    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(lines), inner);
}

fn render_task_detail(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::Detail;
    let Some((task_id, status)) = src.selected_task_entry() else {
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Task ")
            .border_style(pane_border_style(is_active));
        f.render_widget(
            Paragraph::new(Span::styled(" no task selected", Style::default().fg(Color::DarkGray)))
                .block(block),
            area,
        );
        return;
    };

    // Look up the TaskSummary for config fields
    let task_summary = src.selected_active_run().and_then(|(run_id, _)| {
        // We need to find the TaskSummary in the run detail store.
        // The run's tasks IndexMap holds TaskStatus (runtime), not TaskSummary (config).
        // Config fields are stored separately — look them up via the run detail cache.
        src.run_task_summary(run_id, task_id)
    });

    let mut lines: Vec<Line> = Vec::new();

    // Config fields (if available from task summary)
    if let Some(ts) = task_summary {
        if let Some(ty) = &ts.task_type {
            lines.push(Line::from(vec![
                Span::styled("type:     ", Style::default().fg(Color::DarkGray)),
                Span::raw(ty.clone()),
            ]));
        }
        if let Some(exec) = &ts.task_exec {
            lines.push(Line::from(vec![
                Span::styled("exec:     ", Style::default().fg(Color::DarkGray)),
                Span::raw(exec.clone()),
            ]));
        }
        if let Some(when) = &ts.task_when {
            lines.push(Line::from(vec![
                Span::styled("when:     ", Style::default().fg(Color::DarkGray)),
                Span::raw(when.clone()),
            ]));
        }
        if let Some(abort) = &ts.task_abort_if {
            lines.push(Line::from(vec![
                Span::styled("abort_if: ", Style::default().fg(Color::DarkGray)),
                Span::raw(abort.clone()),
            ]));
        }
        if !lines.is_empty() {
            lines.push(Line::from(""));
        }
    }

    // Runtime status
    match status {
        TaskStatus::Finished { stdout, stderr, exit_code, success } => {
            lines.push(Line::from(vec![
                Span::styled("exit:     ", Style::default().fg(Color::DarkGray)),
                Span::styled(
                    exit_code.to_string(),
                    Style::default().fg(if *success { Color::Green } else { Color::Red }),
                ),
            ]));

            // Prefer structured task_logs if available; fall back to raw stdout/stderr
            let log_key = src.selected_active_run().map(|(run_id, _)| (run_id.clone(), task_id.clone()));
            let structured = log_key.as_ref().and_then(|k| src.task_logs.get(k));

            if let Some(log_entries) = structured.filter(|e| !e.is_empty()) {
                let has_stdout = log_entries.iter().any(|e| e.stream == "stdout");
                let has_stderr = log_entries.iter().any(|e| e.stream == "stderr");
                if has_stdout {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        "── stdout ──────────────────────────────────",
                        Style::default().fg(Color::DarkGray),
                    )));
                    for entry in log_entries.iter().filter(|e| e.stream == "stdout") {
                        lines.push(Line::from(entry.line.clone()));
                    }
                }
                if has_stderr {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        "── stderr ──────────────────────────────────",
                        Style::default().fg(Color::Red),
                    )));
                    for entry in log_entries.iter().filter(|e| e.stream == "stderr") {
                        lines.push(Line::from(Span::styled(
                            entry.line.clone(),
                            Style::default().fg(Color::Red),
                        )));
                    }
                }
            } else {
                // No structured logs — render raw stdout/stderr from task status
                if !stdout.is_empty() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        "── stdout ──────────────────────────────────",
                        Style::default().fg(Color::DarkGray),
                    )));
                    for line in stdout.lines() {
                        lines.push(Line::from(line.to_string()));
                    }
                }
                if !stderr.is_empty() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        "── stderr ──────────────────────────────────",
                        Style::default().fg(Color::Red),
                    )));
                    for line in stderr.lines() {
                        lines.push(Line::from(Span::styled(
                            line.to_string(),
                            Style::default().fg(Color::Red),
                        )));
                    }
                }
                if stdout.is_empty() && stderr.is_empty() {
                    lines.push(Line::from(Span::styled(
                        if *success { "(no output — success)" } else { "(no output)" },
                        Style::default().fg(Color::DarkGray),
                    )));
                }
            }
        }
        TaskStatus::Running => {
            lines.push(Line::from(Span::styled("running…", Style::default().fg(Color::Yellow))));
        }
        TaskStatus::Skipped => {
            lines.push(Line::from(Span::styled("skipped", Style::default().fg(Color::DarkGray))));
        }
    }

    let total = lines.len();
    let view_height = area.height.saturating_sub(2) as usize;
    let scroll = src.task_scroll.min(total.saturating_sub(view_height));
    let scroll_suffix = if total > view_height {
        format!(" [{}/{}]", (scroll + view_height).min(total), total)
    } else {
        String::new()
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {task_id}{scroll_suffix} "))
        .border_style(pane_border_style(is_active));

    let inner = block.inner(area);
    f.render_widget(block, area);
    f.render_widget(Paragraph::new(lines).scroll((scroll as u16, 0)), inner);
}

fn render_graph_modal(f: &mut Frame, src: &SourceState, area: Rect) {
    let Some(graph) = &src.graph else { return };

    let modal = centered_rect(80, 80, area);
    f.render_widget(Clear, modal);

    let title = format!(" DAG: {} ", graph.workflow);
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_alignment(Alignment::Left);

    let inner = block.inner(modal);
    f.render_widget(block, modal);

    let task_statuses = src.selected_run_in_workflow().map(|(_, r)| &r.tasks);

    let mut lines: Vec<Line> = Vec::new();
    for node in &graph.nodes {
        lines.push(task_line(node, task_statuses));
        if !node.deps.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("  deps: {}", node.deps.join(", ")),
                Style::default().fg(Color::DarkGray),
            )));
        }
        if let Some(expr) = &node.when {
            lines.push(Line::from(Span::styled(
                format!("  when: {expr}"),
                Style::default().fg(Color::DarkGray),
            )));
        }
        lines.push(Line::from(""));
    }

    f.render_widget(Paragraph::new(lines), inner);
}

fn task_line<'a>(
    node: &'a TaskNode,
    task_statuses: Option<&'a indexmap::IndexMap<String, TaskStatus>>,
) -> Line<'a> {
    let (symbol, color) = task_statuses
        .and_then(|ts| ts.get(&node.id))
        .map(|status| match status {
            TaskStatus::Running                         => ("▶", Color::Yellow),
            TaskStatus::Finished { success: true, .. }  => ("✓", Color::Green),
            TaskStatus::Finished { success: false, .. } => ("✗", Color::Red),
            TaskStatus::Skipped                         => ("─", Color::DarkGray),
        })
        .unwrap_or(("○", Color::DarkGray));

    let indent = "  ".repeat(node.depth);
    Line::from(vec![
        Span::styled(format!("{indent}{symbol} "), Style::default().fg(color)),
        Span::styled(node.id.clone(), Style::default().add_modifier(Modifier::BOLD)),
    ])
}

fn render_statusbar(f: &mut Frame, src: &SourceState, area: Rect) {
    let text = match src.focus {
        Focus::TriggerList =>
            " q: quit   j/k ↑↓: triggers   l/→/Enter: runs   Tab: next source   T/W: mode   [/]: panels",
        Focus::WorkflowList =>
            " q: quit   j/k ↑↓: workflows   l/→/Enter: runs   Tab: next source   T/W: mode   [/]: panels",
        Focus::Runs =>
            " q: quit   j/k ↑↓: runs   h/←/Esc: back   l/→/Enter: tasks   Tab: next source   [/]: panels",
        Focus::Tasks =>
            " q: quit   j/k ↑↓: tasks   h/←/Esc: back   l/→: detail   g: graph   [/]: panels",
        Focus::Detail =>
            " q: quit   j/k ↑↓: scroll   h/←/Esc: back",
    };
    f.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
