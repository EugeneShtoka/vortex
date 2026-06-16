use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs},
    Frame,
};

use crate::app::{App, ConnectionStatus, Focus, RunStatus, SourceState, TaskStatus};
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
        let panes = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(20),
                Constraint::Percentage(35),
                Constraint::Percentage(45),
            ])
            .split(outer[1]);

        render_workflows(f, src, panes[0]);
        render_runs_pane(f, src, panes[1]);
        render_tasks_pane(f, src, panes[2]);
    }
    render_statusbar(f, src, outer[2]);

    if src.show_graph {
        render_graph_modal(f, src, area);
    }

    if src.focus == Focus::TaskDetail {
        render_task_detail_overlay(f, src, area);
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

fn render_workflows(f: &mut Frame, src: &SourceState, area: Rect) {
    let is_active = src.focus == Focus::Workflows;
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
        ListItem::new(Line::from(vec![
            Span::styled(format!("{symbol} "), Style::default().fg(color)),
            Span::raw(name.clone()),
        ]))
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

fn render_task_detail_overlay(f: &mut Frame, src: &SourceState, area: Rect) {
    let Some((task_id, status)) = src.selected_task_entry() else { return };

    let modal = centered_rect(90, 85, area);
    f.render_widget(Clear, modal);

    let mut lines: Vec<Line> = Vec::new();

    match status {
        TaskStatus::Finished { stdout, stderr, exit_code, success } => {
            if !stdout.is_empty() {
                lines.push(Line::from(Span::styled(
                    "── stdout ──────────────────────────────────────────────────────────",
                    Style::default().fg(Color::DarkGray),
                )));
                for line in stdout.lines() {
                    lines.push(Line::from(line.to_string()));
                }
            }
            if !stderr.is_empty() {
                if !lines.is_empty() { lines.push(Line::from("")); }
                lines.push(Line::from(Span::styled(
                    "── stderr ──────────────────────────────────────────────────────────",
                    Style::default().fg(Color::Red),
                )));
                for line in stderr.lines() {
                    lines.push(Line::from(Span::styled(
                        line.to_string(),
                        Style::default().fg(Color::Red),
                    )));
                }
            }
            if lines.is_empty() {
                let note = if *success {
                    "(no output — success)".to_string()
                } else {
                    format!("(no output — exit {exit_code})")
                };
                lines.push(Line::from(Span::styled(note, Style::default().fg(Color::DarkGray))));
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
    let view_height = modal.height.saturating_sub(2) as usize;
    let scroll = src.task_scroll.min(total.saturating_sub(view_height));

    let scroll_suffix = if total > view_height {
        format!(" [{}/{}]", (scroll + view_height).min(total), total)
    } else {
        String::new()
    };

    let title = format!(" {task_id}{scroll_suffix} ");
    let block = Block::default()
        .borders(Borders::ALL)
        .title(title)
        .title_alignment(Alignment::Left);

    let inner = block.inner(modal);
    f.render_widget(block, modal);

    let paragraph = Paragraph::new(lines).scroll((scroll as u16, 0));
    f.render_widget(paragraph, inner);
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
        Focus::Workflows =>
            " q: quit   j/k ↑↓: workflows   l/→/Enter: runs   Tab: next source",
        Focus::Runs =>
            " q: quit   j/k ↑↓: runs   h/←/Esc: back   l/→/Enter: tasks   Tab: next source",
        Focus::Tasks =>
            " q: quit   j/k ↑↓: tasks   h/←/Esc: back   Enter: output   g: graph",
        Focus::TaskDetail =>
            " q: quit   j/k ↑↓: scroll   Esc/h/←: close",
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
