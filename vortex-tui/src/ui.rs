use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Tabs},
    Frame,
};

use crate::app::{App, ConnectionStatus, RunStatus, SourceState, TaskStatus};
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

    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(outer[1]);

    let src = app.active_source();
    if let ConnectionStatus::Disconnected(err) = &src.connection {
        render_disconnected(f, err.as_deref(), outer[1]);
    } else {
        render_runs(f, src, panes[0]);
        render_detail(f, src, panes[1]);
    }
    render_statusbar(f, outer[2], src.graph.is_some());

    if src.show_graph {
        render_graph_modal(f, src, area);
    }
}

fn render_tabs(f: &mut Frame, app: &App, area: Rect) {
    let titles: Vec<Line> = app.sources.iter().map(|src| {
        let (indicator, color) = match &src.connection {
            ConnectionStatus::Connected         => ("●", Color::Green),
            ConnectionStatus::Connecting        => ("◌", Color::Yellow),
            ConnectionStatus::Disconnected(_)   => ("○", Color::Red),
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
    if secs < 60        { format!("  {}s ago", secs) }
    else if secs < 3600 { format!("  {}m ago", secs / 60) }
    else if secs < 86400 { format!("  {}h ago", secs / 3600) }
    else                { format!("  {}d ago", secs / 86400) }
}

fn render_runs(f: &mut Frame, src: &SourceState, area: Rect) {
    let items: Vec<ListItem> = src
        .sorted_runs()
        .into_iter()
        .map(|(_, run)| {
            let (symbol, color) = match &run.status {
                RunStatus::Running         => ("●", Color::Yellow),
                RunStatus::Finished(true)  => ("✓", Color::Green),
                RunStatus::Finished(false) => ("✗", Color::Red),
                RunStatus::Rejected(_)     => ("✗", Color::Red),
            };
            let label = if run.workflow.is_empty() { "rejected" } else { &run.workflow };
            let duration = match run.finished_at_ms {
                Some(fin) if run.started_at_ms > 0 => {
                    let ms = fin.saturating_sub(run.started_at_ms);
                    format!("  {:.1}s", ms as f64 / 1000.0)
                }
                _ => String::new(),
            };
            let age = format_age(run.started_at_ms);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{symbol} "), Style::default().fg(color)),
                Span::raw(label.to_string()),
                Span::styled(duration, Style::default().fg(Color::DarkGray)),
                Span::styled(age, Style::default().fg(Color::DarkGray)),
            ]))
        })
        .collect();

    let mut state = ListState::default();
    if !src.runs.is_empty() {
        state.select(Some(src.selected));
    }

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(" Runs "))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    f.render_stateful_widget(list, area, &mut state);
}

fn render_detail(f: &mut Frame, src: &SourceState, area: Rect) {
    let block = Block::default().borders(Borders::ALL).title(" Tasks ");

    let Some((_, run)) = src.selected_run() else {
        f.render_widget(block, area);
        return;
    };

    let mut lines: Vec<Line> = Vec::new();

    for (task_id, status) in &run.tasks {
        let (symbol, color) = match status {
            TaskStatus::Running                         => ("▶", Color::Yellow),
            TaskStatus::Finished { success: true, .. }  => ("✓", Color::Green),
            TaskStatus::Finished { success: false, .. } => ("✗", Color::Red),
            TaskStatus::Skipped                         => ("─", Color::DarkGray),
        };
        lines.push(Line::from(vec![
            Span::styled(format!(" {symbol} "), Style::default().fg(color)),
            Span::raw(task_id.clone()),
        ]));

        if let TaskStatus::Finished { stdout, stderr, .. } = status {
            for line in stdout.lines().take(3) {
                lines.push(Line::from(Span::styled(
                    format!("     {line}"),
                    Style::default().fg(Color::DarkGray),
                )));
            }
            for line in stderr.lines().take(3) {
                lines.push(Line::from(Span::styled(
                    format!("     {line}"),
                    Style::default().fg(Color::Red),
                )));
            }
        }
    }

    let paragraph = Paragraph::new(lines).block(block);
    f.render_widget(paragraph, area);
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

    let task_statuses = src.selected_run().map(|(_, r)| &r.tasks);

    let mut lines: Vec<Line> = Vec::new();
    for node in &graph.nodes {
        lines.push(task_line(node, task_statuses));
        if let Some(expr) = &node.when {
            lines.push(Line::from(Span::styled(
                format!("  when: {expr}"),
                Style::default().fg(Color::DarkGray),
            )));
        }
        lines.push(Line::from("")); // blank separator
    }

    let paragraph = Paragraph::new(lines);
    f.render_widget(paragraph, inner);
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

    Line::from(vec![
        Span::styled(format!("{symbol} "), Style::default().fg(color)),
        Span::styled(node.id.clone(), Style::default().add_modifier(Modifier::BOLD)),
    ])
}

fn render_statusbar(f: &mut Frame, area: Rect, graph_available: bool) {
    let graph_hint = if graph_available { "   g: graph" } else { "" };
    let text = Paragraph::new(format!(" q: quit   ↑/k: up   ↓/j: down   Tab: next source{graph_hint}"))
        .style(Style::default().fg(Color::DarkGray));
    f.render_widget(text, area);
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
