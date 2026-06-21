use std::collections::{HashMap, HashSet};

use serde::Serialize;

use crate::config::{TaskKind, WorkflowConfig};

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationIssue {
    pub severity: Severity,
    pub task_id: Option<String>,
    pub code: &'static str,
    pub message: String,
}

pub fn validate(config: &WorkflowConfig) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    let task_ids: HashSet<&str> = config.tasks.iter().map(|t| t.id.as_str()).collect();

    issues.extend(check_missing_deps(config, &task_ids));
    issues.extend(check_circular_deps(config));
    issues.extend(check_cel_expressions(config));

    issues
}

fn check_missing_deps(config: &WorkflowConfig, task_ids: &HashSet<&str>) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    for task in &config.tasks {
        if let Some(deps) = &task.depends_on {
            for dep in deps {
                if !task_ids.contains(dep.as_str()) {
                    issues.push(ValidationIssue {
                        severity: Severity::Error,
                        task_id: Some(task.id.clone()),
                        code: "missing_dep",
                        message: format!("depends_on references unknown task '{dep}'"),
                    });
                }
            }
        }
    }
    issues
}

fn check_circular_deps(config: &WorkflowConfig) -> Vec<ValidationIssue> {
    let n = config.tasks.len();
    if n == 0 {
        return vec![];
    }

    let task_id_index: HashMap<&str, usize> = config.tasks.iter()
        .enumerate()
        .map(|(i, t)| (t.id.as_str(), i))
        .collect();

    let mut deps: Vec<Vec<usize>> = vec![vec![]; n];
    for (i, task) in config.tasks.iter().enumerate() {
        let mut seen = HashSet::new();
        if let Some(explicit) = &task.depends_on {
            for dep_id in explicit {
                if let Some(&j) = task_id_index.get(dep_id.as_str()) {
                    if seen.insert(j) { deps[i].push(j); }
                }
            }
        } else if let Some(expr) = &task.when {
            for token in expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-') {
                if token.is_empty() || matches!(token, "AND" | "OR" | "NOT") { continue; }
                if let Some(&j) = task_id_index.get(token) {
                    if seen.insert(j) { deps[i].push(j); }
                }
            }
        }
    }

    let mut rev: Vec<Vec<usize>> = vec![vec![]; n];
    for (i, task_deps) in deps.iter().enumerate() {
        for &j in task_deps { rev[j].push(i); }
    }
    let mut in_degree: Vec<usize> = deps.iter().map(|d| d.len()).collect();
    let mut queue: Vec<usize> = in_degree.iter().enumerate()
        .filter(|(_, &d)| d == 0)
        .map(|(i, _)| i)
        .collect();
    let mut count = 0;
    while !queue.is_empty() {
        queue.sort_unstable();
        let cur = queue.remove(0);
        count += 1;
        for &next in &rev[cur] {
            in_degree[next] -= 1;
            if in_degree[next] == 0 { queue.push(next); }
        }
    }

    if count != n {
        vec![ValidationIssue {
            severity: Severity::Error,
            task_id: None,
            code: "circular_dep",
            message: "circular dependency detected in task graph".to_string(),
        }]
    } else {
        vec![]
    }
}

fn normalize_cel(expr: &str) -> String {
    expr.replace(" AND ", " && ")
        .replace(" OR ", " || ")
        .replace("NOT ", "!")
}

fn check_cel_expressions(config: &WorkflowConfig) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();

    for task in &config.tasks {
        if let Some(expr) = &task.when {
            let normalized = normalize_cel(expr);
            if let Err(e) = cel_interpreter::Program::compile(&normalized) {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    task_id: Some(task.id.clone()),
                    code: "cel_parse_error",
                    message: format!("invalid CEL in 'when': {e}"),
                });
            }
        }
        if let Some(expr) = &task.abort_if {
            let normalized = normalize_cel(expr);
            if let Err(e) = cel_interpreter::Program::compile(&normalized) {
                issues.push(ValidationIssue {
                    severity: Severity::Error,
                    task_id: Some(task.id.clone()),
                    code: "cel_parse_error",
                    message: format!("invalid CEL in 'abort_if': {e}"),
                });
            }
        }
        match &task.kind {
            TaskKind::Condition { expr } | TaskKind::Eval { expr } => {
                if let Err(e) = cel_interpreter::Program::compile(expr) {
                    issues.push(ValidationIssue {
                        severity: Severity::Error,
                        task_id: Some(task.id.clone()),
                        code: "cel_parse_error",
                        message: format!("invalid CEL in task expression: {e}"),
                    });
                }
            }
            TaskKind::ForEach { items, accumulate, .. } => {
                for (field, expr) in [("items", items.as_str()), ("accumulate", accumulate.as_str())] {
                    if let Err(e) = cel_interpreter::Program::compile(expr) {
                        issues.push(ValidationIssue {
                            severity: Severity::Error,
                            task_id: Some(task.id.clone()),
                            code: "cel_parse_error",
                            message: format!("invalid CEL in '{field}': {e}"),
                        });
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(expr) = &config.status_eval {
        let normalized = normalize_cel(expr);
        if let Err(e) = cel_interpreter::Program::compile(&normalized) {
            issues.push(ValidationIssue {
                severity: Severity::Error,
                task_id: None,
                code: "cel_parse_error",
                message: format!("invalid CEL in 'status_eval': {e}"),
            });
        }
    }

    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TaskConfig, TaskKind, WorkflowConfig};

    fn shell(id: &str) -> TaskConfig {
        TaskConfig {
            id: id.into(),
            kind: TaskKind::Shell { exec: "true".into() },
            when: None,
            depends_on: None,
            response_template: None,
            abort_if: None,
        }
    }

    fn with_when(mut t: TaskConfig, when: &str) -> TaskConfig {
        t.when = Some(when.into());
        t
    }

    fn with_deps(mut t: TaskConfig, deps: &[&str]) -> TaskConfig {
        t.depends_on = Some(deps.iter().map(|s| s.to_string()).collect());
        t
    }

    fn workflow(tasks: Vec<TaskConfig>) -> WorkflowConfig {
        WorkflowConfig { tasks, cron: None, correlation_id: None, status_eval: None, log_retention: None }
    }

    #[test]
    fn validate_clean_workflow_returns_empty() {
        let wf = workflow(vec![shell("a"), with_when(shell("b"), "a")]);
        assert!(validate(&wf).is_empty());
    }

    #[test]
    fn validate_detects_missing_dependency() {
        let task = with_deps(shell("a"), &["ghost"]);
        let wf = workflow(vec![task]);
        let issues = validate(&wf);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "missing_dep");
        assert_eq!(issues[0].severity, Severity::Error);
        assert_eq!(issues[0].task_id.as_deref(), Some("a"));
        assert!(issues[0].message.contains("ghost"));
    }

    #[test]
    fn validate_detects_circular_dependency() {
        let a = with_deps(shell("a"), &["b"]);
        let b = with_deps(shell("b"), &["a"]);
        let wf = workflow(vec![a, b]);
        let issues = validate(&wf);
        assert!(issues.iter().any(|i| i.code == "circular_dep"));
    }

    #[test]
    fn validate_detects_cel_parse_error_in_when() {
        let task = with_when(shell("a"), "this is $$$ invalid cel");
        let wf = workflow(vec![task]);
        let issues = validate(&wf);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].code, "cel_parse_error");
        assert_eq!(issues[0].task_id.as_deref(), Some("a"));
        assert!(issues[0].message.contains("when"));
    }

    #[test]
    fn validate_detects_cel_parse_error_in_abort_if() {
        let mut task = shell("a");
        task.abort_if = Some("not valid $$$ cel".into());
        let wf = workflow(vec![task]);
        let issues = validate(&wf);
        assert!(issues.iter().any(|i| i.code == "cel_parse_error" && i.task_id.as_deref() == Some("a")));
        assert!(issues.iter().any(|i| i.message.contains("abort_if")));
    }

    #[test]
    fn validate_detects_cel_parse_error_in_status_eval() {
        let mut wf = workflow(vec![shell("a")]);
        wf.status_eval = Some("not valid $$$ cel".into());
        let issues = validate(&wf);
        assert!(issues.iter().any(|i| i.code == "cel_parse_error" && i.task_id.is_none()));
        assert!(issues.iter().any(|i| i.message.contains("status_eval")));
    }

    #[test]
    fn validate_valid_cel_passes() {
        let task = with_when(shell("b"), "tasks.a.success && trigger.env == \"prod\"");
        let wf = workflow(vec![shell("a"), task]);
        assert!(validate(&wf).is_empty());
    }

    #[test]
    fn validate_empty_workflow_returns_empty() {
        assert!(validate(&workflow(vec![])).is_empty());
    }

    #[test]
    fn validate_detects_multiple_missing_deps() {
        let task = with_deps(shell("a"), &["x", "y"]);
        let wf = workflow(vec![task]);
        let issues = validate(&wf);
        assert_eq!(issues.len(), 2);
        assert!(issues.iter().all(|i| i.code == "missing_dep"));
    }
}
