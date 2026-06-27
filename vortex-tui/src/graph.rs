use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowConfigDto {
    pub name: String,
    pub tasks: Vec<TaskConfigDto>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskConfigDto {
    pub id: String,
    #[serde(default)]
    pub exec: Option<String>,
    pub when: Option<String>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct TaskNode {
    pub id: String,
    pub when: Option<String>,
    pub deps: Vec<String>,
    pub depth: usize,
}

#[derive(Debug, Clone)]
pub struct DependencyGraph {
    pub workflow: String,
    pub nodes: Vec<TaskNode>,   // ordered by depth asc, then declaration order
}

impl DependencyGraph {
    pub fn from_config(dto: WorkflowConfigDto) -> Self {
        let task_ids: std::collections::HashMap<&str, usize> = dto
            .tasks
            .iter()
            .enumerate()
            .map(|(i, t)| (t.id.as_str(), i))
            .collect();

        // Build dep lists and compute depths
        let mut nodes: Vec<TaskNode> = dto
            .tasks
            .iter()
            .map(|t| {
                let deps = extract_deps(t.when.as_deref(), t.depends_on.as_deref(), &task_ids);
                TaskNode { id: t.id.clone(), when: t.when.clone(), deps, depth: 0 }
            })
            .collect();

        // Compute depth = max(depth of deps) + 1; roots = 0
        let n = nodes.len();
        let deps_snapshot: Vec<Vec<String>> = nodes.iter().map(|n| n.deps.clone()).collect();
        for i in 0..n {
            nodes[i].depth = compute_depth(i, &deps_snapshot, &task_ids, &mut vec![None; n]);
        }

        // Sort by depth, preserving declaration order within same depth
        nodes.sort_by_key(|n| n.depth);

        Self { workflow: dto.name, nodes }
    }
}

fn extract_deps(when: Option<&str>, depends_on: Option<&[String]>, task_ids: &std::collections::HashMap<&str, usize>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut deps = vec![];

    // Explicit depends_on takes priority
    if let Some(explicit) = depends_on {
        for dep in explicit {
            if task_ids.contains_key(dep.as_str()) && seen.insert(dep.clone()) {
                deps.push(dep.clone());
            }
        }
    }

    // Also extract implicit deps from when expression
    if let Some(expr) = when {
        for token in expr.split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-') {
            if token.is_empty() || matches!(token, "AND" | "OR" | "NOT") {
                continue;
            }
            if task_ids.contains_key(token) && seen.insert(token.to_string()) {
                deps.push(token.to_string());
            }
        }
    }

    deps
}

fn compute_depth(
    idx: usize,
    deps_snapshot: &[Vec<String>],
    task_ids: &std::collections::HashMap<&str, usize>,
    memo: &mut Vec<Option<usize>>,
) -> usize {
    if let Some(d) = memo[idx] {
        return d;
    }
    // Guard against cycles: mark as 0 before recursing
    memo[idx] = Some(0);
    let depth = deps_snapshot[idx]
        .iter()
        .filter_map(|dep| task_ids.get(dep.as_str()).copied())
        .map(|dep_idx| compute_depth(dep_idx, deps_snapshot, task_ids, memo) + 1)
        .max()
        .unwrap_or(0);
    memo[idx] = Some(depth);
    depth
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(name: &str, tasks: &[(&str, Option<&str>)]) -> WorkflowConfigDto {
        WorkflowConfigDto {
            name: name.into(),
            tasks: tasks.iter().map(|(id, when)| TaskConfigDto {
                id: id.to_string(),
                exec: None,
                when: when.map(str::to_string),
                depends_on: None,
            }).collect(),
        }
    }

    fn node_ids(g: &DependencyGraph) -> Vec<&str> {
        g.nodes.iter().map(|n| n.id.as_str()).collect()
    }

    // --- dependency extraction ---

    #[test]
    fn no_when_produces_no_deps() {
        let g = DependencyGraph::from_config(cfg("w", &[("a", None)]));
        assert!(g.nodes[0].deps.is_empty());
    }

    #[test]
    fn simple_dep_extracted() {
        let g = DependencyGraph::from_config(cfg("w", &[("a", None), ("b", Some("a"))]));
        assert_eq!(g.nodes.iter().find(|n| n.id == "b").unwrap().deps, vec!["a"]);
    }

    #[test]
    fn and_expression_extracts_both_deps() {
        let g = DependencyGraph::from_config(cfg("w", &[
            ("a", None), ("b", None), ("c", Some("a AND b")),
        ]));
        let c = g.nodes.iter().find(|n| n.id == "c").unwrap();
        assert!(c.deps.contains(&"a".to_string()));
        assert!(c.deps.contains(&"b".to_string()));
    }

    #[test]
    fn or_expression_extracts_both_deps() {
        let g = DependencyGraph::from_config(cfg("w", &[
            ("a", None), ("b", None), ("c", Some("a OR b")),
        ]));
        let c = g.nodes.iter().find(|n| n.id == "c").unwrap();
        assert!(c.deps.contains(&"a".to_string()));
        assert!(c.deps.contains(&"b".to_string()));
    }

    #[test]
    fn not_expression_extracts_dep() {
        let g = DependencyGraph::from_config(cfg("w", &[("a", None), ("b", Some("NOT a"))]));
        assert_eq!(g.nodes.iter().find(|n| n.id == "b").unwrap().deps, vec!["a"]);
    }

    #[test]
    fn complex_expression_extracts_all_deps() {
        let g = DependencyGraph::from_config(cfg("w", &[
            ("a", None), ("b", None), ("c", None),
            ("d", Some("(a AND b) OR c")),
        ]));
        let d = g.nodes.iter().find(|n| n.id == "d").unwrap();
        assert!(d.deps.contains(&"a".to_string()));
        assert!(d.deps.contains(&"b".to_string()));
        assert!(d.deps.contains(&"c".to_string()));
    }

    // --- depth computation ---

    #[test]
    fn roots_have_depth_zero() {
        let g = DependencyGraph::from_config(cfg("w", &[("a", None), ("b", None)]));
        assert!(g.nodes.iter().all(|n| n.depth == 0));
    }

    #[test]
    fn linear_chain_assigns_increasing_depths() {
        let g = DependencyGraph::from_config(cfg("w", &[
            ("a", None), ("b", Some("a")), ("c", Some("b")),
        ]));
        assert_eq!(g.nodes.iter().find(|n| n.id == "a").unwrap().depth, 0);
        assert_eq!(g.nodes.iter().find(|n| n.id == "b").unwrap().depth, 1);
        assert_eq!(g.nodes.iter().find(|n| n.id == "c").unwrap().depth, 2);
    }

    #[test]
    fn diamond_dep_takes_max_depth() {
        // a(0) → b(1), a(0) → c(1), b AND c → d(2)
        let g = DependencyGraph::from_config(cfg("w", &[
            ("a", None), ("b", Some("a")), ("c", Some("a")),
            ("d", Some("b AND c")),
        ]));
        assert_eq!(g.nodes.iter().find(|n| n.id == "d").unwrap().depth, 2);
    }

    // --- ordering ---

    #[test]
    fn nodes_ordered_by_depth_ascending() {
        let g = DependencyGraph::from_config(cfg("w", &[
            ("a", None), ("c", Some("b")), ("b", Some("a")),
        ]));
        let depths: Vec<usize> = g.nodes.iter().map(|n| n.depth).collect();
        assert!(depths.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn declaration_order_preserved_within_same_depth() {
        let g = DependencyGraph::from_config(cfg("w", &[
            ("x", None), ("y", None), ("z", None),
        ]));
        // all depth 0 — should stay in declaration order
        assert_eq!(node_ids(&g), vec!["x", "y", "z"]);
    }

    #[test]
    fn workflow_name_preserved() {
        let g = DependencyGraph::from_config(cfg("my-workflow", &[("a", None)]));
        assert_eq!(g.workflow, "my-workflow");
    }
}
