use std::collections::HashMap;

use anyhow::Result;
use evalexpr::*;

use crate::engine::TaskResult;

/// Evaluate a gate expression against the current set of task results.
///
/// Supports the natural-language operators from `vortex.yaml` as well as
/// their symbolic equivalents:
///
/// | YAML style          | Symbolic  |
/// |---------------------|-----------|
/// | `NOT a`             | `!a`      |
/// | `a AND b`           | `a && b`  |
/// | `a OR b`            | `a \|\| b`|
/// | `(a AND b) OR c`    | same      |
///
/// Any task ID not present in `results` (skipped / not yet run) is treated
/// as `false`. `all_task_ids` pre-populates the context so evalexpr never
/// sees undefined-variable errors for known tasks.
pub fn evaluate(
    expr: &str,
    results: &HashMap<String, TaskResult>,
    all_task_ids: &[&str],
) -> Result<bool> {
    let normalized = normalize(expr);
    let mut ctx = HashMapContext::new();

    for &id in all_task_ids {
        ctx.set_value(id.to_string(), Value::Boolean(false))?;
    }
    for (id, result) in results {
        ctx.set_value(id.clone(), Value::Boolean(result.success))?;
    }

    eval_boolean_with_context(&normalized, &ctx)
        .map_err(|e| anyhow::anyhow!("Gate expression error in '{expr}': {e}"))
}

/// Translate YAML-style boolean keywords to evalexpr operators.
fn normalize(expr: &str) -> String {
    expr.replace(" AND ", " && ")
        .replace(" OR ", " || ")
        .replace("NOT ", "!")
}

// ──────────────────────────────────────────────
// Tests (written before implementation — TDD)
// ──────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    fn r(id: &str, success: bool) -> (String, TaskResult) {
        (
            id.to_string(),
            TaskResult {
                id: id.into(),
                stdout: String::new(),
                stderr: String::new(),
                exit_code: if success { 0 } else { 1 },
                success,
                output: None,
                status: None,
                response: None,
            },
        )
    }

    fn results(pairs: &[(&str, bool)]) -> HashMap<String, TaskResult> {
        pairs.iter().map(|&(id, ok)| r(id, ok)).collect()
    }

    fn ids<'a>(pairs: &[(&'a str, bool)]) -> Vec<&'a str> {
        pairs.iter().map(|&(id, _)| id).collect()
    }

    // --- simple task reference ---

    #[test]
    fn bare_id_passes_when_succeeded() {
        let data = &[("pull_code", true)];
        assert!(evaluate("pull_code", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn bare_id_fails_when_task_failed() {
        let data = &[("pull_code", false)];
        assert!(!evaluate("pull_code", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn undefined_task_is_false() {
        assert!(!evaluate("ghost", &HashMap::new(), &["ghost"]).unwrap());
    }

    // --- NOT ---

    #[test]
    fn not_yaml_style_inverts() {
        let data = &[("step", false)];
        assert!(evaluate("NOT step", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn not_symbolic_inverts() {
        let data = &[("step", false)];
        assert!(evaluate("!step", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn not_true_is_false() {
        let data = &[("step", true)];
        assert!(!evaluate("NOT step", &results(data), &ids(data)).unwrap());
    }

    // --- AND ---

    #[test]
    fn and_yaml_both_true() {
        let data = &[("a", true), ("b", true)];
        assert!(evaluate("a AND b", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn and_yaml_one_false() {
        let data = &[("a", true), ("b", false)];
        assert!(!evaluate("a AND b", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn and_symbolic() {
        let data = &[("a", true), ("b", true)];
        assert!(evaluate("a && b", &results(data), &ids(data)).unwrap());
    }

    // --- OR ---

    #[test]
    fn or_yaml_one_true() {
        let data = &[("a", false), ("b", true)];
        assert!(evaluate("a OR b", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn or_yaml_both_false() {
        let data = &[("a", false), ("b", false)];
        assert!(!evaluate("a OR b", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn or_symbolic() {
        let data = &[("a", false), ("b", true)];
        assert!(evaluate("a || b", &results(data), &ids(data)).unwrap());
    }

    // --- complex ---

    #[test]
    fn complex_and_or_with_parens() {
        // a=T, b=F, c=T → (a AND b) OR c = F OR T = T
        let data = &[("a", true), ("b", false), ("c", true)];
        assert!(evaluate("(a AND b) OR c", &results(data), &ids(data)).unwrap());
        // a=T, b=F, c=F → a AND (b OR c) = T AND F = F
        let data2 = &[("a", true), ("b", false), ("c", false)];
        assert!(!evaluate("a AND (b OR c)", &results(data2), &ids(data2)).unwrap());
    }

    #[test]
    fn complex_nested_all_false() {
        // a=T, b=F, c=F → (a AND b) OR c = F OR F = F
        let data = &[("a", true), ("b", false), ("c", false)];
        assert!(!evaluate("(a AND b) OR c", &results(data), &ids(data)).unwrap());
    }

    #[test]
    fn complex_not_with_and() {
        // NOT a AND b → !a && b = F && T = F
        let data = &[("a", true), ("b", true)];
        assert!(!evaluate("NOT a AND b", &results(data), &ids(data)).unwrap());
    }

    // --- error cases ---

    #[test]
    fn invalid_expression_returns_error() {
        assert!(evaluate("((broken", &HashMap::new(), &[]).is_err());
    }
}
