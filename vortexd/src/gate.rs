use std::collections::HashMap;

use anyhow::Result;
use cel_interpreter::{Context, Program, Value};

use crate::engine::TaskResult;

/// Evaluate a CEL gate expression against the full workflow context.
/// Returns `bool`; undeclared variable references → `false` (task not yet run).
///
/// Context variables available in `expr`:
///
/// | Variable                     | Type    | Example                                    |
/// |------------------------------|---------|--------------------------------------------|
/// | `tasks.<id>.success`         | bool    | `tasks.build.success`                      |
/// | `tasks.<id>.stdout`          | string  | `tasks.price.stdout`                       |
/// | `tasks.<id>.stderr`          | string  | `tasks.build.stderr`                       |
/// | `tasks.<id>.exit_code`       | int     | `tasks.build.exit_code == 0`               |
/// | `trigger.<key>`              | string  | `trigger.sender == "@alice:server"`        |
/// | `env.<KEY>`                  | any     | `trigger.sender in env.MATRIX_CONTACTS`    |
/// | `globals.<key>`              | string  | `globals.deploy_count`                     |
/// | `correlation_id`             | string  | `correlation_id == "req-1"`                |
/// | `<task_id>`                  | bool    | `build` (backward-compat bare task bools)  |
///
/// Natural-language boolean keywords (`AND`, `OR`, `NOT`) are accepted alongside
/// their symbolic equivalents (`&&`, `||`, `!`).
pub fn evaluate(
    expr: &str,
    results: &HashMap<String, TaskResult>,
    all_ids: &[&str],
    trigger_params: &HashMap<String, String>,
    globals: &HashMap<String, String>,
    correlation_id: &str,
) -> Result<bool> {
    let normalized = normalize(expr);
    let program = Program::compile(&normalized)
        .map_err(|e| anyhow::anyhow!("Gate compile error in '{expr}': {e}"))?;
    let ctx = build_context(results, all_ids, trigger_params, globals, correlation_id)?;
    match program.execute(&ctx) {
        Ok(Value::Bool(b)) => Ok(b),
        Ok(other) => Err(anyhow::anyhow!("Gate expression '{expr}' returned {other:?}, expected bool")),
        // Undeclared variable = task ID not registered → treat as false (not yet run / unknown)
        Err(e) if e.to_string().contains("Undeclared reference") => Ok(false),
        Err(e) => Err(anyhow::anyhow!("Gate eval error in '{expr}': {e}")),
    }
}

/// Evaluate a CEL expression and return the raw value. All errors (including
/// undeclared references and runtime panics like index-out-of-bounds) propagate
/// as `Err` — callers map errors to task failure.
pub fn evaluate_value(
    expr: &str,
    results: &HashMap<String, TaskResult>,
    all_ids: &[&str],
    trigger_params: &HashMap<String, String>,
    globals: &HashMap<String, String>,
    correlation_id: &str,
) -> Result<Value> {
    let normalized = normalize(expr);
    let program = Program::compile(&normalized)
        .map_err(|e| anyhow::anyhow!("Eval compile error in '{expr}': {e}"))?;
    let ctx = build_context(results, all_ids, trigger_params, globals, correlation_id)?;
    program.execute(&ctx).map_err(|e| anyhow::anyhow!("Eval error in '{expr}': {e}"))
}

fn build_context<'a>(
    results: &'a HashMap<String, TaskResult>,
    all_ids: &'a [&'a str],
    trigger_params: &'a HashMap<String, String>,
    globals: &'a HashMap<String, String>,
    correlation_id: &'a str,
) -> Result<Context<'a>> {
    let mut ctx = Context::default();

    // tasks.{id}.{success, stdout, stderr, exit_code} — all task IDs present; unrun → defaults
    let tasks_json: serde_json::Value = {
        let mut m = serde_json::Map::new();
        for &id in all_ids {
            let entry = match results.get(id) {
                Some(r) => serde_json::json!({
                    "success":   r.success,
                    "stdout":    r.stdout.trim(),
                    "stderr":    r.stderr.trim(),
                    "exit_code": r.exit_code,
                }),
                None => serde_json::json!({
                    "success":   false,
                    "stdout":    "",
                    "stderr":    "",
                    "exit_code": -1,
                }),
            };
            m.insert(id.to_string(), entry);
        }
        serde_json::Value::Object(m)
    };
    ctx.add_variable_from_value("tasks", to_cel(&tasks_json)?);

    // trigger.{key}
    ctx.add_variable_from_value("trigger", to_cel(trigger_params)?);

    // env.{KEY} — JSON-parsed where possible
    let env_json: serde_json::Value = serde_json::Value::Object(
        std::env::vars()
            .map(|(k, v)| {
                let val = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
                (k, val)
            })
            .collect(),
    );
    ctx.add_variable_from_value("env", to_cel(&env_json)?);

    // globals.{key}
    ctx.add_variable_from_value("globals", to_cel(globals)?);

    // correlation_id
    ctx.add_variable_from_value("correlation_id", to_cel(correlation_id)?);

    // backward compat: bare task-ID booleans for `when = "task_id"` style
    for &id in all_ids {
        if is_cel_ident(id) {
            let success = results.get(id).map_or(false, |r| r.success);
            if let Ok(v) = to_cel(&success) {
                ctx.add_variable_from_value(id, v);
            }
        }
    }

    Ok(ctx)
}

fn to_cel<T: serde::Serialize + ?Sized>(v: &T) -> Result<Value> {
    cel_interpreter::to_value(v).map_err(|e| anyhow::anyhow!("CEL context serialization: {e}"))
}

fn is_cel_ident(s: &str) -> bool {
    let mut chars = s.chars();
    chars.next().map_or(false, |c| c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
}

fn normalize(expr: &str) -> String {
    expr.replace(" AND ", " && ")
        .replace(" OR ", " || ")
        .replace("NOT ", "!")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(id: &str, success: bool) -> (String, TaskResult) {
        (id.to_string(), TaskResult {
            id: id.into(), stdout: String::new(), stderr: String::new(),
            exit_code: if success { 0 } else { 1 }, success,
            output: None, status: None, response: None,
        })
    }

    fn results(pairs: &[(&str, bool)]) -> HashMap<String, TaskResult> {
        pairs.iter().map(|&(id, ok)| r(id, ok)).collect()
    }

    fn ids<'a>(pairs: &[(&'a str, bool)]) -> Vec<&'a str> {
        pairs.iter().map(|&(id, _)| id).collect()
    }

    fn eval(expr: &str, pairs: &[(&str, bool)]) -> bool {
        evaluate(expr, &results(pairs), &ids(pairs), &HashMap::new(), &HashMap::new(), "").unwrap()
    }

    // --- backward-compat bare task booleans ---

    #[test]
    fn bare_id_true_when_succeeded() {
        assert!(eval("build", &[("build", true)]));
    }

    #[test]
    fn bare_id_false_when_failed() {
        assert!(!eval("build", &[("build", false)]));
    }

    #[test]
    fn bare_id_false_when_not_run() {
        assert!(!eval("build", &[]));
    }

    #[test]
    fn not_inverts() {
        assert!(eval("NOT build", &[("build", false)]));
        assert!(!eval("NOT build", &[("build", true)]));
    }

    #[test]
    fn and_expression() {
        assert!(eval("a AND b",  &[("a", true),  ("b", true)]));
        assert!(!eval("a AND b", &[("a", true),  ("b", false)]));
        assert!(!eval("a AND b", &[("a", false), ("b", true)]));
    }

    #[test]
    fn or_expression() {
        assert!(eval("a OR b",  &[("a", false), ("b", true)]));
        assert!(!eval("a OR b", &[("a", false), ("b", false)]));
    }

    #[test]
    fn complex_parens() {
        assert!(eval("(a AND b) OR c",  &[("a", true),  ("b", false), ("c", true)]));
        assert!(!eval("(a AND b) OR c", &[("a", true),  ("b", false), ("c", false)]));
    }

    // --- tasks.* dot notation ---

    #[test]
    fn tasks_success_field() {
        assert!(eval("tasks.build.success",  &[("build", true)]));
        assert!(!eval("tasks.build.success", &[("build", false)]));
    }

    #[test]
    fn tasks_exit_code_comparison() {
        let mut rs = results(&[("build", false)]);
        rs.get_mut("build").unwrap().exit_code = 42;
        assert!(evaluate("tasks.build.exit_code == 42", &rs, &["build"], &HashMap::new(), &HashMap::new(), "").unwrap());
        assert!(!evaluate("tasks.build.exit_code == 0", &rs, &["build"], &HashMap::new(), &HashMap::new(), "").unwrap());
    }

    #[test]
    fn tasks_stdout_string_eq() {
        let mut rs = results(&[("price", true)]);
        rs.get_mut("price").unwrap().stdout = "150\n".into();
        assert!(evaluate("tasks.price.stdout == \"150\"", &rs, &["price"], &HashMap::new(), &HashMap::new(), "").unwrap());
    }

    // --- trigger.* ---

    #[test]
    fn trigger_field_eq() {
        let mut params = HashMap::new();
        params.insert("event_id".into(), "".into());
        assert!(evaluate("trigger.event_id == \"\"", &HashMap::new(), &[], &params, &HashMap::new(), "").unwrap());
        params.insert("event_id".into(), "$abc:server".into());
        assert!(!evaluate("trigger.event_id == \"\"", &HashMap::new(), &[], &params, &HashMap::new(), "").unwrap());
    }

    // --- env.* with JSON list membership ---

    #[test]
    fn env_list_membership() {
        std::env::set_var("VORTEX_TEST_CONTACTS", r#"["@alice:server","@bob:server"]"#);
        let mut params = HashMap::new();
        params.insert("sender".into(), "@alice:server".into());
        assert!(evaluate("trigger.sender in env.VORTEX_TEST_CONTACTS", &HashMap::new(), &[], &params, &HashMap::new(), "").unwrap());
        params.insert("sender".into(), "@unknown:server".into());
        assert!(!evaluate("trigger.sender in env.VORTEX_TEST_CONTACTS", &HashMap::new(), &[], &params, &HashMap::new(), "").unwrap());
        std::env::remove_var("VORTEX_TEST_CONTACTS");
    }

    // --- globals ---

    #[test]
    fn globals_field_eq() {
        let mut globals = HashMap::new();
        globals.insert("mode".into(), "active".into());
        assert!(evaluate("globals.mode == \"active\"", &HashMap::new(), &[], &HashMap::new(), &globals, "").unwrap());
    }

    // --- combined ---

    #[test]
    fn combined_task_and_trigger() {
        let rs = results(&[("check", true)]);
        let mut params = HashMap::new();
        params.insert("event_id".into(), "".into());
        assert!(evaluate(
            "check AND trigger.event_id == \"\"",
            &rs, &["check"], &params, &HashMap::new(), ""
        ).unwrap());
    }
}
