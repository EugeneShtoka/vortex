use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use cel_interpreter::{Context, ExecutionError, Program, Value};

use crate::engine::TaskResult;
use vortex_core::TaskStatus;

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
/// | `tasks.<id>.output`          | any     | `tasks.fetch.output.filter(e, e.ok)`       |
/// | `trigger.<key>`              | string  | `trigger.sender == "@alice:server"`        |
/// | `env.<KEY>`                  | any     | `trigger.sender in env.MATRIX_CONTACTS`    |
/// | `globals.<key>`              | any     | `globals.room_map['!r:s'] == 'friends'`    |
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

/// Like `evaluate` but with additional variables injected into the CEL context.
/// Used by `foreach` to bind `item` and `acc` per-iteration.
pub fn evaluate_with_extras(
    expr: &str,
    results: &HashMap<String, TaskResult>,
    all_ids: &[&str],
    trigger_params: &HashMap<String, String>,
    globals: &HashMap<String, String>,
    correlation_id: &str,
    extras: &[(&str, Value)],
) -> Result<bool> {
    let normalized = normalize(expr);
    let program = Program::compile(&normalized)
        .map_err(|e| anyhow::anyhow!("Gate compile error in '{expr}': {e}"))?;
    let mut ctx = build_context(results, all_ids, trigger_params, globals, correlation_id)?;
    for (name, val) in extras {
        ctx.add_variable_from_value(*name, val.clone());
    }
    match program.execute(&ctx) {
        Ok(Value::Bool(b)) => Ok(b),
        Ok(other) => Err(anyhow::anyhow!("Gate expression '{expr}' returned {other:?}, expected bool")),
        Err(e) if e.to_string().contains("Undeclared reference") => Ok(false),
        Err(e) => Err(anyhow::anyhow!("Gate eval error in '{expr}': {e}")),
    }
}

/// Like `evaluate_value` but with additional variables injected into the CEL context.
pub fn evaluate_value_with_extras(
    expr: &str,
    results: &HashMap<String, TaskResult>,
    all_ids: &[&str],
    trigger_params: &HashMap<String, String>,
    globals: &HashMap<String, String>,
    correlation_id: &str,
    extras: &[(&str, Value)],
) -> Result<Value> {
    let normalized = normalize(expr);
    let program = Program::compile(&normalized)
        .map_err(|e| anyhow::anyhow!("Eval compile error in '{expr}': {e}"))?;
    let mut ctx = build_context(results, all_ids, trigger_params, globals, correlation_id)?;
    for (name, val) in extras {
        ctx.add_variable_from_value(*name, val.clone());
    }
    program.execute(&ctx).map_err(|e| anyhow::anyhow!("Eval error in '{expr}': {e}"))
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

/// Convert a `TaskResult` to a CEL value with the same shape as `tasks.<id>` entries in context.
/// Used to bind `self` when evaluating `abort_if` expressions.
pub fn task_result_to_cel(result: &TaskResult) -> Result<Value> {
    let json = serde_json::json!({
        "success":   result.is_success(),
        "stdout":    result.stdout.trim(),
        "stderr":    result.stderr.trim(),
        "exit_code": result.exit_code,
        "output":    result.output,
    });
    to_cel(&json)
}

fn build_context<'a>(
    results: &'a HashMap<String, TaskResult>,
    all_ids: &'a [&'a str],
    trigger_params: &'a HashMap<String, String>,
    globals: &'a HashMap<String, String>,
    correlation_id: &'a str,
) -> Result<Context<'a>> {
    let mut ctx = Context::default();

    // tasks.{id}.{success, stdout, stderr, exit_code, output} — all task IDs present; unrun → defaults
    let tasks_json: serde_json::Value = {
        let mut m = serde_json::Map::new();
        for &id in all_ids {
            let entry = match results.get(id) {
                Some(r) => serde_json::json!({
                    "success":   r.is_success(),
                    "stdout":    r.stdout.trim(),
                    "stderr":    r.stderr.trim(),
                    "exit_code": r.exit_code,
                    "output":    r.output,
                }),
                None => serde_json::json!({
                    "success":   false,
                    "stdout":    "",
                    "stderr":    "",
                    "exit_code": -1,
                    "output":    null,
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

    // globals.{key} — JSON-parsed where possible (same as env)
    let globals_json: serde_json::Value = serde_json::Value::Object(
        globals
            .iter()
            .map(|(k, v)| {
                let val = serde_json::from_str(v).unwrap_or(serde_json::Value::String(v.clone()));
                (k.clone(), val)
            })
            .collect(),
    );
    ctx.add_variable_from_value("globals", to_cel(&globals_json)?);

    // correlation_id
    ctx.add_variable_from_value("correlation_id", to_cel(correlation_id)?);

    // Custom functions
    ctx.add_function("toMap", cel_to_map);
    ctx.add_function("merge", cel_merge);
    ctx.add_function("localpart", cel_localpart);

    // backward compat: bare task-ID booleans for `when = "task_id"` style
    for &id in all_ids {
        if is_cel_ident(id) {
            let success = results.get(id).map_or(false, |r| r.is_success());
            if let Ok(v) = to_cel(&success) {
                ctx.add_variable_from_value(id, v);
            }
        }
    }

    Ok(ctx)
}

/// `toMap(list, keyField, value)` — builds `{item[keyField]: value}` for each map in `list`.
fn cel_to_map(list: Arc<Vec<Value>>, key_field: Arc<String>, val: Arc<String>) -> Result<Value, ExecutionError> {
    let mut obj = serde_json::Map::new();
    for item in list.iter() {
        if let Value::Map(m) = item {
            for (k, v) in m.map.iter() {
                if k.to_string() == *key_field.as_ref() {
                    if let Value::String(s) = v {
                        obj.insert(s.as_ref().clone(), serde_json::Value::String(val.as_ref().clone()));
                    }
                    break;
                }
            }
        }
    }
    cel_interpreter::to_value(&serde_json::Value::Object(obj))
        .map_err(|e| ExecutionError::function_error("toMap", e))
}

/// `localpart(s)` — extracts the local part of a Matrix ID: `@alice:server` → `alice`.
fn cel_localpart(s: Arc<String>) -> Result<Value, ExecutionError> {
    let local = s.as_str()
        .strip_prefix('@')
        .and_then(|p| p.split(':').next())
        .unwrap_or(s.as_str());
    Ok(Value::String(local.to_string().into()))
}

/// `merge(a, b)` — merges two CEL maps (b wins on conflict) or concatenates two lists.
fn cel_merge(a: Value, b: Value) -> Result<Value, ExecutionError> {
    match (a, b) {
        (Value::Map(m1), Value::Map(m2)) => {
            let mut merged: serde_json::Map<String, serde_json::Value> =
                m1.map.iter().map(|(k, v)| (k.to_string(), cel_val_to_json(v))).collect();
            merged.extend(m2.map.iter().map(|(k, v)| (k.to_string(), cel_val_to_json(v))));
            cel_interpreter::to_value(&serde_json::Value::Object(merged))
                .map_err(|e| ExecutionError::function_error("merge", e))
        }
        (Value::List(l1), Value::List(l2)) => {
            let combined: Vec<serde_json::Value> =
                l1.iter().chain(l2.iter()).map(cel_val_to_json).collect();
            cel_interpreter::to_value(&serde_json::Value::Array(combined))
                .map_err(|e| ExecutionError::function_error("merge", e))
        }
        _ => Err(ExecutionError::function_error("merge", "arguments must both be maps or both be lists")),
    }
}

fn cel_val_to_json(val: &Value) -> serde_json::Value {
    match val {
        Value::Int(i)    => serde_json::Value::Number((*i).into()),
        Value::UInt(u)   => serde_json::Value::Number((*u).into()),
        Value::Float(f)  => serde_json::json!(*f),
        Value::String(s) => serde_json::Value::String(s.as_ref().clone()),
        Value::Bool(b)   => serde_json::Value::Bool(*b),
        Value::Null      => serde_json::Value::Null,
        Value::List(l)   => serde_json::Value::Array(l.iter().map(cel_val_to_json).collect()),
        Value::Map(m)    => serde_json::Value::Object(
            m.map.iter().map(|(k, v)| (k.to_string(), cel_val_to_json(v))).collect()
        ),
        _ => serde_json::Value::Null,
    }
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
            exit_code: if success { 0 } else { 1 },
            status: if success { TaskStatus::Success } else { TaskStatus::Failed },
            output: None, http_status: None, response: None,
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

    // --- tasks.*.output (Bug 1 fix) ---

    #[test]
    fn tasks_output_map_accessible() {
        let mut rs = results(&[("fetch", true)]);
        rs.get_mut("fetch").unwrap().output = Some(serde_json::json!({"code": 200, "body": "ok"}));
        assert!(evaluate("tasks.fetch.output.code == 200", &rs, &["fetch"], &HashMap::new(), &HashMap::new(), "").unwrap());
    }

    #[test]
    fn tasks_output_list_size() {
        let mut rs = results(&[("fetch", true)]);
        rs.get_mut("fetch").unwrap().output = Some(serde_json::json!([
            {"type": "m.space.child", "state_key": "!room1:s"},
            {"type": "m.room.member", "state_key": "@alice:s"},
        ]));
        assert!(evaluate("tasks.fetch.output.size() == 2", &rs, &["fetch"], &HashMap::new(), &HashMap::new(), "").unwrap());
    }

    #[test]
    fn tasks_output_null_when_unset() {
        let rs = results(&[("fetch", true)]);
        assert!(evaluate("tasks.fetch.output == null", &rs, &["fetch"], &HashMap::new(), &HashMap::new(), "").unwrap());
    }

    // --- globals JSON-parsed ---

    #[test]
    fn globals_json_list_membership() {
        let mut globals = HashMap::new();
        globals.insert("rooms".into(), r#"["!room1:server","!room2:server"]"#.into());
        assert!(evaluate(r#"'!room1:server' in globals.rooms"#, &HashMap::new(), &[], &HashMap::new(), &globals, "").unwrap());
        assert!(!evaluate(r#"'!unknown:server' in globals.rooms"#, &HashMap::new(), &[], &HashMap::new(), &globals, "").unwrap());
    }

    #[test]
    fn globals_json_map_index() {
        let mut globals = HashMap::new();
        globals.insert("space_map".into(), r#"{"!room1:server":"friends","!room2:server":"work"}"#.into());
        assert!(evaluate(r#"globals.space_map['!room1:server'] == 'friends'"#, &HashMap::new(), &[], &HashMap::new(), &globals, "").unwrap());
    }

    // --- toMap custom function ---

    #[test]
    fn to_map_builds_reverse_map() {
        let mut rs = results(&[("fetch", true)]);
        rs.get_mut("fetch").unwrap().output = Some(serde_json::json!([
            {"type": "m.space.child", "state_key": "!room1:s"},
            {"type": "m.space.child", "state_key": "!room2:s"},
            {"type": "m.room.member", "state_key": "@alice:s"},
        ]));
        assert!(evaluate(
            r#"toMap(tasks.fetch.output.filter(e, e.type == 'm.space.child'), 'state_key', 'friends')['!room1:s'] == 'friends'"#,
            &rs, &["fetch"], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    #[test]
    fn to_map_empty_list_gives_empty_map() {
        assert!(evaluate(
            "toMap([], 'key', 'val').size() == 0",
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    // --- merge custom function ---

    #[test]
    fn merge_combines_two_maps() {
        assert!(evaluate(
            r#"merge({'a': '1'}, {'b': '2'})['a'] == '1' && merge({'a': '1'}, {'b': '2'})['b'] == '2'"#,
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    #[test]
    fn merge_second_wins_on_conflict() {
        assert!(evaluate(
            r#"merge({'a': 'old'}, {'a': 'new'})['a'] == 'new'"#,
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    #[test]
    fn merge_with_empty_map() {
        assert!(evaluate(
            r#"merge({}, {'a': '1'})['a'] == '1'"#,
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    #[test]
    fn merge_concatenates_two_lists() {
        assert!(evaluate(
            "merge(['a', 'b'], ['c']).size() == 3",
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    #[test]
    fn merge_list_preserves_order() {
        assert!(evaluate(
            "merge(['x'], ['y'])[0] == 'x' && merge(['x'], ['y'])[1] == 'y'",
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
    }

    #[test]
    fn merge_empty_list_with_list() {
        assert!(evaluate(
            "merge([], ['a'])[0] == 'a'",
            &HashMap::new(), &[], &HashMap::new(), &HashMap::new(), ""
        ).unwrap());
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
