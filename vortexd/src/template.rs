use std::collections::HashMap;

use anyhow::Result;
use handlebars::{Context, Handlebars, Helper, HelperDef, HelperResult, Output, RenderContext};
use serde_json::{json, Map, Value};

use crate::engine::TaskResult;

struct JsonHelper;

impl HelperDef for JsonHelper {
    fn call<'reg: 'rc, 'rc>(
        &self,
        h: &Helper<'rc>,
        _: &'reg Handlebars<'reg>,
        _: &'rc Context,
        _: &mut RenderContext<'reg, 'rc>,
        out: &mut dyn Output,
    ) -> HelperResult {
        let param = h.param(0)
            .map(|v| v.value())
            .unwrap_or(&Value::Null);
        let s = serde_json::to_string(param)
            .unwrap_or_else(|_| "null".to_string());
        out.write(&s)?;
        Ok(())
    }
}

/// Render a Handlebars template with full workflow context:
///
/// - `{{tasks.<id>.stdout}}` — captured stdout of a completed task
/// - `{{tasks.<id>.stderr}}` — captured stderr
/// - `{{tasks.<id>.success}}` — `true` / `false`
/// - `{{tasks.<id>.exit_code}}` — integer exit code
/// - `{{trigger.<key>}}` — trigger param
/// - `{{globals.<key>}}` — value from the SQLite global store
/// - `{{env.<NAME>}}` — environment variable
/// - `{{correlation_id}}` — correlation ID for the current workflow run
/// - `{{json <value>}}` — serialize value as a JSON string (with quotes, escaped)
///
/// Missing keys render as empty string (non-strict mode).
pub fn render(
    template: &str,
    task_results: &HashMap<String, TaskResult>,
    globals: &HashMap<String, String>,
    trigger_params: &HashMap<String, String>,
    correlation_id: &str,
) -> Result<String> {
    let mut hb = Handlebars::new();
    hb.register_escape_fn(handlebars::no_escape);
    hb.register_helper("json", Box::new(JsonHelper));

    let mut tasks = Map::new();
    for (id, r) in task_results {
        tasks.insert(
            id.clone(),
            json!({
                "stdout":    r.stdout,
                "stderr":    r.stderr,
                "success":   r.success,
                "exit_code": r.exit_code,
                "output":    r.output,
                "status":    r.status,
            }),
        );
    }

    let env: Map<String, Value> =
        std::env::vars().map(|(k, v)| (k, Value::String(v))).collect();

    let globals_val: Map<String, Value> =
        globals.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect();

    let trigger_val: Map<String, Value> =
        trigger_params.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect();

    let ctx = json!({
        "tasks":          tasks,
        "env":            env,
        "globals":        globals_val,
        "trigger":        trigger_val,
        "correlation_id": correlation_id,
    });

    Ok(hb.render_template(template, &ctx)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(id: &str, stdout: &str, stderr: &str, success: bool, exit_code: i32) -> TaskResult {
        TaskResult { id: id.into(), stdout: stdout.into(), stderr: stderr.into(), exit_code, success, output: None, status: None, response: None }
    }

    fn results(items: &[TaskResult]) -> HashMap<String, TaskResult> {
        items.iter().map(|r| (r.id.clone(), r.clone())).collect()
    }

    fn no_globals() -> HashMap<String, String> { HashMap::new() }
    fn no_params() -> HashMap<String, String> { HashMap::new() }
    fn no_cid() -> &'static str { "" }

    // --- passthrough ---

    #[test]
    fn plain_string_unchanged() {
        assert_eq!(render("echo hello", &HashMap::new(), &no_globals(), &no_params(), no_cid()).unwrap(), "echo hello");
    }

    // --- task variables ---

    #[test]
    fn task_stdout_substituted() {
        let rs = results(&[result("step1", "hello world\n", "", true, 0)]);
        assert_eq!(
            render("echo {{tasks.step1.stdout}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap(),
            "echo hello world\n"
        );
    }

    #[test]
    fn task_stderr_substituted() {
        let rs = results(&[result("step1", "", "oops\n", false, 1)]);
        assert_eq!(
            render("cat <<< {{tasks.step1.stderr}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap(),
            "cat <<< oops\n"
        );
    }

    #[test]
    fn task_success_substituted() {
        let rs = results(&[result("build", "", "", true, 0)]);
        let out = render("status={{tasks.build.success}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "status=true");
    }

    #[test]
    fn task_exit_code_substituted() {
        let rs = results(&[result("build", "", "", false, 2)]);
        let out = render("code={{tasks.build.exit_code}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "code=2");
    }

    // --- env variables ---

    #[test]
    fn env_var_substituted() {
        std::env::set_var("VORTEX_TEST_VAR", "injected");
        let out = render("echo {{env.VORTEX_TEST_VAR}}", &HashMap::new(), &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "echo injected");
    }

    #[test]
    fn missing_env_var_renders_empty() {
        std::env::remove_var("VORTEX_DEFINITELY_MISSING_VAR");
        let out = render("echo {{env.VORTEX_DEFINITELY_MISSING_VAR}}", &HashMap::new(), &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "echo ");
    }

    // --- globals ---

    #[test]
    fn globals_substituted() {
        let mut g = HashMap::new();
        g.insert("deploy_count".into(), "42".into());
        let out = render("echo {{globals.deploy_count}}", &HashMap::new(), &g, &no_params(), no_cid()).unwrap();
        assert_eq!(out, "echo 42");
    }

    #[test]
    fn missing_global_renders_empty() {
        let out = render("echo {{globals.missing}}", &HashMap::new(), &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "echo ");
    }

    // --- shell-unsafe characters are NOT html-escaped ---

    #[test]
    fn ampersand_not_escaped() {
        let rs = results(&[result("step", "foo & bar", "", true, 0)]);
        let out = render("{{tasks.step.stdout}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "foo & bar");
    }

    // --- multiple substitutions ---

    #[test]
    fn multiple_variables_in_one_template() {
        let rs = results(&[result("build", "artifact.tar", "", true, 0)]);
        let mut g = HashMap::new();
        g.insert("dest".into(), "/tmp/deploy".into());
        let out = render("cp {{tasks.build.stdout}} {{globals.dest}}", &rs, &g, &HashMap::new(), no_cid()).unwrap();
        assert_eq!(out, "cp artifact.tar /tmp/deploy");
    }

    // --- trigger params ---

    #[test]
    fn trigger_param_substituted() {
        let mut p = HashMap::new();
        p.insert("msg".into(), "hello world".into());
        let out = render("echo {{trigger.msg}}", &HashMap::new(), &HashMap::new(), &p, no_cid()).unwrap();
        assert_eq!(out, "echo hello world");
    }

    #[test]
    fn missing_trigger_param_renders_empty() {
        let out = render("echo {{trigger.missing}}", &HashMap::new(), &HashMap::new(), &HashMap::new(), no_cid()).unwrap();
        assert_eq!(out, "echo ");
    }

    #[test]
    fn task_status_substituted() {
        let mut r = result("call", "", "", true, 200);
        r.status = Some(200);
        let rs = results(&[r]);
        let out = render("status={{tasks.call.status}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "status=200");
    }

    #[test]
    fn task_output_substituted_when_string() {
        let mut r = result("call", "", "", true, 0);
        r.output = Some(serde_json::Value::String("hello".into()));
        let rs = results(&[r]);
        let out = render("out={{tasks.call.output}}", &rs, &no_globals(), &no_params(), no_cid()).unwrap();
        assert_eq!(out, "out=hello");
    }

    #[test]
    fn trigger_param_combined_with_task_output() {
        let rs = results(&[result("step", "artifact.tar", "", true, 0)]);
        let mut p = HashMap::new();
        p.insert("dest".into(), "/srv/deploy".into());
        let out = render("cp {{tasks.step.stdout}} {{trigger.dest}}", &rs, &HashMap::new(), &p, no_cid()).unwrap();
        assert_eq!(out, "cp artifact.tar /srv/deploy");
    }

    // --- correlation_id ---

    #[test]
    fn correlation_id_substituted() {
        let out = render("id={{correlation_id}}", &HashMap::new(), &no_globals(), &no_params(), "req-42").unwrap();
        assert_eq!(out, "id=req-42");
    }

    #[test]
    fn missing_correlation_id_renders_empty() {
        let out = render("id={{correlation_id}}", &HashMap::new(), &no_globals(), &no_params(), "").unwrap();
        assert_eq!(out, "id=");
    }

    // --- json helper ---

    #[test]
    fn json_helper_escapes_string() {
        let mut p = HashMap::new();
        p.insert("text".into(), r#"hello "world""#.into());
        let out = render("{{json trigger.text}}", &HashMap::new(), &no_globals(), &p, no_cid()).unwrap();
        assert_eq!(out, r#""hello \"world\"""#);
    }

    #[test]
    fn json_helper_escapes_backslash() {
        let mut p = HashMap::new();
        p.insert("text".into(), r"back\slash".into());
        let out = render("{{json trigger.text}}", &HashMap::new(), &no_globals(), &p, no_cid()).unwrap();
        assert_eq!(out, r#""back\\slash""#);
    }

    #[test]
    fn json_helper_in_json_template() {
        let mut p = HashMap::new();
        p.insert("text".into(), r#"say "hi""#.into());
        p.insert("room".into(), "!abc:server".into());
        let out = render(
            r#"{"text":{{json trigger.text}},"room":{{json trigger.room}}}"#,
            &HashMap::new(), &no_globals(), &p, no_cid(),
        ).unwrap();
        assert_eq!(out, r#"{"text":"say \"hi\"","room":"!abc:server"}"#);
    }
}
