# TUI Observability Redesign

## Goal

Replace the current `[Workflows] [Runs] [Tasks]` layout with a fully configurable
multi-column TUI. Key improvements:

- Two navigation modes — **Trigger view (T)** and **Workflow view (W)**
- Configurable number of navigation columns (1–3) with relative widths
- A permanent **Detail column** always showing rich info for the selected element
- All triggers visible, including rejected ones
- Task detail shows config fields + gate evaluation, not just stdout/stderr
- Run detail shows globals diff (before → after)

---

## Navigation Model

Both modes share the same Run and Task data. Mode only determines the leftmost column.

**Trigger view (T)** — "what came in? what happened to each request?"
```
Triggers → Runs → Tasks | Detail
```

**Workflow view (W)** — "how is this workflow doing?"
```
Workflows → Runs → Tasks | Detail
```

A trigger starts a workflow run. One trigger → one run in practice, but the model
supports 1→N (future). Rejected triggers appear in Trigger view with no run.

---

## Configuration

```toml
[tui]
panels = 3        # navigation columns shown: 1, 2, or 3; detail is always shown
default_mode = "triggers"   # "triggers" | "workflows"

[tui.widths]
workflows = 1     # column 1 (triggers in T mode, workflows in W mode)
runs      = 1     # column 2
tasks     = 1     # column 3
detail    = 3     # detail panel — always present
```

Widths are relative ratios. `panels = 3` with `1/1/1/3` → each nav col ≈ 16.7%,
detail ≈ 50%. `panels = 2` omits the tasks column; navigation auto-descends into
tasks when a run is selected.

---

## Detail Panel Content (context-sensitive)

| Active focus     | Detail shows                                                                 |
|------------------|------------------------------------------------------------------------------|
| Trigger selected | trigger_id, source (http/uds), remote_addr, received_at, params, status, rejection reason |
| Workflow selected | task count, last run timestamp, definition summary; validation issues (Sprint 18) |
| Run selected     | globals diff (pre-run → post-run), task stats (N passed / N skipped / N failed), duration, trigger link |
| Task selected    | type, all config fields, `when` expr + evaluated result, `abort_if` + result, exit_code, duration, stdout/stderr |

---

## Keybindings

| Key         | Action                                      |
|-------------|---------------------------------------------|
| `T`         | Switch to Trigger view                      |
| `W`         | Switch to Workflow view                     |
| `Tab`       | Cycle daemon sources (unchanged)            |
| `j/k` / `↑↓` | Navigate in active column               |
| `l/→/Enter` | Drill right (focus next column)             |
| `h/←/Esc`  | Go back left                                |
| `g`         | DAG graph modal (unchanged)                 |
| `q`         | Quit                                        |

---

## Sprint 16 — Config + Two Navigation Modes + Trigger Column

**Goal**: Triggers visible in TUI; T/W toggle; configurable column widths.

### 16.1 TUI layout config — `vortex-tui/src/config.rs`

Add `TuiWidths` and `TuiLayout` structs:

```rust
pub struct TuiWidths {
    pub workflows: u16,  // col 1
    pub runs:      u16,  // col 2
    pub tasks:     u16,  // col 3
    pub detail:    u16,
}
impl Default for TuiWidths {
    fn default() -> Self { Self { workflows: 1, runs: 1, tasks: 1, detail: 3 } }
}

pub struct TuiLayout {
    pub panels:       u8,   // 1–3, default 3
    pub default_mode: ViewMode,
    pub widths:       TuiWidths,
}
```

`TuiSourceConfig` gains an optional `layout: TuiLayout` field (falls back to
default). Parse from `[tui.widths]` and `[tui] panels`.

### 16.2 App state — `vortex-tui/src/app.rs`

Add `ViewMode` enum and `TriggerEntry`:

```rust
pub enum ViewMode { Triggers, Workflows }

pub struct TriggerEntry {
    pub id:           String,
    pub workflow:     Option<String>,
    pub run_id:       Option<String>,     // links into runs map
    pub source:       String,             // "http" | "uds"
    pub status:       TriggerEntryStatus,
    pub rejection:    Option<String>,
    pub received_at:  u64,
}

pub enum TriggerEntryStatus { Running, Finished(bool), Rejected(String) }
```

Add to `SourceState`:
- `view_mode: ViewMode`
- `triggers: IndexMap<String, TriggerEntry>` (keyed by trigger_id / run_id)
- `selected_trigger: usize`

Update `Focus` enum:
```rust
pub enum Focus {
    TriggerList,    // T mode col 1
    WorkflowList,   // W mode col 1 (replaces current Workflows)
    Runs,
    Tasks,
    Detail,
}
```

Navigation methods for trigger mode: `select_next_trigger`, `select_prev_trigger`,
`sorted_triggers`, `selected_trigger_entry`, `runs_for_selected_trigger`.

`navigate_down/up/left/right` and `enter/escape_pane` updated to handle both modes.

### 16.3 Event handling for triggers

`handle()` in `SourceState`:

- `TriggerRejected { run_id, reason }` → insert `TriggerEntry` with status `Rejected`
- `WorkflowStarted { run_id, workflow }` → upsert trigger (set workflow, status Running,
  link run_id); existing `runs` insert unchanged
- `WorkflowFinished { run_id, success }` → update trigger status to `Finished(success)`

The `workflow_names()` and `runs_for_selected_workflow()` methods stay unchanged for W mode.

### 16.4 REST fetch on connect — `vortex-tui/src/ws.rs`

After `GET /runs` fetch, also `GET /triggers` → deserialize into `Vec<TriggerEntry>` →
`apply_triggers()`. The `/triggers` endpoint exists from Sprint 15.

`apply_triggers()` in `SourceState`: upsert all entries, then `clamp_selections()`.

### 16.5 UI render — `vortex-tui/src/ui.rs`

- `compute_constraints(layout, mode)` — returns `Vec<Constraint>` from ratio widths,
  omitting columns beyond `panels` count
- `render_col1(f, src, area)` — dispatches to `render_trigger_list` or `render_workflow_list`
- `render_trigger_list(f, src, area)`:
  - Status icon: `⊘` rejected (red), `●` running (yellow), `✓` success (green), `✗` failed (red)
  - Row: `{icon} {workflow_or_reason_dimmed}  {source_badge}  {age}`
  - Rejected triggers show reason in place of workflow name, dimmed
- Status bar hints updated for T/W mode and Detail focus

### Tests (TDD first)

- `trigger_entry_inserted_on_trigger_rejected`
- `trigger_entry_upserted_on_workflow_started`
- `trigger_entry_status_updated_on_workflow_finished`
- `apply_triggers_populates_from_rest_response`
- `view_mode_starts_at_configured_default`
- `focus_advances_through_trigger_hierarchy`
- `focus_advances_through_workflow_hierarchy`
- `runs_for_selected_trigger_returns_linked_run`
- `sorted_triggers_newest_first`

STATUS: ✅ Done

---

## Sprint 17 — Detail Panel

**Goal**: Replace task detail overlay with a permanent detail column; context-sensitive
content per selected element.

### 17.1 Detail panel infrastructure — `vortex-tui/src/ui.rs`

Replace `render_task_detail_overlay` with `render_detail(f, src, area)` dispatcher:

```
Focus::TriggerList | Detail (trigger selected) → render_trigger_detail
Focus::WorkflowList | Detail (workflow selected) → render_workflow_detail (stub)
Focus::Runs | Detail                             → render_run_detail
Focus::Tasks | Detail                            → render_task_detail
```

Detail panel is always rendered in the rightmost slot regardless of focus.

`render_trigger_detail(f, entry, area)`:
- trigger_id (short: last 8 chars, full on hover/scroll)
- source badge, remote_addr
- received_at formatted as datetime + age
- status with icon + color
- rejection reason (if rejected) in red
- params (if any) as key: value table

`render_workflow_detail(f, name, src, area)`:
- workflow name
- total runs, last run age, success rate (N/M)
- Validation issues section — stub "no issues detected" for now

`render_run_detail(f, run, globals_diff, area)`:
- duration, task stats (N passed / N skipped / N failed)
- globals diff table (see 17.2)
- link to trigger id (dimmed, for cross-reference)

`render_task_detail(f, task_id, task_summary, config_fields, area)`:
- type: `shell` / `http` / `notify` / `sleep` / `foreach`
- exec / url / etc (type-specific fields)
- `when: <expr>  → true ✓` or `→ false (skipped)` or `→ (no gate)`
- `abort_if: <expr>  → (not triggered)` or `→ true (aborted)`
- exit_code, duration
- stdout / stderr sections (same content as current overlay, now inline)

Remove `render_task_detail_overlay`. Remove the `Focus::TaskDetail` arm that triggered it.
`Focus::Detail` replaces it.

### 17.2 Globals diff — `vortex-tui/src/app.rs`

Add to `SourceState`:
```rust
pub globals_pre:  HashMap<String, serde_json::Value>,  // run_id → snapshot at WorkflowStarted
pub globals_post: HashMap<String, serde_json::Value>,  // run_id → snapshot at WorkflowFinished
```

In `handle()`:
- `WorkflowStarted { run_id }` → spawn async fetch `GET /globals` → store in `globals_pre[run_id]`
- `WorkflowFinished { run_id }` → spawn async fetch `GET /globals` → store in `globals_post[run_id]`

`diff_globals(pre, post)` → `Vec<GlobalsDiffEntry>`:
```rust
pub enum GlobalsDiffEntry {
    Changed { key: String, before: Value, after: Value },
    Added   { key: String, value: Value },
    Removed { key: String, value: Value },
}
```

`render_run_detail` displays:
- Changed: `key   old_val → new_val` (old dimmed, new bright)
- Added:   `+ key   val` in green
- Removed: `- key   val` in red dimmed
- Unchanged keys omitted (or shown fully dimmed if no changes exist)

### 17.3 Task config in detail

**Server change**: `GET /runs/{id}` `TaskSummary` response gains optional fields:
- `task_type: Option<String>`
- `task_exec: Option<String>`
- `task_when: Option<String>`
- `task_abort_if: Option<String>`

These come from the workflow config stored at engine load time. Alternatively TUI
can join: fetch `GET /workflows/{name}` config and look up by task_id.

**TUI**: `TaskSummary` struct in app.rs gains the same optional fields.
`apply_run_detail` maps them through. `render_task_detail` renders them.

Evaluated gate values (`when` result, `abort_if` result) require a new event field or
a separate endpoint — deferred to Sprint 18 if needed.

### Tests (TDD first)

- `globals_pre_stored_on_workflow_started`
- `globals_post_stored_on_workflow_finished`
- `diff_globals_detects_changed_key`
- `diff_globals_detects_added_key`
- `diff_globals_detects_removed_key`
- `diff_globals_empty_when_no_change`
- `detail_focus_navigable_from_tasks`
- `render_detail_does_not_panic_on_empty_state` (integration smoke)

STATUS: ✅ Done

---

## Sprint 18 — Workflow Validation

**Goal**: Static analysis of workflow definitions; health badges in workflow list.

### Server — new `vortexd/src/validator.rs`

```rust
pub struct ValidationIssue {
    pub severity: Severity,         // Error | Warning
    pub task_id:  Option<String>,
    pub code:     &'static str,     // e.g. "missing_dep", "cel_parse_error"
    pub message:  String,
}
pub enum Severity { Error, Warning }

pub fn validate(config: &WorkflowConfig) -> Vec<ValidationIssue>
```

Checks in order:
1. **Missing dependency**: task `when` references a task_id not in the workflow
2. **Circular dependency**: cycle detected in the DAG
3. **CEL parse error**: `when` / `abort_if` expression fails `cel_interpreter::Program::compile`
4. **Type-specific missing field**: shell task without `exec`, http task without `url`, foreach without `each`
5. **Unknown task reference in CEL** (heuristic): identifier in CEL expression is not a known task_id or known global key
6. **Undefined global** (heuristic): `globals.<key>` referenced but key never appears in any stored globals row

`GET /workflows/{name}` response gains `"issues": [...]`.
`GET /workflows` (list) response gains per-workflow `"issue_count": { "errors": N, "warnings": N }`.

### TUI changes

`WorkflowEntry` struct gains `issues: Vec<ValidationIssue>`.
Workflow list badge: `✓` (no issues), `⚠ N` (warnings only, yellow), `✗ N` (errors, red).
`render_workflow_detail` renders issues list with severity icon + message + task_id context.

### Tests (TDD first, server-side)

- `validate_detects_missing_dependency`
- `validate_detects_circular_dependency`
- `validate_detects_cel_parse_error`
- `validate_detects_missing_exec_for_shell_task`
- `validate_detects_missing_url_for_http_task`
- `validate_clean_workflow_returns_empty`

STATUS: ✅ Done

---

## Sprint 19 — Structured Log Storage

**Goal**: Persist per-task stdout/stderr as structured log lines in SQLite with
configurable retention; expose via REST; show in TUI task detail.

### Storage — `task_logs` table

```sql
CREATE TABLE task_logs (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id     TEXT    NOT NULL,
    task_id    TEXT    NOT NULL,
    stream     TEXT    NOT NULL,   -- "stdout" | "stderr"
    line       TEXT    NOT NULL,
    logged_at  INTEGER NOT NULL,   -- unix ms
    expires_at INTEGER             -- NULL = keep forever; unix ms
);
CREATE INDEX task_logs_run ON task_logs(run_id, task_id);
CREATE INDEX task_logs_expiry ON task_logs(expires_at);
```

`expires_at` is computed at write time — no need to join with workflow config at cleanup.

### Config — per-workflow retention

```toml
[workflows.deploy]
log_retention = 14   # days; overrides default of 7
# log_retention = 0   # disable logs for this workflow
# log_retention = -1  # keep forever
```

Default: 7 days (no global override needed).

### Store methods

- `insert_task_log(run_id, task_id, stream, line, logged_at, expires_at)`
- `get_task_logs(run_id, task_id) -> Vec<LogRow>`
- `get_workflow_logs(workflow, limit, offset) -> Vec<LogRow>`
- `cleanup_expired_logs() -> usize`

### Engine

After `run_task` gets a `TaskOutcome`, split stdout/stderr by `\n` and bulk-insert
into `task_logs` (skip if `log_retention = 0`). Compute `expires_at` from
`self.config.log_retention`.

### API endpoints

- `GET /runs/{id}/tasks/{task_id}/logs` — log lines for a specific task
- `GET /workflows/{name}/logs?limit=N&offset=N` — all log lines across all runs of a workflow, newest first

### Cleanup

Hourly background task in `scheduler.rs` calls `store.cleanup_expired_logs()`.

### TUI

- `LogRow` struct deserialized from both endpoints
- `task_logs: HashMap<(run_id, task_id), Vec<LogRow>>` on `SourceState`
- Task detail pane: fetch `GET /runs/{run_id}/tasks/{task_id}/logs` when task focused; show lines grouped stdout/stderr
- Workflow detail pane: link to `GET /workflows/{name}/logs` (future; not in this sprint)

### Tests (TDD first)

**Store:**
- `insert_and_retrieve_task_log`
- `get_task_logs_returns_in_order`
- `cleanup_expired_logs_deletes_expired`
- `cleanup_expired_logs_keeps_unexpired`
- `cleanup_expired_logs_keeps_no_limit`
- `get_workflow_logs_joins_across_runs`

**Config:**
- `log_retention_defaults_to_none`
- `log_retention_zero_disables_logs`

**Engine:**
- `task_logs_written_after_shell_task`
- `task_logs_not_written_when_retention_zero`
- `task_logs_expiry_set_from_retention_days`
- `task_logs_no_expiry_when_retention_minus_one`

**Server:**
- `get_task_logs_returns_lines`
- `get_task_logs_returns_404_for_unknown_run`
- `get_workflow_logs_returns_lines_across_runs`

STATUS: ✅ Done

---

## Open Questions

1. **Runtime panel count adjustment**: should `1`/`2`/`3` keys toggle panel count at
   runtime, or is config-only sufficient?
2. **Single-panel behavior**: when `panels = 1`, does navigating right auto-replace
   the single column content, or does the detail pane fill all remaining space?
3. **Globals fetch overhead**: 2 extra REST calls per run (pre + post). Acceptable for
   homelab scale; could be opt-out via config if needed.
4. **Trigger → Workflow cross-navigation**: from T mode, pressing `W` while a trigger
   is selected — should it jump to W mode pre-filtered to that workflow?
5. **Graph modal**: currently a separate `g` overlay. Should it move into the Detail
   panel when a workflow is selected, or stay as a full-screen modal?
