Sprint 1: The Core Engine & Local TriggerGoal: A daemon that listens on a Unix Socket and executes a linear DAG.Project Setup: Initialize a workspace with tokio (async), serde (config), and custom DAG engine.TOML Parser: Create a parser that maps vortex.toml into workflow/task structures.Unix Listener: Implement a tokio::net::UnixListener that accepts a JSON trigger.Basic Executor: Link the listener to the engine. When a trigger arrives, the engine builds and runs the DAG.Logging: Implement tracing to capture stdout/stderr of each task.STATUS: ✅ Done

Sprint 2: Logic Gates & Variable ContextGoal: Support complex branching and data passing.The Gatekeeper: Integrate evalexpr. Evaluate the `when` boolean expression against results of prior tasks (AND/OR/NOT keywords normalized).Variable Injection: Use handlebars (no_escape mode). Before a task executes, replace {{variables}} in the exec string with data from: env, tasks.*.stdout/stderr/success/exit_code, globals.Global Store: Setup rusqlite (bundled). Implement a "Global Context" that loads from SQLite at the start of every workflow run.STATUS: ✅ Done

Sprint 3: The Network & Security LayerGoal: Enable remote triggers and secure communication.Unified Server: Use axum 0.8 to host an HTTP server on a configurable port.Auth: Bearer token validated on /trigger (in-handler, to preserve TriggerReceived→TriggerRejected event ordering) and on /ws (Tower route-level middleware, because WebSocketUpgrade extractor runs before handler body).WebSocket Telemetry: /ws route with broadcast channel. All events streamed to connected observers.Trigger event lifecycle: TriggerReceived → TriggerAccepted → WorkflowStarted → ... → WorkflowFinished, or TriggerReceived → TriggerRejected.STATUS: ✅ Done

Sprint 4: Trigger ParametersGoal: Pass arbitrary key/value data from the trigger into task exec strings.TriggerRequest gains #[serde(default)] params: HashMap<String, String>. Engine gains trigger_params field via .with_params() builder. template::render() adds a trigger namespace to Handlebars context. Missing keys render empty (non-strict). Params also in TriggerReceived/TriggerAccepted events.STATUS: ✅ Done

Sprint 5: TUI MVPGoal: Visual real-time observer.New workspace crate vortex-tui; shared vortex-core crate for Event type. Ratatui two-pane UI: runs list + task detail. Connects via WebSocket. Reconnect on disconnect.STATUS: ✅ Done

Sprint 6: Run HistoryGoal: Persistent run history in SQLite + REST history API.Timestamps (now_ms()) in all events. runs + task_results tables in SQLite. GET /runs + GET /runs/{id}. TuiConfig loaded from vortex.toml [tui] section. TUI pre-fetches history on startup.STATUS: ✅ Done

Sprint 7: DAG Graph ViewGoal: Visual workflow graph in TUI.GET /workflows/{name}/config endpoint. graph.rs: DependencyGraph::from_config() with memoized depth. 'g' modal in TUI showing tasks in depth order with when expressions and run outcome symbols.STATUS: ✅ Done

Sprint 8: Multi-source TUIGoal: Connect TUI to multiple vortexd instances simultaneously.[[tui.sources]] config (name/url/token/history_limit per source). SourceState + App refactor. Tab bar with per-source connection indicators. Tab/Shift+Tab switching. Per-source WS reconnect with backoff. Legacy [tui] single-source block still supported.STATUS: ✅ Done

Sprint 9: Processor EndpointGoal: Synchronous request/response endpoint for integrations (e.g. Matrix bot via mx-proxy).POST /process/{workflow}: unauthenticated, runs workflow synchronously, returns last task stdout. Security via bind address (127.0.0.1 only). Trigger params from JSON body, injected as {{trigger.<key>}} and as VORTEX_TRIGGER_PARAMS env var in subprocesses. Homelab deployment: vortexd on NixOS home-lab, integrated with mx-proxy for Matrix message processing.STATUS: ✅ Done

Sprint 10: API CleanupGoal: Consistent, RESTful API surface and improved local IPC.POST /trigger/{workflow}: workflow moved from request body into the URL path; body is now a flat params map (same shape as /execute). POST /process/{workflow} → POST /execute/{workflow}: renamed for clarity. UDS Ok response gains output: Option<String> (last successful task stdout) so local callers (e.g. mx-proxy over UDS) get the result without HTTP. Homelab: process-message workflow renamed to matrix-message-handle (object-action convention).STATUS: ✅ Done

Sprint 11: UDS Protocol UpgradeGoal: Make UDS a proper persistent request-response transport with traceability.Persistent connection: handle_connection loops until client disconnects instead of one-shot read/close. Each connection handles unlimited sequential requests.Request id echo: TriggerRequest gains optional id: Option<String>; all Response variants include id echoed back. Enables clients (e.g. mx-proxy baseTransport) to pipeline requests and correlate responses by id.output as JSON value: Response::Ok { output: Option<serde_json::Value> } — task stdout is auto-parsed as JSON; if valid JSON it appears as a proper object in the response rather than a quoted string; falls back to Value::String for non-JSON stdout.UDS semantics: always synchronous (execute model) — client always receives the workflow result. HTTP keeps both /trigger (async, 202) and /execute (sync). No trigger/execute distinction on UDS.Listener refactor: handle_connection (I/O loop) → handle_request (event lifecycle) → execute_workflow (engine) + last_output (result extraction). engine::run → run_task extracted (owns shell execution + per-task store writes + events). spawn_workflow extracted from server::trigger_workflow. extractASMessageData / extractCSMessageData extracted in mx-proxy.Test DB fix: all test helpers switched from ":memory:" to unique temp file paths (rusqlite::Connection is !Sync so run_task must open its own connection; two :memory: opens create separate isolated DBs causing FK failures).STATUS: ✅ Done

Sprint 4: The Observer (TUI) — BRAINSTORMING
Goal: Build the visual command center.

--- SPLIT INTO TWO HALVES ---

Sprint 4a: Live TUI (self-contained, no schema changes)
Goal: Connect to a running vortexd and observe it in real time.

Connection Manager:
  - New workspace crate: vortex-tui (binary: vortex)
  - Dial either unix://path.sock or ws://host:port/ws
  - Reconnect on disconnect with backoff

Ratatui UI Layout:
  - Sidebar: list of configured workflows (read from /config endpoint — needs new daemon endpoint)
  - Main View: task tree for the active run — TaskStarted/TaskFinished/TaskSkipped events drive state
  - Log View: scrollable pane with real-time task output (stdout/stderr via events or streamed separately)
  - Status bar: connection state, run_id, duration counter

Multi-Source Switcher:
  - State machine: disconnect from one WebSocket, connect to another
  - Keyboard shortcut to cycle between configured sources

Sprint 4b: History & Analytics Dashboard (requires daemon changes)
Goal: Persistent run history, stats, dashboard view.

Daemon changes (vortexd):
  - Persist run history to SQLite: runs table (run_id, workflow, triggered_at, finished_at, success, trigger_source)
  - Persist task results to SQLite: task_results table (run_id, task_id, started_at, finished_at, exit_code, stdout, stderr)
  - New HTTP endpoint: GET /history?workflow=X&limit=N
  - New HTTP endpoint: GET /stats — aggregated counts and durations

Dashboard view (vortex-tui):
  - Runs count per workflow
  - Success/failure rate per workflow
  - Average and p95 duration per workflow
  - Duration breakdown by trigger source (HTTP vs UDS)
  - Sparkline of recent run durations

Open questions / brainstorm:
  - Should stats be pre-aggregated by daemon on write, or computed by TUI on read?
  - Should /history stream via WebSocket (live updates) or be poll-based HTTP?
  - Duration by trigger type requires knowing trigger_source at run time — listener.rs and server.rs would tag events with source="uds"|"http"
  - Log View: pull stdout/stderr from SQLite after the fact, or stream them as events in real time? Real-time requires adding stdout/stderr fields to TaskFinished event.

Technical Debt to Watch Out For:
  Zombies: Ensure the Engine properly reaps child processes if a workflow is cancelled or the daemon restarts.
  Circular Dependencies: Validation step during TOML loading to detect infinite loops in the task graph.
  Backpressure: If a trigger fires 100 times per second, the broadcast channel has a capacity limit (currently 256). Add queuing or drop-oldest strategy.
  Compound gate topo sort: when = "a AND b" only creates a dep edge on "a". Full expression analysis needed to handle multi-dep compound gates correctly.

---

Sprint 15 — Trigger Tracking
Goal: Persist every incoming trigger in SQLite with full lifecycle status, giving complete visibility into what hit the system, where it came from, and why it succeeded or was rejected.

## New `triggers` table

```sql
CREATE TABLE IF NOT EXISTS triggers (
    id               TEXT PRIMARY KEY,   -- same as run_id generated on receipt
    workflow         TEXT NOT NULL,      -- target workflow; empty string for unknown_workflow rejection
    status           TEXT NOT NULL,      -- see TriggerStatus below
    params           TEXT NOT NULL,      -- JSON trigger payload
    source           TEXT NOT NULL,      -- "http" | "ntfy" | "cron" | "peer"
    rejection_cause  TEXT,               -- "unauthorized" | "workflow_not_found" when rejected
    remote_addr      TEXT,               -- client IP for http source; null otherwise
    received_at      INTEGER NOT NULL,   -- unix ms when trigger hit the system
    finished_at      INTEGER             -- unix ms when status became terminal
);
```

## TriggerStatus enum (vortex-core)

```rust
pub enum TriggerStatus {
    Received,   // trigger hit the system, auth not yet checked
    Accepted,   // auth passed, workflow found, queued for execution
    Rejected,   // denied; see rejection_cause
    Running,    // workflow engine started
    Finished,   // workflow completed (check runs table for outcome)
}
```

Happy path:   Received → Accepted → Running → Finished
Rejected path: Received → Rejected

`Finished` is terminal regardless of whether the workflow succeeded or failed — the run outcome lives in the `runs` table, not here. Join on `triggers.id = runs.id` to get both.

## Store methods

- `insert_trigger(id, workflow, params, source, remote_addr, received_at)` — inserts row with status `Received`
- `update_trigger_status(id, status, rejection_cause, finished_at)` — transitions status; sets `rejection_cause` on `Rejected`, `finished_at` on `Rejected`/`Finished`

## Wiring (event → store call)

| Event            | Store call                                                  |
|------------------|-------------------------------------------------------------|
| TriggerReceived  | insert_trigger (source + remote_addr injected here)         |
| TriggerAccepted  | update_trigger_status(Accepted)                             |
| TriggerRejected  | update_trigger_status(Rejected, rejection_cause, finished_at)|
| WorkflowStarted  | update_trigger_status(Running)                              |
| WorkflowFinished | update_trigger_status(Finished, finished_at)                |

Server.rs and listener.rs emit TriggerReceived with `source` tag already; remote_addr extracted from axum ConnectInfo for http source.

## Cleanup

- Remove dead `reject_run()` from store.rs and its test
- Remove `rejection: Option<String>` column from `runs` table and `RunRow` struct
- Update `RunRow` status comment: `"running"|"success"|"failed"` only (rejected is now triggers-only)

## New API endpoints

- `GET /triggers?limit=N&offset=N` — list triggers newest-first, same shape as /runs
- `GET /triggers/{id}` — single trigger detail

## TUI

- Run list: show source badge on each run row (h/n/c/p icon in a narrow column)
- Future: dedicated triggers view showing rejected triggers (currently invisible in history)

## Tests (TDD)

- Store: insert_trigger, update_trigger_status for each status transition
- Server: trigger row persisted with Accepted status on valid POST /trigger/{workflow}
- Server: trigger row persisted with Rejected status + correct rejection_cause on bad auth / unknown workflow
- Engine: trigger transitions to Running on WorkflowStarted, Finished on WorkflowFinished
