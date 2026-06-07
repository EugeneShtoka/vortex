# Vortex

A headless automation orchestrator. Define pipelines in TOML, trigger them over a Unix socket or HTTP, observe them live in a terminal UI — without coupling your secrets or network topology to the tool.

## Philosophy

- **Passive presence** — a lightweight daemon consuming near-zero resources until signaled.
- **Infrastructure agnostic** — treats all connections as plain streams; works the same over localhost, LAN, Tailscale, or an SSH tunnel.
- **Visible & verifiable** — every task state is observable in real time; secrets never live in `vortex.toml`.

## Workspace

| Crate | Binary | Purpose |
|---|---|---|
| `vortex-core` | — | Shared `Event` type (serde only, no heavy deps) |
| `vortexd` | `vortexd` | Daemon — DAG engine, HTTP API, WebSocket telemetry, SQLite history |
| `vortex-tui` | `vortex-tui` | Terminal observer — live runs, history, DAG graph view |

## Architecture

```
┌─────────────────────────────────────────────────┐
│  vortexd  (daemon)                              │
│                                                 │
│  Trigger ──▶ Engine (DAG + Gates + Templates)   │
│  UDS / HTTP       │                             │
│                   ├──▶ Event Bus (broadcast)     │
│                   └──▶ SQLite (globals + history)│
│                                                 │
│  HTTP API:                                      │
│    POST /trigger/{workflow}                     │
│    POST /execute/{workflow}  (synchronous)      │
│    GET  /runs  •  GET /runs/{id}                │
│    GET  /workflows/{name}/config                │
│    GET  /ws  (WebSocket event stream)           │
└─────────────────────────┬───────────────────────┘
                          │ WebSocket + REST
                          ▼
┌─────────────────────────────────────────────────┐
│  vortex-tui  (observer)                         │
│  • two-pane: runs list + task detail            │
│  • 'g' modal: DAG graph with when expressions   │
│  • pre-loads history on startup                 │
└─────────────────────────────────────────────────┘
```

## Configuration

```toml
[server]
unix_socket = "/run/user/1000/vortex.sock"
db_path     = "/var/lib/vortex/state.db"

[server.network]
enabled     = true
bind        = "0.0.0.0:9000"
auth_method = "env"           # "env" | "cmd" — never plain text
auth_key    = "VORTEX_TOKEN"

# Multi-source TUI (Sprint 8)
[tui]
history_limit = 10   # global default; per-source can override

[[tui.sources]]
name  = "local"
url   = "ws://localhost:9000/ws"
token = "mysecret"

[[tui.sources]]
name          = "prod"
url           = "wss://prod.example.com:9000/ws"
token         = "prodtoken"
history_limit = 25

# Legacy single-source (still supported)
# [tui]
# url           = "ws://localhost:9000/ws"
# token         = "mysecret"
# history_limit = 10

[workflows.process-message]
# correlation_id is a template expression evaluated against trigger params.
# Fallback chain: trigger.correlation_id → trigger.id → UUID.
correlation_id = "{{trigger.id}}"

tasks = [
  { id = "check_spam",  type = "spawn",  exe = "spam-filter", args = ["--json"] },
  { id = "check_voice", type = "shell",  exec = "is_voice.sh '{{trigger.type}}'",   when = "NOT check_spam" },
  { id = "stt",         type = "shell",  exec = "stt.sh '{{trigger.audio}}'",        when = "check_voice" },
  { id = "send_voice",  type = "shell",  exec = "send.sh '{{tasks.stt.stdout}}'",    when = "stt" },
  { id = "check_group", type = "shell",  exec = "is_group.sh '{{trigger.group}}'",   when = "NOT check_spam" },
  { id = "translate",   type = "shell",  exec = "translate.sh '{{trigger.msg}}'",    when = "check_group" },
  # response task: renders template directly, no subprocess.
  # Its output becomes the synchronous response body.
  { id = "reply",       type = "response",
    template = '{"id":"{{correlation_id}}","status":"ok","text":{{json tasks.translate.stdout}}}',
    when = "translate" },
]
```

### Task types

Every task requires a `type` field:

| `type`       | Required fields     | Description |
|---|---|---|
| `shell`      | `exec`              | Shell command string; templated, stdout captured |
| `spawn`      | `exe`, `args`       | Binary + args array (no shell); trigger params JSON piped to stdin and in `VORTEX_TRIGGER_PARAMS` |
| `response`   | `template`          | Renders template, no subprocess; output becomes the workflow response |
| `notify`     | `topic`, `message`  | Push notification via ntfy; optional `title`, `priority`, `tags`, `server`, `token` |
| `http`       | `url`               | HTTP request; optional `method`, `headers`, `body`; success = 2xx |
| `email`      | `to`, `subject`, `body` | Send email via SMTP; requires `[email]` config section; optional `cc` |
| `sleep`      | `duration`          | Pause execution; formats: `100ms`, `5s`, `2m` |
| `store_set`  | `set`               | Write one or more key/value pairs to the SQLite global store |
| `store_get`  | `get`               | Read a key from the SQLite global store; value lands in `{{tasks.<id>.stdout}}` |

Any task (including `shell`/`spawn`) can also carry `response_template` — a template rendered after the task succeeds, which becomes the workflow response. If more than one task produces a response, a warning is logged and the last one wins.

### Terminology

| Term | Meaning |
|---|---|
| **Trigger** | The incoming event that starts a workflow (UDS message or HTTP POST) |
| **Workflow** | A named pipeline — a list of tasks in `vortex.toml` |
| **Task** | An atomic unit of work within a workflow |
| **Gate** | A boolean `when` expression controlling whether a task runs |
| **Run** | A single workflow execution, identified by `run_id` |
| **Correlation ID** | A caller-provided ID threaded through the run; available as `{{correlation_id}}` |

### Gate expressions

| `when` value | Meaning |
|---|---|
| *(omitted)* | Always run |
| `task_id` | Run only if that task succeeded (exit 0) |
| `NOT task_id` | Run only if that task failed |
| `a AND b` | Run only if both succeeded |
| `a OR b` | Run if either succeeded |
| `(a AND b) OR c` | Full boolean logic |

### Template variables

`exec`, `response`, and template fields support `{{…}}` substitution (Handlebars):

| Variable | Value |
|---|---|
| `{{trigger.<key>}}` | Parameter from the trigger payload |
| `{{correlation_id}}` | Correlation ID for this run (see `correlation_id` on workflow) |
| `{{tasks.<id>.stdout}}` | Captured stdout of a completed task |
| `{{tasks.<id>.stderr}}` | Captured stderr |
| `{{tasks.<id>.success}}` | `true` / `false` |
| `{{tasks.<id>.exit_code}}` | Integer exit code |
| `{{env.<NAME>}}` | Environment variable |
| `{{globals.<key>}}` | Value from the SQLite global store |

Missing keys render as empty string (non-strict mode).

**Helpers:**

| Helper | Example | Output |
|---|---|---|
| `{{json value}}` | `{{json trigger.name}}` | `"Alice"` — JSON-serializes the value with quotes and escaping |

### Secret providers

The auth token is never stored in plain text. Supported sources:

| `auth_method` | Reads from |
|---|---|
| `env` | Environment variable named by `auth_key` |
| `cmd` | stdout of shell command named by `auth_key` (e.g. `bw get password vortex`) |

## Building

Requires Rust 1.78+.

```bash
cargo build --release
# vortexd:     target/release/vortexd
# vortex-tui:  target/release/vortex-tui
```

## Running the daemon

```bash
# Start (reads vortex.toml in current directory)
vortexd

# Or point at a specific config
vortexd /etc/vortex/vortex.toml
```

## Triggering workflows

```bash
# Via Unix socket (local — no auth, filesystem perms apply)
echo '{"workflow": "matrix-message-handle", "params": {"msg": "hello"}}' \
  | nc -U /run/user/1000/vortex.sock

# Via HTTP (remote — bearer auth required)
curl -X POST http://host:9000/trigger/matrix-message-handle \
  -H "Authorization: Bearer $VORTEX_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"msg": "hello", "type": "text"}'
```

HTTP response (202 Accepted):
```json
{"run_id": "550e8400-…", "workflow": "matrix-message-handle"}
```

UDS response — the rendered output of the last `response` task or `response_template` that succeeded:
```json
{"id": "mx-proxy-a3f2…", "status": "ok", "text": "hello"}
```

If no response task is configured, returns `{"id": "<correlation_id>"}`. If the workflow errors before any response is produced, returns `{"id": "…", "status": "error", "message": "…"}`.

## REST API

All endpoints except `/execute` require `Authorization: Bearer <token>`.

| Method | Path | Auth | Description |
|---|---|---|---|
| `POST` | `/trigger/{workflow}` | Bearer | Start a workflow run asynchronously → 202 `{run_id, workflow}` |
| `POST` | `/execute/{workflow}` | none | Run workflow synchronously; returns last task stdout. Bind to `127.0.0.1` only. |
| `GET` | `/runs` | Bearer | List runs (`?limit=50&offset=0`) |
| `GET` | `/runs/{run_id}` | Bearer | Full run detail with task results |
| `GET` | `/workflows/{name}/config` | Bearer | Workflow task graph (for TUI graph view) |
| `GET` | `/ws` | Bearer | WebSocket event stream |

### Synchronous execution

`POST /execute/{workflow}` runs a workflow inline and returns the last task's stdout as the response body. Designed for request/response integrations (e.g. a Matrix bot). Security comes from the bind address — use `127.0.0.1` only.

Trigger params are passed as a flat JSON object body and injected as `{{trigger.<key>}}` in exec strings and as the `VORTEX_TRIGGER_PARAMS` environment variable in subprocess shells.

```bash
curl -s -X POST http://127.0.0.1:9000/execute/matrix-message-handle \
  -H "Content-Type: application/json" \
  -d '{"msg": "hello", "room": "!abc:example.com"}'
```

## Live telemetry

Connect to `/ws` to receive a stream of newline-delimited JSON events:

```bash
websocat -H "Authorization: Bearer $VORTEX_TOKEN" ws://host:9000/ws
```

Event lifecycle:
```
TriggerReceived → TriggerAccepted → WorkflowStarted
                                      → TaskStarted → TaskFinished  (success/fail)
                                      → TaskSkipped
                                      → WorkflowFinished
TriggerReceived → TriggerRejected  (reason: "unauthorized" | "unknown_workflow")
```

Each event carries `run_id` (ties a run together), `timestamp` (unix ms), and — for `TriggerReceived`/`TriggerAccepted` — the trigger `params` map.

## Terminal observer (vortex-tui)

```bash
# Config from vortex.toml [tui] section
vortex-tui

# Or pass explicitly
vortex-tui --url ws://host:9000/ws --token $VORTEX_TOKEN

# Point at a different config file
vortex-tui --config /etc/vortex/vortex.toml
```

On startup, `vortex-tui` fetches the last `history_limit` runs from the REST API, then subscribes to the WebSocket for live updates. Both sources populate the same runs list with no live/historical distinction.

Multiple daemon sources can be configured via `[[tui.sources]]` — each gets its own tab with a connection indicator. `Tab`/`Shift+Tab` switches between sources. A legacy single `[tui]` block is still supported.

**Key bindings:**

| Key | Action |
|---|---|
| `j` / `↓` | Select next run |
| `k` / `↑` | Select previous run |
| `g` | Toggle DAG graph modal for selected run |
| `Tab` / `Shift+Tab` | Switch between daemon sources |
| `q` | Quit |

The DAG modal shows tasks in depth order (roots first), with their `when` expression printed below each name and a status symbol (✓ / ✗ / ▶ / ─) reflecting the run's actual outcome.

## Roadmap

| Sprint | Status | Deliverable |
|---|---|---|
| 1 — Core engine | ✅ Done | Daemon, UDS, DAG execution, basic gates |
| 2 — Logic & variables | ✅ Done | Boolean gates (`evalexpr`), `{{variable}}` injection, SQLite globals |
| 3 — Network & security | ✅ Done | HTTP server, bearer auth (`env`/`cmd`), WebSocket telemetry |
| 4 — Trigger parameters | ✅ Done | `{{trigger.<key>}}` in task exec strings |
| 5 — TUI MVP | ✅ Done | `vortex-tui` binary, live run observer (Ratatui) |
| 6 — Run history | ✅ Done | Timestamps in events, SQLite persistence, REST history API, TUI history |
| 7 — DAG graph view | ✅ Done | Workflow config endpoint, graph.rs, `g` modal with `when` expressions |
| 8 — Multi-source TUI | ✅ Done | `[[tui.sources]]` config, tab bar with connection indicators, `Tab`/`Shift+Tab` switching, per-source WS reconnect |
| 9 — Processor endpoint | ✅ Done | `POST /process/{workflow}` synchronous endpoint; `VORTEX_TRIGGER_PARAMS` env var; homelab deployment |
| 10 — API cleanup | ✅ Done | `POST /trigger/{workflow}` (workflow in path); `/process` → `/execute`; UDS response includes `output`; `matrix-message-handle` workflow rename |
| 11 — Explicit types + response protocol | ✅ Done | `type =` required on all tasks; `Response` task kind; `response_template` field; `correlation_id` on workflow; `{{json}}` helper; flat JSON UDS response |

## License

MIT — see [LICENSE](LICENSE).
