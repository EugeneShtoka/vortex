Vortex: The Headless Orchestrator
Philosophy: Decouple the trigger from the logic; keep the engine light, the observer rich, and the secrets sealed.

I. Core Philosophy
Vortex is a "central nervous system" for automation. It follows three guiding principles:

Passive Presence: A lightweight daemon consuming near-zero resources until signaled.

Infrastructure Agnostic: The tool handles the logic; the OS/Network handles the transport. It treats all remote connections as standard stream-based endpoints.

Visible & Verifiable: Automation is not a "black box." Every task state is observable, and every secret is protected.

II. System Architecture
1. The Engine (The Daemon)
A single-binary background process that manages pipeline lifecycles.

Trigger entry points:

Unix Domain Socket (UDS): Local-only, leveraging filesystem permissions. Persistent connection; multiple requests per connection, responses correlated by optional `id` field.

HTTP (axum): For remote triggers. `/trigger/{workflow}` (async, 202) and `/execute/{workflow}` (sync, returns stdout). Bearer token auth.

Execution Engine: Custom Kahn's topological sort for DAG ordering; sequential task execution with gate evaluation at each step.

Gatekeeper: A logic evaluator (evalexpr) that resolves boolean when strings to determine branching.

State Store: A local SQLite database for history, auditing, and persistent global variables.

2. The Observer (The TUI/CLI)
The user-facing interface used to trigger and monitor work.

Transport Drivers:

unix://: Local connection.

tcp://: Network connection (agnostic to VPNs, tunnels, or LAN).

Live Telemetry: A WebSocket client that receives real-time task updates.

Multi-Source Switcher: A UI component allowing the user to hot-swap between different engines (e.g., local-dev vs. vps-production).

III. Security & Secret Management
Vortex avoids "Secret Leakage" by ensuring no sensitive data is stored in the vortex.yaml file.

Secret Providers: The engine and observer can be configured to fetch tokens/keys from:

Environment Variables: VORTEX_AUTH_TOKEN.

External Command: Running a helper like bw get password vortex-token or pass vortex.

System Secret Stores: Integration with libsecret (Linux) or keychain (macOS).

Authentication: All network-based signals must provide a Bearer Token. If the token is missing or incorrect, the daemon drops the connection immediately to prevent brute-forcing.

IV. Refined Logic & Terminology
Trigger: The entry event (e.g., `POST /trigger/deploy-app`).

Workflow: A named pipeline of tasks (was "Signal" — renamed for clarity).

Task: The atomic unit of work (a shell command).

Gate: The boolean logic deciding if a task runs (`when = "a AND b"`). Evaluated by evalexpr; AND/OR/NOT keywords normalized to &&/||/!.

Run: A single workflow execution, identified by `run_id`.

Context: Ephemeral data passed between tasks in one run (`{{tasks.pull.stdout}}`).

Globals: Data that persists in SQLite across multiple runs (`{{globals.deploy_count}}`).

V. Configuration Schema (vortex.toml)
```toml
[server]
unix_socket = "/run/user/1000/vortex.sock"
db_path     = "/var/lib/vortex/state.db"

[server.network]
enabled     = true
bind        = "0.0.0.0:9000"
auth_method = "env"   # "env" | "cmd"
auth_key    = "VORTEX_TOKEN"

[workflows.sync-and-build]
tasks = [
  { id = "pull_code", exec = "git pull" },
  { id = "run_mx",    exec = "mxctl sync",                              when = "pull_code" },
  { id = "alert_fail", exec = "curl -X POST {{globals.SLACK_URL}} -d 'Fail'", when = "NOT run_mx" },
]
```
VI. Implementation Guidelines
1. The "Observer" Experience
The TUI should be built with Ratatui. It should provide:

Dashboard: A list of currently active pipelines.

Graph View: A visual tree of the selected pipeline, highlighting the active "Gate" and Task status.

History: A searchable table of previous runs pulled from the Engine's SQLite store.

2. Networking Implementation
Stream Agnosticism: Use tokio-util to treat both Unix Sockets and TCP Sockets as generic AsyncRead + AsyncWrite streams.

Heartbeats: Implement a simple PING/PONG over the WebSocket to detect dropped network connections immediately and trigger a "Reconnecting" UI state.

3. Variable Template Injection
Tasks should support dynamic variable injection using a library like Handlebars.

Local: {{tasks.pull_code.stdout}}

Global: {{globals.deploy_count}} (Fetched from the DB).

VII. Why this works
By moving the Network Layer to the OS level and the Secret Layer to specialized tools, Vortex remains a lean, "pure" logic engine. It doesn't need to know about Tailscale or Vault; it just knows how to listen on a socket and how to ask the environment for its password. This makes it incredibly portable and secure for both local dev and production VPS environments.
