# LocalShim

LocalShim is a local, single-node, OpenAI-compatible middleware shim that sits between an AI client or agent and a backend model server.

It keeps the client-facing endpoint stable while LocalShim owns middleware behavior such as request plugins, response plugins, streaming passthrough, runtime-safe config swaps, runtime generation pinning, bounded event recording, and operator control.

LocalShim is currently a **pre-alpha release** intended for local AI middleware development, local model workflows, and early integration testing.

```text
Client / Agent  ⇄  LocalShim data plane  ⇄  Backend model server
```

With a side control plane:

```text
External Control/API  ⇄  LocalShim control plane
```

The core idea:

```text
Keep the client endpoint stable.
Let LocalShim own middleware behavior.
Pin each request attempt to one runtime generation.
Apply config changes through guarded runtime swaps.
Record runtime events without exposing prompts, messages, secrets, headers, or full bodies.
```

---

## Current Status

Current development version:

```text
v0.5.0-dev
```

Current milestone:

```text
v0.5 operator CLI and runtime-control polish
```

LocalShim currently provides a working local data plane, control plane, plugin pipeline, streaming passthrough path, runtime config apply/reload workflow, and operator CLI.

---

## What LocalShim Supports

LocalShim currently supports:

```text
OpenAI-compatible chat completion passthrough
OpenAI-compatible /v1/models passthrough
stream=true SSE passthrough
upstream/downstream plugin chains for non-streaming JSON requests
upstream plugin support before streaming passthrough
safe rejection for downstream streaming plugins
runtime generation pinning
runtime fingerprints
active-run tracking
runtime admission gate
dynamic config apply/reload
late_gate and early_gate apply modes
bounded event recording
control API inspection
operator CLI read commands
operator CLI config apply/reload commands
llama.cpp-compatible OpenAI /v1 backend workflows
```

Implemented HTTP surface:

```text
GET  /
GET  /v1/models
POST /v1/chat/completions

GET  /shim/status
GET  /shim/runtime
GET  /shim/events
PUT  /shim/config
POST /shim/config/reload
```

Implemented CLI surface:

```text
python main.py run
python main.py doctor
python main.py config render
python main.py ping
python main.py models
python main.py status
python main.py runtime
python main.py events
python main.py config apply
python main.py config reload
python main.py version
```

---

## Architecture Overview

LocalShim has two main planes.

### Data Plane

The data plane handles model-style client traffic:

```text
GET  /v1/models
POST /v1/chat/completions
```

For chat completions, the request path is:

```text
FastAPI route
  → PipelineRun
  → RuntimeManager.enter_run()
  → active runtime snapshot captured
  → PipelineAttempt
  → selected execution path
  → backend model server
  → client response
```

Each request attempt captures one runtime generation. An attempt does not mix upstream plugins from one generation with backend or downstream behavior from another generation.

### Control Plane

The control plane handles runtime inspection and runtime replacement:

```text
GET  /shim/status
GET  /shim/runtime
GET  /shim/events
PUT  /shim/config
POST /shim/config/reload
```

Config changes go through one mutation path:

```text
candidate config
  → candidate runtime build
  → runtime gate/drain coordination
  → all-or-none runtime swap
```

The control plane does not directly mutate active plugin chains or backend URLs in place.

---

## Installation

Use Python 3.12 or a compatible modern Python 3 version.

Clone the repository:

```bash
git clone https://github.com/cicanovi/localshim.git
cd localshim
```

Use the install script:

```bash
./install.sh
source .venv/bin/activate
```

Or install manually:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Runtime dependencies:

```text
fastapi
httpx
requests
uvicorn
```

---

## Quick Start

### 1. Start a backend

LocalShim expects a backend that exposes OpenAI-compatible `/v1` routes.

By default, `config.json` points to:

```text
http://127.0.0.1:8080
```

Start an OpenAI-compatible backend on that address, or update `backend.url` in `config.json` to point at your backend.

For example, with `llama.cpp` server:

```bash
./build/bin/llama-server \
  -m /path/to/model.gguf \
  --host 127.0.0.1 \
  --port 8080
```

### 2. Start LocalShim

Using the CLI:

```bash
python main.py run -c config.json
```

Or using Uvicorn directly:

```bash
python -m uvicorn server:app --host 127.0.0.1 --port 5413
```

### 3. Check LocalShim

```bash
python main.py ping
python main.py status
python main.py runtime
python main.py events --limit 20
```

Expected basic output includes:

```text
LocalShim reachable: ok
LocalShim status: ok
Runtime generation: 1
```

### 4. List models through LocalShim

```bash
python main.py models
```

Equivalent curl:

```bash
curl -sS http://127.0.0.1:5413/v1/models | jq
```

### 5. Send a non-streaming chat request

```bash
curl -sS http://127.0.0.1:5413/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "local-model",
    "stream": false,
    "messages": [
      {
        "role": "user",
        "content": "Say hello from LocalShim."
      }
    ],
    "temperature": 0.7,
    "max_tokens": 80
  }' | jq
```

### 6. Send a streaming chat request

```bash
curl -N http://127.0.0.1:5413/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "local-model",
    "stream": true,
    "messages": [
      {
        "role": "user",
        "content": "Write one short sentence celebrating LocalShim."
      }
    ],
    "temperature": 0.7,
    "max_tokens": 80
  }'
```

---

## Default Ports

Default LocalShim server:

```text
127.0.0.1:5413
```

Default backend model server:

```text
127.0.0.1:8080
```

Default config path:

```text
config.json
```

You can also set:

```bash
LOCALSHIM_CONFIG=/path/to/config.json
```

---

## Configuration

Current default `config.json` shape:

```json
{
  "server": {
    "host": "127.0.0.1",
    "port": 5413
  },
  "backend": {
    "url": "http://127.0.0.1:8080"
  },
  "plugins": {
    "upstream": [
      {
        "name": "inject_system_prompt",
        "source": "plugins/inject.py",
        "entrypoint": "InjectSystemPrompt",
        "enabled": true,
        "fail_mode": "abort",
        "timeout_seconds": null,
        "params": {
          "content": "Respond concisely.",
          "skip_if_system_exists": false
        }
      }
    ],
    "downstream": []
  },
  "pipeline": {
    "in_flight_policy": "drain",
    "max_replays_per_request": 0,
    "max_retries_per_request": 0,
    "max_reattempts_per_request": 0
  }
}
```

During runtime build, LocalShim normalizes supported defaults for:

```text
control.enabled
control.config_apply.mode
control.config_apply.gate_policy
control.config_apply.gate_timeout_seconds
control.config_apply.queue_policy
observability.events.enabled
observability.events.level
observability.events.max_events
```

Default normalized control settings include:

```text
control.enabled: true
control.config_apply.mode: late_gate
control.config_apply.gate_policy: wait
control.config_apply.gate_timeout_seconds: 10.0
control.config_apply.queue_policy: latest_wins
```

Default event settings include:

```text
observability.events.enabled: true
observability.events.level: basic
observability.events.max_events: 500
```

---

## Operator CLI

The CLI is built with standard-library `argparse`.

Top-level help:

```bash
python main.py --help
```

Current command set:

```text
run       Run LocalShim.
doctor    Validate LocalShim config and local runtime setup.
ping      Ping a running LocalShim instance.
models    List models through a running LocalShim instance.
status    Show LocalShim service and runtime status.
runtime   Show the active LocalShim runtime snapshot.
events    Show recent LocalShim events.
config    Config inspection and runtime update commands.
version   Print the LocalShim CLI version.
```

### Local/startup commands

These operate on local files and startup configuration:

```text
python main.py run
python main.py doctor
python main.py config render
python main.py version
```

### HTTP/operator commands

These talk to a running LocalShim instance:

```text
python main.py ping
python main.py models
python main.py status
python main.py runtime
python main.py events
python main.py config apply
python main.py config reload
```

### Target resolution

For HTTP/operator commands, LocalShim resolves the target base URL in this order:

```text
--url
  > --host / --port
  > --config server.host / server.port
  > http://127.0.0.1:5413
```

Examples:

```bash
python main.py status
python main.py status --port 5413
python main.py status --host 127.0.0.1 --port 5413
python main.py status --url http://127.0.0.1:5413
python main.py status --config config.json
```

### Exit codes

```text
0  success
1  command failed or LocalShim returned an error/rejected result
2  usage error
3  target unreachable / timeout / invalid response body
```

---

## CLI Examples

### Run LocalShim

```bash
python main.py run -c config.json
```

With overrides:

```bash
python main.py run \
  -c config.json \
  --host 127.0.0.1 \
  --port 5413 \
  --backend-url http://127.0.0.1:8080
```

By default, `run` refuses to bind to non-localhost hosts.

Allowed by default:

```text
127.0.0.1
localhost
::1
```

To bind to a non-localhost interface:

```bash
python main.py run --host 0.0.0.0 --allow-network
```

### Validate config

```bash
python main.py doctor -c config.json
```

Also check backend reachability:

```bash
python main.py doctor -c config.json --check-backend
```

### Render effective config

```bash
python main.py config render -c config.json
```

Show unredacted values:

```bash
python main.py config render -c config.json --show-secrets
```

### Inspect a running instance

```bash
python main.py ping
python main.py models
python main.py status
python main.py runtime
python main.py events --limit 50
```

Watch status:

```bash
python main.py status --watch --interval 2
```

Watch events:

```bash
python main.py events --watch --interval 2 --limit 20
```

### Apply config to the running runtime

```bash
python main.py config apply config.json --mode late_gate
```

Apply with candidate backend override:

```bash
python main.py config apply config.json \
  --backend-url http://127.0.0.1:8081 \
  --mode late_gate
```

Important distinction:

```text
--host / --port / --url / --config
  choose which running LocalShim target to contact

--backend-url
  changes the candidate config sent to that running LocalShim
```

### Reload the active config path from disk

```bash
python main.py config reload --mode late_gate
```

By default, write commands only allow localhost targets.

Allowed by default:

```text
127.0.0.1
localhost
::1
```

To apply or reload config against a non-localhost target:

```bash
python main.py config apply config.json \
  --url http://192.168.1.50:5413 \
  --allow-remote-control
```

---

## Data Plane Endpoints

### `GET /`

Health endpoint.

```bash
curl -sS http://127.0.0.1:5413/ | jq
```

Expected shape:

```json
{
  "status": "ok"
}
```

### `GET /v1/models`

OpenAI-compatible model-list passthrough.

```bash
curl -sS http://127.0.0.1:5413/v1/models | jq
```

Behavior:

```text
forwards to active runtime backend /v1/models
preserves backend JSON body
preserves backend status code
filters hop-by-hop response headers
returns LocalShim backend errors if the backend cannot be reached
releases active run after request completion
```

### `POST /v1/chat/completions`

OpenAI-compatible chat completions endpoint.

LocalShim supports both non-streaming JSON responses and `stream=true` SSE responses.

---

## Chat Completion Execution Modes

### Non-streaming JSON path

For `stream=false` or omitted `stream`, LocalShim uses the JSON/plugin pipeline.

```text
client request
  → parse JSON
  → upstream plugin chain
  → backend /v1/chat/completions
  → downstream plugin chain
  → client JSON response
```

Directional behavior:

```text
stream=false, upstream=[], downstream=[]
  → JSON identity passthrough

stream=false, upstream=[...], downstream=[]
  → run upstream plugins, call backend, return backend JSON as-is

stream=false, upstream=[], downstream=[...]
  → call backend with original request, run downstream plugins

stream=false, upstream=[...], downstream=[...]
  → run upstream plugins, call backend, run downstream plugins
```

### Streaming raw passthrough path

For `stream=true` with no plugins enabled, LocalShim uses raw/SSE passthrough.

```text
client request body
  → LocalShim
  → raw body forwarded to backend
  → backend SSE response streamed back
  → active run released after stream cleanup
```

Expected wire shape:

```text
data: {...}
data: {...}
data: [DONE]
```

### Streaming with upstream plugins

For `stream=true` with upstream plugins and no downstream plugins, LocalShim runs upstream request plugins before forwarding a streaming backend request.

```text
client request
  → parse JSON
  → upstream plugin chain
  → modified stream=true request forwarded to backend
  → backend SSE response streamed back unchanged
  → active run released after stream cleanup
```

Example use cases:

```text
inject system prompt
normalize request fields
add routing metadata
add request tags
```

### Streaming with downstream plugins

For `stream=true` with downstream plugins enabled, LocalShim rejects the request before calling the backend.

Reason:

```text
Downstream plugins currently operate on complete JSON response objects.
SSE streams are chunked.
Applying normal response plugins to streaming chunks would be unsafe and misleading.
```

Current error type:

```text
streaming_downstream_plugins_unsupported
```

Expected response shape:

```json
{
  "error": {
    "type": "streaming_downstream_plugins_unsupported",
    "message": "Streaming downstream plugins are not supported yet. Remove downstream plugins or set stream=false."
  }
}
```

---

## Plugin Model

Plugins inherit from `ShimPlugin`.

```python
class ShimPlugin:
    def __init__(self, **params):
        self.params = params

    def on_request(self, req, ctx=None):
        return req

    def on_response(self, res, ctx=None):
        return res
```

Plugin specs live under:

```json
{
  "plugins": {
    "upstream": [],
    "downstream": []
  }
}
```

Example plugin spec:

```json
{
  "name": "inject_system_prompt",
  "source": "plugins/inject.py",
  "entrypoint": "InjectSystemPrompt",
  "enabled": true,
  "fail_mode": "abort",
  "timeout_seconds": null,
  "params": {
    "content": "Respond concisely.",
    "skip_if_system_exists": false
  }
}
```

Plugin fields:

```text
name
source
entrypoint
enabled
fail_mode
timeout_seconds
params
```

Supported fail modes:

```text
abort
continue
```

Behavior:

```text
abort
  plugin error aborts the request

continue
  plugin error is recorded in context and the pipeline continues
```

Disabled plugins are not loaded.

Enabled plugin source contents are included in the runtime fingerprint.

---

## Built-In Example Plugin

`plugins/inject.py` provides:

```text
InjectSystemPrompt
```

It inserts a system message at the start of the OpenAI-style `messages` list.

Default behavior:

```text
content: Respond concisely.
skip_if_system_exists: false
```

When `skip_if_system_exists` is true, the plugin leaves an existing first system message alone.

---

## Runtime Model

### `ShimRuntime`

A `ShimRuntime` is one runtime snapshot of LocalShim behavior.

It contains:

```text
runtime generation
config path
normalized config snapshot
backend URL
runtime fingerprint
upstream plugin runtimes
downstream plugin runtimes
pipeline policy
creation timestamp
```

A successful config apply or reload creates a new runtime generation.

A rejected config apply or reload preserves the current active runtime.

### `RuntimeManager`

`RuntimeManager` owns:

```text
active runtime pointer
active run count
runtime gate open/closed state
generation reservation
runtime swap
drain waiting
```

A request must enter the runtime manager before it runs. This gives the request an `ActiveRun` and pins it to the active runtime at that moment.

### `ActiveRun`

An `ActiveRun` increments active-run count on entry and decrements it on release.

For non-streaming JSON requests, the active run is released when the response is returned.

For streaming requests, the active run is held until stream cleanup finishes.

### `PipelineRun`

A `PipelineRun` represents one accepted client request.

It tracks:

```text
request_id
pipeline_run_id
original ingress body
attempts
replay count
backend retry count
post-backend reattempt count
start/finish timestamps
final status
```

### `PipelineAttempt`

A `PipelineAttempt` represents one execution attempt under one captured runtime.

It tracks:

```text
attempt number
runtime generation
runtime fingerprint
working request JSON
working response JSON
backend commit boundary
checkpoint
backend response
raw streaming handles when applicable
```

Core invariant:

```text
A PipelineAttempt uses exactly one ShimRuntime generation.
```

A request may have multiple attempts only when policy explicitly allows replay, retry, or reattempt.

---

## Config Apply and Reload

`ConfigApplyCoordinator` owns the config apply workflow.

It handles:

```text
apply IDs
latest desired config
candidate runtime building
late_gate apply
early_gate apply
latest-wins superseding
runtime gate close/open
wait-for-drain
runtime swap
old-runtime preservation on failure
event recording
```

Candidate runtime construction happens outside the runtime manager lock.

The intended mutation path is:

```text
candidate config
  → build candidate runtime
  → close gate when appropriate
  → wait for active runs to drain when appropriate
  → swap runtime all-or-none
  → reopen gate
```

### `late_gate`

`late_gate` is the default mode.

Behavior:

```text
candidate config is accepted
candidate runtime builds while old runtime keeps serving
stale/superseded candidates are discarded
gate closes near final swap
active runs drain
runtime swaps all-or-none
gate reopens
future runs use new runtime
```

Example:

```bash
python main.py config apply config.json --mode late_gate
```

Equivalent curl:

```bash
curl -sS -X PUT 'http://127.0.0.1:5413/shim/config?mode=late_gate' \
  -H 'Content-Type: application/json' \
  --data-binary @config.json | jq
```

### `early_gate`

`early_gate` closes the runtime gate before building the candidate runtime.

Behavior:

```text
config apply request is accepted
gate closes before candidate runtime build
new runs wait or fail according to gate behavior
candidate builds while gate is closed
active runs drain
runtime swaps all-or-none
gate reopens
future runs use new runtime
```

Example:

```bash
python main.py config apply config.json --mode early_gate
```

Equivalent curl:

```bash
curl -sS -X PUT 'http://127.0.0.1:5413/shim/config?mode=early_gate' \
  -H 'Content-Type: application/json' \
  --data-binary @config.json | jq
```

### Reload config from disk

```bash
python main.py config reload --mode late_gate
```

Equivalent curl:

```bash
curl -sS -X POST 'http://127.0.0.1:5413/shim/config/reload?mode=late_gate' | jq
```

---

## Pipeline Policies

Pipeline policy lives under:

```json
{
  "pipeline": {
    "in_flight_policy": "drain",
    "max_replays_per_request": 0,
    "max_retries_per_request": 0,
    "max_reattempts_per_request": 0
  }
}
```

Supported policies:

```text
drain
pre_backend_replay
post_backend_generation_reattempt
```

Backend transport/status retry is separate from generation replay.

### `drain`

Default behavior.

```text
Already-started attempts continue on their captured runtime generation.
New runs may be gated during config apply.
Config swaps do not mutate active attempts.
```

Example:

```text
Request A starts on generation 1.
Config applies generation 2.
Request A finishes on generation 1.
Request B starts on generation 2.
```

### `pre_backend_replay`

Allows a request to replay from the original ingress body if a runtime swap begins before backend commit.

Rules:

```text
allowed only before backend commit
starts again from ingress_body
creates a fresh attempt and context
bounded by max_replays_per_request
abandoned attempt must not call backend
```

### Backend transport/status retry

Backend retry retries the same already-transformed request when the backend request fails or returns a retryable server error.

Rules:

```text
does not rebuild from ingress_body
does not switch runtime generation
bounded by max_retries_per_request
records retry events/counters
```

### `post_backend_generation_reattempt`

Allows a full request reattempt from the original ingress body after backend commit if a generation change is detected before response return.

Rules:

```text
allowed only after backend commit
starts again from ingress_body
may create a second backend call
bounded by max_reattempts_per_request
records reattempt events/counters
```

---

## Runtime Fingerprints

Each runtime has a fingerprint:

```text
sha256:<digest>
```

The fingerprint is computed from runtime-defining behavior, including:

```text
normalized config snapshot
backend URL
pipeline policy
upstream/downstream plugin specs and order
plugin params
plugin fail modes
plugin timeout settings
enabled plugin source file contents
```

The fingerprint is intended as a stable identity token for runtime behavior.

Useful invariants:

```text
same normalized config + same enabled plugin source → same fingerprint
backend URL change → fingerprint changes
plugin params change → fingerprint changes
plugin order change → fingerprint changes
enabled plugin source change → fingerprint changes
successful config apply → active fingerprint changes
rejected config apply → active fingerprint stays the same
pipeline events include the runtime fingerprint that handled the request
config apply/reload responses expose the active/preserved runtime fingerprint
```

---

## Event Model

`EventRecorder` stores bounded runtime and control events.

Event records can include:

```text
request ID
pipeline run ID
attempt number
runtime generation
runtime fingerprint
apply ID
phase
checkpoint
elapsed time
sanitized details
```

Event detail sanitization avoids unsafe fields such as:

```text
authorization
api_key
body
headers
messages
password
prompt
request
response
secret
token
```

Inspect events with:

```bash
python main.py events --limit 50
python main.py events --event-type config_apply_completed
python main.py events --json
```

---

## Control Plane Endpoints

### `GET /shim/status`

Returns compact service/runtime status.

```bash
curl -sS http://127.0.0.1:5413/shim/status | jq
```

Typical fields:

```text
status
service
runtime_generation
runtime_fingerprint
runtime_fingerprint_short
active_runs
gate_open
backend_url
plugins
events
capabilities
```

### `GET /shim/runtime`

Returns detailed active runtime state.

```bash
curl -sS http://127.0.0.1:5413/shim/runtime | jq
```

Typical fields:

```text
runtime_generation
runtime_fingerprint
runtime_fingerprint_short
created_at
config_path
backend_url
plugins_enabled
state
apply
control_policy
pipeline_policy
config summary
plugin summaries
```

Plugin params are redacted. The endpoint exposes `params_keys`, not full private values.

### `GET /shim/events`

Returns recent events and recorder stats.

```bash
curl -sS 'http://127.0.0.1:5413/shim/events?limit=20' | jq
```

Supported query parameters:

```text
limit
since_id
event_type
level
```

### `PUT /shim/config`

Applies a full candidate config to the live runtime.

```bash
curl -sS -X PUT 'http://127.0.0.1:5413/shim/config?mode=late_gate' \
  -H 'Content-Type: application/json' \
  --data-binary @config.json | jq
```

Optional query parameters:

```text
mode=late_gate
mode=early_gate
persist=false
```

Current persistence behavior:

```text
persist=true is rejected
runtime-only apply is supported
```

### `POST /shim/config/reload`

Reloads config from the active runtime config path and applies it through the same coordinator.

```bash
curl -sS -X POST 'http://127.0.0.1:5413/shim/config/reload?mode=late_gate' | jq
```

Supported query parameters:

```text
mode=late_gate
mode=early_gate
```

Failed reloads preserve the active runtime.

---

## Security and Privacy Notes

LocalShim is designed for local-first operation.

Current safeguards include:

```text
run refuses non-localhost binds unless --allow-network is passed
control write commands refuse non-localhost targets unless --allow-remote-control is passed
config render redacts sensitive-looking keys by default
/shim/runtime returns redacted config summaries and plugin param keys
event sanitization removes unsafe-looking prompt/body/header/token/secret details
```

Treat config files and plugin params as sensitive if they contain credentials, headers, tokens, or private backend details.

---

## Current Boundaries

LocalShim is currently focused on local, single-node OpenAI-compatible middleware.

Current boundaries:

```text
single-node runtime
local OpenAI-compatible backend focus
no plugin marketplace
no dashboard/UI
no distributed routing
no auth management layer
no chunk-level SSE rewrite engine
no downstream streaming plugin system yet
no persistent config write-back yet
```

---

## Development

LocalShim is currently distributed as a source-based pre-alpha release.

The public repository includes the runtime, control API, operator CLI, default config, install script, dependency list, README, and built-in example plugins. Internal test and validation assets are not included in this pre-alpha source package.

Useful local checks:

```bash
python main.py --help
python main.py doctor -c config.json
python main.py config render -c config.json
```

To validate against a running backend:

```bash
python main.py run -c config.json
python main.py ping
python main.py status
python main.py models
```

For real local inference, start an OpenAI-compatible backend first, then point `backend.url` in `config.json` at that backend.

---

## Project Layout

```text
server.py
main.py
config.json
install.sh
requirements.txt
README.md

control/
  api.py

core/
  app.py
  apply.py
  config.py
  context.py
  errors.py
  events.py
  fingerprint.py
  forwarder.py
  lifecycle.py
  logging.py
  overrides.py
  pipeline.py
  plugins.py
  policy.py
  runtime.py
  runtime_builder.py

plugins/
  broken.py
  debug.py
  inject.py
```

---

## LocalShim in One Sentence

LocalShim is a local OpenAI-compatible middleware runtime that keeps the client endpoint stable while supporting request/response plugins, streaming passthrough, runtime-safe config swaps, bounded event inspection, and a lightweight operator CLI between the client and a backend model server.
