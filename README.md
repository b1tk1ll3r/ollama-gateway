# Ollama Fair Gateway

A high-performance Go gateway in front of one or more Ollama workers. The gateway exposes the native Ollama API and Ollama's OpenAI-compatible API while adding authentication, hierarchical fair scheduling, compute-credit quotas, usage metering, model-aware routing, optional durable batch execution and an embedded operations UI.

The inference coordination engine remains **100% in-memory**, while restart-worthy control-plane state is persisted to local files under `storage.data_dir`. There is no Redis client, SQL/database server, distributed lease system or external message bus. The project uses only the Go standard library and builds as a static binary with `CGO_ENABLED=0`.

## Architecture

```text
Clients / Applications
        |
        v
+-------------------------------+
| Ollama Fair Gateway           |
|                               |
| OIDC / API Keys / IP bypass   |
| Admission + compute credits   |
| Tenant -> Actor WFQ           |
| In-memory worker routing      |
| Live request event bus        |
| Usage aggregation             |
| Durable local state           |
| Embedded Web UI               |
+---------------+---------------+
                |
       +--------+---------+
       |        |         |
       v        v         v
    Ollama A  Ollama B  Ollama C
```

All fairness decisions are therefore made from one authoritative in-process state. Multiple Ollama workers are supported. If strict global fairness is required, all clients should enter through this one gateway process.

## Main features

- Native Ollama `/api/*` passthrough, including streaming and large management/blob requests.
- OpenAI-compatible `/v1/*` passthrough via Ollama's own compatibility layer.
- OIDC/JWT validation using discovery and JWKS.
- Browser OIDC Authorization Code + PKCE.
- Static configuration API keys plus persistent UI-created API-key creation/revocation from the admin UI; only SHA-256 key hashes are stored.
- CIDR-based authentication bypass for trusted IPs.
- Safe `X-Forwarded-For` handling behind explicitly trusted proxies.
- Two-level hierarchical weighted fair queueing: tenant fairness first, actor/application fairness inside each tenant.
- Per-actor and per-tenant compute-credit token buckets.
- Model-dependent cost estimation and reconciliation using actual Ollama usage data.
- Capability-aware preflight using Ollama `/api/show` (`tools`, `vision`, `thinking`, `embedding`, completion) plus context-window guard.
- Model-affinity and adaptive routing across configured Ollama workers using load, learned tok/s, GPU utilization and VRAM pressure.
- Worker circuit breaker with safe pre-stream transport retries, persistent drain/disable maintenance state and manual circuit reset in the admin UI.
- Virtual model aliases (for example `fast` or `coding`) with ordered physical-model fallback and optional capability requirements.
- Model ACLs at tenant level and per API key; denied models are removed from discovery and rejected before scheduling.
- Persistent per-worker **Model Placement** rules (allow-all/whitelist + exact/prefix allow/deny patterns) enforced before adaptive routing, with an interactive model × worker matrix in the admin UI.
- Per-model concurrency limits with exact and trailing-prefix-wildcard matching.
- Optional built-in NVIDIA telemetry through `nvidia-smi` without CGO or an exporter; native host-memory telemetry is available on Linux, macOS and Windows.
- Hard per-worker concurrency using lock-free atomic counters.
- Live request lifecycle tracking: queued -> routing -> running -> streaming -> completed/failed/cancelled.
- Optional durable batch jobs with owner-scoped APIs, admin pause/resume/cancel/output controls, restart recovery, retention and execution through the normal quota/scheduler/routing/accounting pipeline using service class `batch`.
- Embedded Canvas pulse visualization and local LLM infrastructure map.
- Worker/model inventory, pull/stop/delete operations and persistent live policy overrides.
- Prometheus-compatible metrics with durable counter/histogram snapshots.
- Persistent quota-bucket snapshots, preventing a normal restart from resetting tenant/actor burst budgets.
- Asynchronous JSONL usage journal with startup replay, so dashboard usage totals and recent history survive restarts.
- Atomically persisted UI configuration override loaded on the next process start.
- No prompt or generated response content is stored by the telemetry/usage layer. Content persistence exists only in explicit opt-in features such as encrypted Responses conversations and durable batch payload spooling.

## Runtime vs durable state

The hot-path scheduler remains in memory:

```text
Fair queue / virtual clocks
Running concurrency
Per-worker active slots
OIDC browser sessions
Live request registry
Infrastructure topology
Model operation/transient inference-job state
```

These objects are intentionally not restored because requests and sockets cannot survive a process restart. Durable control-plane/accounting state is stored locally instead:

```text
<data_dir>/gateway-config.json   UI configuration override
<data_dir>/api-keys.json         UI-created API-key hashes + metadata
<data_dir>/policies.json         tenant policy overrides
<data_dir>/metrics.json          Prometheus counter/histogram state
<data_dir>/quota.json            actor/tenant credit bucket state
<data_dir>/worker-performance.json learned per-worker/model tok/s routing state
<data_dir>/model-placement.json persistent live worker/model placement overrides
<data_dir>/worker-state.json     persistent worker drain/disable state
<data_dir>/conversations.enc.json optional encrypted Responses context
<data_dir>/batch-jobs.json       optional durable batch metadata/state
<data_dir>/batch/input/*         optional durable batch request payloads
<data_dir>/batch/output/*        optional durable batch response payloads
<data_dir>/usage/usage-*.jsonl   completed request accounting/history
```

State-file writes use temp-file + fsync + atomic rename. Usage remains append-only and is replayed at startup. None of these files is consulted for every inference token or proxy chunk, so durable storage is not in the inference hot path. Use a persistent filesystem and preferably an absolute `storage.data_dir` in production.

Browser OIDC sessions remain intentionally volatile; after a gateway restart a browser logs in again. Transient inference jobs that were running at restart are disconnected and are not resurrected. Durable batch definitions survive; an interrupted batch attempt is recovered to a restart-safe queued state and can execute again.

The supplied `docker-compose.yml` mounts a named volume at `/data`, so the default relative `./data` state directory is durable across container replacement. If you run the image differently, mount persistent storage at `/data` or set an absolute `storage.data_dir`. The Compose file accepts `GATEWAY_CONFIG=./your-config.json` to select the bootstrap config; if omitted it intentionally uses `config.example.json` for development. Before a production rollout, run the candidate binary/container with `-check-config` and see `docs/DEPLOYMENT-HARDENING.md`.

## Persistent storage configuration

```json
"storage": {
  "data_dir": "./data",
  "config_file": "gateway-config.json",
  "api_keys_file": "api-keys.json",
  "policies_file": "policies.json",
  "metrics_file": "metrics.json",
  "quota_file": "quota.json",
  "worker_performance_file": "worker-performance.json",
  "model_placement_file": "model-placement.json",
  "worker_state_file": "worker-state.json",
  "auto_tune_file": "auto-tune.json",
  "warm_models_file": "warm-models.json",
  "alerts_file": "alerts.json",
  "conversations_file": "conversations.enc.json",
  "batch_jobs_file": "batch-jobs.json",
  "batch_jobs_dir": "batch",
  "flush_interval": "10s"
}
```

`storage` is bootstrap-only: edit it in the startup configuration, not through the web JSON editor. Other configuration changes saved in **Admin -> Konfiguration** are validated and written to `<data_dir>/gateway-config.json`; that file becomes the effective configuration at the next start. Delete the persistent override from the UI (or remove the file offline) to fall back to the bootstrap configuration.

**Admin -> Persistenz** shows the durable files, tiered usage-retention footprint and last compaction, can force an immediate flush or retention compaction, and can download a ZIP backup containing state, detail journals and rollups.

## Reliability, maintenance, aliases and model ACLs

The P0 reliability layer is configured independently from routing scores:

```json
"reliability": {
  "enabled": true,
  "failure_threshold": 3,
  "open_duration": "30s",
  "retry_attempts": 2,
  "retry_backoff": "50ms"
}
```

A retry is attempted only when the upstream transport fails **before any response is committed to the client**. Once headers/body are visible, the response is never replayed. Backend 5xx responses still contribute to the worker circuit breaker but are not retried after commitment.

Workers can be placed into `active`, `draining`, or `disabled` state from **Admin -> Worker**. Draining rejects new work while existing streams finish; the state survives restart in `worker-state.json`.

Virtual model aliases expose stable names to clients:

```json
"model_aliases": {
  "fast": { "models": ["qwen3:8b", "gemma3:12b"] },
  "coding": {
    "models": ["qwen3.6:27b-q4_K_M", "qwen3:8b"],
    "required_capabilities": ["completion"]
  }
}
```

Aliases appear in `/api/tags` and `/v1/models`; the gateway resolves them to the first currently routable target. The response includes `X-Gateway-Model-Alias` and `X-Gateway-Resolved-Model`.

Model access is separate from worker placement:

```json
"model_access": {
  "default": { "mode": "allow_all" },
  "tenants": {
    "interns": {
      "mode": "whitelist",
      "allowed_models": ["fast", "qwen3:8b"],
      "denied_models": ["expensive-*"]
    }
  }
}
```

API keys created in the web UI can additionally define their own allow/deny model patterns. API-key ACLs take precedence over the tenant baseline for that identity. Tenant baseline ACLs can also be created, edited and reset at runtime from **Fairness & Quotas → Tenant Model Access**; changes are persisted before immediate publication.

- Runtime model-alias CRUD is available from the admin model page and is persisted before immediate publication to new requests.

For production rollout and rollback steps, see `docs/PRODUCTION-UPDATE.md`; the current release validation is recorded in `docs/RELEASE-VERIFICATION-CHECKPOINT-28.md`.

See `docs/ROADMAP-P0-P3.md` for the staged plan through P3. Checkpoint 28 completes the active P3 roadmap with the optional public read-only status dashboard. Multi-gateway HA has been removed from the active roadmap; the existing HA-readiness tools remain available as diagnostics if future capacity or availability requirements change.

## Build

Requires Go 1.23 or newer.

```bash
go test ./...
go vet ./...
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
  -o dist/ollama-gateway ./cmd/ollama-gateway
```

For Apple Silicon:

```bash
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags="-s -w" \
  -o dist/ollama-gateway-darwin-arm64 ./cmd/ollama-gateway
```

## Quick start

Copy the example configuration:

```bash
cp config.example.json config.json
./dist/ollama-gateway-darwin-arm64 -config ./config.json
```

The default example expects Ollama on `http://127.0.0.1:11434` and exposes the gateway on port `8080`.

Native Ollama example:

```bash
curl http://127.0.0.1:8080/api/chat \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3:8b","messages":[{"role":"user","content":"hello"}]}'
```

OpenAI-compatible example:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen3:8b","messages":[{"role":"user","content":"hello"}]}'
```

The example configuration grants localhost an admin IP bypass. Replace that for production.

## OpenWebUI

Use the gateway as an **Ollama API connection**, not as a raw browser-side URL. Current OpenWebUI performs model discovery with `GET /api/tags` and connection verification with `GET /api/version`; when a connection key is configured it sends that key as `Authorization: Bearer <key>`. The gateway accepts both configuration API keys and keys created under **Admin -> Sicherheit -> API Keys**. UI-created keys are persisted as hashes, so OpenWebUI keeps working with the same key across gateway restarts.

A ready-to-use configuration is included as `config.openwebui.example.json`. Set a secret before starting the gateway:

```bash
export OPENWEBUI_GATEWAY_KEY='replace-with-a-long-random-secret'
./dist/ollama-gateway-darwin-arm64 -config ./config.openwebui.example.json
```

In OpenWebUI go to **Settings -> Admin -> Connections -> Manage Ollama API Connections** and use:

```text
URL:     http://host.docker.internal:8080
API Key: the value of OPENWEBUI_GATEWAY_KEY
```

Do **not** append `/api` to the Ollama base URL; OpenWebUI appends `/api/version`, `/api/tags`, `/api/show`, and the inference paths itself. When OpenWebUI runs outside Docker, use the hostname/IP at which its backend can actually reach the gateway. `localhost` inside the OpenWebUI container refers to that container, not the Mac host.

Verify the path independently before configuring OpenWebUI:

```bash
curl -sS -H "Authorization: Bearer $OPENWEBUI_GATEWAY_KEY" \
  http://127.0.0.1:8080/api/version

curl -sS -H "Authorization: Bearer $OPENWEBUI_GATEWAY_KEY" \
  http://127.0.0.1:8080/api/tags
```

For OpenAI-compatible connections, use `http://host.docker.internal:8080/v1` with the same key. `/v1/models` is generated from the same aggregated worker inventory as `/api/tags`.

Model discovery is gateway-native rather than a control-worker passthrough: `/api/tags` queries all configured Ollama workers in parallel, de-duplicates model IDs and normalizes both `name` and `model`. `/api/ps` is also aggregated. The worker router tracks installed models from `/api/tags` in addition to loaded models from `/api/ps`, preventing a selected model from being routed to a worker that does not have it installed.

If OpenWebUI is in Docker, relying on the default localhost IP bypass will normally fail because the TCP peer seen by the gateway is the Docker/host-network address rather than `127.0.0.1`. Prefer the dedicated static API key above instead of widening the bypass CIDR.

## Scheduling and fairness

A compute request is assigned an estimated credit cost before it enters the queue. The scheduler uses a two-level weighted fair queue:

1. tenants compete according to `tenant_weight`;
2. inside a tenant, actors compete according to `actor_weight` and request cost.

This prevents one tenant from gaining extra global capacity merely by creating many users, while also preventing one heavy actor from blocking all other actors in the same tenant.

Key settings:

```json
"scheduler": {
  "global_concurrency": 4,
  "max_queue": 2048,
  "max_queue_per_actor": 64,
  "queue_timeout": "10m",
  "default_tenant_weight": 1,
  "default_actor_weight": 1
}
```

`global_concurrency` should normally be close to the total useful inference concurrency of the configured workers. Worker-level `max_concurrent` remains a second hard limit.

## Compute credits and quotas

Each tenant policy can limit long-term consumption:

```json
"*": {
  "tenant_weight": 1,
  "actor_weight": 1,
  "actor_credits_per_minute": 60,
  "actor_burst_credits": 180,
  "tenant_credits_per_minute": 300,
  "tenant_burst_credits": 900
}
```

The gateway reserves estimated credits before admission. After Ollama finishes, the reservation is reconciled with actual usage. Native Ollama responses expose prompt/completion token counts and evaluation timings, allowing the estimator to incorporate model-specific token and compute weights.

Policy overrides made in the web UI take effect immediately for new requests and are persisted in `policies.json`, so they survive restarts. The baseline `scheduler.policies` in configuration remains the fallback.

## Worker routing

Workers are polled using Ollama `/api/ps` and `/api/tags`. Installed-model ownership is used as a routing constraint when known; a worker that already has the requested model loaded receives an additional strong affinity preference, while its current active-slot ratio increases its routing score.

### Model Placement / worker whitelist

Placement is a **hard constraint before routing score calculation**. It answers "where may this model run?" while tenant policies answer "who may consume how much?". They are intentionally separate.

Static worker defaults can be declared in `config.json`:

```json
{
  "name": "rtx-4090",
  "url": "http://10.10.11.124:11434",
  "max_concurrent": 2,
  "model_placement": {
    "mode": "whitelist",
    "allowed_models": [
      "qwen3:8b",
      "gemma4:*",
      "embeddinggemma:*"
    ],
    "denied_models": [
      "gemma4:e4b"
    ]
  }
}
```

`mode: "allow_all"` allows models by default; `mode: "whitelist"` blocks models unless an allow rule matches. Patterns are exact names, `*`, or a single trailing wildcard such as `gemma4:*`. The most specific rule wins; exact rules beat prefix rules and deny wins ties. This permits useful exceptions such as denying `gemma4:*` while explicitly allowing `gemma4:latest`.

The admin UI exposes this under **Model Placement** as a model × worker matrix. A cell shows whether the model is allowed, installed and loaded. Clicking a cell creates/removes an exact persistent exception; the worker editor manages broad patterns and offers presets for **allow all**, **only installed**, and **block all**. UI overrides are written atomically to `<data_dir>/model-placement.json` and affect new requests immediately without a gateway restart. Resetting a worker restores its `config.json` baseline.

Client-facing `/api/tags` and `/v1/models` only expose models installed on at least one worker on which placement permits them, so OpenWebUI does not offer a model that the gateway itself would refuse to route. Model-routed control requests such as `/api/show` use the same placement eligibility and cannot bypass it through the control worker.

See [`docs/MODEL-PLACEMENT.md`](docs/MODEL-PLACEMENT.md) for operational examples and UI semantics.

A two-worker M5-Ultra + RTX-4090 example using the model names from the development setup is included as `config.placement.example.json`.

```json
"workers": [
  {
    "name": "mac-studio-m5-ultra",
    "url": "http://127.0.0.1:11434",
    "max_concurrent": 4,
    "health_interval": "5s",
    "local_system_stats": true
  }
]
```

The gateway's worker concurrency is enforced entirely with atomic counters in the process. No lease renewal or external slot backend is involved.

`local_system_stats` can expose host RAM data for a **local** worker. For remote workers, leave it false and use `telemetry_url`; the shipped `cmd/worker-telemetry` agent can export host RAM plus NVIDIA or Linux AMDGPU sysfs metrics. See `docs/WORKER-TELEMETRY.md`. Ollama `size_vram` is shown as resident model footprint, not as GPU-utilization percentage.

## Authentication

Three API authentication methods can coexist:

- OIDC bearer tokens;
- API keys from configuration and persistent UI-created API keys;
- trusted-IP bypass.

Administrators can create and revoke keys in the embedded UI. The plaintext secret is returned exactly once at creation time; only its SHA-256 digest and non-secret metadata are written to the durable API-key store. Static keys loaded from the bootstrap configuration are shown as read-only and cannot be deleted from the UI.

For IP bypass, the TCP peer is authoritative by default. `auth.trusted_proxies` still controls forwarded client-IP resolution for observability, but a forwarded address cannot satisfy credential-free IP bypass unless `auth.ip_bypass_use_forwarded_ip=true` is explicitly enabled. Keep bypass and proxy networks narrow.

See `config.oidc.example.json` for a browser-OIDC configuration.

## Web control plane

Enable with:

```json
"ui": {
  "enabled": true,
  "path": "/admin",
  "title": "Ollama Fair Gateway"
}
```

The UI is embedded in the Go binary and has no CDN, npm or runtime frontend dependency.

Important pages include:

- Overview: queue, running requests, usage and workers.
- Live Flow: animated request lifecycle visualization.
- Infrastructure: local in-memory topology Tenant -> Gateway -> Queue -> Worker -> Model.
- Workers: health, active/per-model slots, GPU/VRAM telemetry and learned per-model token throughput.
- Models: inventory, detected capabilities/context length and administrative pull/stop/delete actions. **Stop** unloads a resident Ollama model using `POST /api/generate` with `keep_alive: 0`; it is not a request-cancellation mechanism.
- Model Placement: persistent worker whitelists/deny rules, current inventory/loaded state and one-click exact routing exceptions.
- Jobs: active queued/running/streaming inference requests with administrator-triggered cancellation.
- Fairness: scheduler limits, usage and persistent policy overrides.
- Usage: recent request metadata and token/credit accounting.
- Security: OIDC, API-key creation/revocation, static keys, IP bypass and trusted proxy configuration.
- Config: validated JSON editor for a persistent configuration override. Saves are atomic and take full effect after restart; storage-path changes remain bootstrap-only.

Live metadata is delivered to the browser over authenticated Server-Sent Events. Animation is rendered with Canvas in the browser; the server sends only bounded metadata snapshots.


## Job cancellation

Administrators can cancel active compute requests from **Admin -> Jobs** or directly from the **Live Flow** request list. The control-plane endpoints are:

```text
GET  /gateway/ui-api/jobs
POST /gateway/ui-api/jobs/<request-id>/cancel
```

Cancellation is implemented with the Go request context rather than a second Ollama-specific control request:

- a **queued** job is removed from the fair-scheduler heap immediately and its reserved credits are released;
- a job waiting for a worker slot is cancelled immediately;
- a **running/streaming** job cancels the upstream Ollama HTTP context, releases the worker and scheduler slots, and reconciles any usage already observable from the response;
- live/accounting metadata records cancelled jobs with internal status `499`. If Ollama has already emitted HTTP `200` for a streaming response, that wire status cannot be changed; the client instead observes the stream ending early while the gateway records `499` internally.

Model **Stop** and job **Cancel** are intentionally different operations. Model Stop only unloads a resident model. Job Cancel terminates a specific inference request.

## Browser OIDC sessions

After Authorization Code + PKCE login, the browser receives an opaque session identifier. The OIDC access token remains in the gateway's in-memory session store. Session cookies are HttpOnly and state-changing cookie-authenticated UI requests require CSRF protection.

Because sessions are in memory, a gateway restart requires browser users to log in again. This is intentional in the single-process design.

## Infrastructure API

Admin-only local infrastructure endpoints:

```text
GET /gateway/ui-api/infrastructure
GET /gateway/ui-api/infrastructure/stream
```

The snapshot contains one gateway process, all configured workers, loaded models and bounded live request metadata. It never contains prompt text or generated output.

## Usage

Gateway API endpoints:

```text
GET /gateway/v1/usage/me
GET /gateway/v1/status
GET /gateway/v1/usage/tenant?tenant=<id>   # admin
```

Completed usage events are appended asynchronously to daily JSONL files. Tiered retention keeps request-level detail for a bounded window, then replaces it with dimension rollups so long-running gateways do not grow an unbounded journal:

```json
"usage": {
  "journal_dir": "./data/usage",
  "buffer": 16384,
  "flush_interval": "1s",
  "retention": {
    "detail_days": 30,
    "daily_days": 400,
    "monthly_months": 0,
    "compaction_interval": "6h"
  }
}
```

With these defaults, the last 30 days remain request-addressable JSONL detail, days 31–400 remain daily aggregates, and older data is folded into monthly aggregates indefinitely (`monthly_months: 0`). Rollups preserve request/error counts, tokens, credits, queue/service time, byte counts and prompt/output evaluation time for global, tenant, actor, application, model and worker dimensions. Performance (`tok/s`) is therefore still available after detail rows are removed.

Historical UI/API queries are available through `GET /gateway/ui-api/usage/rollups?granularity=daily|monthly&dimension=...`. **Admin -> Persistenz -> Jetzt kompaktieren** triggers the same idempotent compactor manually. The supplied configurations use `./data/usage`; keep it on persistent storage if accounting history must survive container/host replacement.

## Model capability preflight

By default the gateway caches Ollama `/api/show` metadata for ten minutes and rejects known-incompatible capability requests before they consume a scheduler/worker slot. This produces a clear gateway error for cases such as a `tools` request sent to a completion-only model. Metadata lookup failure itself is fail-open.

```json
"model_capabilities": {
  "mode": "enforce",
  "cache_ttl": "10m",
  "context_guard": "reject",
  "context": {
    "max_requested_tokens": 32768,
    "default_worker_tokens": 4096,
    "estimation_margin_percent": 15,
    "vision_reserve_tokens_per_image": 2048
  }
}
```

For heterogeneous workers, `default_context_tokens` describes that worker's effective Ollama default when a model is neither loaded nor configured with a Modelfile `num_ctx`. `context_limits` is a hard per-worker/model ceiling using the same exact/prefix/`*` matching style as model concurrency:

```json
{
  "name": "gpu-1",
  "default_context_tokens": 4096,
  "context_limits": {
    "qwen3:*": 16384,
    "*": 32768
  }
}
```

The model table exposes **Model max**, **Configured** (Modelfile `num_ctx`) and **Loaded** (`/api/ps`) separately. Requests without a native `num_ctx` are routed only to workers whose effective context can hold the estimated request.

## Adaptive routing and per-model concurrency

Worker routing can combine model affinity with live GPU/VRAM telemetry and a per-worker/per-model EWMA of observed token throughput. Per-model limits prevent a large model from consuming the same concurrency as a much smaller model.

```json
"model_concurrency": {
  "qwen3:8b": 2,
  "gemma4:*": 1,
  "*": 1
}
```

On NVIDIA Linux or Windows hosts, `"nvidia_smi": true` enables dependency-free GPU utilization, VRAM, temperature and power telemetry. Windows host RAM uses `GlobalMemoryStatusEx`; Linux uses `/proc/meminfo`.

## Prometheus metrics

`/metrics` exposes bounded-cardinality metrics for queue/running state, worker slots, request totals, GPU/VRAM telemetry, per-model active requests, learned token throughput, dropped usage-journal events and raw/daily/monthly retention file/byte gauges plus last-compaction timestamp/reclaimed-byte gauges. Tenant/user identifiers are deliberately not emitted as Prometheus labels.

The exporter uses a **snapshot-then-render** path: registry/scheduler/worker locks are held only while copying state and are released before any HTTP response bytes are written. A slow or stalled Prometheus client therefore cannot block request completion or the inference hot path.

## Operational model

The design intentionally optimizes for one strong gateway process. Go can handle far more concurrent HTTP/streaming connections than a local LLM server can generally infer at once, so the GPU/model execution layer should become the bottleneck long before the gateway.

If the gateway process is replicated independently behind a generic load balancer, each replica has its own queue, quotas and sessions. That setup does **not** provide strict global fairness. For strict fairness, run one authoritative gateway and scale Ollama workers behind it.

## Tests

```bash
node --check internal/webui/assets/app.js
go test ./...
go vet ./...
go test -race ./...
```

See also `docs/ARCHITECTURE.md`, `docs/SECURITY.md` and `docs/IMPROVEMENTS-2026.md`. A tuned RTX 4090 example is available as `config.rtx4090.example.json`.

Multi-gateway HA is not part of the active roadmap after single-process performance proved sufficient for the current deployment. The deterministic benchmark and evidence tooling in `docs/HA-READINESS.md` is retained for future re-evaluation rather than removed.

### OpenWebUI: "Modelle konnten nicht abgerufen werden"

If OpenWebUI uses an Ollama connection with **Authentication: None**, its backend source IP must match an `auth.ip_bypass` CIDR. The default examples only bypass `127.0.0.1`/`::1`; Docker or another machine is therefore intentionally rejected. Authentication failures are logged with `client_ip=...` so an exact `/32` can be added. See `docs/OPENWEBUI.md`.

## P2/P3 operations and durable work

The current gateway includes the P2 operator/state features:

- **Warm Models**: persistent `hot`/`warm`/`cold` residency policies with placement-aware preload, idle unload, model-operation locking, and VRAM eviction suggestions. See `docs/WARM-MODELS.md`.
- **Alerts**: persistent firing/resolved events for worker health, circuit state, queue depth/wait, VRAM, OOM, storage, and quota thresholds. Generic HMAC-signed webhooks use bounded asynchronous delivery and transient retries. See `docs/ALERTS.md`.
- **Optional Responses conversations**: disabled by default; when enabled, `previous_response_id` is resolved from an AES-256-GCM encrypted, retention-bounded tenant/actor-scoped store. `store:false` remains a per-request opt-out. See `docs/CONVERSATIONS.md`.
- **Policy simulator**: evaluates identity/model/capability/placement/routing decisions without running inference.
- **Durable Batch Jobs (P3.1)**: disabled by default; accepted compute requests can be persisted, executed later under service class `batch`, paused/resumed/cancelled, recovered after restart, retained for a bounded window and inspected in the dedicated Admin -> Batch Jobs page. See `docs/BATCH-JOBS.md`.

Warm-model actions never override Model Placement or worker drain/disable state. Model unload/preload acquires a per-worker/per-model maintenance reservation so inference cannot race an unload operation.

## Hardened production Compose

For production Docker deployments, use `docker-compose.production.yml` with an explicit bootstrap config:

```sh
cp .env.production.example .env
# edit .env so GATEWAY_CONFIG points at the production JSON
./scripts/production-preflight.sh
docker compose -f docker-compose.production.yml up -d --build
```

The production Compose publishes `127.0.0.1:9080` by default, requires `GATEWAY_CONFIG`, runs the scratch image as UID/GID 65532 with a read-only root filesystem, drops all capabilities and enables `no-new-privileges`. See `docs/DEPLOYMENT-HARDENING.md` before using a non-loopback bind.

## Admin UI theme

Checkpoint 24 ships the Admin Control Plane as a light, data-dense technical dashboard. The theme uses explicit light browser controls, high-contrast status colors, compact mono-accent typography, light tables/forms/dialogs and matching light Live Flow/Infrastructure canvases. It has no external font/CDN dependency: preferred local IBM Plex/JetBrains/Cascadia faces fall back to platform fonts automatically.

The UI is embedded into the gateway binary. After upgrading from an earlier checkpoint, rebuild/redeploy the gateway image and perform one hard browser refresh if an old stylesheet is still cached. No gateway configuration or persistent-state migration is required for the theme update.


## Checkpoint 25

- Fixed sidebar navigation overflow on shorter viewport heights.
- Sidebar navigation now scrolls independently and keeps the footer visible.
- Added compact spacing rules for shorter screens.

## Checkpoint 26 — Dialog close validation fix

- Fixes **Tenant Policies → Override anlegen**: both the × button and **Abbrechen** now close the modal even while required fields are empty.
- Close controls are explicit `type="button"` actions using the shared `data-close-dialog` handler; only **Speichern** submits the policy form.
- Applies the same validation-independent close behavior to the existing **Modell pullen** dialog.
- Adds WebUI regression tests for close-target validity and required-field dialogs.
- No scheduler, proxy, auth, persistence, routing, quota, or inference behavior changed.

## Checkpoint 27 — Context / num_ctx Hardening

See `docs/CONTEXT-HARDENING.md`. The gateway now derives effective worker context from loaded `/api/ps`, Modelfile `num_ctx`, worker defaults and hard context caps; it also fixes Responses/output/instructions/suffix/vision context estimation.

## Public status dashboard

Checkpoint 28 adds an optional read-only public dashboard at `/status/`. It shows current queue/load, worker capacity, an infrastructure map and anonymized live request activity without requiring an API key. The public API is built from a separate allow-list and does not expose tenant/actor/application identities, worker URLs, policies, quotas, storage paths or error details. Worker and model names are anonymized by default and can be explicitly published or mapped to public display names. See `docs/PUBLIC-DASHBOARD.md`.
