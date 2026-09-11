# Ollama Fair Gateway — P0–P3 implementation roadmap

This roadmap turns the gateway from a local fair scheduler into a production-oriented LLM control plane while preserving the project's core properties: one Go binary, no mandatory external state service, streaming-first proxying, and no prompt/response content persistence by default.

## Delivery principles

1. **Never block the inference hot path on durable storage.** Control-plane writes may be synchronous when correctness requires it; usage/metrics remain buffered/snapshotted.
2. **Never retry after client-visible output starts.** A retry is permitted only before response headers/body are committed.
3. **Hard policies precede adaptive scoring.** Model ACL → alias resolution → capability preflight → placement → maintenance/circuit eligibility → adaptive routing.
4. **Every runtime control must be observable and persistent when operationally meaningful.** Drain/disable state, API keys, policies, placement, aliases and durable batch definitions survive restart; active sockets, transient inference jobs and in-flight batch attempt contexts do not.
5. **Protocol compatibility remains explicit.** Native Ollama, OpenAI-compatible and later Anthropic-compatible errors/streaming semantics are handled independently.

---

## P0 — Production safety and policy foundation

### P0.1 Circuit breaker + safe pre-stream retry

**Goal:** stop repeatedly routing to unhealthy/OOM/transport-failing workers.

- Worker circuit states: `closed`, `open`, `half_open`.
- Configurable consecutive-failure threshold and open duration.
- Transport failures and backend 5xx contribute to the circuit.
- Only transport failures that happen before response commitment are retried.
- Retry excludes workers that already failed the current request.
- Admin UI shows circuit state, last circuit error and manual reset.
- Prometheus counters planned for opens/retries/failures.

**Acceptance:** a connection-reset worker opens its circuit and a request succeeds on another eligible worker without duplicate client-visible output.

### P0.2 Virtual models / aliases

**Goal:** clients use stable names such as `fast`, `coding`, `vision` rather than physical Ollama tags.

- Ordered fallback list of real models.
- Optional required capability set.
- Alias participates in `/api/tags` and `/v1/models` discovery.
- Gateway rewrites the outbound model while returning diagnostic headers:
  - `X-Gateway-Model-Alias`
  - `X-Gateway-Resolved-Model`
- Alias target must still satisfy worker placement and inventory.

**Acceptance:** OpenWebUI can select a virtual model and the backend receives the resolved physical model.

### P0.3 Model ACLs

**Goal:** answer “who may use which model?” independently from placement (“where may it run?”).

- Tenant baseline rules.
- API-key-specific allow/deny rules.
- Exact and trailing `*` patterns; most specific match wins, deny wins ties.
- ACL is enforced for direct model requests and model discovery.
- API-key create UI exposes allow/deny lists.

**Acceptance:** a key allowed only for `coding` cannot discover or directly call a denied physical model.

### P0.4 Worker maintenance / drain

**Goal:** take workers out of rotation without killing existing streams.

- `active`: accepts new work.
- `draining`: no new work; active jobs finish.
- `disabled`: no new work until explicitly re-enabled.
- State persisted in `worker-state.json`.
- UI buttons: Drain, Disable, Activate; circuit reset beside them.

**Acceptance:** a draining worker's active job completes while all new jobs route elsewhere.

---

## P1 — Client reach, QoS and observability

### P1.1 Anthropic `/v1/messages`

- Native Anthropic-compatible request/stream passthrough to Ollama.
- Token/usage extraction and protocol-correct errors.
- Tool, vision and thinking capability preflight.
- Same ACL/quota/placement/scheduler pipeline as Ollama/OpenAI.

### P1.2 Service classes / priority

- Classes: `interactive`, `system`, `background`, `batch`.
- Weighted scheduling without starvation.
- Per-class queue wait and concurrency ceilings.
- API-key/default mapping and optional request header override with scope.

### P1.3 Auto-tuning and benchmark profiles

- Measure TTFT, prompt tok/s, output tok/s, throughput and VRAM by model/concurrency.
- Suggest `max_concurrent` / per-model concurrency.
- Optional “apply recommendation” workflow with audit entry.
- Never auto-change production settings unless explicitly enabled.

### P1.4 OpenTelemetry

- OTLP traces/metrics, content capture off by default.
- Spans: auth, admission, queue, route, upstream, first-byte/stream.
- Correlate with `X-Request-ID`.

---

## P2 — Capacity automation and operator workflows

### P2.1 Warm/preload policies — ✅ implemented

- `hot`, `warm`, `cold` model classes.
- Explicit/preferred workers.
- Idle unload using Ollama `keep_alive: 0`.
- Optional pre-warm at gateway start/worker recovery.
- Memory-pressure-aware eviction suggestions.

### P2.2 Alerts and webhooks — ✅ implemented

- Conditions: worker down, circuit open, repeated OOM, queue depth/wait, storage growth, quota near exhaustion.
- Cooldown/deduplication.
- Generic signed webhook first; provider-specific integrations later.

### P2.3 Optional stateful conversations — ✅ implemented

- Opt-in conversation store for clients that need `previous_response_id` semantics.
- Separate encryption/retention policy because this stores content.
- Disabled by default to preserve current privacy posture.

### P2.4 Policy simulator — ✅ implemented early in P0/P1

- Simulate identity/model/capabilities/context without executing inference.
- Explain ACL, alias, placement, inventory, circuit, drain, concurrency and final routing score.
- UI provides a step-by-step decision trace.

---

## P3 — Batch and public operations visibility

### P3.1 Batch jobs — ✅ implemented

- Durable job definitions and status.
- Separate background scheduling class.
- Pause/resume/cancel.
- Input/output references rather than embedding large payloads in control state.
- Retention and accounting integrated with existing usage rollups.

### P3.2 Public status dashboard — ✅ implemented

- Separate unauthenticated, strictly read-only status surface.
- Current queue/load, worker capacity, infrastructure map and anonymized live flow.
- Explicit public-data allow-list; no tenant/actor/application identities or admin/control-plane state.
- Worker/model-name privacy controls and public worker aliases.
- HA/gateway clustering is removed from the active roadmap because observed single-process performance does not justify the distributed coordination cost. The HA-readiness tooling remains available if that operational assumption changes later.

---

## Planned implementation sequence

1. **Release A — P0 reliability foundation**: circuit breaker, safe retry, drain/disable persistence, UI controls.
2. **Release B — P0 policy surface**: model aliases and model ACLs with full runtime CRUD UI and persistence.
3. **Release C — P1 protocol/QoS**: Anthropic Messages + service classes.
4. **Release D — P1 observability/tuning**: OpenTelemetry + benchmark recommendations.
5. **Release E — P2 automation**: warm model manager, alerts, policy simulator.
6. **Release F — P2 stateful optional layer**: conversations with explicit content-retention controls.
7. **Release G — P3 batch**.
8. **Release H — P3 public status dashboard**; multi-gateway HA is deferred outside the active roadmap.

## Current implementation status

- P0.1: **implemented** (circuit breaker, safe pre-stream retry and reliability metrics).
- P0.2: **implemented** (aliases, discovery, capability-qualified resolution and persistent runtime CRUD from the admin API/UI with immediate publication for new requests).
- P0.3: **implemented** (tenant/API-key ACL enforcement, API-key ACL CRUD and persistent runtime Tenant Model Access CRUD from the admin API/UI).
- P0.4: **implemented** (persistent drain/disable + UI controls).
- P1.1: **implemented** (Anthropic Messages routing, metering and protocol-aware handling).
- P1.2: **implemented** (weighted service classes with queue/concurrency controls and scoped override header).
- P1.3: **implemented** (explicit benchmark profiles and apply-recommendation workflow).
- P1.4: **implemented** (OTLP/HTTP tracing with content capture off by default).
- P2.1: **implemented** (warm/preload policies).
- P2.2: **implemented** (alerts + signed webhooks).
- P2.3: **implemented** (optional AES-256-GCM encrypted Responses conversation store with retention, `store:false`, identity scoping and `previous_response_id` expansion).
- P2.4: **implemented** (policy simulator).
- P3.1: **implemented** (durable metadata + separate content spool, batch service-class execution through the normal gateway pipeline, pause/resume/cancel, restart recovery, retention, owner API, admin UI/API and accounting integration).
- P3.2: **implemented** as the public status dashboard in checkpoint 28. Multi-gateway HA is intentionally removed from the active roadmap after observed single-process performance showed no current capacity justification. The existing HA-readiness tools remain as diagnostic evidence tooling, not as a commitment to add distributed coordination.

### Checkpoint 23 note

At checkpoint 23, HA remained gated. Checkpoint 23 further reduces single-node production risk by hardening the Docker/reverse-proxy boundary: loopback-only publishing by default, an explicit production Compose file with no example-config fallback, read-only container rootfs/capability dropping/no-new-privileges, and a host preflight that rejects accidental public binding. This does not constitute HA implementation and does not change inference or persistent-state semantics.

### Checkpoint 24 note

At checkpoint 24, HA remained gated. Checkpoint 24 does not alter runtime scheduling, routing, persistence or HA semantics; it replaces the dark admin presentation with a professional light, data-dense control-plane theme. The embedded Live Flow and Infrastructure canvases were updated together with CSS so the UI remains visually coherent. No config/state migration is required.

### Checkpoint 27 note

At checkpoint 27, HA remained gated. Checkpoint 27 hardens context-window admission and routing before any HA work: the gateway no longer equates the theoretical `/api/show` model maximum with the context actually available on a worker. Effective context now derives from loaded `/api/ps context_length`, Modelfile `num_ctx`, explicit worker defaults and per-worker/model caps. Native `options.num_ctx` is validated/capped, while OpenAI/Responses estimation now covers `max_output_tokens`, `instructions`, `suffix`, vision reserves and an admission margin. Context-suitable workers are selected before inference, and the policy simulator/model UI expose the effective context evidence.


### Checkpoint 28 note

Checkpoint 28 closes the active P3 roadmap with an optional public read-only status dashboard. `/status/` exposes current load, queue, worker capacity, an infrastructure map and anonymized live request state through a dedicated sanitized API. It never reuses the admin snapshot schema. HA remains available only as a future re-evaluation if measured availability or capacity requirements change.
