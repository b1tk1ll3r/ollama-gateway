# Release verification — Checkpoint 28 public status dashboard

Checkpoint 28 replaces the active P3.2 HA roadmap item with an optional public, unauthenticated, read-only operational dashboard. Existing HA-readiness tools remain packaged but no cluster/coordinator behavior is introduced.

## Runtime scope

Changed runtime areas:

- strict configuration schema: new `public_dashboard` block;
- unauthenticated routing for the configured public dashboard path only;
- a dedicated sanitized public snapshot builder;
- embedded `internal/publicui` assets.

Unchanged semantics:

- inference/proxy protocol handling;
- scheduler/fairness/quota algorithms;
- worker selection and model placement;
- durable state schemas and storage files;
- admin authentication and admin UI authorization;
- batch/conversation persistence.

No state migration is required.

## Privacy verification

The public API is not derived by JSON-marshalling admin/infrastructure objects. Tests require an explicit allow-list and fail if known sensitive fixture values appear in the public response. Verified absent fields/data include tenant, actor, application, internal request ID, worker URL, labels, internal error strings, telemetry errors and estimated credits.

Worker names and model names default to anonymized display names. Worker aliases can be explicitly supplied in configuration. Resource metrics contain only numeric operational values.

## Automated verification

- `go test ./...` — PASS
- `go vet ./...` — PASS
- `go test -race ./...` — PASS
- `node --check internal/publicui/assets/app.js` — PASS
- `node --check internal/webui/assets/app.js` — PASS
- all four gateway release targets rebuilt — PASS
- every entry in `dist/SHA256SUMS` — PASS
- `config.example.json` strict parse test — PASS
- production-derived checkpoint-28 config through `-check-config` — PASS (2 workers, `/data` writable)

## Release-binary smoke

The packaged Linux amd64 gateway was started with a deterministic mock Ollama backend and an isolated state directory.

Verified:

- `/readyz` — 200
- `/status/` without credentials — 200
- `/status/api/snapshot` without credentials — 200
- `/gateway/ui-api/session` without credentials — 401
- `/gateway/ui-api/session` with admin API key — 200
- `/v1/chat/completions` with API key — 200
- post-request public snapshot contains the public worker alias and opaque `REQ-*` request ID
- post-request public snapshot does not contain private gateway/worker names, auth key, tenant/actor/application fields or worker URL
- SIGTERM shutdown — gateway exit code 0

## Deployment

Enable the dashboard explicitly in configuration. A recommended production-derived example is supplied separately as `gateway-config.checkpoint28.public-dashboard.json`; it publishes model names but maps the two internal M75q worker names to `GPU Node A` and `GPU Node B`.
