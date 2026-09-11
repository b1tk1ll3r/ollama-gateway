# Checkpoint 27 release verification

Checkpoint 27 hardens effective context-window handling and `num_ctx` admission/routing.

## Scope

Runtime changes are intentionally limited to context estimation, model metadata, context-aware worker eligibility/routing, the policy simulator, configuration validation, and the corresponding Admin UI context columns. Scheduler fairness, authentication, persistence formats, batch execution, conversations, and quota accounting are otherwise unchanged.

## Effective context model

The gateway now keeps these values distinct:

1. **Model maximum** from Ollama `/api/show` model metadata.
2. **Configured context** from a Modelfile `PARAMETER num_ctx`, when present.
3. **Loaded context** from Ollama `/api/ps context_length`, when the model is resident.
4. **Worker default** from `workers[].default_context_tokens` or the gateway context default.
5. **Administrative cap** from `workers[].context_limits` and `model_capabilities.context.max_requested_tokens`.

For OpenAI-compatible requests, the effective usable context is derived from the actually loaded/configured/default context and administrative limits; the theoretical model maximum is no longer treated as the runtime context by itself.

For native Ollama `/api/*` requests, a valid explicit `options.num_ctx` can request a resize/reload, but is still bounded by the model maximum, gateway cap, and worker context limit. A worker whose currently loaded context is smaller remains eligible only as a resize candidate and does not receive a misleading loaded-context routing advantage.

## Request estimation fixes

Context admission now includes:

- Responses API `max_output_tokens`;
- Responses API `instructions`;
- native `/api/generate` `suffix`;
- configurable estimation margin;
- configurable per-image vision reserve;
- the largest positive output-token budget rather than silently preferring a smaller field.

`options.num_ctx` is strictly validated as a positive integer. It is accepted only on native Ollama `/api/*` endpoints. OpenAI-compatible requests cannot inject it as a false context override.

## Validation performed

The release candidate passed:

- `go test ./...`
- `go vet ./...`
- `node --check internal/webui/assets/app.js`
- `go test -race ./...`
- strict `-check-config` against the supplied two-M75q production configuration
- Linux amd64, Linux arm64, macOS arm64, and Windows amd64 gateway cross-builds
- `dist/SHA256SUMS` verification
- a real Linux-amd64 release-binary smoke test against the deterministic mock Ollama backend:
  - `/readyz` ready
  - Admin API-key session HTTP 200
  - `/v1/chat/completions` HTTP 200
  - native string-valued `options.num_ctx` HTTP 400 before backend execution
  - OpenAI `options.num_ctx` spoof attempt HTTP 400
  - SIGTERM shutdown exit code 0

The final ZIP is additionally unpacked and tested again before release.

## Recommended production settings for the current two-M75q deployment

The supplied production config uses conservative starting bounds for the ~16 GiB AMD worker class:

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

and per worker:

```json
"default_context_tokens": 4096,
"context_limits": {
  "*": 16384
}
```

`16384` is an operational safety cap, not a claim that every model/workload will fit efficiently at 16k. Increase it only after measuring actual VRAM pressure, offload behavior, latency, and throughput on the target worker/model combination.
