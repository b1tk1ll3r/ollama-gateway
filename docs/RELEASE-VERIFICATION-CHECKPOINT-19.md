# Checkpoint 19 — deployment preflight and container hardening

Validation date: 2026-09-08

## Scope

Checkpoint 19 follows the Checkpoint 18 Admin UI hotfix. It does not implement P3.2/HA and does not change any persistent-state schema. Its purpose is to reduce single-node production update risk after real deployment feedback exposed configuration mount/entrypoint and file-permission mistakes that were otherwise only visible after startup.

The inference request/scheduler/quota/proxy path is unchanged by this checkpoint. Runtime changes are limited to startup configuration loading being factored through the same helper used by the new preflight, plus two explicit CLI-only modes (`-check-config` and `-probe`).

## Changes

- `-check-config`: strict effective configuration validation plus persistent override loading and state-directory write probe.
- Secret-free JSON preflight summary with worker count and deployment warnings.
- `-probe`: minimal HTTP probe implemented inside the gateway binary for scratch/container health checks.
- Dockerfile/Compose liveness healthcheck against public `/healthz`.
- Corrected Compose command: arguments only, no duplicate `/ollama-gateway` after the image `ENTRYPOINT`.
- `GATEWAY_CONFIG` Compose variable for selecting the bootstrap config explicitly.
- Regression tests for persistent-secret restoration, preflight output, HTTP probing and Compose entrypoint/healthcheck rules.
- Production deployment documentation.

## Verification completed

The final source tree passed:

- `go test ./... -count=1`
- `go vet ./...`
- Race detector over all packages, split as `go test -race ./cmd/...` and `go test -race ./internal/...` so each command completed with an observable exit status. (The single monolithic invocation exceeded the execution harness timeout; no package was omitted from the split run.)
- `node --check internal/webui/assets/app.js`
- Compose YAML parse/assertions for the corrected command, `GATEWAY_CONFIG` mount and healthcheck. The Docker CLI itself was not available in the verification environment, so `docker compose config` was not claimed.
- Real Linux-amd64 binary `-check-config` run against the migrated production configuration: `status=ok`, exactly 2 effective workers, writable state directory, expected remote `local_system_stats` warnings, and no API-key secret in the preflight output.
- Real gateway + deterministic mock Ollama smoke: `/healthz` probe, `/readyz` probe, authenticated `/gateway/ui-api/session`, OpenAI-compatible inference, SIGTERM, and gateway exit code 0.
- Cross-builds for Linux amd64, Linux arm64, macOS arm64 and Windows amd64.
- `dist/SHA256SUMS` verification for every packaged binary/helper.

## Gateway binary SHA-256

| Target | SHA-256 |
|---|---|
| Linux amd64 | `e30a7b01f8a96f6396c3582ab208bf8e4e330c6a882b724998448dc66dfa7cf7` |
| Linux arm64 | `675a72c054c21ba383c36891b9bdeb1b93cbd4690abedc22b74ae02c8bc081e4` |
| macOS arm64 | `586e489cf00c54aa5906a63f4ceada6ff22c90943a80d31fb1c864b557dde559` |
| Windows amd64 | `1d24976651a358bc80031f36eb2f59cca9233dfa89bf3189bb1bb4b6e9de96fe` |

The HA helper binaries were not modified by this checkpoint and their existing checksums remain listed and verified in `dist/SHA256SUMS`.

## Update guidance

No configuration-schema or persistent-state migration is required from Checkpoint 18. Container deployments should rebuild/recreate the image because the Dockerfile healthcheck and gateway CLI modes are part of this release.

Before starting production traffic, run the candidate container in preflight mode with the exact config/state mounts:

```sh
GATEWAY_CONFIG=./gateway-config.json docker compose run --rm gateway \
  -config /etc/ollama-gateway/config.json -check-config
```

Then start the service, wait for liveness, require `/readyz`, authenticate to the admin UI/API and send representative inference traffic. See `docs/DEPLOYMENT-HARDENING.md` and `docs/PRODUCTION-UPDATE.md`.
