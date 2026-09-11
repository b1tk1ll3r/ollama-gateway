# Release verification: checkpoint 23

Checkpoint 23 hardens the Docker/reverse-proxy deployment boundary. It does not change the inference hot path or persisted-state schemas.

## Scope verification

`cmd/` and `internal/` were byte-for-byte/diff checked against checkpoint 22 after the deployment changes. No runtime Go source changed. Existing release binaries therefore remain unchanged and all entries in `dist/SHA256SUMS` still verify.

## Deployment changes

- Development Compose defaults to `127.0.0.1:9080 -> :8080` instead of publishing on all host interfaces.
- `docker-compose.production.yml` requires `GATEWAY_CONFIG`; there is no production fallback to `config.example.json`.
- Container root filesystem is read-only; `/data` is the persistent writable volume and `/tmp` is tmpfs.
- Runtime explicitly uses UID/GID `65532:65532`, drops all Linux capabilities and enables `no-new-privileges`.
- JSON-file logging is bounded to five 10 MiB files in the production Compose example.
- `scripts/production-preflight.sh` rejects `config.example.json`, rejects non-loopback publishing unless explicitly acknowledged, renders Compose, and invokes the gateway's secret-redacted `-check-config` in the candidate container.
- `scripts/production-preflight_test.sh` regression-tests loopback success, example-config rejection, wildcard-bind rejection and explicit remote-proxy opt-in.

## Production config validation

The hardened two-worker production configuration was validated with the release binary:

- status: `ok`
- workers: `2`
- data directory: `/data`
- state directory writable: `true`
- broad trusted-proxy warnings: none

The configuration narrows `auth.trusted_proxies` to loopback plus the observed Docker peer `172.30.3.1/32`; `auth.ip_bypass_use_forwarded_ip` remains false.

## Test results

- `go test ./...`: PASS
- `go vet ./...`: PASS
- `go test -race ./cmd/...`: PASS
- `go test -race ./internal/...`: PASS
- `sh -n scripts/production-preflight.sh`: PASS
- `sh -n scripts/production-preflight_test.sh`: PASS
- `scripts/production-preflight_test.sh`: PASS
- Compose YAML parse check: PASS
- `dist/SHA256SUMS`: all 10 packaged binaries PASS

## Release-binary smoke test

The shipped Linux-amd64 gateway binary was started against an isolated deterministic Ollama mock and temporary state directory.

- `/healthz`: 200 / `status=ok`
- `/readyz`: 200 / `status=ready`
- authenticated `/gateway/ui-api/session`: admin identity returned
- authenticated `/v1/chat/completions`: successful response (`chatcmpl-mock`)
- SIGTERM gateway shutdown: exit code 0 and `gateway stopped` logged

The mock process is test infrastructure and is not part of the production artifact.

## Deployment recommendation

For the observed topology, keep `GATEWAY_PUBLISH_ADDRESS=127.0.0.1` so only a reverse proxy on the Docker host can reach port 9080. If the reverse proxy is remote, bind to one specific host interface, set `ALLOW_NON_LOOPBACK_BIND=1` only for the preflight, and firewall the published port to the exact reverse-proxy source IP.
