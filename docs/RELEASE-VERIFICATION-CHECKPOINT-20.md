# Checkpoint 20 — remote worker telemetry

Date: 2026-09-08

## Scope

Checkpoint 20 extends the single-node production hardening path with an optional `worker-telemetry` agent for remote Ollama hosts. It also fixes the existing external telemetry merge so omitted JSON fields no longer overwrite previously collected values with zero, while explicit zero values remain meaningful.

No persistent-state schema changes are introduced. The inference, quota and scheduler request paths are unchanged.

## New artifacts

- `cmd/worker-telemetry`
- `dist/ollama-gateway-worker-telemetry-linux-amd64`
- `dist/ollama-gateway-worker-telemetry-linux-arm64`
- `docs/WORKER-TELEMETRY.md`
- Linux AMDGPU sysfs collector in `internal/hoststats`

## Verification completed

The final source tree passed:

- `go test ./...`
- `go vet ./...`
- JavaScript syntax validation with `node --check internal/webui/assets/app.js`
- race tests across all command packages and all tested internal packages, including `internal/worker` and `internal/hoststats`
- SHA-256 verification for every file listed in `dist/SHA256SUMS`
- cross-builds for gateway Linux amd64/arm64, macOS arm64 and Windows amd64
- cross-builds for worker telemetry Linux amd64/arm64

## Remote telemetry E2E

A Linux-amd64 release gateway was started with:

- one mock Ollama worker,
- `local_system_stats: false`,
- `telemetry_url` pointing at the release worker-telemetry agent,
- a temporary simulated AMDGPU sysfs tree.

The gateway reported the worker healthy and exposed telemetry source:

`telemetry-url:host-memory+amdgpu-sysfs`

The simulated GPU values were preserved end-to-end:

- VRAM used: 4,294,967,296 bytes
- VRAM total: 17,179,869,184 bytes
- GPU utilization: 73%
- GPU temperature: 62.5 C
- GPU power: 88 W

Both the gateway and telemetry agent exited cleanly with status 0 after SIGTERM.

## Merge regression coverage

`externalTelemetry` uses pointer fields. This explicitly distinguishes an omitted JSON property from a supplied zero. Tests verify that omitted memory/GPU fields preserve existing telemetry while an explicit zero for VRAM usage or GPU utilization is applied.

## Production rollout

Enabling the agent is optional and can be staged independently from the gateway update. For a remote worker:

```json
{
  "local_system_stats": false,
  "telemetry_url": "http://WORKER-IP:11500/telemetry"
}
```

Validate the agent locally with `-once` first and restrict network access using `-allow-cidrs` plus the worker host firewall. See `docs/WORKER-TELEMETRY.md`.
