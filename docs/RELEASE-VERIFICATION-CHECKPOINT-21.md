# Checkpoint 21 — telemetry freshness hardening

Date: 2026-09-08

## Scope

Checkpoint 21 hardens the optional remote worker telemetry path introduced in checkpoint 20. No persistent-state schema changes are introduced and the inference/quota/scheduler hot paths are unchanged.

Changes:

- parse optional external `updated_at`,
- reject samples older than `max(30s, 6 x health_interval)`,
- reject timestamps more than 30 seconds in the future,
- keep backward compatibility for exporters without timestamps,
- append external HTTP/decode/freshness errors instead of overwriting an existing collector warning,
- send `Cache-Control: no-store` and `X-Content-Type-Options: nosniff` from the shipped telemetry agent.

## Safety behavior

A rejected external sample is not merged into `ResourceTelemetry`. Static worker capacity hints, Ollama health/inventory and all other routing inputs remain available. The worker remains usable; the telemetry error is surfaced for operators.

## Verification

Release verification includes unit tests for fresh/stale/future/missing timestamps, merge semantics, full repository tests/vet, race tests, release cross-builds, dist checksums and a real gateway + mock Ollama + telemetry-agent smoke path.

## E2E freshness evidence

Fresh-agent path:

- agent response included `Cache-Control: no-store`,
- source was `telemetry-url:host-memory+amdgpu-sysfs`,
- simulated VRAM/GPU/temperature/power values were merged correctly.

Stale-exporter path:

- exporter was made ready before gateway startup,
- payload timestamp was intentionally more than one hour old,
- gateway reported `telemetry sample is stale: ... exceeds 30s`,
- stale VRAM/GPU pressure values were not merged,
- gateway still shut down cleanly with exit status 0.
