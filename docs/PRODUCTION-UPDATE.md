# Production update checklist

This release is intended to be safe to stage as an update of an existing single-process Ollama Fair Gateway. The sustained HA-readiness work is isolated from the inference hot path: `cmd/ha-sampler`, the readiness shell wrapper and report tooling do not participate in normal request handling.

## Scope of this checkpoint

Relative to checkpoint 16, the gateway runtime behavior and persisted-state schemas are unchanged. The release adds/refines only HA-readiness tooling and documentation. The packaged gateway binaries should therefore remain byte-identical to checkpoint 16 when built with the same Go toolchain and flags; the release verification records this explicitly.

If the production system is older than checkpoint 16, treat this as a normal application upgrade because earlier checkpoints added runtime alias/tenant ACL controls, conversations and durable batch state. The configuration loader supplies defaults for omitted optional sections, but a backup is still required before replacing an older binary.

## Before updating

1. Record the currently deployed binary checksum and keep the old binary available for rollback.
2. Back up the bootstrap configuration and `storage.data_dir`. For the most consistent backup, stop the gateway before copying the data directory; see `docs/PERSISTENCE.md`.
3. Validate the intended effective configuration with `ollama-gateway -config <bootstrap.json> -check-config`. For containers, run this in the candidate image with the production config/state mounts so file permissions are tested too; see `docs/DEPLOYMENT-HARDENING.md`.
4. Confirm there is enough free space for the existing usage/batch/conversation retention settings.
5. Do not run the HA readiness sweep against production traffic unless the additional synthetic inference load is acceptable.

## Update procedure

1. Stop the supervised gateway gracefully with SIGTERM and wait for the `gateway stopped` log line.
2. Replace only the gateway executable appropriate for the host architecture. Verify its SHA-256 value against `dist/SHA256SUMS`.
3. Keep the existing bootstrap config and data directory in place.
4. Start the gateway under the same supervisor/service account.
5. Require both `/healthz` and `/readyz` to succeed before restoring external traffic.
6. Send at least one representative non-streaming and, if used in production, one streaming request through the normal authenticated client path.
7. Verify `/metrics`, the admin UI/API used operationally, and recent logs for persistence/configuration errors.

## Rollback

If startup/readiness or representative inference fails, stop the new process and restore the previous executable. When upgrading from checkpoint 16 specifically, no state-schema rollback is required by this checkpoint because the runtime/persistence code is unchanged. When upgrading from an older build, retain the pre-upgrade data-directory backup until the update has been observed successfully under normal load.

## HA readiness tooling in production

`GATEWAY_PID` must be the actual gateway PID. With it set, `scripts/ha-readiness.sh` captures before/after snapshots and starts `cmd/ha-sampler` during each load level. The sampler is read-only with respect to the gateway process; on Linux it reads `/proc` and uses `ps`, and it writes evidence only to the selected `OUT_DIR`. It does not write secrets or gateway state.

## Checkpoint 18 Admin UI hotfix

If upgrading from checkpoint 17, no configuration or persistent-state migration is required. Rebuild/redeploy the gateway executable or container because the Admin UI assets are embedded in the binary. Checkpoint 18 restores the Model Placement UI implementation and adds a regression test for missing render-dispatch functions.


## Checkpoint 19 deployment hardening

Checkpoint 19 keeps P3.2/HA gated and instead hardens the single-node production update path. The shipped Compose command no longer repeats the image entrypoint, the scratch image has a built-in liveness healthcheck via the gateway's `-probe` mode, and `-check-config` validates the effective bootstrap+persistent configuration plus state-directory writability before startup. No persisted-state schema migration is introduced by this checkpoint.

## Checkpoint 20 remote worker telemetry

Checkpoint 20 adds an optional out-of-band worker telemetry agent and hardens the existing `workers[].telemetry_url` merge path. It does **not** change any persistent-state schema and the agent is not part of inference request execution. Existing deployments can upgrade without enabling the agent; behavior remains unchanged until `telemetry_url` is configured.

For remote workers, prefer `local_system_stats: false` and point `telemetry_url` at the agent running on the actual Ollama host. Roll this out in two stages: first deploy/verify each agent with `-once` and its `/telemetry` endpoint, then change the gateway worker configuration. Keep the static `memory_capacity_bytes` and `vram_capacity_bytes` values as capacity hints even when runtime telemetry is enabled.

If telemetry becomes unavailable, the gateway continues operating; the worker telemetry snapshot records the error and routing falls back to the remaining configured/observed signals. Do not expose the agent on an untrusted network. Restrict `-allow-cidrs` to the gateway source address or protect the endpoint with a TLS-authenticated reverse proxy.

## Checkpoint 21 telemetry freshness hardening

Checkpoint 21 does not change persistent-state schemas or inference semantics. It validates timestamps from `telemetry_url` exporters, rejects stale/future samples before they affect routing pressure, preserves pre-existing local collector warnings when external telemetry fails, and makes the shipped worker agent explicitly non-cacheable. Existing exporters without `updated_at` remain compatible.


## Checkpoint 22 IP-bypass hardening

Checkpoint 22 changes only the authentication source used by credential-free `auth.ip_bypass`: the TCP peer is now authoritative by default. Forwarded client-IP resolution remains available for logs and usage attribution. Deployments that intentionally relied on an original `X-Forwarded-For` client address to satisfy IP bypass must explicitly set `auth.ip_bypass_use_forwarded_ip=true` and should restrict `auth.trusted_proxies` to exact proxy addresses plus network ACLs. API-key and OIDC authentication are unaffected. No persistent-state schema migration is introduced.

## Checkpoint 23 reverse-proxy/deployment boundary hardening

Checkpoint 23 does not change inference, scheduler, quota or persistent-state schemas. The supplied Compose topology now defaults to host-loopback publishing (`127.0.0.1:9080`), a read-only root filesystem, `no-new-privileges` and all Linux capabilities dropped. Deployments whose reverse proxy is on another host must explicitly opt into a non-loopback bind and should firewall the published port to that proxy only.

Before rollout, run `scripts/production-preflight.sh` with the production bootstrap config. The script refuses the development `config.example.json` and non-loopback publishing unless explicitly acknowledged, then runs the gateway's secret-redacted `-check-config` inside the candidate container with the production mounts.
