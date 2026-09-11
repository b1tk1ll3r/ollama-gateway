# Persistent local state

The gateway deliberately separates the inference hot path from durable state. Fair queue ordering, active worker slots, live requests and browser OIDC sessions remain in memory. Restart-worthy control-plane/accounting data is stored under `storage.data_dir` using only the Go standard library.

## Files

| File | Contents | Write pattern |
|---|---|---|
| `gateway-config.json` | configuration override saved from Admin -> Konfiguration | atomic replace |
| `api-keys.json` | UI-created key metadata + SHA-256 hashes | atomic replace |
| `policies.json` | tenant policy overrides | atomic replace |
| `metrics.json` | Prometheus counters and histogram state | periodic atomic snapshot + shutdown |
| `quota.json` | actor/tenant credit bucket balances | periodic atomic snapshot + shutdown |
| `worker-performance.json` | learned per-worker/model prompt/output tok/s EWMA | periodic atomic snapshot + shutdown |
| `model-placement.json` | live per-worker model placement overrides from the admin UI | atomic replace |
| `conversations.enc.json` | optional encrypted Responses conversation contexts | synchronous encrypted atomic replace + retention cleanup |
| `batch-jobs.json` | optional durable batch definitions, identity metadata, state and spool references | synchronous atomic replace on state transitions |
| `batch/input/*.json` | durable batch request payloads | create + fsync + rename, mode 0600 |
| `batch/output/*.response` | durable batch response payloads | temp file + fsync + rename, mode 0600 |
| `usage/usage-YYYY-MM-DD.jsonl` | request-level accounting inside detail retention | append-only, buffered |
| `usage/rollups/daily/rollup-daily-YYYY-MM-DD.json` | daily dimensional usage aggregates | atomic replace during compaction |
| `usage/rollups/monthly/rollup-monthly-YYYY-MM.json` | long-term monthly dimensional aggregates | idempotent atomic replace |

By default, no prompt text or generated response content is written by these stores. There are two explicit opt-in exceptions: `conversations.enabled=true` stores Responses context encrypted at rest, while `batch_jobs.enabled=true` allows submitted batch request/response content to be written to the local spool in plaintext files protected by filesystem permissions. See `docs/CONVERSATIONS.md` and `docs/BATCH-JOBS.md`.

## Bootstrap and persistent configuration

The file passed with `-config` is the bootstrap configuration. It determines `storage.data_dir` and the state filenames. If `<data_dir>/gateway-config.json` exists and validates, it becomes the effective configuration for the process.

The web JSON editor saves a complete validated override but never exposes API-key secrets, the UI session secret or the browser OIDC client secret. Redacted secret values are preserved from the currently effective configuration. API keys themselves should be managed through the dedicated Security page.

The `storage` section is bootstrap-only. This prevents the persistent configuration from moving the file that is needed to locate itself. To move the state directory, stop the gateway, update the bootstrap config and move/copy the state directory explicitly.

## Crash and shutdown semantics

Atomic state files are written through a temporary file, `fsync`, and rename. API keys, tenant policies, model-placement overrides and durable batch state transitions are saved synchronously when correctness requires it. Metrics, quota and learned worker-performance snapshots are refreshed every `storage.flush_interval` and once again during graceful shutdown. A hard process/host crash can therefore lose at most the most recent snapshot interval for those snapshot files; a batch attempt left as `running` is repaired to `queued` during startup recovery.

Usage events are appended asynchronously and flushed according to `usage.flush_interval`. On normal shutdown the queue is drained and the current journal is flushed/synced. Before startup replay, retention compaction runs once so expired request rows are never needlessly loaded back into memory. Startup then reconstructs all-time counters from monthly rollups, daily rollups and the remaining detail journals. Only detail-journal events populate the recent-request UI.


## Tiered usage retention and aggregation

The default retention policy is:

```json
"retention": {
  "detail_days": 30,
  "daily_days": 400,
  "monthly_months": 0,
  "compaction_interval": "6h"
}
```

- **Detail**: raw request metadata for the most recent 30 calendar days.
- **Daily**: older request rows are replaced by one aggregate file per day until day 400.
- **Monthly**: older daily files are folded into monthly files. `monthly_months: 0` means keep them forever.

Each rollup stores independent aggregate dimensions for global traffic, tenants, actors, applications, models and workers. Stored measures include requests/errors, prompt/completion/cached tokens, credits, queue/service duration, prompt/eval nanoseconds and transferred bytes. Prompt/output token throughput can therefore be derived after request details have expired.

Monthly files keep their source day keyed internally. This is deliberate: if the process crashes after the monthly file is synced but before the daily source file is deleted, retrying compaction replaces that day contribution instead of adding it twice.

Compaction only touches closed historical journal days. The active day's writer is never rewritten. The background interval can be configured, and admins can trigger the same compaction with `POST /gateway/ui-api/storage/compact` or **Admin -> Persistenz -> Jetzt kompaktieren**.

Historical aggregate queries are exposed as:

```text
GET /gateway/ui-api/usage/rollups?granularity=daily&dimension=global&limit=120
GET /gateway/ui-api/usage/rollups?granularity=daily&dimension=tenant&name=team-a
GET /gateway/ui-api/usage/rollups?granularity=daily&dimension=actor&name=team-a%00alice
GET /gateway/ui-api/usage/rollups?granularity=monthly&dimension=model&name=qwen3:8b
```

The Prometheus endpoint exports gauges for raw/daily/monthly file counts and byte footprints, plus the timestamp of the last compaction and the number of bytes reclaimed by that run. The cumulative Prometheus counter snapshot itself remains fixed-size and does not need retention compaction.

## Intentionally non-persistent state

- fair-queue heap and virtual clocks;
- running/streaming transient inference jobs and cancellation handles;
- active batch attempt contexts/cancellation handles (the durable batch definition and state remain persistent);
- active worker/model slots;
- live-flow/infrastructure animation state;
- browser OIDC sessions;
- transient model pull operation state.

These objects are tied to sockets, requests or process-local execution and cannot safely be resumed after a restart.

## Backup

For a consistent offline backup, stop the gateway and copy `storage.data_dir`. For normal filesystem snapshots, the atomic JSON files and append-only usage journals are safe to copy while the process is running, though the newest buffered usage records may not yet be present on disk.

## Admin storage controls

**Admin -> Persistenz** shows every durable state file plus detail/daily/monthly usage and batch-spool footprint. **Jetzt flushen** forces metrics, quota, worker performance, encrypted conversation retention state, durable batch metadata, and buffered usage to disk. **Jetzt kompaktieren** applies usage/conversation/batch retention immediately. **Backup herunterladen** flushes first and streams a ZIP containing durable state, batch spool content when present, remaining detail journals and all rollups. Treat the backup as sensitive because configuration can contain deployment secrets and enabled batch jobs can contain prompt/response content.
