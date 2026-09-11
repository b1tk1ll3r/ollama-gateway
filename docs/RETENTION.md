# Usage retention and long-term aggregation

The gateway stores request-level usage as append-only daily JSONL journals and compacts old data into bounded rollups.

## Default policy

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

- `detail_days`: keep per-request records for this many calendar days.
- `daily_days`: retain one daily rollup until this age. Must be >= `detail_days`.
- `monthly_months`: retain monthly rollups for this many months. `0` means forever.
- `compaction_interval`: background compaction cadence; minimum one minute.

## Storage layout

```text
data/usage/
├── usage-2026-09-06.jsonl
└── rollups/
    ├── daily/
    │   └── rollup-daily-2026-07-23.json
    └── monthly/
        └── rollup-monthly-2025-08.json
```

Each daily/monthly rollup retains aggregate dimensions for global traffic, tenant, actor, application, model, and worker.

Measures retained after request details expire include request/error counts, prompt/completion/cached tokens, credits, queue/service time, prompt/eval nanoseconds, bytes in/out, and last-request timestamp. Prompt and output tokens/second remain derivable from aggregate evaluation durations.

## Crash/idempotency behavior

Raw -> daily compaction writes and fsyncs the new daily file before deleting the source journal. Repeating this after a crash replaces the same daily period rather than summing it twice.

Daily -> monthly files retain a map of their source days. If a process dies after the monthly file is committed but before the source daily file is deleted, replaying compaction replaces that day entry. This makes the monthly fold idempotent.

## Startup and all-time accounting

Before replaying request journals, startup applies retention once. It then reconstructs all-time usage from retained monthly rollups, retained daily rollups, and the remaining request-level journals. Only remaining request-level journals populate the recent-request table.

If `monthly_months > 0`, data older than that window is deleted completely and removed from reconstructed all-time usage.

## Admin/API

```text
POST /gateway/ui-api/storage/compact
GET  /gateway/ui-api/usage/rollups?granularity=daily&dimension=global&limit=120
GET  /gateway/ui-api/usage/rollups?granularity=daily&dimension=tenant&name=team-a
GET  /gateway/ui-api/usage/rollups?granularity=monthly&dimension=model&name=qwen3:8b
```

Allowed dimensions: `global`, `tenant`, `actor`, `application`, `model`, `worker`.

The Admin UI exposes the same controls under **Persistenz** and historical aggregates under **Nutzung**.

## Prometheus

```text
ollama_gateway_usage_raw_files
ollama_gateway_usage_daily_rollup_files
ollama_gateway_usage_monthly_rollup_files
ollama_gateway_usage_raw_bytes
ollama_gateway_usage_daily_rollup_bytes
ollama_gateway_usage_monthly_rollup_bytes
ollama_gateway_usage_last_compaction_timestamp_seconds
ollama_gateway_usage_last_reclaimed_bytes
```

No tenant/user labels are added to `/metrics`; dimensional long-term analysis belongs to the rollup API/UI, avoiding unbounded Prometheus cardinality.
