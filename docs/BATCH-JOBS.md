# Durable batch jobs

Durable batch jobs are the P3.1 background-execution layer. They are **disabled by default** because each submitted job intentionally persists its request body and, when available, its response body.

## Configuration

```json
"batch_jobs": {
  "enabled": false,
  "retention": "168h",
  "max_jobs": 1000,
  "max_concurrent": 1,
  "max_input_bytes": 16777216
}
```

The service class `batch` must also exist under `service_classes.classes`. `batch_jobs.max_input_bytes` must not exceed `server.max_body_bytes`.

Storage paths are bootstrap-only:

```json
"storage": {
  "batch_jobs_file": "batch-jobs.json",
  "batch_jobs_dir": "batch"
}
```

## Client API

A batch job wraps one normal configured compute `POST` endpoint. The request body is the JSON body that the gateway will replay later.

```http
POST /gateway/v1/batches
Authorization: Bearer <credential>
Content-Type: application/json

{
  "path": "/api/chat",
  "body": {
    "model": "qwen3:8b",
    "messages": [{"role":"user","content":"Summarize the report"}],
    "stream": false
  }
}
```

A successful submission returns `202 Accepted`, a `Location: /gateway/v1/batches/<id>` header and the durable job metadata.

Owner-scoped endpoints:

```text
GET  /gateway/v1/batches
GET  /gateway/v1/batches/<id>
GET  /gateway/v1/batches/<id>/output
POST /gateway/v1/batches/<id>/pause
POST /gateway/v1/batches/<id>/resume
POST /gateway/v1/batches/<id>/cancel
```

A caller can only see or control jobs created by the same authenticated tenant and scheduler actor. A cross-identity lookup is returned as not found.

## Admin UI and API

Administrators have a dedicated **Batch Jobs** page with all durable jobs, state, attempts, identity metadata, model/path, pause/resume/cancel controls and authenticated output download.

Admin endpoints require `gateway:admin`:

```text
GET  /gateway/ui-api/batches
GET  /gateway/ui-api/batches/<id>
GET  /gateway/ui-api/batches/<id>/output
POST /gateway/ui-api/batches/<id>/pause
POST /gateway/ui-api/batches/<id>/resume
POST /gateway/ui-api/batches/<id>/cancel
```

## Execution semantics

Every attempt is replayed into the same gateway compute path as an interactive request, with the original identity metadata and a forced service class of `batch`. Therefore the attempt still passes through:

- tenant model ACLs and the submitted key-specific ACL snapshot;
- alias resolution, capability preflight, placement, maintenance state and circuit eligibility;
- quota reservation/reconciliation;
- weighted fair scheduling and the configured `batch` class concurrency ceiling;
- adaptive worker routing and normal safe pre-stream retry behavior;
- usage metering, request IDs, alerts and OpenTelemetry.

The submission credential itself is never stored. The durable identity snapshot contains tenant/subject/application/scopes and the key-specific model ACL needed to execute an already accepted job. Current tenant policy, worker state, placement, quota state and routing configuration are evaluated again at execution time. Revoking the submitting API key does not retroactively cancel an already accepted durable job; cancel the job explicitly if that is required operationally.

## States

```text
queued -> running -> completed
   |        |  \
   |        |   -> failed
   |        -> pausing -> paused -> queued
   -> paused
   -> cancelled
running/pausing -> cancelling -> cancelled
```

`attempts` increments when an execution attempt starts. A failed gateway/backend response is terminal; the durable batch manager itself does not blindly retry application failures.

## Restart and shutdown behavior

The metadata snapshot is written atomically. Input/output payloads live in separate spool files referenced from metadata.

On startup:

- `running` becomes `queued` and is eligible for retry;
- `pausing` becomes `paused`;
- `cancelling` becomes `cancelled`.

During graceful shutdown the root context cancels active attempts, and the process waits for those attempts to persist their restart-safe `queued` state before final shutdown accounting is closed. A hard kill can still leave metadata in `running`; startup recovery converts that state to `queued`.

This gives durable batches **at-least-once execution across an interruption boundary**, not exactly-once execution. If a worker completed side effects but the gateway did not durably commit the batch result before shutdown/crash, that job can run again after restart. Use idempotent batch workloads or application-level idempotency keys when duplicate execution would be harmful.

## Storage, privacy and retention

Layout:

```text
<data_dir>/batch-jobs.json
<data_dir>/batch/input/<batch-id>.json
<data_dir>/batch/output/<batch-id>.response
```

Metadata and spool payload files are created with mode `0600`; spool directories use `0750`. Input/output references are stored in `batch-jobs.json`, not the potentially large payloads themselves.

**Batch payloads are content-bearing and are not encrypted by the gateway.** This differs from the optional encrypted conversation store. Keep `batch_jobs.enabled=false` unless durable content persistence is intended, protect `storage.data_dir` with filesystem/disk encryption and access controls, and treat gateway backups as sensitive.

Terminal jobs and their input/output files are removed after `batch_jobs.retention`. `max_jobs` caps retained metadata/jobs; `max_input_bytes` caps one submitted request body.

## Accounting

Each execution attempt receives its own normal `X-Request-ID`. The durable job stores the latest execution request ID and HTTP status. The attempt appears in the ordinary usage journal with `service_class: "batch"`; prompt/completion tokens and compute credits therefore use the same accounting and rollup path as other inference traffic.

## Current scope

P3.1 deliberately implements one durable compute request per job. It is not an OpenAI Batch API clone and does not yet ingest multi-request JSONL files. Global batch coordination across multiple gateway replicas remains intentionally deferred outside the active roadmap; the current design stays single-process and durable.
