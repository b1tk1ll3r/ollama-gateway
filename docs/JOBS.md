# Jobs and cancellation

The gateway distinguishes between **model lifecycle operations**, **transient inference jobs**, and **durable batch jobs**.

## Model Stop

The Models page uses **Stop** to unload a resident Ollama model. Ollama does not expose `/api/stop`; the gateway sends:

```http
POST /api/generate
Content-Type: application/json

{"model":"<model>","keep_alive":0,"stream":false}
```

This asks Ollama to unload the model immediately. It does not cancel a specific request.

## Inference jobs

Every compute request receives an `X-Request-ID` and is registered as an in-memory job while it is queued, routing, running, or streaming.

Admin endpoints:

```text
GET  /gateway/ui-api/jobs
POST /gateway/ui-api/jobs/<request-id>/cancel
```

The web UI exposes these under **Jobs** and also places an **Abbrechen** button next to active requests in Live Flow.

### Cancellation semantics

- **queued**: remove the ticket from the WFQ heap immediately and release the credit reservation;
- **waiting for worker**: cancel worker acquisition and release the scheduler lease;
- **running/streaming**: cancel the upstream HTTP context so the Ollama request terminates, then release worker/scheduler slots;
- reconcile any usage already observable from the response;
- store terminal live state `cancelled` with internal status `499`.

For a streaming response whose upstream HTTP headers were already forwarded, the downstream client may still have HTTP status `200`; cancellation is observable as an early stream termination. Internal gateway telemetry records the request as `499`.

Transient inference-job state is process-local and disappears on gateway restart, consistent with the in-memory hot-path design.

## Durable batch jobs

P3.1 adds a separate durable job type under `/gateway/v1/batches`. Batch definitions, state, input references and output references survive restart; an active execution attempt itself is cancelled on shutdown and converted back to a restart-safe queued state.

The Admin UI exposes durable work on the dedicated **Batch Jobs** page rather than mixing it into the transient **Jobs** page. See `docs/BATCH-JOBS.md` for API, state-machine, persistence, privacy, retention and accounting semantics.
