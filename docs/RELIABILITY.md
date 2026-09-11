# Reliability and worker maintenance

## Circuit breaker

`reliability.enabled` activates a per-worker circuit breaker. Consecutive transport/backend failures increment the worker failure counter. At `failure_threshold` the circuit enters `open` for `open_duration`. After that period the next eligible request acts as the half-open probe; success closes the circuit, failure reopens it.

```json
"reliability": {
  "enabled": true,
  "failure_threshold": 3,
  "open_duration": "30s",
  "retry_attempts": 2,
  "retry_backoff": "50ms"
}
```

Retries are deliberately conservative: only transport failures before any upstream response is committed to the client are retried. A backend HTTP 5xx contributes to the circuit breaker but is not replayed once its headers/body have begun.

## Worker maintenance

The admin Worker page exposes:

- `active`: accepts new inference jobs;
- `draining`: no new inference jobs, existing jobs continue;
- `disabled`: no new inference jobs until explicitly enabled.

Drain/disable state is persisted in `<data_dir>/worker-state.json`. Circuit state is intentionally transient and can be manually reset from the Worker page.
