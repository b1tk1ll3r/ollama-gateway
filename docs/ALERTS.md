# Alerts and signed webhooks

The alert manager evaluates bounded operational conditions and persists firing/resolved history. It never includes prompt, response, tool, or conversation content in webhook payloads.

## Conditions

Supported conditions include:

- worker unhealthy for `worker_down_for`;
- circuit breaker open;
- queue depth threshold;
- oldest queue wait threshold;
- VRAM pressure;
- repeated OOM indication from the worker/circuit error;
- local gateway storage size;
- actor or tenant quota remaining percentage.

Example:

```json
{
  "alerts": {
    "enabled": true,
    "evaluation_interval": "15s",
    "cooldown": "5m",
    "history_limit": 500,
    "webhook_timeout": "5s",
    "webhook_max_concurrent": 4,
    "webhook_queue": 1024,
    "webhook_retry_attempts": 3,
    "webhook_retry_backoff": "500ms",
    "thresholds": {
      "worker_down_for": "30s",
      "circuit_open": true,
      "queue_depth": 100,
      "queue_wait": "30s",
      "vram_percent": 95,
      "storage_bytes": 0,
      "quota_remaining_percent": 10,
      "oom": true
    },
    "webhooks": []
  }
}
```

## Webhook delivery

A webhook payload has a stable event ID and is delivered asynchronously from a bounded queue. Transient transport failures, HTTP 429, and HTTP 5xx are retried with bounded exponential backoff. Other HTTP 4xx responses are treated as permanent failures.

Headers:

- `X-Ollama-Gateway-Event-ID`
- `X-Ollama-Gateway-Timestamp`
- `X-Ollama-Gateway-Delivery-Attempt`
- `X-Ollama-Gateway-Signature` when a secret is configured

Signature format:

```text
sha256=HMAC_SHA256(secret, timestamp + "." + raw_json_body)
```

Consumers should deduplicate by event ID because retries intentionally reuse the same event ID.

## Hot-path isolation

Quota observations happen during request admission, but disk persistence and webhook I/O are not performed synchronously on that request path. State persistence is coalesced asynchronously and webhook delivery uses a bounded queue.

## Secrets

Webhook secrets are redacted from the Admin JSON configuration view. OpenTelemetry header values are redacted there as well. Saving the redacted configuration restores the existing secret values instead of persisting the literal `<redacted>` placeholder.

## Metrics

- `ollama_gateway_queue_oldest_wait_seconds`
- `ollama_gateway_alerts_active`
- `ollama_gateway_alerts_last_evaluate_timestamp_seconds`
