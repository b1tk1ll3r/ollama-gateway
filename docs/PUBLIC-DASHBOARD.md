# Public Status Dashboard

Checkpoint 28 adds an optional unauthenticated **read-only** dashboard for users who should be able to observe current gateway capacity without receiving admin access.

## Enable

```json
"public_dashboard": {
  "enabled": true,
  "path": "/status",
  "title": "Ollama Gateway Status",
  "subtitle": "Live-Auslastung und Infrastruktur",
  "refresh_interval": "2s",
  "max_live_requests": 64,
  "show_worker_names": false,
  "show_model_names": true,
  "show_resource_metrics": true,
  "worker_display_names": {
    "internal-worker-name": "GPU Node A"
  }
}
```

The dashboard is then available at `/status/` without a gateway credential. It is independent of the authenticated `/admin/` UI and also works when the admin UI itself is disabled.

## Privacy boundary

The public API does **not** reuse or serialize the admin snapshots. It builds a new response from an explicit allow-list. The public response excludes:

- tenant, actor, subject and application identifiers;
- API keys, scopes and authentication metadata;
- worker URLs, IP labels and arbitrary worker labels;
- prompts, responses and request bodies;
- persistent storage paths and configuration;
- policies, quota buckets, aliases and ACLs;
- internal error strings, circuit error details and telemetry errors;
- cost/credit estimates.

Request IDs are replaced by short SHA-256-derived opaque IDs. Worker names are anonymized by default and can be replaced with explicit public display names. Model names are also anonymized unless `show_model_names` is explicitly enabled.

## Public data

The dashboard exposes only operational information intended for status display:

- current queue/running/streaming counts;
- gateway uptime;
- healthy/total worker count;
- number of loaded models;
- worker slot usage and health state;
- loaded model display names when enabled;
- numeric RAM/VRAM/GPU/temperature/power values when resource metrics are enabled;
- anonymized recent/live request state, queue/service timing and token counts.

The snapshot endpoint is `GET <path>/api/snapshot`. It is cacheable for one second to reduce repeated work behind a reverse proxy. The browser refresh interval is configurable but cannot be set below one second.

## Reverse proxy

Checkpoint 23+ binds the production Compose service to loopback by default. Expose `/status/` through the same trusted reverse proxy used for the public service. Do not expose the gateway container port directly merely to publish the dashboard.

If the reverse proxy applies authentication globally, carve out only the configured public dashboard path. Keep `/admin/`, `/gateway/`, `/api/`, `/v1/` and `/metrics` under their existing gateway authentication rules.

## Security headers

The public UI sends a restrictive CSP, denies framing, disables referrer leakage and disables browser permissions such as camera, microphone, geolocation, payments and USB.
