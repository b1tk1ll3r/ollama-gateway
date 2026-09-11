# Migration to the in-memory engine

This release removes the external coordination backend entirely.

## Removed configuration

Delete these old top-level/settings from existing configuration files:

```text
redis
scheduler.distributed
scheduler.lease_ttl
scheduler.poll_interval
workers[].request_lease_ttl
usage.redis_aggregates
cluster
```

Replace the old local/cluster visualization settings with:

```json
"infrastructure": {
  "node_name": "mac-studio-gateway",
  "refresh_interval": "250ms",
  "max_requests": 256
}
```

The configuration parser uses `DisallowUnknownFields`, so obsolete settings fail fast instead of being silently ignored.

## Runtime behavior

The following state now exists only inside one gateway process:

- weighted fair queue state;
- running concurrency;
- per-worker slots;
- actor and tenant quota buckets;
- runtime policy overrides;
- browser sessions;
- usage summaries;
- live/infrastructure telemetry.

Active scheduling state still resets on restart. Durable control-plane state (UI-created API-key hashes, policy overrides, quota balances, metrics snapshots and usage history) is now stored locally under `storage.data_dir`; the inference scheduler itself remains in memory.

## Multiple Ollama workers

No change is required. One gateway may still route to many Ollama workers and maintains one fair queue across all of them.

## Multiple gateway processes

Do not place multiple independent gateway processes behind a generic load balancer if you require strict global fairness. Each process is now a separate fairness/quota/session domain.

For the intended deployment, use one authoritative gateway and scale the Ollama worker pool behind it.

## API change

The former cluster visualization endpoints are now local infrastructure endpoints:

```text
GET /gateway/ui-api/infrastructure
GET /gateway/ui-api/infrastructure/stream
```

The web UI has been updated accordingly.
