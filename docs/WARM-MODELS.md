# Warm Model Management

The warm-model manager controls Ollama model residency without putting model-management I/O on the inference hot path.

## Classes

- `hot`: keep the model resident on the requested number of eligible workers. A hot model is proactively preloaded.
- `warm`: optionally preload the model and unload it after `idle_timeout`.
- `cold`: do not preload and unload aggressively after `idle_timeout`.

Rules support exact names and one trailing `*` wildcard. The most specific matching rule wins.

```json
{
  "warm_models": {
    "enabled": true,
    "reconcile_interval": "30s",
    "operation_timeout": "2m",
    "policies": {
      "qwen3:8b": {
        "class": "hot",
        "workers": ["rtx-4090"],
        "replicas": 1,
        "preload": true,
        "idle_timeout": "30m"
      },
      "gemma4:*": {
        "class": "warm",
        "replicas": 1,
        "preload": false,
        "idle_timeout": "20m"
      }
    }
  }
}
```

## Safety rules

Warm-model operations are subordinate to normal routing policy:

1. worker must be healthy and in `active` maintenance mode;
2. preload must be allowed by Model Placement;
3. when worker inventory is known, preload requires the model to be installed;
4. workers with an open/half-open circuit are not used for proactive preload;
5. a preload/unload reserves a per-worker/per-model maintenance slot before calling Ollama;
6. inference acquisition for the same worker/model is blocked during that short operation;
7. unload is refused while the model has active inference jobs.

The maintenance reservation closes the race where an idle unload could otherwise begin at the same moment a new request is routed to the model.

## Persistence

Runtime rules edited in **Admin → Warm Models** are stored in `warm-models.json`. The static config remains the baseline and can be restored with **Auf Config zurücksetzen**.

Action history is retained with the same state file. Last-use timestamps are deliberately runtime state; after restart, currently loaded models receive a fresh grace period before idle eviction.

## VRAM pressure

The manager does not automatically evict arbitrary models under memory pressure. It exposes eviction suggestions for inactive, non-hot models. This avoids surprise unloads and lets the operator decide whether to act.

## Metrics

- `ollama_gateway_warm_actions_running`
- `ollama_gateway_warm_eviction_suggestions`
