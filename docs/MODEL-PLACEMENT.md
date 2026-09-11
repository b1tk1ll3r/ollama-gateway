# Model Placement

Model Placement is the gateway's hard model-to-worker routing policy. It is evaluated **before** adaptive worker scoring, so GPU load, VRAM pressure, model affinity or learned throughput can never override a placement prohibition.

## Why it is separate from tenant policies

Tenant/Fairness policies answer:

> How much compute may a tenant or actor consume?

Model Placement answers:

> On which Ollama workers may a model execute?

Keeping those concerns separate makes both the configuration and the UI predictable.

## UI workflow

Open **Admin -> Model Placement**.

The matrix has one row per discovered model and one column per worker:

- `✓ installed` — allowed and installed on this worker.
- `○ allowed` — policy allows it, but the model is not currently installed there.
- `● loaded` — allowed, installed and currently resident according to `/api/ps`.
- `⛔ blocked` — the effective placement rule denies the model.
- `↳` — the cell is resolved by an exact model rule.

Click a cell to create an exact allow/deny exception. Clicking an exact UI exception again removes that exact rule so the broader prefix/default rule becomes effective again.

The worker cards open a ruleset editor with:

- **Allow all**: allow by default and optionally deny selected models/prefixes.
- **Whitelist**: deny by default and only allow selected models/prefixes.
- **Only installed** preset: builds a whitelist from the worker's current `/api/tags` inventory.
- **Block all** preset: an empty whitelist.
- **Reset to config default**: deletes the persistent UI override and immediately restores the worker's bootstrap/persistent-config baseline.

All UI changes are written to `storage.model_placement_file` (default `model-placement.json`) using the gateway's atomic state writer and take effect for new requests immediately.

## Pattern semantics

Patterns support:

```text
qwen3:8b       exact model
gemma4:*       prefix wildcard
orcarouter/*   prefix wildcard
*              all models
```

Wildcards are only supported as a single trailing `*`.

Resolution uses specificity:

1. exact model rule;
2. longest matching prefix rule;
3. worker mode (`allow_all` or `whitelist`).

At equal specificity, deny wins.

This means the following is valid and useful:

```json
{
  "mode": "allow_all",
  "allowed_models": ["gemma4:latest"],
  "denied_models": ["gemma4:*"]
}
```

`gemma4:latest` is allowed because the exact rule is more specific, while other `gemma4:*` variants are blocked.

## Example: model A on node 1, model B on node 1 + 2

```json
"workers": [
  {
    "name": "node-1",
    "url": "http://10.10.11.10:11434",
    "max_concurrent": 4,
    "model_placement": {
      "mode": "whitelist",
      "allowed_models": ["model-a:*", "model-b:*"],
      "denied_models": []
    }
  },
  {
    "name": "node-2",
    "url": "http://10.10.11.20:11434",
    "max_concurrent": 2,
    "model_placement": {
      "mode": "whitelist",
      "allowed_models": ["model-b:*"],
      "denied_models": []
    }
  }
]
```

The adaptive router then chooses between node 1 and node 2 for `model-b:*`, but `model-a:*` can only run on node 1.

## Inventory behavior

Each worker's `/api/tags` inventory is tracked separately from `/api/ps` loaded state.

Routing behavior is:

1. remove unhealthy workers;
2. remove workers blocked by Model Placement;
3. if inventory is known, require the model to be installed;
4. enforce global and per-model concurrency;
5. score the remaining workers by loaded affinity, load, learned tok/s, GPU utilization and VRAM pressure.

If at least one eligible worker has a known installed copy, only those installed workers are considered. If all eligible inventories are known and none contains the model, the gateway returns a descriptive `model ... is not installed on any eligible worker` error instead of sending the request to an arbitrary node.

A worker whose inventory has never been successfully retrieved is treated as **inventory unknown**, not as "model absent". That provides a controlled fail-open path for transient `/api/tags` discovery failures while still respecting hard placement rules.

## Client model discovery

`/api/tags` and `/v1/models` are gateway-native aggregated endpoints. Models that are installed only on placement-blocked workers are excluded from client discovery. This keeps OpenWebUI and other clients aligned with the gateway's actual routing policy.

The admin model inventory remains unfiltered so operators can still see and manage models that are installed but deliberately blocked from inference.

## Persistence precedence

For a worker:

```text
persistent UI placement override
        ↓ (if absent)
config.json / persistent config baseline
        ↓
effective routing rule
```

Deleting the UI override restores the baseline immediately. The placement file is included in gateway ZIP backups and is shown on **Admin -> Persistenz**.
