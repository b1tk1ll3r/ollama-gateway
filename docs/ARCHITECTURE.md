# Architecture

## Request path

```text
HTTP request
  -> authentication / identity
  -> compute request parsing + cost estimate
  -> in-memory quota reservation
  -> hierarchical in-memory WFQ
  -> model-aware worker selection
  -> streaming reverse proxy
  -> actual usage reconciliation
  -> in-memory usage aggregation
  -> optional asynchronous JSONL journal
```

Management and blob endpoints do not enter the compute scheduler and keep their request body streaming end-to-end.

## Single authoritative engine

The gateway deliberately has no external coordination backend. All mutable control state belongs to one process. This means there is exactly one authoritative view of queue order, quota buckets and worker slots and therefore no distributed lock, lease renewal, consensus or fail-open state split.

The hot-path structures are Go-native:

- scheduler maps and heaps protected by one scheduler mutex;
- worker load using atomic counters;
- quota buckets protected by a small mutex;
- runtime policy and session maps behind RWMutex/Mutex;
- live request snapshots behind the lifecycle tracker;
- usage summaries updated in memory and journaled asynchronously.

## Fair scheduler

`internal/scheduler` implements two-level hierarchical weighted fair queueing.

A tenant has a root service score. Inside that tenant each queued request has an actor virtual-finish score derived from request cost and actor weight. When capacity becomes free, the scheduler selects the tenant with the lowest root score and then its request with the lowest actor finish score.

This yields two properties:

- tenant fairness is not defeated by creating many actors;
- actors within one tenant still share service fairly.

The scheduler is event-driven. A worker slot release signals the scheduler immediately; there is no polling backend.

## Quotas

`internal/quota` is an in-memory hierarchical token-bucket ledger whose bucket balances are periodically snapshotted for restart continuity. A request reserves estimated compute credits from actor and tenant buckets. Final usage reconciles the reservation. Token buckets refill lazily from monotonic wall-clock deltas.

## Worker routing

`internal/worker` maintains health and model residency snapshots for each Ollama endpoint. Local `active` counters are atomic and are the hard worker slot limiter. Candidate scoring combines active-slot pressure with a strong affinity bonus when the requested model is already loaded.

There are no worker leases or renewal goroutines.

## Live flow and infrastructure map

`internal/liveflow` tracks bounded prompt-free lifecycle metadata. `internal/infrastructure` turns that local state into the UI topology snapshot. The browser receives it over SSE and renders the animated pulse map with Canvas.

The infrastructure view is explicitly local to the process. It shows one gateway node plus all configured Ollama workers and models.

## Runtime policy store and sessions

Tenant policy overrides are backed by the local persistent policy store. Browser OIDC sessions remain in memory and disappear on restart by design.

## Usage

`internal/usage` maintains actor, tenant and global summaries in memory. The optional file journal runs asynchronously on a buffered channel and never participates in admission or request dispatch.

## Scaling model

Scale inference by adding Ollama workers to the one gateway. Running multiple independent gateway replicas would create independent fairness domains. If strict global fairness is required, requests must pass through the same gateway engine.


## Optional content-bearing conversation state

The Responses conversation layer is separate from browser OIDC sessions. Browser sessions remain volatile. When explicitly enabled, Responses conversation contexts are tenant/actor scoped, encrypted at rest, retention bounded, and used only to expand `previous_response_id` before normal admission/routing. The ordinary proxy path does not capture response bodies; bounded capture is enabled only for store-eligible `/v1/responses` requests.
