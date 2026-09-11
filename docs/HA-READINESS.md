> **Status: archived diagnostic path.** Multi-gateway HA was removed from the active roadmap in checkpoint 28 because current single-process performance does not justify distributed coordination. Keep this tooling for future re-evaluation if requirements change.

# P3.2 HA readiness and single-process limit characterization

P3.2 is deliberately gated: do not add distributed consensus, global queue state or leader election until the single-process gateway is shown to be the limiting availability/capacity component rather than Ollama/GPU inference.

This repository includes a small evidence toolchain for that decision:

- `cmd/mock-ollama`: deterministic Ollama/OpenAI-compatible mock backend that removes model compute from the measurement;
- `cmd/bench`: concurrent OpenAI request driver with latency, TTFB, status, bytes, token throughput and JSON output;
- `scripts/ha-readiness.sh`: repeatable concurrency sweep that writes one JSON result per level and captures Prometheus snapshots before/after each level;
- `cmd/ha-snapshot`: optional host/process endpoint snapshotter for gateway RSS/CPU/threads/FDs and host CPU/memory/load evidence;
- `cmd/ha-sampler`: sustained resource sampler for each load level, including sampled peak RSS/CPU/threads/FDs and host-memory/load pressure;
- `cmd/ha-report`: reconciles client benchmark counts with gateway Prometheus deltas, attaches endpoint and sustained resource evidence, and writes machine-readable JSON plus a Markdown evidence summary.

## 1. Build/run the mock backend

```bash
go run ./cmd/mock-ollama \
  -listen 127.0.0.1:11435 \
  -model qwen3:8b \
  -delay 0 \
  -response-bytes 128
```

Point one gateway worker at `http://127.0.0.1:11435`. The mock implements the worker inventory/metadata endpoints plus native and OpenAI chat endpoints required by the gateway.

Useful mock controls:

```text
-delay 5ms          fixed pre-response latency
-stream-delay 10ms  delay between streaming chunks
-response-bytes N   generated payload size
-fail-every N       deterministic HTTP 503 every Nth inference request
```

The zero-delay mode approximates gateway/control-plane overhead. Delay/stream-delay modes exercise many concurrent in-flight connections without requiring a GPU.

## 2. Run a concurrency sweep

For an authenticated gateway:

```bash
export GATEWAY_BENCH_API_KEY='...'
export GATEWAY_PID="$(pgrep -n ollama-gateway)"
BASE_URL=http://127.0.0.1:8080 \
MODEL=qwen3:8b \
REQUESTS=1000 \
WARMUP=50 \
CONCURRENCIES='1 4 16 32 64 128' \
./scripts/ha-readiness.sh
```

Each level prints a human summary and writes `concurrency-<N>.json` plus `metrics-before-c<N>.prom` and `metrics-after-c<N>.prom`. When `GATEWAY_PID` is set, the sweep also writes `resources-before-c<N>.json`, `resources-after-c<N>.json`, and `resources-samples-c<N>.json`. After the sweep, `cmd/ha-report` writes `report.json` and `report.md`. The report verifies that the gateway request, queue and service-observation deltas equal **warmup + measured requests** for every level and records endpoint-resource and sustained-resource evidence completeness separately.

The sweep authenticates `/metrics` with `GATEWAY_BENCH_API_KEY` when that environment variable is set; the key is used only as an HTTP header and is never written to the result directory. Set `METRICS_URL` when metrics are exposed at a separate admin endpoint. Set `CAPTURE_METRICS=false` only for an intentionally client-only run; such a run does not produce reconciled HA evidence.

Endpoint resource capture defaults to `auto`: it is enabled when `GATEWAY_PID` is present and otherwise skipped. Set `CAPTURE_RESOURCES=true` to require it explicitly, or `CAPTURE_RESOURCES=false` to suppress it. Sustained capture separately defaults to `auto` through `CAPTURE_SUSTAINED_RESOURCES`; set it to `false` when an external profiler already supplies the sustained evidence. `GATEWAY_PID` must identify the actual gateway process, not a shell wrapper. `RESOURCE_SAMPLE_INTERVAL` defaults to `250ms`, and `RESOURCE_SAMPLE_MAX_DURATION` defaults to `15m` as a safety bound for a stuck benchmark.

The `resources-before/after` files are **endpoint snapshots immediately before and after a load level**. The `resources-samples` file is collected throughout the load level. On Linux, sampled process CPU uses deltas between `/proc/<pid>/stat` and aggregate `/proc/stat`, scaled to all logical CPUs, so a multi-core process can legitimately exceed 100%. The sampler also records sampled peak RSS, threads, open FDs and load plus minimum available host memory. On other platforms the tool falls back to repeated `ps` sampling and leaves unavailable fields absent.

Each benchmark JSON contains:

- successful/error counts and HTTP status distribution;
- requests/second;
- latency p50/p95/p99/max;
- TTFB p50/p95/p99/max;
- bytes received and bytes/second;
- prompt/completion tokens and completion tokens/second when non-streaming usage is available;
- stream/keep-alive/service-class settings.

To isolate connection setup cost, run an additional pass with `cmd/bench -disable-keepalive`. For long-lived response pressure, run with `STREAM=true` and configure a non-zero mock `-stream-delay`.

## 3. Measure three distinct regimes

### A. Gateway ceiling

Mock Ollama with `-delay 0`, small response body, non-streaming. Increase concurrency until throughput flattens or latency/error rate rises sharply. This measures HTTP/auth/admission/scheduler/routing/proxy/accounting overhead rather than LLM inference.

### B. Connection/stream pressure

Mock Ollama with streaming enabled and `-stream-delay` long enough to keep many requests open. This tests goroutine/socket/buffer behavior and cancellation/shutdown paths.

### C. Real inference

Repeat against the actual Ollama workers. This gives the practical service curve and reveals whether GPU/VRAM/model residency saturates far earlier than the gateway process.

## 4. Record gateway-side signals

The default sweep captures `/metrics` automatically. `cmd/ha-report` consumes the full `ollama_gateway_*` Prometheus names and derives per-level deltas. Important signals include:

- scheduler queued/running and queue wait;
- service-class queued/running;
- worker active/model-active slots;
- request/error/retry/circuit counters;
- usage-journal drop/storage gauges;
- process CPU/RSS/thread/file-descriptor and host load/memory endpoint snapshots from `cmd/ha-snapshot`;
- sampled peak process CPU/RSS/thread/file-descriptor and host pressure from `cmd/ha-sampler`, plus socket/runtime metrics from sustained host/container monitoring when deeper evidence is required.

The benchmark numbers are meaningful only together with CPU, memory and worker saturation. A throughput plateau while GPU/worker slots are saturated is not evidence that HA is needed for gateway capacity.

## 5. HA decision gate

Proceed from readiness work to a real P3.2 coordinator/cluster design only when at least one of these is demonstrated:

1. **Capacity:** gateway CPU/network/connection capacity saturates materially before the Ollama worker pool and cannot be resolved by tuning one process.
2. **Availability:** the required recovery objective cannot tolerate one authoritative gateway process, even with supervisor restart and durable local state.
3. **Operational topology:** multiple gateway instances are mandatory across failure domains, while strict global fairness/quota/batch semantics must still be preserved.

Do **not** deploy independent replicas behind a generic load balancer and call that P3.2. Independent replicas create separate fair queues, quota buckets, transient job registries and batch dispatchers. They improve process redundancy only by weakening global correctness.

### Evidence report gate states

- `incomplete`: one or more metric snapshots are missing, or gateway request/queue/service counters do not reconcile with the expected warmup + measured request count. Fix the measurement before drawing conclusions.
- `not-proven`: client and gateway counters reconcile, but the evidence still does not demonstrate one of the capacity/availability/topology criteria above. `resource_evidence_complete` states whether every level has before/after snapshots; `sustained_resource_evidence_complete` states whether every level has a valid sustained sampler trace. This is the normal result of a mock-backend smoke test.

The tool intentionally does **not** auto-promote the project into clustered HA based only on a throughput plateau. Sustained sampling closes the short-peak gap in endpoint snapshots, but it still must be interpreted with socket/runtime metrics and, for real inference runs, worker/GPU saturation.

## 6. P3.2 design constraints once the gate is met

The HA design must preserve the current hot-path properties:

- one authoritative global order for fair admission;
- globally consistent quota and durable batch state;
- fencing/epochs so a stale leader cannot dispatch duplicate authoritative work;
- membership and leader failover without making every streamed token depend on consensus;
- local worker proxying remains direct after a request is admitted/assigned;
- content-bearing batch/conversation storage retains its current explicit privacy boundaries;
- no mandatory Redis dependency is introduced merely as a shortcut.

A likely architecture is a small consensus-backed coordinator/control plane plus local proxy workers, not replicated copies of today’s independent in-memory scheduler.
