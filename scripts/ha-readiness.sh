#!/usr/bin/env sh
set -eu

BASE_URL=${BASE_URL:-http://127.0.0.1:8080}
MODEL=${MODEL:-qwen3:8b}
REQUESTS=${REQUESTS:-500}
WARMUP=${WARMUP:-20}
CONCURRENCIES=${CONCURRENCIES:-"1 4 16 32 64"}
OUT_DIR=${OUT_DIR:-"ha-readiness-$(date +%Y%m%d-%H%M%S)"}
TIMEOUT=${TIMEOUT:-2m}
STREAM=${STREAM:-false}
SERVICE_CLASS=${SERVICE_CLASS:-}
CAPTURE_METRICS=${CAPTURE_METRICS:-true}
METRICS_URL=${METRICS_URL:-"${BASE_URL%/}/metrics"}
GATEWAY_PID=${GATEWAY_PID:-}
CAPTURE_RESOURCES=${CAPTURE_RESOURCES:-auto}
CAPTURE_SUSTAINED_RESOURCES=${CAPTURE_SUSTAINED_RESOURCES:-auto}
RESOURCE_SAMPLE_INTERVAL=${RESOURCE_SAMPLE_INTERVAL:-250ms}
RESOURCE_SAMPLE_MAX_DURATION=${RESOURCE_SAMPLE_MAX_DURATION:-15m}

if [ "$CAPTURE_RESOURCES" = "auto" ]; then
  if [ -n "$GATEWAY_PID" ]; then
    CAPTURE_RESOURCES=true
  else
    CAPTURE_RESOURCES=false
  fi
fi
if [ "$CAPTURE_RESOURCES" = "true" ] && [ -z "$GATEWAY_PID" ]; then
  echo "GATEWAY_PID is required when CAPTURE_RESOURCES=true" >&2
  exit 2
fi

if [ "$CAPTURE_SUSTAINED_RESOURCES" = "auto" ]; then
  if [ -n "$GATEWAY_PID" ]; then
    CAPTURE_SUSTAINED_RESOURCES=true
  else
    CAPTURE_SUSTAINED_RESOURCES=false
  fi
fi
if [ "$CAPTURE_SUSTAINED_RESOURCES" = "true" ] && [ -z "$GATEWAY_PID" ]; then
  echo "GATEWAY_PID is required when CAPTURE_SUSTAINED_RESOURCES=true" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
SAMPLER_BIN=""
SAMPLER_BG_PID=""
SAMPLER_STOP_FILE=""

cleanup() {
  if [ -n "$SAMPLER_STOP_FILE" ]; then
    : > "$SAMPLER_STOP_FILE" 2>/dev/null || true
  fi
  if [ -n "$SAMPLER_BG_PID" ]; then
    wait "$SAMPLER_BG_PID" 2>/dev/null || true
  fi
  if [ -n "$SAMPLER_BIN" ]; then
    rm -f "$SAMPLER_BIN"
  fi
  if [ -n "$SAMPLER_STOP_FILE" ]; then
    rm -f "$SAMPLER_STOP_FILE"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ "$CAPTURE_SUSTAINED_RESOURCES" = "true" ]; then
  SAMPLER_BIN="$OUT_DIR/.ha-sampler"
  CGO_ENABLED=0 go build -trimpath -o "$SAMPLER_BIN" ./cmd/ha-sampler
fi

echo "HA readiness sweep"
echo "base_url=$BASE_URL model=$MODEL requests=$REQUESTS warmup=$WARMUP stream=$STREAM"
echo "concurrency=$CONCURRENCIES"
echo "capture_metrics=$CAPTURE_METRICS metrics_url=$METRICS_URL"
echo "capture_resources=$CAPTURE_RESOURCES gateway_pid=${GATEWAY_PID:-unset}"
echo "capture_sustained_resources=$CAPTURE_SUSTAINED_RESOURCES sample_interval=$RESOURCE_SAMPLE_INTERVAL"
echo "output=$OUT_DIR"

capture_metrics() {
  dst=$1
  if [ "$CAPTURE_METRICS" != "true" ]; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required when CAPTURE_METRICS=true" >&2
    exit 2
  fi
  if [ -n "${GATEWAY_BENCH_API_KEY:-}" ]; then
    curl -fsS --max-time 30 -H "Authorization: Bearer ${GATEWAY_BENCH_API_KEY}" "$METRICS_URL" > "$dst"
  else
    curl -fsS --max-time 30 "$METRICS_URL" > "$dst"
  fi
}

capture_resources() {
  dst=$1
  if [ "$CAPTURE_RESOURCES" != "true" ]; then
    return 0
  fi
  go run ./cmd/ha-snapshot -pid "$GATEWAY_PID" -json-out "$dst"
}

start_resource_sampler() {
  c=$1
  if [ "$CAPTURE_SUSTAINED_RESOURCES" != "true" ]; then
    return 0
  fi
  SAMPLER_STOP_FILE="$OUT_DIR/.resource-sampler-stop-c$c"
  rm -f "$SAMPLER_STOP_FILE"
  "$SAMPLER_BIN" \
    -pid "$GATEWAY_PID" \
    -interval "$RESOURCE_SAMPLE_INTERVAL" \
    -max-duration "$RESOURCE_SAMPLE_MAX_DURATION" \
    -stop-file "$SAMPLER_STOP_FILE" \
    -json-out "$OUT_DIR/resources-samples-c$c.json" \
    >"$OUT_DIR/resource-sampler-c$c.log" 2>&1 &
  SAMPLER_BG_PID=$!
  # Give the already-built sampler enough time to capture its initial baseline.
  sleep 0.1
  if ! kill -0 "$SAMPLER_BG_PID" 2>/dev/null; then
    wait "$SAMPLER_BG_PID" || true
    echo "resource sampler failed to start for concurrency $c" >&2
    cat "$OUT_DIR/resource-sampler-c$c.log" >&2 || true
    exit 1
  fi
}

stop_resource_sampler() {
  c=$1
  if [ "$CAPTURE_SUSTAINED_RESOURCES" != "true" ]; then
    return 0
  fi
  : > "$SAMPLER_STOP_FILE"
  sampler_rc=0
  wait "$SAMPLER_BG_PID" || sampler_rc=$?
  rm -f "$SAMPLER_STOP_FILE"
  SAMPLER_STOP_FILE=""
  SAMPLER_BG_PID=""
  if [ "$sampler_rc" -ne 0 ]; then
    echo "resource sampler failed for concurrency $c (exit $sampler_rc)" >&2
    cat "$OUT_DIR/resource-sampler-c$c.log" >&2 || true
    exit "$sampler_rc"
  fi
}

for c in $CONCURRENCIES; do
  echo "== concurrency $c =="
  capture_metrics "$OUT_DIR/metrics-before-c$c.prom"
  capture_resources "$OUT_DIR/resources-before-c$c.json"
  start_resource_sampler "$c"

  set -- go run ./cmd/bench \
    -base-url "$BASE_URL" \
    -model "$MODEL" \
    -concurrency "$c" \
    -requests "$REQUESTS" \
    -warmup "$WARMUP" \
    -timeout "$TIMEOUT" \
    -stream="$STREAM" \
    -json-out "$OUT_DIR/concurrency-$c.json"
  if [ -n "$SERVICE_CLASS" ]; then
    set -- "$@" -service-class "$SERVICE_CLASS"
  fi
  bench_rc=0
  "$@" || bench_rc=$?
  stop_resource_sampler "$c"
  if [ "$bench_rc" -ne 0 ]; then
    echo "benchmark failed for concurrency $c (exit $bench_rc)" >&2
    exit "$bench_rc"
  fi

  capture_resources "$OUT_DIR/resources-after-c$c.json"
  capture_metrics "$OUT_DIR/metrics-after-c$c.prom"
done

if [ "$CAPTURE_METRICS" = "true" ]; then
  go run ./cmd/ha-report \
    -input "$OUT_DIR" \
    -json-out "$OUT_DIR/report.json" \
    -markdown-out "$OUT_DIR/report.md"
else
  echo "metrics capture disabled; no reconciled HA evidence report generated"
fi

echo "completed: $OUT_DIR"
