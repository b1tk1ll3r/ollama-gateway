package metrics

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type histogram struct {
	buckets []float64
	counts  []atomic.Uint64
	sum     atomic.Uint64
	total   atomic.Uint64
}

func newHistogram(b []float64) *histogram {
	return &histogram{buckets: b, counts: make([]atomic.Uint64, len(b))}
}
func (h *histogram) observe(v float64) {
	h.total.Add(1)
	atomicAddFloat(&h.sum, v)
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i].Add(1)
		}
	}
}

type Registry struct {
	mu               sync.RWMutex
	dynamicMu        sync.RWMutex
	requests         map[string]*atomic.Uint64
	errors           map[string]*atomic.Uint64
	queue            *histogram
	service          *histogram
	prompt           atomic.Uint64
	completion       atomic.Uint64
	creditsBits      atomic.Uint64
	bytesIn          atomic.Uint64
	bytesOut         atomic.Uint64
	usageDropped     atomic.Uint64
	upstreamFailures map[string]*atomic.Uint64
	retries          map[string]*atomic.Uint64
	circuitOpens     map[string]*atomic.Uint64
	circuitResets    map[string]*atomic.Uint64
	dynamic          func() Dynamic
}
type Dynamic struct {
	Queued, Running         int64
	OldestQueueWaitSeconds  float64
	Workers                 []WorkerMetric
	UsageRawFiles           int
	UsageDailyFiles         int
	UsageMonthlyFiles       int
	UsageRawBytes           int64
	UsageDailyBytes         int64
	UsageMonthlyBytes       int64
	UsageLastCompactionUnix int64
	UsageLastReclaimedBytes int64
	ServiceClasses          map[string]ServiceClassMetric
	OTelExportedSpans       uint64
	OTelFailedSpans         uint64
	OTelDroppedSpans        uint64
	WarmActionsRunning      int
	WarmEvictionSuggestions int
	AlertsActive            int
	AlertsLastEvaluateUnix  int64
}
type ServiceClassMetric struct{ Queued, Running int64 }
type WorkerMetric struct {
	Name              string
	Healthy           bool
	Active            int64
	Max               int
	MemoryUsedBytes   int64
	MemoryTotalBytes  int64
	VRAMUsedBytes     int64
	VRAMTotalBytes    int64
	GPUUtilizationPct float64
	GPUTemperatureC   float64
	GPUPowerWatts     float64
	ModelActive       map[string]int
	Performance       []ModelPerformanceMetric
	CircuitState      string
	Maintenance       string
}

type ModelPerformanceMetric struct {
	Model     string
	PromptTPS float64
	OutputTPS float64
	Samples   int64
}

func New() *Registry {
	return &Registry{requests: map[string]*atomic.Uint64{}, errors: map[string]*atomic.Uint64{}, upstreamFailures: map[string]*atomic.Uint64{}, retries: map[string]*atomic.Uint64{}, circuitOpens: map[string]*atomic.Uint64{}, circuitResets: map[string]*atomic.Uint64{}, queue: newHistogram([]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300}), service: newHistogram([]float64{.1, .25, .5, 1, 2.5, 5, 10, 20, 30, 60, 120, 300, 600, 1200})}
}
func (r *Registry) SetDynamic(f func() Dynamic) {
	r.dynamicMu.Lock()
	r.dynamic = f
	r.dynamicMu.Unlock()
}
func (r *Registry) Record(api string, status int, queue, service time.Duration, prompt, completion int64, credits float64, in, out int64) {
	class := fmt.Sprintf("%dxx", status/100)
	key := api + "|" + class
	r.mu.Lock()
	c := r.requests[key]
	if c == nil {
		c = &atomic.Uint64{}
		r.requests[key] = c
	}
	c.Add(1)
	if status >= 400 {
		e := r.errors[key]
		if e == nil {
			e = &atomic.Uint64{}
			r.errors[key] = e
		}
		e.Add(1)
	}
	r.mu.Unlock()
	r.queue.observe(queue.Seconds())
	r.service.observe(service.Seconds())
	if prompt > 0 {
		r.prompt.Add(uint64(prompt))
	}
	if completion > 0 {
		r.completion.Add(uint64(completion))
	}
	atomicAddFloat(&r.creditsBits, credits)
	if in > 0 {
		r.bytesIn.Add(uint64(in))
	}
	if out > 0 {
		r.bytesOut.Add(uint64(out))
	}
}
func (r *Registry) DropUsage() { r.usageDropped.Add(1) }

func (r *Registry) incBounded(m map[string]*atomic.Uint64, key string) {
	r.mu.Lock()
	c := m[key]
	if c == nil {
		c = &atomic.Uint64{}
		m[key] = c
	}
	c.Add(1)
	r.mu.Unlock()
}
func (r *Registry) RecordUpstreamFailure(worker, class string) {
	r.incBounded(r.upstreamFailures, worker+"|"+class)
}
func (r *Registry) RecordRetry(worker string)        { r.incBounded(r.retries, worker) }
func (r *Registry) RecordCircuitOpen(worker string)  { r.incBounded(r.circuitOpens, worker) }
func (r *Registry) RecordCircuitReset(worker string) { r.incBounded(r.circuitResets, worker) }

type requestMetric struct {
	API         string
	StatusClass string
	Value       uint64
}

type namedCounter struct {
	Key   string
	Value uint64
}
type prometheusSnapshot struct {
	Requests         []requestMetric
	UpstreamFailures []namedCounter
	Retries          []namedCounter
	CircuitOpens     []namedCounter
	CircuitResets    []namedCounter
	Queue            histogramState
	Service          histogramState
	Prompt           uint64
	Completion       uint64
	Credits          float64
	BytesIn          uint64
	BytesOut         uint64
	UsageDropped     uint64
	Dynamic          Dynamic
}

// snapshotPrometheus copies all state needed by the exporter before any bytes
// are written to the client. In particular, no registry lock is ever held
// across network I/O. This matters because a slow or stalled Prometheus client
// must not be able to block Record() calls in the inference hot path.
func (r *Registry) snapshotPrometheus() prometheusSnapshot {
	s := prometheusSnapshot{
		Queue:        histogramSnapshot(r.queue),
		Service:      histogramSnapshot(r.service),
		Prompt:       r.prompt.Load(),
		Completion:   r.completion.Load(),
		Credits:      atomicLoadFloat(&r.creditsBits),
		BytesIn:      r.bytesIn.Load(),
		BytesOut:     r.bytesOut.Load(),
		UsageDropped: r.usageDropped.Load(),
	}

	// Only the map shape requires a lock. Counter values themselves are atomic.
	r.mu.RLock()
	keys := make([]string, 0, len(r.requests))
	for k := range r.requests {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		p := strings.SplitN(k, "|", 2)
		if len(p) != 2 {
			continue
		}
		s.Requests = append(s.Requests, requestMetric{API: p[0], StatusClass: p[1], Value: r.requests[k].Load()})
	}
	copyCounters := func(m map[string]*atomic.Uint64) []namedCounter {
		ks := make([]string, 0, len(m))
		for k := range m {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		out := make([]namedCounter, 0, len(ks))
		for _, k := range ks {
			out = append(out, namedCounter{Key: k, Value: m[k].Load()})
		}
		return out
	}
	s.UpstreamFailures = copyCounters(r.upstreamFailures)
	s.Retries = copyCounters(r.retries)
	s.CircuitOpens = copyCounters(r.circuitOpens)
	s.CircuitResets = copyCounters(r.circuitResets)
	r.mu.RUnlock()

	// The dynamic callback snapshots scheduler/worker state. It is deliberately
	// invoked after releasing the metrics registry lock, so it cannot form a
	// lock-order cycle with request completion, worker telemetry, or persistence.
	r.dynamicMu.RLock()
	dynamic := r.dynamic
	r.dynamicMu.RUnlock()
	if dynamic != nil {
		s.Dynamic = dynamic()
	}
	return s
}

func (r *Registry) WritePrometheus(w io.Writer) {
	s := r.snapshotPrometheus()
	io.WriteString(w, "# HELP ollama_gateway_requests_total Requests handled by API and status class.\n# TYPE ollama_gateway_requests_total counter\n")
	for _, x := range s.Requests {
		fmt.Fprintf(w, "ollama_gateway_requests_total{api=%q,status_class=%q} %d\n", x.API, x.StatusClass, x.Value)
	}
	writeHistState(w, "ollama_gateway_queue_seconds", "Queue wait time.", s.Queue)
	writeHistState(w, "ollama_gateway_service_seconds", "Backend service time.", s.Service)
	fmt.Fprintf(w, "# TYPE ollama_gateway_prompt_tokens_total counter\nollama_gateway_prompt_tokens_total %d\n", s.Prompt)
	fmt.Fprintf(w, "# TYPE ollama_gateway_completion_tokens_total counter\nollama_gateway_completion_tokens_total %d\n", s.Completion)
	fmt.Fprintf(w, "# TYPE ollama_gateway_credits_total counter\nollama_gateway_credits_total %.6f\n", s.Credits)
	fmt.Fprintf(w, "# TYPE ollama_gateway_bytes_in_total counter\nollama_gateway_bytes_in_total %d\n", s.BytesIn)
	fmt.Fprintf(w, "# TYPE ollama_gateway_bytes_out_total counter\nollama_gateway_bytes_out_total %d\n", s.BytesOut)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_events_dropped_total counter\nollama_gateway_usage_events_dropped_total %d\n", s.UsageDropped)
	fmt.Fprintln(w, "# TYPE ollama_gateway_upstream_failures_total counter")
	for _, x := range s.UpstreamFailures {
		p := strings.SplitN(x.Key, "|", 2)
		class := "other"
		if len(p) > 1 {
			class = p[1]
		}
		fmt.Fprintf(w, "ollama_gateway_upstream_failures_total{worker=%q,class=%q} %d\n", p[0], class, x.Value)
	}
	fmt.Fprintln(w, "# TYPE ollama_gateway_retries_total counter")
	for _, x := range s.Retries {
		fmt.Fprintf(w, "ollama_gateway_retries_total{worker=%q} %d\n", x.Key, x.Value)
	}
	fmt.Fprintln(w, "# TYPE ollama_gateway_circuit_opens_total counter")
	for _, x := range s.CircuitOpens {
		fmt.Fprintf(w, "ollama_gateway_circuit_opens_total{worker=%q} %d\n", x.Key, x.Value)
	}
	fmt.Fprintln(w, "# TYPE ollama_gateway_circuit_resets_total counter")
	for _, x := range s.CircuitResets {
		fmt.Fprintf(w, "ollama_gateway_circuit_resets_total{worker=%q} %d\n", x.Key, x.Value)
	}

	d := s.Dynamic
	fmt.Fprintf(w, "# TYPE ollama_gateway_queue_depth gauge\nollama_gateway_queue_depth %d\n# TYPE ollama_gateway_running gauge\nollama_gateway_running %d\n", d.Queued, d.Running)
	classNames := make([]string, 0, len(d.ServiceClasses))
	for n := range d.ServiceClasses {
		classNames = append(classNames, n)
	}
	sort.Strings(classNames)
	for _, n := range classNames {
		x := d.ServiceClasses[n]
		fmt.Fprintf(w, "ollama_gateway_service_class_queued{name=%q} %d\nollama_gateway_service_class_running{name=%q} %d\n", n, x.Queued, n, x.Running)
	}
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_raw_files gauge\nollama_gateway_usage_raw_files %d\n", d.UsageRawFiles)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_daily_rollup_files gauge\nollama_gateway_usage_daily_rollup_files %d\n", d.UsageDailyFiles)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_monthly_rollup_files gauge\nollama_gateway_usage_monthly_rollup_files %d\n", d.UsageMonthlyFiles)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_raw_bytes gauge\nollama_gateway_usage_raw_bytes %d\n", d.UsageRawBytes)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_daily_rollup_bytes gauge\nollama_gateway_usage_daily_rollup_bytes %d\n", d.UsageDailyBytes)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_monthly_rollup_bytes gauge\nollama_gateway_usage_monthly_rollup_bytes %d\n", d.UsageMonthlyBytes)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_last_compaction_timestamp_seconds gauge\nollama_gateway_usage_last_compaction_timestamp_seconds %d\n", d.UsageLastCompactionUnix)
	fmt.Fprintf(w, "# TYPE ollama_gateway_usage_last_reclaimed_bytes gauge\nollama_gateway_usage_last_reclaimed_bytes %d\n", d.UsageLastReclaimedBytes)
	fmt.Fprintf(w, "# TYPE ollama_gateway_otel_exported_spans_total counter\nollama_gateway_otel_exported_spans_total %d\n", d.OTelExportedSpans)
	fmt.Fprintf(w, "# TYPE ollama_gateway_otel_failed_spans_total counter\nollama_gateway_otel_failed_spans_total %d\n", d.OTelFailedSpans)
	fmt.Fprintf(w, "# TYPE ollama_gateway_otel_dropped_spans_total counter\nollama_gateway_otel_dropped_spans_total %d\n", d.OTelDroppedSpans)
	for _, x := range d.Workers {
		h := 0
		if x.Healthy {
			h = 1
		}
		fmt.Fprintf(w, "ollama_gateway_worker_healthy{name=%q} %d\nollama_gateway_worker_active{name=%q} %d\nollama_gateway_worker_capacity{name=%q} %d\n", x.Name, h, x.Name, x.Active, x.Name, x.Max)
		if x.MemoryTotalBytes > 0 {
			fmt.Fprintf(w, "ollama_gateway_worker_memory_used_bytes{name=%q} %d\nollama_gateway_worker_memory_total_bytes{name=%q} %d\n", x.Name, x.MemoryUsedBytes, x.Name, x.MemoryTotalBytes)
		}
		if x.VRAMTotalBytes > 0 {
			fmt.Fprintf(w, "ollama_gateway_worker_vram_used_bytes{name=%q} %d\nollama_gateway_worker_vram_total_bytes{name=%q} %d\n", x.Name, x.VRAMUsedBytes, x.Name, x.VRAMTotalBytes)
		}
		if x.GPUUtilizationPct > 0 {
			fmt.Fprintf(w, "ollama_gateway_worker_gpu_utilization_percent{name=%q} %.3f\n", x.Name, x.GPUUtilizationPct)
		}
		if x.GPUTemperatureC > 0 {
			fmt.Fprintf(w, "ollama_gateway_worker_gpu_temperature_celsius{name=%q} %.3f\n", x.Name, x.GPUTemperatureC)
		}
		if x.GPUPowerWatts > 0 {
			fmt.Fprintf(w, "ollama_gateway_worker_gpu_power_watts{name=%q} %.3f\n", x.Name, x.GPUPowerWatts)
		}
		open, half, accepting := 0, 0, 1
		if x.CircuitState == "open" {
			open = 1
			accepting = 0
		}
		if x.CircuitState == "half_open" {
			half = 1
		}
		if x.Maintenance != "active" {
			accepting = 0
		}
		fmt.Fprintf(w, "ollama_gateway_worker_circuit_open{name=%q} %d\nollama_gateway_worker_circuit_half_open{name=%q} %d\nollama_gateway_worker_accepting_new{name=%q} %d\n", x.Name, open, x.Name, half, x.Name, accepting)
		models := make([]string, 0, len(x.ModelActive))
		for model := range x.ModelActive {
			models = append(models, model)
		}
		sort.Strings(models)
		for _, model := range models {
			fmt.Fprintf(w, "ollama_gateway_worker_model_active{name=%q,model=%q} %d\n", x.Name, model, x.ModelActive[model])
		}
		for _, perf := range x.Performance {
			if perf.PromptTPS > 0 {
				fmt.Fprintf(w, "ollama_gateway_worker_model_prompt_tokens_per_second{name=%q,model=%q} %.6f\n", x.Name, perf.Model, perf.PromptTPS)
			}
			if perf.OutputTPS > 0 {
				fmt.Fprintf(w, "ollama_gateway_worker_model_output_tokens_per_second{name=%q,model=%q} %.6f\n", x.Name, perf.Model, perf.OutputTPS)
			}
			fmt.Fprintf(w, "ollama_gateway_worker_model_performance_samples{name=%q,model=%q} %d\n", x.Name, perf.Model, perf.Samples)
		}
	}
}

func writeHistState(w io.Writer, name, help string, h histogramState) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	for i, b := range h.Buckets {
		var count uint64
		if i < len(h.Counts) {
			count = h.Counts[i]
		}
		fmt.Fprintf(w, "%s_bucket{le=\"%g\"} %d\n", name, b, count)
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %.9f\n%s_count %d\n", name, h.Total, name, h.Sum, name, h.Total)
}

// Atomic float helpers use CAS so simultaneous observations do not lose increments.
func atomicAddFloat(a *atomic.Uint64, v float64) {
	for {
		old := a.Load()
		nv := mathFloat64frombits(old) + v
		if a.CompareAndSwap(old, mathFloat64bits(nv)) {
			return
		}
	}
}

func atomicLoadFloat(a *atomic.Uint64) float64 { return mathFloat64frombits(a.Load()) }

// Tiny wrappers keep the package dependency-free while using math's bit representation.
func mathFloat64bits(f float64) uint64     { return math.Float64bits(f) }
func mathFloat64frombits(b uint64) float64 { return math.Float64frombits(b) }
