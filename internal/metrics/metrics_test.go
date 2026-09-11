package metrics

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrometheusIncludesWorkerTelemetryAndModelThroughput(t *testing.T) {
	r := New()
	r.SetDynamic(func() Dynamic {
		return Dynamic{UsageRawFiles: 3, UsageDailyFiles: 12, UsageMonthlyFiles: 7, UsageRawBytes: 100, UsageDailyBytes: 200, UsageMonthlyBytes: 300, UsageLastCompactionUnix: 123456, UsageLastReclaimedBytes: 789, Workers: []WorkerMetric{{
			Name: "gpu-1", Healthy: true, Active: 1, Max: 2,
			VRAMUsedBytes: 12, VRAMTotalBytes: 24, GPUUtilizationPct: 87.5,
			GPUTemperatureC: 63, GPUPowerWatts: 310,
			ModelActive: map[string]int{"qwen3:8b": 1},
			Performance: []ModelPerformanceMetric{{Model: "qwen3:8b", OutputTPS: 101.25, Samples: 3}},
		}}}
	})
	var b bytes.Buffer
	r.WritePrometheus(&b)
	out := b.String()
	for _, want := range []string{
		`ollama_gateway_worker_vram_used_bytes{name="gpu-1"} 12`,
		`ollama_gateway_worker_gpu_utilization_percent{name="gpu-1"} 87.500`,
		`ollama_gateway_worker_model_active{name="gpu-1",model="qwen3:8b"} 1`,
		`ollama_gateway_worker_model_output_tokens_per_second{name="gpu-1",model="qwen3:8b"} 101.250000`,
		`ollama_gateway_usage_raw_files 3`,
		`ollama_gateway_usage_daily_rollup_files 12`,
		`ollama_gateway_usage_monthly_rollup_bytes 300`,
		`ollama_gateway_usage_last_compaction_timestamp_seconds 123456`,
		`ollama_gateway_usage_last_reclaimed_bytes 789`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in metrics:\n%s", want, out)
		}
	}
}

// blockingWriter simulates a Prometheus client that stops reading after the
// exporter has started writing request metrics. WritePrometheus must not hold
// the registry mutex while this writer is blocked, otherwise all request
// completions stall in Record().
type blockingWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "ollama_gateway_requests_total{") {
		w.once.Do(func() { close(w.started) })
		<-w.release
	}
	return len(p), nil
}

func TestPrometheusSlowClientDoesNotBlockRecord(t *testing.T) {
	r := New()
	r.Record("ollama", 200, 0, time.Millisecond, 1, 1, 1, 1, 1)

	bw := &blockingWriter{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		r.WritePrometheus(bw)
		close(done)
	}()

	select {
	case <-bw.started:
	case <-time.After(time.Second):
		t.Fatal("metrics writer did not reach blocking write")
	}

	recorded := make(chan struct{})
	go func() {
		r.Record("ollama", 200, 0, time.Millisecond, 1, 1, 1, 1, 1)
		close(recorded)
	}()
	select {
	case <-recorded:
		// success: no registry lock is held by the stalled exporter
	case <-time.After(250 * time.Millisecond):
		close(bw.release)
		t.Fatal("Record blocked behind a stalled /metrics writer")
	}

	close(bw.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("metrics writer did not finish after release")
	}
}
