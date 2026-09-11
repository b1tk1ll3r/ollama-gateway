package worker

import (
	"path/filepath"
	"testing"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestPerformancePersistenceRoundTrip(t *testing.T) {
	cfg := []config.WorkerConfig{{Name: "gpu-a", URL: "http://127.0.0.1:11434", MaxConcurrent: 2}}
	p := New(cfg, "")
	p.RestorePerformance(PersistentPerformanceState{Version: 1, Workers: map[string]map[string]ModelPerformance{
		"gpu-a": {"qwen3:8b": {Model: "qwen3:8b", PromptTPS: 123.5, OutputTPS: 77.25, Samples: 9}},
	}})
	path := filepath.Join(t.TempDir(), "worker-performance.json")
	if err := p.SavePerformance(path); err != nil {
		t.Fatal(err)
	}
	q := New(cfg, "")
	if err := q.LoadPerformance(path); err != nil {
		t.Fatal(err)
	}
	s := q.Snapshots()
	if len(s) != 1 || len(s[0].Performance) != 1 {
		t.Fatalf("unexpected performance snapshot: %#v", s)
	}
	got := s[0].Performance[0]
	if got.Model != "qwen3:8b" || got.PromptTPS != 123.5 || got.OutputTPS != 77.25 || got.Samples != 9 {
		t.Fatalf("unexpected restored performance: %#v", got)
	}
}
