package autotune

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func waitProfile(t *testing.T, m *Manager, id string) Profile {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		p, ok := m.Get(id)
		if ok && p.Status != "queued" && p.Status != "running" {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("profile %s did not finish", id)
	return Profile{}
}

func TestBenchmarkRecommendationApplyAndReload(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/generate" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		f, _ := w.(http.Flusher)
		fmt.Fprintln(w, `{"response":"x","done":false}`)
		if f != nil {
			f.Flush()
		}
		time.Sleep(20 * time.Millisecond)
		fmt.Fprintln(w, `{"done":true,"prompt_eval_count":10,"prompt_eval_duration":10000000,"eval_count":20,"eval_duration":20000000}`)
	}))
	defer ts.Close()

	wc := config.WorkerConfig{Name: "gpu", URL: ts.URL, MaxConcurrent: 2}
	pool := worker.New([]config.WorkerConfig{wc}, "gpu")
	path := filepath.Join(t.TempDir(), "auto-tune.json")
	cfg := config.AutoTuningConfig{Enabled: true, MaxConcurrency: 2, SamplesPerLevel: 1, MaxTokens: 32, Timeout: config.Duration(5 * time.Second), Prompt: "test", TTFTWeight: .05, ThroughputWeight: 1}
	m, err := New(cfg, pool, path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := m.Start(context.Background(), StartRequest{Worker: "gpu", Model: "qwen:test"})
	if err != nil {
		t.Fatal(err)
	}
	p = waitProfile(t, m, p.ID)
	if p.Status != "completed" {
		t.Fatalf("status=%s err=%s", p.Status, p.Error)
	}
	if len(p.Levels) != 2 {
		t.Fatalf("levels=%d", len(p.Levels))
	}
	if p.RecommendedConcurrency != 2 {
		t.Fatalf("recommendation=%d want 2; levels=%+v", p.RecommendedConcurrency, p.Levels)
	}
	if _, err := m.Apply(p.ID); err != nil {
		t.Fatal(err)
	}

	m2, err := New(cfg, pool, path)
	if err != nil {
		t.Fatal(err)
	}
	if got := m2.Applied()["gpu"]["qwen:test"]; got != 2 {
		t.Fatalf("reloaded applied=%d want 2", got)
	}
}
