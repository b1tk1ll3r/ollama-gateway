package infrastructure

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/liveflow"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func TestSnapshotIsLocalInMemoryTopology(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/ps" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"models":[{"name":"qwen3:8b","size":100,"size_vram":80,"context_length":32768}]}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer backend.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := worker.New([]config.WorkerConfig{{Name: "ollama", URL: backend.URL, MaxConcurrent: 2, HealthInterval: config.Duration(time.Hour)}}, "ollama")
	pool.Start(ctx)
	sched := scheduler.NewLocal(2, 16, 4)
	live := liveflow.New(10*time.Second, 32)
	live.Begin(liveflow.Request{ID: "r1", Tenant: "team", Actor: "app", Worker: "ollama", Model: "qwen3:8b", State: liveflow.StateStreaming})
	h := New(config.InfrastructureConfig{NodeName: "gateway", RefreshInterval: config.Duration(50 * time.Millisecond), MaxRequests: 32}, live, sched, pool)
	h.Start(ctx)

	s := h.Snapshot()
	if s.Mode != "in-memory" || len(s.Gateways) != 1 || s.Gateways[0].NodeName != "gateway" {
		t.Fatalf("unexpected gateway snapshot: %+v", s)
	}
	if len(s.Workers) != 1 || s.Counts.Models != 1 {
		t.Fatalf("unexpected worker/model snapshot: %+v", s)
	}
	if len(s.Requests) != 1 || s.Requests[0].GatewayName != "gateway" {
		t.Fatalf("unexpected request snapshot: %+v", s.Requests)
	}
}
