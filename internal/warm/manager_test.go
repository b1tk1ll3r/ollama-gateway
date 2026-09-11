package warm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func TestHotPolicyPreloadsInstalledEligibleModel(t *testing.T) {
	var mu sync.Mutex
	var keep any
	called := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen3:8b", "model": "qwen3:8b"}}})
		case "/api/generate":
			var in map[string]any
			_ = json.NewDecoder(r.Body).Decode(&in)
			mu.Lock()
			keep = in["keep_alive"]
			mu.Unlock()
			select {
			case called <- struct{}{}:
			default:
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"done": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := worker.New([]config.WorkerConfig{{Name: "w", URL: ts.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}}, "w")
	p.Start(ctx)
	m, err := New(config.WarmModelsConfig{Enabled: true, OperationTimeout: config.Duration(time.Second), Policies: map[string]config.WarmModelPolicy{"qwen3:*": {Class: "hot", Replicas: 1, IdleTimeout: config.Duration(time.Hour)}}}, p, t.TempDir()+"/warm.json")
	if err != nil {
		t.Fatal(err)
	}
	m.Reconcile(ctx)
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("preload was not issued")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := m.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := keep
	mu.Unlock()
	if n, ok := got.(float64); !ok || n != -1 {
		t.Fatalf("keep_alive=%#v want -1", got)
	}
}

func TestColdPolicyUnloadsAfterIdle(t *testing.T) {
	called := make(chan struct{}, 2)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen3:8b", "model": "qwen3:8b"}}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen3:8b", "model": "qwen3:8b"}}})
		case "/api/generate":
			var in map[string]any
			_ = json.NewDecoder(r.Body).Decode(&in)
			if v, ok := in["keep_alive"].(float64); ok && v == 0 {
				select {
				case called <- struct{}{}:
				default:
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"done": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := worker.New([]config.WorkerConfig{{Name: "w", URL: ts.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}}, "w")
	p.Start(ctx)
	m, err := New(config.WarmModelsConfig{Enabled: true, OperationTimeout: config.Duration(time.Second), Policies: map[string]config.WarmModelPolicy{"qwen3:8b": {Class: "cold", Replicas: 1, IdleTimeout: config.Duration(10 * time.Millisecond)}}}, p, t.TempDir()+"/warm.json")
	if err != nil {
		t.Fatal(err)
	}
	m.Reconcile(ctx) // establishes last-use grace
	time.Sleep(20 * time.Millisecond)
	m.Reconcile(ctx)
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("unload was not issued")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := m.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimePoliciesPersist(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/warm.json"
	p := worker.New([]config.WorkerConfig{{Name: "w", URL: "http://127.0.0.1:1", MaxConcurrent: 1}}, "w")
	base := config.WarmModelsConfig{Policies: map[string]config.WarmModelPolicy{"a:*": {Class: "warm", Replicas: 1, IdleTimeout: config.Duration(time.Minute)}}}
	m, err := New(base, p, path)
	if err != nil {
		t.Fatal(err)
	}
	over := map[string]config.WarmModelPolicy{"b:*": {Class: "hot", Replicas: 1, Workers: []string{"w"}, IdleTimeout: config.Duration(time.Minute)}}
	if err := m.SetPolicies(over); err != nil {
		t.Fatal(err)
	}
	m2, err := New(base, p, path)
	if err != nil {
		t.Fatal(err)
	}
	st := m2.Status()
	if !st.Override || st.Policies["b:*"].Class != "hot" {
		t.Fatalf("status=%#v", st)
	}
	if err := m2.Reset(); err != nil {
		t.Fatal(err)
	}
	if m2.Status().Override {
		t.Fatal("override still active")
	}
}

func TestColdPolicyDoesNotUnloadDrainingWorker(t *testing.T) {
	called := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen3:8b", "model": "qwen3:8b"}}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "qwen3:8b", "model": "qwen3:8b"}}})
		case "/api/generate":
			called <- struct{}{}
			_ = json.NewEncoder(w).Encode(map[string]any{"done": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := worker.New([]config.WorkerConfig{{Name: "w", URL: ts.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}}, "w")
	p.Start(ctx)
	if err := p.SetMaintenance("w", "draining"); err != nil {
		t.Fatal(err)
	}
	m, err := New(config.WarmModelsConfig{Enabled: true, OperationTimeout: config.Duration(time.Second), Policies: map[string]config.WarmModelPolicy{"qwen3:8b": {Class: "cold", Replicas: 1, IdleTimeout: config.Duration(time.Millisecond)}}}, p, t.TempDir()+"/warm.json")
	if err != nil {
		t.Fatal(err)
	}
	m.Reconcile(ctx)
	time.Sleep(5 * time.Millisecond)
	m.Reconcile(ctx)
	select {
	case <-called:
		t.Fatal("warm manager issued model action while worker was draining")
	case <-time.After(100 * time.Millisecond):
	}
}
