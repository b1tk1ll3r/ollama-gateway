package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/batch"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	px "github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func TestDurableBatchEndToEndThroughGatewayPipeline(t *testing.T) {
	var upstreamBody string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"models":[{"name":"qwen3:8b","model":"qwen3:8b"}]}`)
		case "/api/tags":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"models":[{"name":"qwen3:8b"}]}`)
		case "/api/chat":
			b, _ := io.ReadAll(r.Body)
			upstreamBody = string(b)
			w.Header().Set("Content-Type", "application/x-ndjson")
			_, _ = io.WriteString(w, "{\"message\":{\"content\":\"batch-ok\"},\"done\":true,\"prompt_eval_count\":3,\"eval_count\":2}\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server:    config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:      config.AuthConfig{APIKeys: []config.APIKeyConfig{{Name: "client", Key: "batch-key", Tenant: "team-a", Subject: "alice"}}},
		Scheduler: config.SchedulerConfig{GlobalConcurrency: 2, MaxQueue: 32, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/api/chat"}},
		Cost:      config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 16},
		Workers:   []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 2, HealthInterval: config.Duration(time.Hour)}},
		ServiceClasses: config.ServiceClassesConfig{Default: "interactive", Classes: map[string]config.ServiceClassConfig{
			"interactive": {Weight: 1, MaxQueueWait: config.Duration(time.Second), MaxConcurrent: 2},
			"batch":       {Weight: 0.25, MaxQueueWait: config.Duration(time.Second), MaxConcurrent: 1},
		}},
		BatchJobs: config.BatchJobsConfig{Enabled: true, Retention: config.Duration(time.Hour), MaxJobs: 100, MaxConcurrent: 1, MaxInputBytes: 1 << 20},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "w")
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(root)
	rec, err := usage.New("", 100, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	dir := t.TempDir()
	bm, err := batch.New(cfg.BatchJobs, filepath.Join(dir, "batch-jobs.json"), filepath.Join(dir, "batch"))
	if err != nil {
		t.Fatal(err)
	}
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(2, 32, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.Default(), BatchJobs: bm})
	bm.Start(root, sv.ExecuteBatch)
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	payload := []byte(`{"path":"/api/chat","body":{"model":"qwen3:8b","messages":[{"role":"user","content":"run batch"}]}}`)
	req, _ := http.NewRequest(http.MethodPost, front.URL+"/gateway/v1/batches", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer batch-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("create status=%d body=%s", resp.StatusCode, body)
	}
	var created batch.Job
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.ServiceClass != "batch" || created.Identity.Tenant != "team-a" || created.Identity.Actor != "alice" {
		t.Fatalf("created=%#v", created)
	}
	if !strings.HasPrefix(resp.Header.Get("Location"), "/gateway/v1/batches/") {
		t.Fatalf("location=%q", resp.Header.Get("Location"))
	}

	var completed batch.Job
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		req, _ = http.NewRequest(http.MethodGet, front.URL+"/gateway/v1/batches/"+created.ID, nil)
		req.Header.Set("Authorization", "Bearer batch-key")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("get status=%d body=%s", resp.StatusCode, body)
		}
		if err := json.Unmarshal(body, &completed); err != nil {
			t.Fatal(err)
		}
		if completed.State == batch.StateCompleted {
			break
		}
		if completed.State == batch.StateFailed || completed.State == batch.StateCancelled {
			t.Fatalf("unexpected terminal state: %#v", completed)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if completed.State != batch.StateCompleted || completed.HTTPStatus != http.StatusOK || completed.ExecutionRequestID == "" || completed.OutputRef == "" {
		t.Fatalf("completed=%#v", completed)
	}
	if !strings.Contains(upstreamBody, `"model":"qwen3:8b"`) || !strings.Contains(upstreamBody, `"run batch"`) {
		t.Fatalf("upstream body=%s", upstreamBody)
	}

	req, _ = http.NewRequest(http.MethodGet, front.URL+"/gateway/v1/batches/"+created.ID+"/output", nil)
	req.Header.Set("Authorization", "Bearer batch-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"batch-ok"`) {
		t.Fatalf("output status=%d body=%s", resp.StatusCode, body)
	}

	events := rec.Recent(10)
	found := false
	for _, e := range events {
		if e.ID == completed.ExecutionRequestID {
			found = true
			if e.ServiceClass != "batch" || e.Tenant != "team-a" || e.Actor != "alice" || e.Usage.PromptTokens != 3 || e.Usage.CompletionTokens != 2 {
				t.Fatalf("usage event=%#v", e)
			}
		}
	}
	if !found {
		t.Fatalf("execution request %s missing from usage: %#v", completed.ExecutionRequestID, events)
	}
}
