package server

import (
	"context"
	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	px "github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/worker"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNativeStreamingPassthroughAndMetering(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/ps" {
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"models":[{"name":"qwen3:8b"}]}`)
			return
		}
		if r.URL.Path == "/api/chat" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			io.WriteString(w, "{\"message\":{\"content\":\"hi\"},\"done\":false}\n{\"done\":true,\"prompt_eval_count\":10,\"eval_count\":2,\"eval_duration\":1000}\n")
			return
		}
		w.WriteHeader(404)
	}))
	defer backend.Close()
	cfg := &config.Config{Server: config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute), MetricsPublic: true}, Auth: config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "test", Subject: "u"}}}, Scheduler: config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1}, Cost: config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 16}, Workers: []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}}}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	met := metrics.New()
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: met, Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"qwen3:8b","messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	want := "{\"message\":{\"content\":\"hi\"},\"done\":false}\n{\"done\":true,\"prompt_eval_count\":10,\"eval_count\":2,\"eval_duration\":1000}\n"
	if string(b) != want {
		t.Fatalf("body changed:\n%s", b)
	}
	var s usage.Summary
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s = rec.Actor(context.Background(), "test", "u")
		if s.PromptTokens == 10 && s.CompletionTokens == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if s.PromptTokens != 10 || s.CompletionTokens != 2 {
		t.Fatalf("usage not metered: %#v", s)
	}
}

func TestNativeNonComputeRequestBodyStreamsPastComputeLimit(t *testing.T) {
	const bodySize = 256 << 10
	gotSize := make(chan int64, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"models":[]}`)
		case "/api/blobs/sha256:test":
			n, _ := io.Copy(io.Discard, r.Body)
			gotSize <- n
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backend.Close()

	cfg := &config.Config{
		Server:    config.ServerConfig{MaxBodyBytes: 32, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:      config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "test", Subject: "u", Scopes: []string{"gateway:admin"}}}},
		Scheduler: config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/api/chat"}},
		Cost:      config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 16},
		Workers:   []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}},
		Native:    config.NativeConfig{ManagementRequiresAdmin: true, ControlWorker: "w"},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "w")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	met := metrics.New()
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: met, Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	req, _ := http.NewRequest(http.MethodPut, front.URL+"/api/blobs/sha256:test", io.LimitReader(strings.NewReader(strings.Repeat("x", bodySize)), bodySize))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	select {
	case n := <-gotSize:
		if n != bodySize {
			t.Fatalf("backend received %d bytes, want %d", n, bodySize)
		}
	case <-time.After(time.Second):
		t.Fatal("backend did not receive streamed body")
	}
}

func TestModelAliasAndTenantACL(t *testing.T) {
	seen := make(chan string, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			io.WriteString(w, `{"models":[{"name":"real:1","model":"real:1"}]}`)
		case "/api/tags":
			io.WriteString(w, `{"models":[{"name":"real:1","model":"real:1"}]}`)
		case "/api/show":
			io.WriteString(w, `{"capabilities":["completion"]}`)
		case "/api/chat":
			b, _ := io.ReadAll(r.Body)
			seen <- string(b)
			w.Header().Set("Content-Type", "application/x-ndjson")
			io.WriteString(w, `{"done":true,"prompt_eval_count":1,"eval_count":1}`+"\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer backend.Close()
	visible := true
	cfg := &config.Config{
		Server:            config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:              config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "t", Subject: "u"}}},
		Scheduler:         config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/api/chat"}},
		Cost:              config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 16},
		Workers:           []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}},
		ModelAliases:      map[string]config.ModelAliasConfig{"fast": {Models: []string{"real:1"}, Visible: &visible}},
		ModelAccess:       config.ModelAccessConfig{Default: config.ModelAccessRule{Mode: "whitelist", AllowedModels: []string{"fast"}}},
		ModelCapabilities: config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "off"},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "")
	wp.SetModelCapabilitiesConfig(cfg.ModelCapabilities)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	resp, err := http.Get(front.URL + "/api/tags")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(b), `"model":"fast"`) || strings.Contains(string(b), `"model":"real:1"`) {
		t.Fatalf("unexpected discovery: %s", b)
	}
	resp, err = http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"fast","messages":[{"role":"user","content":"x"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("alias status=%d", resp.StatusCode)
	}
	select {
	case body := <-seen:
		if !strings.Contains(body, `"model":"real:1"`) {
			t.Fatalf("backend body=%s", body)
		}
	case <-time.After(time.Second):
		t.Fatal("backend not called")
	}
	resp, err = http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"real:1","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("real model should be ACL denied, got %d", resp.StatusCode)
	}
}

func TestSafeRetryBeforeResponseAndCircuitOpen(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			io.WriteString(w, `{"models":[]}`)
		case "/api/tags":
			io.WriteString(w, `{"models":[{"name":"m","model":"m"}]}`)
		case "/api/show":
			io.WriteString(w, `{"capabilities":["completion"]}`)
		case "/api/chat":
			c, _, _ := w.(http.Hijacker).Hijack()
			_ = c.Close()
		default:
			http.NotFound(w, r)
		}
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			io.WriteString(w, `{"models":[]}`)
		case "/api/tags":
			io.WriteString(w, `{"models":[{"name":"m","model":"m"}]}`)
		case "/api/show":
			io.WriteString(w, `{"capabilities":["completion"]}`)
		case "/api/chat":
			w.Header().Set("Content-Type", "application/x-ndjson")
			io.WriteString(w, `{"done":true,"prompt_eval_count":2,"eval_count":1}`+"\n")
		default:
			http.NotFound(w, r)
		}
	}))
	defer good.Close()
	cfg := &config.Config{Server: config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)}, Auth: config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "t", Subject: "u"}}}, Scheduler: config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/api/chat"}}, Cost: config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 8}, Workers: []config.WorkerConfig{{Name: "bad", URL: bad.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}, {Name: "good", URL: good.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}}, Reliability: config.ReliabilityConfig{Enabled: true, FailureThreshold: 1, OpenDuration: config.Duration(time.Hour), RetryAttempts: 2}, ModelCapabilities: config.ModelCapabilitiesConfig{Mode: "off", ContextGuard: "off"}}
	a, _ := auth.New(context.Background(), cfg.Auth)
	wp := worker.New(cfg.Workers, "")
	wp.SetReliabilityConfig(cfg.Reliability)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"m","messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Gateway-Retry-Count"); got != "1" {
		t.Fatalf("retry header=%q", got)
	}
	if got := resp.Header.Get("X-Gateway-Worker"); got != "good" {
		t.Fatalf("worker=%q", got)
	}
	for _, snap := range wp.Snapshots() {
		if snap.Name == "bad" && snap.CircuitState != "open" {
			t.Fatalf("bad circuit=%s", snap.CircuitState)
		}
	}
}

func TestAPIKeyModelACLCanNarrowButNotWidenTenantACL(t *testing.T) {
	cfg := &config.Config{}
	cfg.ModelAccess = config.ModelAccessConfig{Default: config.ModelAccessRule{Mode: "whitelist", AllowedModels: []string{"fast", "qwen3:8b"}}}
	s := &Server{cfg: cfg}
	id := auth.Identity{Tenant: "team", ModelACLSet: true, ModelAccess: config.ModelAccessRule{Mode: "whitelist", AllowedModels: []string{"fast", "gemma4:*"}}}
	if !s.modelAllowed(id, "fast") {
		t.Fatal("expected intersection to allow fast")
	}
	if s.modelAllowed(id, "qwen3:8b") {
		t.Fatal("API key ACL should narrow tenant ACL")
	}
	if s.modelAllowed(id, "gemma4:latest") {
		t.Fatal("API key ACL must not widen tenant ACL")
	}
}
