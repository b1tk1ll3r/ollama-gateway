package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	px "github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func newPreflightServer(t *testing.T, capabilities []string, contextLength int64) *httptest.Server {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "codellama:7b", "model": "codellama:7b"}}})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "codellama:7b", "model": "codellama:7b"}}})
		case "/api/show":
			_ = json.NewEncoder(w).Encode(map[string]any{"capabilities": capabilities, "model_info": map[string]any{"llama.context_length": contextLength}})
		case "/api/chat":
			_ = json.NewEncoder(w).Encode(map[string]any{"done": true, "message": map[string]any{"content": "ok"}, "prompt_eval_count": 2, "eval_count": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(backend.Close)
	cfg := &config.Config{
		Server:            config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:              config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "t", Subject: "u"}}},
		Scheduler:         config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/api/chat"}},
		Cost:              config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3, CachedInputFactor: 1}, DefaultMaxOutputTokens: 16},
		Workers:           []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}},
		ModelCapabilities: config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "reject"},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "w")
	wp.SetModelCapabilitiesConfig(cfg.ModelCapabilities)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	wp.Start(ctx)
	met := metrics.New()
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: met, Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	t.Cleanup(front.Close)
	return front
}

func TestRejectsUnsupportedToolsBeforeOllama(t *testing.T) {
	front := newPreflightServer(t, []string{"completion"}, 16384)
	resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"codellama:7b","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"f"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if !strings.Contains(string(b), "does not support required capability: tools") {
		t.Fatalf("unexpected body: %s", b)
	}
}

func TestAllowsSupportedTools(t *testing.T) {
	front := newPreflightServer(t, []string{"completion", "tools"}, 16384)
	resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"codellama:7b","messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"f"}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("X-Gateway-Model-Capabilities"); !strings.Contains(got, "tools") {
		t.Fatalf("capability header=%q", got)
	}
}

func TestRejectsExplicitContextOverModelMaximum(t *testing.T) {
	front := newPreflightServer(t, []string{"completion"}, 4096)
	resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"codellama:7b","messages":[{"role":"user","content":"x"}],"options":{"num_ctx":8192}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "exceeds model context length") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
}

func newContextGuardServer(t *testing.T, modelMax, loadedContext int64, parameters string, ctxCfg config.ContextPolicyConfig, workerLimits map[string]int64) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var inferenceCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			models := []any{}
			if loadedContext > 0 {
				models = append(models, map[string]any{"name": "ctx:latest", "model": "ctx:latest", "context_length": loadedContext})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"models": models})
		case "/api/tags":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "ctx:latest", "model": "ctx:latest"}}})
		case "/api/show":
			_ = json.NewEncoder(w).Encode(map[string]any{"capabilities": []string{"completion", "vision"}, "model_info": map[string]any{"ctx.context_length": modelMax}, "parameters": parameters})
		case "/api/chat", "/v1/chat/completions", "/v1/responses", "/api/generate":
			inferenceCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"done": true, "message": map[string]any{"content": "ok"}, "choices": []any{}, "usage": map[string]any{"prompt_tokens": 2, "completion_tokens": 1}, "prompt_eval_count": 2, "eval_count": 1})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(backend.Close)
	cfg := &config.Config{
		Server:            config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:              config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "t", Subject: "u"}}},
		Scheduler:         config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/api/chat", "/api/generate", "/v1/chat/completions", "/v1/responses"}},
		Cost:              config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3, CachedInputFactor: 1}, DefaultMaxOutputTokens: 16},
		Workers:           []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour), ContextLimits: workerLimits}},
		ModelCapabilities: config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "reject", Context: ctxCfg},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "w")
	wp.SetModelCapabilitiesConfig(cfg.ModelCapabilities)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	wp.Start(ctx)
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	t.Cleanup(front.Close)
	return front, &inferenceCalls
}

func TestInvalidNumCtxRejectedBeforeBackend(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 0, "", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 15, VisionReserveTokensPerImage: 2048}, nil)
	for _, raw := range []string{`-1`, `0`, `"8192"`, `1.5`, `null`} {
		resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"ctx:latest","messages":[{"role":"user","content":"x"}],"options":{"num_ctx":`+raw+`}}`))
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 400 || !strings.Contains(string(b), "options.num_ctx must be a positive integer") {
			t.Fatalf("num_ctx=%s status=%d body=%s", raw, resp.StatusCode, b)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("backend called %d times", calls.Load())
	}
}

func TestResponsesInstructionsAndMaxOutputRespectEffectiveContext(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 4096, "", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 15, VisionReserveTokensPerImage: 2048}, nil)
	body := `{"model":"ctx:latest","instructions":"` + strings.Repeat("x", 8000) + `","input":"hello","max_output_tokens":3000}`
	resp, err := http.Post(front.URL+"/v1/responses", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "effective worker context") {
		t.Fatalf("status=%d body=%s headers=%v", resp.StatusCode, b, resp.Header)
	}
	if calls.Load() != 0 {
		t.Fatalf("backend called %d times", calls.Load())
	}
}

func TestOpenAIUsesModelfileNumCtxWhenModelNotLoaded(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 0, "num_ctx 16384\ntemperature 0.7", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 10, VisionReserveTokensPerImage: 2048}, nil)
	body := `{"model":"ctx:latest","messages":[{"role":"user","content":"` + strings.Repeat("x", 12000) + `"}],"max_tokens":1024}`
	resp, err := http.Post(front.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s headers=%v", resp.StatusCode, b, resp.Header)
	}
	if calls.Load() != 1 {
		t.Fatalf("backend calls=%d", calls.Load())
	}
	if got := resp.Header.Get("X-Gateway-Model-Configured-Context"); got != "16384" {
		t.Fatalf("configured context header=%q", got)
	}
}

func TestExplicitNumCtxHonorsGatewayAndWorkerCaps(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 4096, "", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 10, VisionReserveTokensPerImage: 2048}, map[string]int64{"*": 16384})
	resp, err := http.Post(front.URL+"/api/chat", "application/json", strings.NewReader(`{"model":"ctx:latest","messages":[{"role":"user","content":"x"}],"options":{"num_ctx":20000}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "cannot be served by any eligible worker") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if calls.Load() != 0 {
		t.Fatalf("backend called %d times", calls.Load())
	}
}

func TestVisionReserveParticipatesInContextGuard(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 4096, "", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 0, VisionReserveTokensPerImage: 3000}, nil)
	body := `{"model":"ctx:latest","messages":[{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAA"}}]}],"max_tokens":1500}`
	resp, err := http.Post(front.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || resp.Header.Get("X-Gateway-Context-Vision-Reserve") != "3000" {
		t.Fatalf("status=%d reserve=%q body=%s", resp.StatusCode, resp.Header.Get("X-Gateway-Context-Vision-Reserve"), b)
	}
	if calls.Load() != 0 {
		t.Fatalf("backend called %d times", calls.Load())
	}
}

func TestContextGuardRoutesToWorkerWithSufficientLoadedContext(t *testing.T) {
	var smallCalls, largeCalls atomic.Int64
	backend := func(ctxTokens int64, calls *atomic.Int64) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/ps":
				_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "ctx:latest", "model": "ctx:latest", "context_length": ctxTokens}}})
			case "/api/tags":
				_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{map[string]any{"name": "ctx:latest", "model": "ctx:latest"}}})
			case "/api/show":
				_ = json.NewEncoder(w).Encode(map[string]any{"capabilities": []string{"completion"}, "model_info": map[string]any{"ctx.context_length": 131072}})
			case "/v1/chat/completions":
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(map[string]any{"choices": []any{}, "usage": map[string]any{"prompt_tokens": 3000, "completion_tokens": 100}})
			default:
				http.NotFound(w, r)
			}
		}))
	}
	small := backend(4096, &smallCalls)
	defer small.Close()
	large := backend(16384, &largeCalls)
	defer large.Close()
	cfg := &config.Config{
		Server:    config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:      config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "t", Subject: "u"}}},
		Scheduler: config.SchedulerConfig{GlobalConcurrency: 2, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/v1/chat/completions"}},
		Cost:      config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3, CachedInputFactor: 1}, DefaultMaxOutputTokens: 16},
		Workers: []config.WorkerConfig{
			{Name: "small", URL: small.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)},
			{Name: "large", URL: large.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)},
		},
		ModelCapabilities: config.ModelCapabilitiesConfig{Mode: "enforce", CacheTTL: config.Duration(time.Hour), ContextGuard: "reject", Context: config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 15, VisionReserveTokensPerImage: 2048}},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "small")
	wp.SetModelCapabilitiesConfig(cfg.ModelCapabilities)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	rec, _ := usage.New("", 100, time.Second, nil)
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(2, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.Default()})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	body := `{"model":"ctx:latest","messages":[{"role":"user","content":"` + strings.Repeat("x", 12000) + `"}],"max_tokens":4096}`
	resp, err := http.Post(front.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("X-Gateway-Worker"); got != "large" {
		t.Fatalf("worker=%q, want large; small=%d large=%d", got, smallCalls.Load(), largeCalls.Load())
	}
	if smallCalls.Load() != 0 || largeCalls.Load() != 1 {
		t.Fatalf("unexpected backend calls small=%d large=%d", smallCalls.Load(), largeCalls.Load())
	}
}

func TestOpenAICannotSpoofEffectiveContextWithOptionsNumCtx(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 4096, "", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 15, VisionReserveTokensPerImage: 2048}, nil)
	resp, err := http.Post(front.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"ctx:latest","messages":[{"role":"user","content":"x"}],"options":{"num_ctx":16384}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "only supported on native Ollama /api endpoints") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if calls.Load() != 0 {
		t.Fatalf("backend called %d times", calls.Load())
	}
}

func TestGenerateSuffixParticipatesInContextGuard(t *testing.T) {
	front, calls := newContextGuardServer(t, 131072, 4096, "", config.ContextPolicyConfig{MaxRequestedTokens: 32768, DefaultWorkerTokens: 4096, EstimationMarginPercent: 10, VisionReserveTokensPerImage: 2048}, nil)
	body := `{"model":"ctx:latest","prompt":"hello","suffix":"` + strings.Repeat("s", 14000) + `","options":{"num_predict":1024}}`
	resp, err := http.Post(front.URL+"/api/generate", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || !strings.Contains(string(b), "effective worker context") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
	if calls.Load() != 0 {
		t.Fatalf("backend called %d times", calls.Load())
	}
}
