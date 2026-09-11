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

func TestOpenWebUIOllamaCompatibility(t *testing.T) {
	var betaShowOnB atomic.Bool
	var betaChatOnB atomic.Bool

	backendA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			writeJSON(w, 200, map[string]any{"models": []any{}})
		case "/api/tags":
			// Deliberately omit "model" to verify gateway normalization for
			// clients such as OpenWebUI that key discovery by that field.
			io.WriteString(w, `{"models":[{"name":"alpha:latest","size":100,"details":{"family":"alpha"}}]}`)
		case "/api/version":
			io.WriteString(w, `{"version":"0.99.0"}`)
		case "/api/show":
			io.WriteString(w, `{"error":"model not found"}`)
		case "/api/chat":
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backendA.Close()

	backendB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			writeJSON(w, 200, map[string]any{"models": []any{}})
		case "/api/tags":
			io.WriteString(w, `{"models":[{"name":"beta:latest","model":"beta:latest","size":200,"details":{"family":"beta"}}]}`)
		case "/api/version":
			io.WriteString(w, `{"version":"0.99.0"}`)
		case "/api/show":
			b, _ := io.ReadAll(r.Body)
			var v map[string]any
			_ = json.Unmarshal(b, &v)
			if v["model"] == "beta:latest" {
				betaShowOnB.Store(true)
				io.WriteString(w, `{"modelfile":"FROM beta"}`)
				return
			}
			w.WriteHeader(http.StatusNotFound)
		case "/api/chat":
			betaChatOnB.Store(true)
			w.Header().Set("Content-Type", "application/x-ndjson")
			io.WriteString(w, "{\"message\":{\"content\":\"ok\"},\"done\":true,\"prompt_eval_count\":2,\"eval_count\":1}\n")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backendB.Close()

	cfg := &config.Config{
		Server: config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth: config.AuthConfig{APIKeys: []config.APIKeyConfig{{
			Name: "openwebui", Key: "owui-secret", Tenant: "apps", Subject: "openwebui", Application: "openwebui",
		}}},
		Scheduler: config.SchedulerConfig{
			GlobalConcurrency: 2, MaxQueue: 16, MaxQueuePerActor: 8,
			QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1,
			ComputePaths: []string{"/api/chat", "/api/generate", "/api/embed", "/api/embeddings", "/v1/chat/completions"},
		},
		Cost: config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 1}, DefaultMaxOutputTokens: 16},
		Workers: []config.WorkerConfig{
			{Name: "a", URL: backendA.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)},
			{Name: "b", URL: backendB.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)},
		},
		Native: config.NativeConfig{ControlWorker: "a"},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "a")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)
	rec, _ := usage.New("", 128, time.Second, nil)
	sv := New(cfg, Dependencies{
		Auth: a, Scheduler: scheduler.NewLocal(2, 16, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost),
		Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.Default(),
	})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	do := func(method, path, body string, withKey bool) (int, string) {
		req, _ := http.NewRequest(method, front.URL+path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if withKey {
			req.Header.Set("Authorization", "Bearer owui-secret")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, string(b)
	}

	if status, body := do(http.MethodGet, "/api/tags", "", false); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /api/tags status=%d, want 401", status)
	} else if !strings.Contains(body, `"error":"authentication required"`) {
		t.Fatalf("native Ollama error shape is not compatible: %s", body)
	}

	if status, body := do(http.MethodGet, "/v1/models", "", false); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /v1/models status=%d, want 401", status)
	} else if !strings.Contains(body, `"message":"authentication required"`) {
		t.Fatalf("OpenAI error shape changed unexpectedly: %s", body)
	}

	status, body := do(http.MethodGet, "/api/version", "", true)
	if status != 200 || !strings.Contains(body, `"version":"0.99.0"`) {
		t.Fatalf("version status=%d body=%s", status, body)
	}

	status, body = do(http.MethodGet, "/api/tags", "", true)
	if status != 200 || !strings.Contains(body, `"model":"alpha:latest"`) || !strings.Contains(body, `"model":"beta:latest"`) {
		t.Fatalf("tags status=%d body=%s", status, body)
	}

	status, body = do(http.MethodGet, "/v1/models", "", true)
	if status != 200 || !strings.Contains(body, `"id":"alpha:latest"`) || !strings.Contains(body, `"id":"beta:latest"`) || !strings.Contains(body, `"object":"list"`) {
		t.Fatalf("v1 models status=%d body=%s", status, body)
	}

	status, body = do(http.MethodPost, "/api/show", `{"model":"beta:latest"}`, true)
	if status != 200 || !betaShowOnB.Load() {
		t.Fatalf("show was not model-routed to backend B: status=%d body=%s", status, body)
	}

	status, body = do(http.MethodPost, "/api/chat", `{"model":"beta:latest","messages":[{"role":"user","content":"hi"}]}`, true)
	if status != 200 || !betaChatOnB.Load() || !strings.Contains(body, `"content":"ok"`) {
		t.Fatalf("chat was not model-routed to backend B: status=%d body=%s", status, body)
	}
}
