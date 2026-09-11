package server

import (
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
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/conversation"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	"github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func newConversationTestServer(t *testing.T) (*Server, *conversation.Store) {
	t.Helper()
	cc := config.ConversationsConfig{Enabled: true, EncryptionKey: strings.Repeat("k", 32), Retention: config.Duration(time.Hour), MaxEntries: 100, MaxContentBytes: 1 << 20}
	store, err := conversation.New(cc, filepath.Join(t.TempDir(), "conversations.enc.json"))
	if err != nil {
		t.Fatal(err)
	}
	return &Server{cfg: &config.Config{Conversations: cc}, conversations: store, log: slog.New(slog.NewTextHandler(io.Discard, nil))}, store
}

func TestPrepareResponseConversationExpandsPreviousResponse(t *testing.T) {
	s, store := newConversationTestServer(t)
	id := auth.Identity{Tenant: "tenant-a", Subject: "user-a", AuthType: "oidc"}
	prior := json.RawMessage(`[{"role":"user","content":"first"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]}]`)
	if err := store.Put(conversation.Entry{ID: "resp_prev", Tenant: id.Tenant, Actor: id.Actor(), Context: prior}); err != nil {
		t.Fatal(err)
	}

	body, plan, err := s.prepareResponseConversation([]byte(`{"model":"qwen3:8b","previous_response_id":"resp_prev","input":"follow up"}`), id)
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || !plan.Store || len(plan.RequestItems) != 3 {
		t.Fatalf("plan=%#v", plan)
	}
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		t.Fatal(err)
	}
	if _, exists := req["previous_response_id"]; exists {
		t.Fatal("previous_response_id must not be forwarded after expansion")
	}
	items, ok := req["input"].([]any)
	if !ok || len(items) != 3 {
		t.Fatalf("expanded input=%#v", req["input"])
	}
	last := items[2].(map[string]any)
	if last["role"] != "user" || last["content"] != "follow up" {
		t.Fatalf("last item=%#v", last)
	}
}

func TestPrepareResponseConversationDoesNotCrossIdentityBoundary(t *testing.T) {
	s, store := newConversationTestServer(t)
	if err := store.Put(conversation.Entry{ID: "resp_prev", Tenant: "tenant-a", Actor: "user-a", Context: json.RawMessage(`[]`)}); err != nil {
		t.Fatal(err)
	}
	_, _, err := s.prepareResponseConversation([]byte(`{"previous_response_id":"resp_prev","input":"x"}`), auth.Identity{Tenant: "tenant-a", Subject: "user-b", AuthType: "oidc"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("err=%v", err)
	}
}

func TestPrepareResponseConversationRespectsStoreFalse(t *testing.T) {
	s, _ := newConversationTestServer(t)
	_, plan, err := s.prepareResponseConversation([]byte(`{"input":"x","store":false}`), auth.Identity{Tenant: "t", Subject: "u", AuthType: "oidc"})
	if err != nil {
		t.Fatal(err)
	}
	if plan == nil || plan.Store {
		t.Fatalf("plan=%#v", plan)
	}
}

func TestParseResponsesOutputJSONAndSSE(t *testing.T) {
	id, out, err := parseResponsesOutput([]byte(`{"id":"resp_1","output":[{"type":"message","role":"assistant"}]}`))
	if err != nil || id != "resp_1" || len(out) != 1 {
		t.Fatalf("json: id=%q out=%#v err=%v", id, out, err)
	}

	sse := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_2\",\"output\":[]}}\n\n" +
		"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\"}}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_2\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\"}]}}\n\n"
	id, out, err = parseResponsesOutput([]byte(sse))
	if err != nil || id != "resp_2" || len(out) != 1 {
		t.Fatalf("sse: id=%q out=%#v err=%v", id, out, err)
	}
}

func TestResponsesPreviousResponseEndToEnd(t *testing.T) {
	var responseCalls int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"models":[{"name":"qwen3:8b","model":"qwen3:8b"}]}`)
		case "/api/tags":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"models":[{"name":"qwen3:8b","model":"qwen3:8b"}]}`)
		case "/v1/responses":
			responseCalls++
			var req map[string]any
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode upstream request: %v", err)
				w.WriteHeader(400)
				return
			}
			if _, exists := req["previous_response_id"]; exists {
				t.Errorf("previous_response_id leaked upstream on call %d", responseCalls)
			}
			if responseCalls == 1 {
				if req["input"] != "first" {
					t.Errorf("first input=%#v", req["input"])
				}
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, `{"id":"resp_1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"one"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`)
				return
			}
			items, ok := req["input"].([]any)
			if !ok || len(items) != 3 {
				t.Errorf("expanded second input=%#v", req["input"])
			} else {
				first := items[0].(map[string]any)
				assistant := items[1].(map[string]any)
				last := items[2].(map[string]any)
				if first["role"] != "user" || first["content"] != "first" {
					t.Errorf("first history item=%#v", first)
				}
				if assistant["role"] != "assistant" {
					t.Errorf("assistant history item=%#v", assistant)
				}
				if last["role"] != "user" || last["content"] != "second" {
					t.Errorf("last history item=%#v", last)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"id":"resp_2","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"two"}]}],"usage":{"input_tokens":3,"output_tokens":1}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backend.Close()

	cc := config.ConversationsConfig{Enabled: true, EncryptionKey: strings.Repeat("k", 32), Retention: config.Duration(time.Hour), MaxEntries: 100, MaxContentBytes: 1 << 20}
	conv, err := conversation.New(cc, filepath.Join(t.TempDir(), "conversations.enc.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		Server:            config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:              config.AuthConfig{IPBypass: []config.IPBypassConfig{{CIDRs: []string{"127.0.0.1/32"}, Tenant: "test", Subject: "u"}}},
		Scheduler:         config.SchedulerConfig{GlobalConcurrency: 1, MaxQueue: 8, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, ComputePaths: []string{"/v1/responses"}},
		Cost:              config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 16},
		Workers:           []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 1, HealthInterval: config.Duration(time.Hour)}},
		ModelCapabilities: config.ModelCapabilitiesConfig{Mode: "off", ContextGuard: "off"},
		Conversations:     cc,
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.SetModelCapabilitiesConfig(cfg.ModelCapabilities)
	wp.Start(ctx)
	rec, _ := usage.New("", 100, time.Second, nil)
	defer rec.Close()
	sv := New(cfg, Dependencies{Auth: a, Scheduler: scheduler.NewLocal(1, 8, 8), Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: proxy.New(), Usage: rec, Metrics: metrics.New(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Conversations: conv})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	resp, err := http.Post(front.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"qwen3:8b","input":"first"}`))
	if err != nil {
		t.Fatal(err)
	}
	firstBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(firstBody), `"id":"resp_1"`) {
		t.Fatalf("first status=%d body=%s", resp.StatusCode, firstBody)
	}
	if _, ok, err := conv.Get("resp_1", "test", "u"); err != nil || !ok {
		t.Fatalf("first response not stored: ok=%v err=%v", ok, err)
	}

	resp, err = http.Post(front.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"qwen3:8b","previous_response_id":"resp_1","input":"second"}`))
	if err != nil {
		t.Fatal(err)
	}
	secondBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(secondBody), `"id":"resp_2"`) {
		t.Fatalf("second status=%d body=%s", resp.StatusCode, secondBody)
	}
	stored, ok, err := conv.Get("resp_2", "test", "u")
	if err != nil || !ok {
		t.Fatalf("second response not stored: ok=%v err=%v", ok, err)
	}
	var final []any
	if err := json.Unmarshal(stored.Context, &final); err != nil {
		t.Fatal(err)
	}
	if len(final) != 4 {
		t.Fatalf("stored final context has %d items: %s", len(final), stored.Context)
	}
}
