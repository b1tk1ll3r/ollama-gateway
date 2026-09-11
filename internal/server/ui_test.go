package server

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/batch"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/infrastructure"
	"github.com/example/ollama-fair-gateway/internal/liveflow"
	"github.com/example/ollama-fair-gateway/internal/metrics"
	"github.com/example/ollama-fair-gateway/internal/policy"
	px "github.com/example/ollama-fair-gateway/internal/proxy"
	"github.com/example/ollama-fair-gateway/internal/quota"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/session"
	"github.com/example/ollama-fair-gateway/internal/state"
	"github.com/example/ollama-fair-gateway/internal/usage"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

func newUITestServer(t *testing.T) (*Server, *session.Memory, func()) {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/ps":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"models":[{"name":"qwen3:8b","model":"qwen3:8b","size":5000000000,"size_vram":4200000000,"context_length":32768}]}`)
		case "/api/tags":
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"models":[{"name":"qwen3:8b","size":1234,"details":{"parameter_size":"8B","quantization_level":"Q4_K_M"}}]}`)
		case "/api/chat":
			// Simulate a long-running Ollama NDJSON stream. Cancelling the gateway
			// job must close the upstream request context and end this handler.
			w.Header().Set("Content-Type", "application/x-ndjson")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			t := time.NewTicker(25 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-r.Context().Done():
					return
				case <-t.C:
					_, _ = io.WriteString(w, `{"message":{"content":"x"},"done":false}`+"\n")
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	cfg := &config.Config{
		Server:         config.ServerConfig{MaxBodyBytes: 1 << 20, MaxRequestDuration: config.Duration(time.Minute)},
		Auth:           config.AuthConfig{APIKeys: []config.APIKeyConfig{{Name: "admin", Key: "test-admin-key", Tenant: "ops", Subject: "admin", Scopes: []string{"gateway:admin"}}}},
		Scheduler:      config.SchedulerConfig{GlobalConcurrency: 2, MaxQueue: 32, MaxQueuePerActor: 8, QueueTimeout: config.Duration(time.Second), DefaultTenantWeight: 1, DefaultActorWeight: 1, Policies: map[string]config.TenantPolicy{"*": {TenantWeight: 1, ActorWeight: 1, ActorCreditsPerMinute: 60, ActorBurstCredits: 120, TenantCreditsPerMinute: 300, TenantBurstCredits: 600}}, ComputePaths: []string{"/api/chat"}},
		Cost:           config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 16},
		Workers:        []config.WorkerConfig{{Name: "w", URL: backend.URL, MaxConcurrent: 2, HealthInterval: config.Duration(time.Hour), MemoryCapacityBytes: 256 << 30, VRAMCapacityBytes: 256 << 30}},
		Infrastructure: config.InfrastructureConfig{NodeName: "test-gateway", RefreshInterval: config.Duration(100 * time.Millisecond), MaxRequests: 100},
		UI:             config.UIConfig{Enabled: true, Path: "/admin", Title: "Test Gateway", RecentEvents: 100},
	}
	a, err := auth.New(context.Background(), cfg.Auth)
	if err != nil {
		backend.Close()
		t.Fatal(err)
	}
	wp := worker.New(cfg.Workers, "w")
	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	sched := scheduler.NewLocal(2, 32, 8)
	live := liveflow.New(10*time.Second, 100)
	infra := infrastructure.New(cfg.Infrastructure, live, sched, wp)
	infra.Start(ctx)
	rec, _ := usage.New("", 100, time.Second, nil)
	sessions := session.NewMemory()
	placementStore, err := state.NewModelPlacementStore(filepath.Join(t.TempDir(), "model-placement.json"))
	if err != nil {
		cancel()
		backend.Close()
		t.Fatal(err)
	}
	sv := New(cfg, Dependencies{Auth: a, Scheduler: sched, Quota: quota.Disabled{}, Estimator: cost.New(cfg.Cost), Workers: wp, Proxy: px.New(), Usage: rec, Metrics: metrics.New(), Live: live, Infrastructure: infra, Policies: policy.NewMemory(), Sessions: sessions, Logger: slog.Default(), PlacementStore: placementStore})
	return sv, sessions, func() { cancel(); backend.CloseClientConnections(); backend.Close() }
}

func TestUIStaticAndAuthenticatedSession(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	resp, err := http.Get(front.URL + "/admin/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "Ollama Fair Gateway") || !strings.Contains(string(body), "Request Pulse Map") || !strings.Contains(string(body), "LLM Infrastructure Map") || !strings.Contains(string(body), "Model Placement Matrix") || !strings.Contains(string(body), "Durable Batch Jobs") {
		t.Fatalf("static UI status=%d body=%s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Security-Policy"); !strings.Contains(got, "frame-ancestors 'none'") {
		t.Fatalf("missing CSP: %q", got)
	}

	req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/session", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"admin":true`) {
		t.Fatalf("session status=%d body=%s", resp.StatusCode, body)
	}
}

func TestUICookieMutationRequiresCSRF(t *testing.T) {
	sv, sessions, done := newUITestServer(t)
	defer done()
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	id, err := sessions.Create(context.Background(), "test-admin-key", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"tenant_weight":2,"actor_weight":1,"actor_credits_per_minute":100,"actor_burst_credits":200,"tenant_credits_per_minute":500,"tenant_burst_credits":1000}`

	req, _ := http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/policies/team-a", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(&http.Cookie{Name: uiSessionCookie, Value: id})
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("without CSRF status=%d, want 403", resp.StatusCode)
	}

	req, _ = http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/policies/team-a", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-CSRF-Token", "csrf-test")
	req.AddCookie(&http.Cookie{Name: uiSessionCookie, Value: id})
	req.AddCookie(&http.Cookie{Name: uiCSRFCookie, Value: "csrf-test"})
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("with CSRF status=%d body=%s", resp.StatusCode, b)
	}
}

func TestUIRuntimeAPIKeyCreateListDelete(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	doAdmin := func(method, path, body string) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, front.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-admin-key")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, b
	}

	resp, b := doAdmin(http.MethodGet, "/gateway/ui-api/api-keys", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"source":"config"`) || strings.Contains(string(b), "test-admin-key") {
		t.Fatalf("initial list status=%d body=%s", resp.StatusCode, b)
	}

	resp, b = doAdmin(http.MethodPost, "/gateway/ui-api/api-keys", `{"name":"openwebui","tenant":"interactive","application":"openwebui","scopes":[]}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", resp.StatusCode, b)
	}
	var created struct {
		Key struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"key"`
		Secret string `json:"secret"`
	}
	if err := json.Unmarshal(b, &created); err != nil {
		t.Fatal(err)
	}
	if created.Key.ID == "" || created.Key.Name != "openwebui" || !strings.HasPrefix(created.Secret, "ofg_") {
		t.Fatalf("unexpected create response: %s", b)
	}

	resp, b = doAdmin(http.MethodGet, "/gateway/ui-api/api-keys", "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"source":"runtime"`) || strings.Contains(string(b), created.Secret) {
		t.Fatalf("runtime list leaked or missing key status=%d body=%s", resp.StatusCode, b)
	}

	req, _ := http.NewRequest(http.MethodGet, front.URL+"/api/tags", nil)
	req.Header.Set("Authorization", "Bearer "+created.Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), "qwen3:8b") {
		t.Fatalf("runtime key auth status=%d body=%s", resp.StatusCode, b)
	}

	resp, b = doAdmin(http.MethodDelete, "/gateway/ui-api/api-keys/"+created.Key.ID, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", resp.StatusCode, b)
	}

	req, _ = http.NewRequest(http.MethodGet, front.URL+"/api/tags", nil)
	req.Header.Set("Authorization", "Bearer "+created.Secret)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("deleted key status=%d want 401", resp.StatusCode)
	}
}

func TestUIModelInventory(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/models", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), "qwen3:8b") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}
}

func TestUILiveSnapshotAndStream(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.live.Begin(liveflow.Request{ID: "live-1", Tenant: "team-a", Actor: "user-a", Model: "qwen3:8b", API: "ollama", Path: "/api/chat", EstimatedCredits: 2.5})
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/live", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"id":"live-1"`) || !strings.Contains(string(b), `"state":"queued"`) {
		t.Fatalf("snapshot status=%d body=%s", resp.StatusCode, b)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, front.URL+"/gateway/ui-api/live/stream", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Type"); !strings.Contains(got, "text/event-stream") {
		t.Fatalf("content-type=%q", got)
	}
	reader := bufio.NewReader(resp.Body)
	var block strings.Builder
	deadline := time.After(2 * time.Second)
	for !strings.Contains(block.String(), "\n\n") {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for SSE block: %q", block.String())
		default:
			line, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			block.WriteString(line)
		}
	}
	cancel()
	if got := block.String(); !strings.Contains(got, "event: snapshot") || !strings.Contains(got, `"id":"live-1"`) {
		t.Fatalf("unexpected SSE block: %s", got)
	}
}

func TestUIInfrastructureSnapshot(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.live.Begin(liveflow.Request{ID: "infra-1", Tenant: "team-a", Actor: "app-a", Model: "qwen3:8b", Worker: "w", API: "ollama", Path: "/api/chat", EstimatedCredits: 1.5})
	time.Sleep(150 * time.Millisecond)
	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/infrastructure", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), `"node_name":"test-gateway"`) || !strings.Contains(string(b), `"id":"infra-1"`) || !strings.Contains(string(b), `"size_vram":4200000000`) {
		t.Fatalf("infrastructure status=%d body=%s", resp.StatusCode, b)
	}
}

func TestUIJobCancelAbortsRunningRequest(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	result := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, front.URL+"/api/chat", strings.NewReader(`{"model":"qwen3:8b","messages":[{"role":"user","content":"hello"}]}`))
		req.Header.Set("Authorization", "Bearer test-admin-key")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			result <- 0
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		result <- resp.StatusCode
	}()

	var jobID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/jobs", nil)
		req.Header.Set("Authorization", "Bearer test-admin-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body struct {
			Jobs []struct {
				ID    string `json:"id"`
				State string `json:"state"`
			} `json:"jobs"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if len(body.Jobs) > 0 {
			jobID = body.Jobs[0].ID
			if body.Jobs[0].State == "running" || body.Jobs[0].State == "routing" {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if jobID == "" {
		t.Fatal("job did not appear in UI API")
	}

	req, _ := http.NewRequest(http.MethodPost, front.URL+"/gateway/ui-api/jobs/"+jobID+"/cancel", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("cancel status=%d body=%s", resp.StatusCode, b)
	}

	select {
	case status := <-result:
		// Once Ollama has already sent HTTP 200 for a stream the gateway cannot
		// change the wire status; cancellation is represented by terminating the
		// stream and by internal status 499 in live/accounting metadata.
		if status != http.StatusOK {
			t.Fatalf("stream HTTP status=%d want 200", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled request did not terminate")
	}

	snap := sv.live.Snapshot()
	found := false
	for _, r := range snap.Requests {
		if r.ID == jobID {
			found = true
			if r.State != liveflow.StateCancelled || r.Status != 499 {
				t.Fatalf("cancelled request state=%s status=%d", r.State, r.Status)
			}
		}
	}
	if !found {
		t.Fatal("cancelled request missing from live snapshot")
	}
}

func TestUIJobCancelRemovesQueuedRequest(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	// Force one global slot so the second request stays in the fair queue.
	sv.sched = scheduler.NewLocal(1, 32, 8)
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	startChat := func() <-chan int {
		ch := make(chan int, 1)
		go func() {
			req, _ := http.NewRequest(http.MethodPost, front.URL+"/api/chat", strings.NewReader(`{"model":"qwen3:8b","messages":[{"role":"user","content":"hello"}]}`))
			req.Header.Set("Authorization", "Bearer test-admin-key")
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				ch <- 0
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			ch <- resp.StatusCode
		}()
		return ch
	}
	cancel := func(id string) {
		req, _ := http.NewRequest(http.MethodPost, front.URL+"/gateway/ui-api/jobs/"+id+"/cancel", nil)
		req.Header.Set("Authorization", "Bearer test-admin-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("cancel %s status=%d", id, resp.StatusCode)
		}
	}
	listJobs := func() []jobView {
		req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/jobs", nil)
		req.Header.Set("Authorization", "Bearer test-admin-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out struct {
			Jobs []jobView `json:"jobs"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out.Jobs
	}

	first := startChat()
	var firstID string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, j := range listJobs() {
			if j.State == liveflow.StateRunning || j.State == liveflow.StateStreaming {
				firstID = j.ID
				break
			}
		}
		if firstID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if firstID == "" {
		t.Fatal("first request never became running")
	}

	second := startChat()
	var queuedID string
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, j := range listJobs() {
			if j.ID != firstID && j.State == liveflow.StateQueued {
				queuedID = j.ID
				break
			}
		}
		if queuedID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if queuedID == "" {
		cancel(firstID)
		t.Fatal("second request never entered queue")
	}

	cancel(queuedID)
	select {
	case status := <-second:
		if status != 499 {
			t.Fatalf("queued cancellation status=%d want 499", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued request did not terminate after cancel")
	}

	st := sv.sched.Stats(context.Background())
	if st.Queued != 0 {
		t.Fatalf("queued=%d want 0 after cancellation", st.Queued)
	}

	cancel(firstID)
	select {
	case <-first:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not terminate during cleanup")
	}
}

type testConfigStore struct {
	saved   *config.Config
	deleted bool
}

func (m *testConfigStore) Save(c *config.Config) error { x := *c; m.saved = &x; return nil }
func (m *testConfigStore) Delete() error               { m.deleted = true; return nil }
func (m *testConfigStore) Path() string                { return "/tmp/test-gateway-config.json" }

func TestUIPersistentConfigSaveAndReset(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.cfg.Storage = config.StorageConfig{DataDir: "./data", ConfigFile: "gateway-config.json", APIKeysFile: "api-keys.json", PoliciesFile: "policies.json", MetricsFile: "metrics.json", QuotaFile: "quota.json", WorkerPerformanceFile: "worker-performance.json", ModelPlacementFile: "model-placement.json", FlushInterval: config.Duration(10 * time.Second)}
	sv.cfg.Alerts.Webhooks = []config.WebhookConfig{{Name: "ops", URL: "https://alerts.example.invalid/hook", Secret: "webhook-secret", Enabled: true}}
	sv.cfg.OpenTelemetry.Headers = map[string]string{"Authorization": "Bearer otel-secret"}
	sv.cfg.Conversations = config.ConversationsConfig{Enabled: true, EncryptionKey: "01234567890123456789012345678901", Retention: config.Duration(time.Hour), MaxEntries: 100, MaxContentBytes: 4096}
	store := &testConfigStore{}
	sv.configStore = store
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	raw := sv.redactedConfig()
	rawBytes, _ := json.Marshal(raw)
	if strings.Contains(string(rawBytes), "webhook-secret") || strings.Contains(string(rawBytes), "otel-secret") || strings.Contains(string(rawBytes), "01234567890123456789012345678901") {
		t.Fatalf("redacted config leaked secret: %s", rawBytes)
	}
	costMap := raw["cost"].(map[string]any)
	def := costMap["default"].(map[string]any)
	def["output_credits_per_1k"] = 9.0
	b, _ := json.Marshal(raw)
	req, _ := http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/config", strings.NewReader(string(b)))
	req.Header.Set("Authorization", "Bearer test-admin-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("save status=%d body=%s", resp.StatusCode, body)
	}
	if store.saved == nil || store.saved.Cost.Default.OutputCreditsPer1K != 9 {
		t.Fatalf("saved=%#v", store.saved)
	}
	if len(store.saved.Auth.APIKeys) != 1 || store.saved.Auth.APIKeys[0].Key != "test-admin-key" {
		t.Fatalf("redaction overwrote bootstrap key: %#v", store.saved.Auth.APIKeys)
	}
	if len(store.saved.Alerts.Webhooks) != 1 || store.saved.Alerts.Webhooks[0].Secret != "webhook-secret" {
		t.Fatalf("redaction overwrote webhook secret: %#v", store.saved.Alerts.Webhooks)
	}
	if store.saved.OpenTelemetry.Headers["Authorization"] != "Bearer otel-secret" {
		t.Fatalf("redaction overwrote OTEL header: %#v", store.saved.OpenTelemetry.Headers)
	}
	if store.saved.Conversations.EncryptionKey != "01234567890123456789012345678901" {
		t.Fatalf("redaction overwrote conversation key: %#v", store.saved.Conversations)
	}

	req, _ = http.NewRequest(http.MethodDelete, front.URL+"/gateway/ui-api/config", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || !store.deleted {
		t.Fatalf("reset status=%d deleted=%v", resp.StatusCode, store.deleted)
	}
}

func TestUIStorageFlushStatusAndBackup(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	dir := t.TempDir()
	sv.cfg.Storage = config.StorageConfig{
		DataDir: dir, ConfigFile: "gateway-config.json", APIKeysFile: "api-keys.json",
		PoliciesFile: "policies.json", MetricsFile: "metrics.json", QuotaFile: "quota.json",
		WorkerPerformanceFile: "worker-performance.json", ModelPlacementFile: "model-placement.json", FlushInterval: config.Duration(time.Second),
	}
	sv.cfg.Usage.JournalDir = filepath.Join(dir, "usage")
	rec, err := usage.NewWithRetention(sv.cfg.Usage.JournalDir, 32, time.Hour, usage.RetentionConfig{DetailDays: 1, DailyDays: 400, CompactionInterval: time.Hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Close()
	sv.usage = rec
	sv.configStore = state.NewConfigStore(filepath.Join(dir, "gateway-config.json"))
	sv.metrics.Record("ollama", 200, time.Millisecond, time.Second, 10, 2, 1.25, 20, 30)
	rec.Record(usage.Event{ID: "persist-1", Time: time.Now().UTC(), Tenant: "ops", Subject: "admin", Actor: "admin", Status: 200})
	rec.Record(usage.Event{ID: "persist-old", Time: time.Now().UTC().AddDate(0, 0, -3), Tenant: "ops", Subject: "admin", Actor: "admin", Model: "qwen3:8b", Status: 200})

	front := httptest.NewServer(sv.Handler())
	defer front.Close()
	do := func(method, path string) (*http.Response, []byte) {
		t.Helper()
		req, _ := http.NewRequest(method, front.URL+path, nil)
		req.Header.Set("Authorization", "Bearer test-admin-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, b
	}

	resp, b := do(http.MethodPost, "/gateway/ui-api/storage/flush")
	if resp.StatusCode != 200 {
		t.Fatalf("flush status=%d body=%s", resp.StatusCode, b)
	}
	for _, name := range []string{"metrics.json", "worker-performance.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s not persisted: %v", name, err)
		}
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "usage", "usage-*.jsonl"))
	if len(matches) != 2 {
		t.Fatalf("usage journals missing: %v", matches)
	}

	resp, b = do(http.MethodGet, "/gateway/ui-api/storage")
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"mode":"local-persistent"`) || !strings.Contains(string(b), "worker performance") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, b)
	}

	resp, b = do(http.MethodPost, "/gateway/ui-api/storage/compact")
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"compacted":true`) {
		t.Fatalf("compact status=%d body=%s", resp.StatusCode, b)
	}
	daily, _ := filepath.Glob(filepath.Join(dir, "usage", "rollups", "daily", "rollup-daily-*.json"))
	if len(daily) != 1 {
		t.Fatalf("daily rollup missing after compaction: %v", daily)
	}
	resp, b = do(http.MethodGet, "/gateway/ui-api/usage/rollups?granularity=daily&dimension=model&name=qwen3:8b&limit=30")
	if resp.StatusCode != 200 || !strings.Contains(string(b), `"requests":1`) {
		t.Fatalf("rollup query status=%d body=%s", resp.StatusCode, b)
	}

	resp, b = do(http.MethodGet, "/gateway/ui-api/storage/backup")
	if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), "application/zip") {
		t.Fatalf("backup status=%d", resp.StatusCode)
	}
	zr, err := zip.NewReader(bytes.NewReader(b), int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range zr.File {
		names[f.Name] = true
	}
	if !names["state/metrics.json"] || !names["state/worker-performance.json"] || len(names) < 3 {
		t.Fatalf("backup entries=%v", names)
	}
}

func TestUIModelPlacementPersistsAndAppliesImmediately(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	placementPath := filepath.Join(t.TempDir(), "model-placement.json")
	store, err := state.NewModelPlacementStore(placementPath)
	if err != nil {
		t.Fatal(err)
	}
	sv.placementStore = store
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	put := func(body string) *http.Response {
		req, _ := http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/placement/w", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-admin-key")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	resp := put(`{"mode":"whitelist","allowed_models":["qwen3:8b"],"denied_models":[]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("put status=%d body=%s", resp.StatusCode, body)
	}
	if d, _ := sv.workers.PlacementDecision("w", "qwen3:8b"); !d.Allowed {
		t.Fatalf("qwen should be allowed: %#v", d)
	}
	if d, _ := sv.workers.PlacementDecision("w", "other:latest"); d.Allowed {
		t.Fatalf("other should be blocked by whitelist: %#v", d)
	}

	// A matrix-style exact deny should take effect without a restart.
	req, _ := http.NewRequest(http.MethodPost, front.URL+"/gateway/ui-api/placement/w/model", strings.NewReader(`{"model":"qwen3:8b","action":"deny"}`))
	req.Header.Set("Authorization", "Bearer test-admin-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("model action status=%d body=%s", resp.StatusCode, body)
	}
	if d, _ := sv.workers.PlacementDecision("w", "qwen3:8b"); d.Allowed {
		t.Fatalf("exact deny not applied: %#v", d)
	}

	// Reload the persistent store to prove durability.
	reloaded, err := state.NewModelPlacementStore(placementPath)
	if err != nil {
		t.Fatal(err)
	}
	if rule, ok, _ := reloaded.Get(context.Background(), "w"); !ok || len(rule.DeniedModels) != 1 {
		t.Fatalf("persistent rule=%#v ok=%v", rule, ok)
	}

	req, _ = http.NewRequest(http.MethodDelete, front.URL+"/gateway/ui-api/placement/w", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("reset status=%d", resp.StatusCode)
	}
	if d, _ := sv.workers.PlacementDecision("w", "other:latest"); !d.Allowed {
		t.Fatalf("reset did not restore allow_all baseline: %#v", d)
	}
}

func TestUIBatchAdminControlAndOutput(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	cfg := config.BatchJobsConfig{Enabled: true, Retention: config.Duration(time.Hour), MaxJobs: 100, MaxConcurrent: 1, MaxInputBytes: 1 << 20}
	dir := t.TempDir()
	m, err := batch.New(cfg, filepath.Join(dir, "batch-jobs.json"), filepath.Join(dir, "batch"))
	if err != nil {
		t.Fatal(err)
	}
	sv.batchJobs = m
	sv.cfg.BatchJobs = cfg
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	do := func(method, path string) (*http.Response, []byte) {
		req, _ := http.NewRequest(method, front.URL+path, nil)
		req.Header.Set("Authorization", "Bearer test-admin-key")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return resp, body
	}

	queued, err := m.Create(batch.IdentitySnapshot{Tenant: "tenant-a", Subject: "alice", Actor: "alice", AuthType: "oidc"}, "/api/chat", "qwen3:8b", []byte(`{"model":"qwen3:8b"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, body := do(http.MethodGet, "/gateway/ui-api/batches")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), queued.ID) || !strings.Contains(string(body), `"enabled":true`) {
		t.Fatalf("list status=%d body=%s", resp.StatusCode, body)
	}
	resp, body = do(http.MethodPost, "/gateway/ui-api/batches/"+queued.ID+"/pause")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"state":"paused"`) {
		t.Fatalf("pause status=%d body=%s", resp.StatusCode, body)
	}
	resp, body = do(http.MethodPost, "/gateway/ui-api/batches/"+queued.ID+"/resume")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"state":"queued"`) {
		t.Fatalf("resume status=%d body=%s", resp.StatusCode, body)
	}
	resp, body = do(http.MethodPost, "/gateway/ui-api/batches/"+queued.ID+"/cancel")
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"state":"cancelled"`) {
		t.Fatalf("cancel status=%d body=%s", resp.StatusCode, body)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx, func(ctx context.Context, j batch.Job, in io.Reader, out io.Writer) batch.RunResult {
		_, _ = io.WriteString(out, `{"ok":true}`)
		return batch.RunResult{HTTPStatus: http.StatusOK, ResponseContentType: "application/json", RequestID: "req-batch-ui"}
	})
	completed, err := m.Create(batch.IdentitySnapshot{Tenant: "tenant-b", Subject: "bob", Actor: "bob", AuthType: "oidc"}, "/api/chat", "qwen3:8b", []byte(`{"model":"qwen3:8b"}`))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, ok := m.Get(completed.ID, "", "", true)
		if ok && j.State == batch.StateCompleted {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	resp, body = do(http.MethodGet, "/gateway/ui-api/batches/"+completed.ID+"/output")
	if resp.StatusCode != http.StatusOK || string(body) != `{"ok":true}` || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("output status=%d content-type=%q body=%s", resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
}

func TestUIModelAliasRuntimeCRUDPersistsAndPublishesAtomically(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	store := &testConfigStore{}
	sv.configStore = store
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	put := func(path, body string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut, front.URL+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer test-admin-key")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	resp := put("/gateway/ui-api/model-aliases/quick", `{"models":[" qwen3:8b "],"required_capabilities":[" completion "]}`)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put status=%d body=%s", resp.StatusCode, body)
	}
	got, ok := sv.aliasConfig("quick")
	if !ok || len(got.Models) != 1 || got.Models[0] != "qwen3:8b" || len(got.RequiredCapabilities) != 1 || got.RequiredCapabilities[0] != "completion" {
		t.Fatalf("runtime alias=%#v ok=%v", got, ok)
	}
	if store.saved == nil || store.saved.ModelAliases["quick"].Models[0] != "qwen3:8b" {
		t.Fatalf("persistent alias missing: %#v", store.saved)
	}

	req, _ := http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/model-aliases", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"quick"`) || !strings.Contains(string(body), `"runtime":true`) {
		t.Fatalf("get status=%d body=%s", resp.StatusCode, body)
	}

	req, _ = http.NewRequest(http.MethodDelete, front.URL+"/gateway/ui-api/model-aliases/quick", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status=%d body=%s", resp.StatusCode, body)
	}
	if _, ok := sv.aliasConfig("quick"); ok {
		t.Fatal("deleted alias remains in runtime snapshot")
	}
	if store.saved == nil {
		t.Fatal("delete was not persisted")
	}
	if _, ok := store.saved.ModelAliases["quick"]; ok {
		t.Fatalf("deleted alias remains persisted: %#v", store.saved.ModelAliases)
	}
}

func TestUIModelAliasRejectsInvalidWithoutPublishing(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.configStore = &testConfigStore{}
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	req, _ := http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/model-aliases/broken", strings.NewReader(`{"models":[]}`))
	req.Header.Set("Authorization", "Bearer test-admin-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
	if _, ok := sv.aliasConfig("broken"); ok {
		t.Fatal("invalid alias was published")
	}
}

func TestUITenantModelAccessRuntimeCRUD(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	store := &testConfigStore{}
	sv.configStore = store
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	putBody := `{"mode":"whitelist","allowed_models":[" qwen3:* "],"denied_models":["qwen3:secret*"]}`
	req, _ := http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/model-access/interns", strings.NewReader(putBody))
	req.Header.Set("Authorization", "Bearer test-admin-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("put status=%d body=%s", resp.StatusCode, body)
	}
	id := auth.Identity{Tenant: "interns"}
	if !sv.modelAllowed(id, "qwen3:8b") || sv.modelAllowed(id, "gemma3:12b") || sv.modelAllowed(id, "qwen3:secret-1") {
		t.Fatalf("runtime ACL not enforced: %#v", sv.modelAccessSnapshot())
	}
	if store.saved == nil || store.saved.ModelAccess.Tenants["interns"].Mode != "whitelist" {
		t.Fatalf("persistent ACL missing: %#v", store.saved)
	}

	req, _ = http.NewRequest(http.MethodGet, front.URL+"/gateway/ui-api/model-access", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"interns"`) || !strings.Contains(string(body), `"runtime":true`) {
		t.Fatalf("get status=%d body=%s", resp.StatusCode, body)
	}

	req, _ = http.NewRequest(http.MethodDelete, front.URL+"/gateway/ui-api/model-access/interns", nil)
	req.Header.Set("Authorization", "Bearer test-admin-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete status=%d", resp.StatusCode)
	}
	if !sv.modelAllowed(id, "gemma3:12b") {
		t.Fatal("tenant ACL delete did not revert to allow-all default")
	}
	if _, ok := sv.modelAccessSnapshot().Tenants["interns"]; ok {
		t.Fatal("tenant ACL remains in runtime snapshot")
	}
}

func TestUITenantModelAccessRejectsInvalidPattern(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.configStore = &testConfigStore{}
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	req, _ := http.NewRequest(http.MethodPut, front.URL+"/gateway/ui-api/model-access/interns", strings.NewReader(`{"mode":"whitelist","allowed_models":["bad*pattern"]}`))
	req.Header.Set("Authorization", "Bearer test-admin-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", resp.StatusCode)
	}
	if _, ok := sv.modelAccessSnapshot().Tenants["interns"]; ok {
		t.Fatal("invalid tenant ACL was published")
	}
}

func TestPublicDashboardIsUnauthenticatedAndSanitized(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.cfg.PublicDashboard = config.PublicDashboardConfig{
		Enabled: true, Path: "/status", Title: "Public Test", Subtitle: "Sanitized",
		RefreshInterval: config.Duration(2 * time.Second), MaxLiveRequests: 16,
		ShowWorkerNames: false, ShowModelNames: false, ShowResourceMetrics: true,
		WorkerDisplayNames: map[string]string{"w": "GPU Node A"},
	}
	sv.live.Begin(liveflow.Request{ID: "secret-request-id", Tenant: "super-secret-tenant", Actor: "user:alice@example.test", Application: "private-app", Model: "private-model:latest", Worker: "w", API: "openai", Path: "/v1/chat/completions", EstimatedCredits: 99})

	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	resp, err := http.Get(front.URL + "/status/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Read-only public status") {
		t.Fatalf("public UI status=%d body=%s", resp.StatusCode, body)
	}
	if got := resp.Header.Get("Content-Security-Policy"); !strings.Contains(got, "form-action 'none'") {
		t.Fatalf("public UI missing restrictive CSP: %q", got)
	}

	resp, err = http.Get(front.URL + "/status/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status=%d body=%s", resp.StatusCode, body)
	}
	text := string(body)
	for _, forbidden := range []string{
		"super-secret-tenant", "alice@example.test", "private-app", "secret-request-id",
		`"tenant"`, `"actor"`, `"application"`, `"url"`, `"labels"`, `"last_error"`, `"telemetry_error"`, `"estimated_credits"`,
	} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("public snapshot leaked %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, `"name":"GPU Node A"`) {
		t.Fatalf("configured public worker alias missing: %s", text)
	}
	if strings.Contains(text, "private-model:latest") || !strings.Contains(text, "Model ") {
		t.Fatalf("model anonymization failed: %s", text)
	}
	if strings.Contains(text, `"id":"secret-request-id"`) || !strings.Contains(text, `"id":"REQ-`) {
		t.Fatalf("request id was not anonymized: %s", text)
	}
}

func TestPublicDashboardDisabledIs404WithoutAuthChallenge(t *testing.T) {
	sv, _, done := newUITestServer(t)
	defer done()
	sv.cfg.PublicDashboard = config.PublicDashboardConfig{Enabled: false, Path: "/status"}
	front := httptest.NewServer(sv.Handler())
	defer front.Close()

	resp, err := http.Get(front.URL + "/status/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status=%d want 404", resp.StatusCode)
	}
	if got := resp.Header.Get("WWW-Authenticate"); got != "" {
		t.Fatalf("disabled public dashboard should not enter auth middleware: %q", got)
	}
}
