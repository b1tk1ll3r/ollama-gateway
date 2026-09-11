package alerts

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestSignedWebhookAndResolution(t *testing.T) {
	var mu sync.Mutex
	healthy := false
	received := make(chan string, 4)
	secret := "test-secret"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		tsv := r.Header.Get("X-Ollama-Gateway-Timestamp")
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write([]byte(tsv + "."))
		_, _ = mac.Write(b)
		want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if r.Header.Get("X-Ollama-Gateway-Signature") != want {
			t.Errorf("bad signature")
		}
		received <- string(b)
		w.WriteHeader(204)
	}))
	defer ts.Close()
	cfg := config.AlertsConfig{Enabled: true, EvaluationInterval: config.Duration(time.Second), Cooldown: config.Duration(time.Hour), HistoryLimit: 20, Webhooks: []config.WebhookConfig{{Name: "test", URL: ts.URL, Secret: secret, Enabled: true}}, Thresholds: config.AlertThresholds{WorkerDownFor: config.Duration(time.Nanosecond)}}
	m, err := New(cfg, t.TempDir()+"/alerts.json", func() Snapshot {
		mu.Lock()
		h := healthy
		mu.Unlock()
		return Snapshot{Workers: []Worker{{Name: "w", Healthy: h}}}
	})
	if err != nil {
		t.Fatal(err)
	}
	m.Evaluate()
	time.Sleep(time.Millisecond)
	m.Evaluate()
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("firing webhook missing")
	}
	if len(m.Status().Active) != 1 {
		t.Fatalf("active=%#v", m.Status().Active)
	}
	mu.Lock()
	healthy = true
	mu.Unlock()
	m.Evaluate()
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("resolved webhook missing")
	}
	if len(m.Status().Active) != 0 {
		t.Fatal("alert did not resolve")
	}
}

func TestQuotaNearExhaustion(t *testing.T) {
	cfg := config.AlertsConfig{Enabled: true, Cooldown: config.Duration(time.Hour), HistoryLimit: 20, Thresholds: config.AlertThresholds{QuotaRemainingPct: 10}}
	m, err := New(cfg, t.TempDir()+"/alerts.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	m.ObserveQuota("t", "a", 5, 50, 100, 100)
	st := m.Status()
	if len(st.Active) != 1 || st.Active[0].Type != "quota_near_exhaustion" {
		t.Fatalf("active=%#v", st.Active)
	}
	m.ObserveQuota("t", "a", 50, 50, 100, 100)
	if len(m.Status().Active) != 0 {
		t.Fatal("quota alert did not resolve")
	}
}

func TestWebhookRetriesTransientFailure(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	seen := make(chan int, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		seen <- n
		if n < 3 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()
	cfg := config.AlertsConfig{
		Enabled: true, EvaluationInterval: config.Duration(time.Hour), Cooldown: config.Duration(time.Hour), HistoryLimit: 20,
		WebhookTimeout: config.Duration(time.Second), WebhookMaxConcurrent: 1, WebhookQueue: 8, WebhookRetryAttempts: 3, WebhookRetryBackoff: config.Duration(time.Millisecond),
		Webhooks: []config.WebhookConfig{{Name: "retry", URL: ts.URL, Enabled: true}},
	}
	m, err := New(cfg, t.TempDir()+"/alerts.json", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.TestWebhook("retry"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := attempts
	mu.Unlock()
	if got != 3 {
		t.Fatalf("attempts=%d want 3", got)
	}
	st := m.Status()
	if len(st.Deliveries) < 3 || st.Deliveries[0].StatusCode != http.StatusNoContent {
		t.Fatalf("deliveries=%#v", st.Deliveries)
	}
}
