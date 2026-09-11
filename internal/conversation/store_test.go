package conversation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func testConfig() config.ConversationsConfig {
	return config.ConversationsConfig{Enabled: true, EncryptionKey: strings.Repeat("k", 32), Retention: config.Duration(time.Hour), MaxEntries: 2, MaxContentBytes: 4096}
}

func TestEncryptedRoundTripAndIdentityBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conversations.enc")
	s, err := New(testConfig(), path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := json.RawMessage(`[{"role":"user","content":"secret prompt"}]`)
	if err := s.Put(Entry{ID: "resp_1", Tenant: "t1", Actor: "a1", Context: ctx}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret prompt") {
		t.Fatalf("plaintext content leaked to disk: %s", b)
	}

	s2, err := New(testConfig(), path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := s2.Get("resp_1", "t1", "a1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if string(got.Context) != string(ctx) {
		t.Fatalf("context = %s", got.Context)
	}
	if _, ok, _ := s2.Get("resp_1", "t1", "other"); ok {
		t.Fatal("cross-actor lookup must be hidden")
	}
	if _, ok, _ := s2.Get("resp_1", "other", "a1"); ok {
		t.Fatal("cross-tenant lookup must be hidden")
	}
}

func TestWrongKeyFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conversations.enc")
	s, _ := New(testConfig(), path)
	if err := s.Put(Entry{ID: "resp_1", Tenant: "t", Actor: "a", Context: json.RawMessage(`[]`)}); err != nil {
		t.Fatal(err)
	}
	cfg := testConfig()
	cfg.EncryptionKey = strings.Repeat("z", 32)
	if _, err := New(cfg, path); err == nil {
		t.Fatal("expected wrong key to fail")
	}
}

func TestRetentionAndEntryLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conversations.enc")
	cfg := testConfig()
	cfg.Retention = config.Duration(time.Minute)
	s, err := New(cfg, path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	for i, id := range []string{"r1", "r2", "r3"} {
		if err := s.Put(Entry{ID: id, Tenant: "t", Actor: "a", CreatedAt: now.Add(time.Duration(i) * time.Second), Context: json.RawMessage(`[]`)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, _ := s.Get("r1", "t", "a"); ok {
		t.Fatal("oldest entry should be evicted")
	}
	now = now.Add(2 * time.Minute)
	if _, ok, _ := s.Get("r3", "t", "a"); ok {
		t.Fatal("expired entry should be pruned")
	}
}

func TestContentLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "conversations.enc")
	cfg := testConfig()
	cfg.MaxContentBytes = 2
	s, _ := New(cfg, path)
	if err := s.Put(Entry{ID: "r", Tenant: "t", Actor: "a", Context: json.RawMessage(`[]`)}); err != nil {
		t.Fatalf("2-byte context should fit: %v", err)
	}
	if err := s.Put(Entry{ID: "r2", Tenant: "t", Actor: "a", Context: json.RawMessage(`[1]`)}); err != ErrContextTooLarge {
		t.Fatalf("err=%v", err)
	}
}
