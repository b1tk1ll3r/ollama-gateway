package state

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestPersistentAPIKeySurvivesAuthenticatorRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.json")
	store, err := NewAPIKeyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	a, err := auth.NewWithRuntimeStore(context.Background(), config.AuthConfig{}, store)
	if err != nil {
		t.Fatal(err)
	}
	info, secret, err := a.CreateAPIKey(auth.APIKeyCreate{Name: "openwebui", Tenant: "interactive", Application: "openwebui"})
	if err != nil {
		t.Fatal(err)
	}
	if info.Source != "persistent" {
		t.Fatalf("source=%q", info.Source)
	}

	store2, err := NewAPIKeyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := auth.NewWithRuntimeStore(context.Background(), config.AuthConfig{}, store2)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "http://gateway/api/tags", nil)
	r.Header.Set("Authorization", "Bearer "+secret)
	id, err := b.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if id.Tenant != "interactive" || id.Application != "openwebui" {
		t.Fatalf("identity=%#v", id)
	}
	if _, ok, err := b.DeleteAPIKey(info.ID); err != nil || !ok {
		t.Fatalf("delete ok=%v err=%v", ok, err)
	}

	store3, err := NewAPIKeyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	c, err := auth.NewWithRuntimeStore(context.Background(), config.AuthConfig{}, store3)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Authenticate(r); err == nil {
		t.Fatal("deleted key authenticated after restart")
	}
}

func TestPersistentPolicySurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policies.json")
	a, err := NewPolicyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	want := config.TenantPolicy{TenantWeight: 2, ActorWeight: 3, TenantCreditsPerMinute: 0, ActorCreditsPerMinute: 120}
	if err := a.Put(context.Background(), "team", want); err != nil {
		t.Fatal(err)
	}
	b, err := NewPolicyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.Get(context.Background(), "team")
	if err != nil || !ok {
		t.Fatalf("get ok=%v err=%v", ok, err)
	}
	if got != want {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
}

func TestConfigStoreKeepsBootstrapSecretsOutOfPersistentFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gateway-config.json")
	base := &config.Config{
		Auth:    config.AuthConfig{APIKeys: []config.APIKeyConfig{{Name: "admin", Key: "super-secret", Tenant: "ops"}}},
		Workers: []config.WorkerConfig{{Name: "w", URL: "http://127.0.0.1:11434", MaxConcurrent: 1}},
		UI:      config.UIConfig{SessionSecret: "01234567890123456789012345678901", OIDC: config.UIOIDCConfig{ClientSecret: "oidc-secret"}},
	}
	// Fill defaults/validation through the normal parser so the stored object is realistic.
	b, _ := json.Marshal(base)
	parsed, err := config.ParseBytes(b)
	if err != nil {
		t.Fatal(err)
	}
	store := NewConfigStore(path)
	if err := store.Save(parsed); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("super-secret")) || bytes.Contains(raw, []byte("oidc-secret")) || bytes.Contains(raw, []byte("01234567890123456789012345678901")) {
		t.Fatalf("persistent config leaked bootstrap secret: %s", raw)
	}
	loaded, err := store.LoadWithBootstrap(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Auth.APIKeys[0].Key != "super-secret" || loaded.UI.SessionSecret != parsed.UI.SessionSecret || loaded.UI.OIDC.ClientSecret != "oidc-secret" {
		t.Fatalf("secrets not restored: %#v", loaded.UI)
	}
}

func TestModelPlacementStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "model-placement.json")
	a, err := NewModelPlacementStore(path)
	if err != nil {
		t.Fatal(err)
	}
	want := config.ModelPlacementRule{Mode: "whitelist", AllowedModels: []string{"qwen3:8b", "gemma4:*"}, DeniedModels: []string{"gemma4:e4b"}}
	if err := a.Put(context.Background(), "rtx-4090", want); err != nil {
		t.Fatal(err)
	}
	b, err := NewModelPlacementStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := b.Get(context.Background(), "rtx-4090")
	if err != nil || !ok {
		t.Fatalf("get ok=%v err=%v", ok, err)
	}
	if got.Mode != want.Mode || len(got.AllowedModels) != 2 || len(got.DeniedModels) != 1 {
		t.Fatalf("got=%#v want=%#v", got, want)
	}
	if err := b.Delete(context.Background(), "rtx-4090"); err != nil {
		t.Fatal(err)
	}
	c, err := NewModelPlacementStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := c.Get(context.Background(), "rtx-4090"); ok {
		t.Fatal("deleted placement override survived restart")
	}
}

func TestWorkerRuntimeStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-state.json")
	s, err := NewWorkerRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), "node-2", "draining"); err != nil {
		t.Fatal(err)
	}
	s2, err := NewWorkerRuntimeStore(path)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s2.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if m["node-2"] != "draining" {
		t.Fatalf("modes=%v", m)
	}
	if err := s2.Delete(context.Background(), "node-2"); err != nil {
		t.Fatal(err)
	}
}
