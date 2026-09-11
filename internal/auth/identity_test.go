package auth

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestRuntimeAPIKeyCreateAuthenticateDelete(t *testing.T) {
	a, err := New(context.Background(), config.AuthConfig{APIKeys: []config.APIKeyConfig{{Name: "static", Key: "static-secret", Tenant: "ops", Scopes: []string{"gateway:admin"}}}})
	if err != nil {
		t.Fatal(err)
	}
	info, secret, err := a.CreateAPIKey(APIKeyCreate{Name: "openwebui", Tenant: "interactive", Application: "openwebui", Scopes: []string{"models:read", "models:read"}})
	if err != nil {
		t.Fatal(err)
	}
	if info.ID == "" || secret == "" || !strings.HasPrefix(secret, "ofg_") || !info.Deletable || info.Source != "runtime" {
		t.Fatalf("unexpected created key: %#v secret=%q", info, secret)
	}
	if len(info.Scopes) != 1 || info.Scopes[0] != "models:read" {
		t.Fatalf("unexpected scopes: %#v", info.Scopes)
	}

	r := httptest.NewRequest("GET", "http://gateway/api/tags", nil)
	r.Header.Set("Authorization", "Bearer "+secret)
	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if id.Tenant != "interactive" || id.Application != "openwebui" || id.Actor() != "app:openwebui" {
		t.Fatalf("unexpected identity: %#v", id)
	}

	keys := a.APIKeys()
	if len(keys) != 2 {
		t.Fatalf("keys=%d want 2: %#v", len(keys), keys)
	}
	for _, k := range keys {
		if strings.Contains(k.KeyHint, secret) {
			t.Fatalf("key listing leaked secret: %#v", k)
		}
	}

	deleted, ok, err := a.DeleteAPIKey(info.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || deleted.Name != "openwebui" {
		t.Fatalf("delete failed: ok=%v info=%#v", ok, deleted)
	}
	if _, err := a.Authenticate(r); err == nil {
		t.Fatal("deleted runtime key still authenticates")
	}
	if _, ok, err := a.DeleteAPIKey(info.ID); err != nil || ok {
		t.Fatal("second delete unexpectedly succeeded")
	}
}

func TestRuntimeAPIKeyRejectsDuplicateTenantName(t *testing.T) {
	a, err := New(context.Background(), config.AuthConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.CreateAPIKey(APIKeyCreate{Name: "client", Tenant: "team"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := a.CreateAPIKey(APIKeyCreate{Name: "client", Tenant: "team"}); err == nil {
		t.Fatal("expected duplicate error")
	}
	if _, _, err := a.CreateAPIKey(APIKeyCreate{Name: "client", Tenant: "other"}); err != nil {
		t.Fatalf("same name in other tenant should be allowed: %v", err)
	}
}

func TestRuntimeAPIKeyCarriesModelACL(t *testing.T) {
	a, err := New(context.Background(), config.AuthConfig{})
	if err != nil {
		t.Fatal(err)
	}
	info, secret, err := a.CreateAPIKey(APIKeyCreate{Name: "limited", Tenant: "team", AllowedModels: []string{"coding", "qwen3:*"}, DeniedModels: []string{"qwen3:70b*"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(info.AllowedModels) != 2 || len(info.DeniedModels) != 1 {
		t.Fatalf("info=%#v", info)
	}
	r := httptest.NewRequest("GET", "http://gateway/api/tags", nil)
	r.Header.Set("Authorization", "Bearer "+secret)
	id, err := a.Authenticate(r)
	if err != nil {
		t.Fatal(err)
	}
	if !id.ModelACLSet {
		t.Fatal("expected model ACL on identity")
	}
	if !config.ModelAccessAllowed(id.ModelAccess, "coding") || config.ModelAccessAllowed(id.ModelAccess, "qwen3:70b-q4") {
		t.Fatalf("unexpected ACL: %#v", id.ModelAccess)
	}
}
