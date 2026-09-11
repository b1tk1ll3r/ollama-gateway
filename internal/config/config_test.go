package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadRejectsRemovedRedisConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}],
	  "redis":{"url":"redis://127.0.0.1:6379"}
	}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("expected removed redis field to be rejected, got %v", err)
	}
}

func TestLoadInMemoryExampleShape(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "scheduler":{"global_concurrency":4,"max_queue":100,"max_queue_per_actor":10},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434","max_concurrent":4}],
	  "infrastructure":{"node_name":"gw","refresh_interval":"250ms","max_requests":64}
	}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Infrastructure.NodeName != "gw" || cfg.Scheduler.GlobalConcurrency != 4 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestExampleConfigsStrictParse(t *testing.T) {
	t.Setenv("OIDC_ISSUER", "https://issuer.example.test")
	t.Setenv("OIDC_AUDIENCE", "gateway")
	t.Setenv("GATEWAY_AUTOMATION_KEY", "01234567890123456789012345678901")
	t.Setenv("GATEWAY_UI_SESSION_SECRET", "01234567890123456789012345678901")
	t.Setenv("OIDC_UI_CLIENT_ID", "gateway-ui")
	t.Setenv("OIDC_UI_CLIENT_SECRET", "01234567890123456789012345678901")
	t.Setenv("GATEWAY_UI_REDIRECT_URL", "https://gateway.example.test/admin/callback")
	t.Setenv("OPENWEBUI_GATEWAY_KEY", "01234567890123456789012345678901")
	for _, path := range []string{
		"../../config.example.json",
		"../../config.oidc.example.json",
		"../../config.openwebui.example.json",
		"../../config.rtx4090.example.json",
		"../../config.placement.example.json",
	} {
		if _, err := Load(path); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
}

func TestModelPlacementValidation(t *testing.T) {
	good := ModelPlacementRule{Mode: "whitelist", AllowedModels: []string{"qwen3:8b", "gemma4:*"}, DeniedModels: []string{"gemma4:e4b"}}
	if err := ValidateModelPlacementRule(good); err != nil {
		t.Fatalf("good rule rejected: %v", err)
	}
	for _, bad := range []ModelPlacementRule{
		{Mode: "sometimes"},
		{Mode: "allow_all", AllowedModels: []string{"*gemma*"}},
		{Mode: "allow_all", DeniedModels: []string{"gemma*4*"}},
	} {
		if err := ValidateModelPlacementRule(bad); err == nil {
			t.Fatalf("bad rule accepted: %#v", bad)
		}
	}
}

func TestConversationsRequireEncryptionKeyWhenEnabled(t *testing.T) {
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}],
	  "conversations":{"enabled":true,"retention":"24h","max_entries":100,"max_content_bytes":4096}
	}`
	_, err := ParseBytes([]byte(body))
	if err == nil || !strings.Contains(err.Error(), "encryption_key") {
		t.Fatalf("expected encryption key validation error, got %v", err)
	}
}

func TestConversationsEnabledConfigParses(t *testing.T) {
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}],
	  "conversations":{"enabled":true,"encryption_key":"01234567890123456789012345678901","retention":"2h","max_entries":100,"max_content_bytes":4096}
	}`
	cfg, err := ParseBytes([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Conversations.Enabled || cfg.Storage.ConversationsFile != "conversations.enc.json" {
		t.Fatalf("unexpected conversation config: %+v storage=%+v", cfg.Conversations, cfg.Storage)
	}
}

func TestContextPolicySecureDefaults(t *testing.T) {
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}]
	}`
	cfg, err := ParseBytes([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.ModelCapabilities.Context
	if got.MaxRequestedTokens != 32768 || got.DefaultWorkerTokens != 4096 || got.EstimationMarginPercent != 15 || got.VisionReserveTokensPerImage != 2048 {
		t.Fatalf("unexpected context defaults: %+v", got)
	}
}

func TestWorkerContextLimitsValidation(t *testing.T) {
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434","context_limits":{"qwen:*":0}}]
	}`
	_, err := ParseBytes([]byte(body))
	if err == nil || !strings.Contains(err.Error(), "context_limits") {
		t.Fatalf("expected context limit validation error, got %v", err)
	}
}

func TestPublicDashboardDefaultsAndValidation(t *testing.T) {
	body := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}],
	  "public_dashboard":{"enabled":true}
	}`
	cfg, err := ParseBytes([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PublicDashboard.Path != "/status" || cfg.PublicDashboard.RefreshInterval.Value() != 2*time.Second || cfg.PublicDashboard.MaxLiveRequests != 64 {
		t.Fatalf("unexpected public dashboard defaults: %+v", cfg.PublicDashboard)
	}

	bad := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}],
	  "ui":{"path":"/admin"},
	  "public_dashboard":{"enabled":true,"path":"/admin","refresh_interval":"2s"}
	}`
	if _, err := ParseBytes([]byte(bad)); err == nil || !strings.Contains(err.Error(), "must not overlap") {
		t.Fatalf("expected public/admin path conflict, got %v", err)
	}

	tooFast := `{
	  "auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},
	  "workers":[{"name":"w","url":"http://127.0.0.1:11434"}],
	  "public_dashboard":{"enabled":true,"path":"/status","refresh_interval":"250ms"}
	}`
	if _, err := ParseBytes([]byte(tooFast)); err == nil || !strings.Contains(err.Error(), "at least 1s") {
		t.Fatalf("expected refresh interval validation, got %v", err)
	}
}

func TestPublicDashboardRejectsReservedNamespaceOverlap(t *testing.T) {
	for _, publicPath := range []string{"/gateway/status", "/api/status", "/v1/status", "/admin/public"} {
		body := `{"auth":{"ip_bypass":[{"cidrs":["127.0.0.1/32"],"tenant":"t","subject":"s"}]},"workers":[{"name":"w","url":"http://127.0.0.1:11434"}],"ui":{"path":"/admin"},"public_dashboard":{"enabled":true,"path":"` + publicPath + `","refresh_interval":"2s"}}`
		if _, err := ParseBytes([]byte(body)); err == nil {
			t.Fatalf("expected public path %q to be rejected", publicPath)
		}
	}
}
