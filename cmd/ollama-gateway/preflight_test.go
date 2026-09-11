package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/state"
)

func writeBootstrapConfig(t *testing.T, dataDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	body := `{
  "auth":{"api_keys":[{"name":"admin","key":"01234567890123456789012345678901","tenant":"t","subject":"s","scopes":["gateway:admin"]}]},
  "workers":[{"name":"remote","url":"http://10.2.10.48:11434","local_system_stats":true}],
  "ui":{"enabled":true,"path":"/admin","title":"Bootstrap"},
  "storage":{"data_dir":` + quoteJSON(dataDir) + `,"config_file":"gateway-config.json"}
}`
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func quoteJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestLoadEffectiveConfigUsesPersistentOverrideButBootstrapSecrets(t *testing.T) {
	dataDir := t.TempDir()
	path := writeBootstrapConfig(t, dataDir)
	base, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	persistent := *base
	persistent.UI.Title = "Persistent"
	store := state.NewConfigStore(state.Resolve(base.Storage).Config)
	if err := store.Save(&persistent); err != nil {
		t.Fatal(err)
	}

	loaded, err := loadEffectiveConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.Persistent || loaded.Config.UI.Title != "Persistent" {
		t.Fatalf("persistent override not loaded: %#v", loaded)
	}
	if got := loaded.Config.Auth.APIKeys[0].Key; got != "01234567890123456789012345678901" {
		t.Fatalf("bootstrap secret was not restored, got %q", got)
	}
}

func TestRunConfigCheckReportsWritableStorageAndWarning(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "state")
	path := writeBootstrapConfig(t, dataDir)
	var out bytes.Buffer
	if err := runConfigCheck(path, &out); err != nil {
		t.Fatal(err)
	}
	var got configCheckResult
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Status != "ok" || !got.StorageWritable || got.Workers != 1 {
		t.Fatalf("unexpected result: %+v", got)
	}
	if len(got.Warnings) == 0 || !strings.Contains(strings.Join(got.Warnings, "\n"), "local_system_stats=true") {
		t.Fatalf("expected remote local-system-stats warning, got %#v", got.Warnings)
	}
}

func TestRunProbe(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ok.Close()
	if err := runProbe(ok.URL, time.Second); err != nil {
		t.Fatal(err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	if err := runProbe(bad.URL, time.Second); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("expected HTTP 503 probe error, got %v", err)
	}
}

func TestComposeDoesNotRepeatEntrypointAndHasHealthcheck(t *testing.T) {
	b, err := os.ReadFile("../../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if strings.Contains(s, `command: ["/ollama-gateway"`) {
		t.Fatal("compose command must not repeat the image ENTRYPOINT")
	}
	if !strings.Contains(s, `command: ["-config"`) {
		t.Fatal("compose command must pass -config as an ENTRYPOINT argument")
	}
	if !strings.Contains(s, "healthcheck:") || !strings.Contains(s, "/healthz") {
		t.Fatal("compose must define a liveness healthcheck")
	}
}

func TestConfigWarningsFlagForwardedBypassAndBroadTrustedProxy(t *testing.T) {
	cfg := &config.Config{
		Auth: config.AuthConfig{
			IPBypassUseForwardedIP: true,
			TrustedProxies:         []string{"10.0.0.0/8", "127.0.0.1/8"},
			IPBypass: []config.IPBypassConfig{{
				CIDRs:  []string{"127.0.0.1/32"},
				Tenant: "local",
			}},
		},
		Storage: config.StorageConfig{DataDir: "/var/lib/ollama-gateway"},
	}
	warnings := strings.Join(configWarnings(cfg), "\n")
	if !strings.Contains(warnings, "ip_bypass_use_forwarded_ip=true") {
		t.Fatalf("missing forwarded bypass warning: %s", warnings)
	}
	if !strings.Contains(warnings, `broad CIDR "10.0.0.0/8"`) {
		t.Fatalf("missing broad trusted proxy warning: %s", warnings)
	}
	if strings.Contains(warnings, `broad CIDR "127.0.0.1/8"`) {
		t.Fatalf("loopback trusted proxy should not be flagged as broad: %s", warnings)
	}
}

func TestConfigWarningsFlagUnlimitedContextPolicy(t *testing.T) {
	cfg := &config.Config{
		ModelCapabilities: config.ModelCapabilitiesConfig{Context: config.ContextPolicyConfig{MaxRequestedTokens: -1, DefaultWorkerTokens: -1}},
		Storage:           config.StorageConfig{DataDir: "/var/lib/ollama-gateway"},
	}
	warnings := strings.Join(configWarnings(cfg), "\n")
	if !strings.Contains(warnings, "max_requested_tokens=-1") {
		t.Fatalf("missing unlimited context cap warning: %s", warnings)
	}
	if !strings.Contains(warnings, "default_worker_tokens=-1") {
		t.Fatalf("missing model-max fallback warning: %s", warnings)
	}
}
