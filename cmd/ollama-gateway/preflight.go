package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/state"
)

type effectiveConfig struct {
	Bootstrap  *config.Config
	Config     *config.Config
	Paths      state.Paths
	Store      *state.ConfigStore
	Persistent bool
}

func loadEffectiveConfig(configPath string) (*effectiveConfig, error) {
	bootstrapCfg, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("configuration error: %w", err)
	}
	paths := state.Resolve(bootstrapCfg.Storage)
	configStore := state.NewConfigStore(paths.Config)
	cfg := bootstrapCfg
	persistentCfg, ok, err := configStore.LoadIfExists(bootstrapCfg)
	if err != nil {
		return nil, fmt.Errorf("persistent configuration error (%s): %w", configStore.Path(), err)
	}
	if ok {
		if persistentCfg.Storage != bootstrapCfg.Storage {
			return nil, fmt.Errorf("persistent configuration changes bootstrap-only storage settings: bootstrap=%+v persistent=%+v", bootstrapCfg.Storage, persistentCfg.Storage)
		}
		cfg = persistentCfg
	}
	return &effectiveConfig{
		Bootstrap:  bootstrapCfg,
		Config:     cfg,
		Paths:      state.Resolve(cfg.Storage),
		Store:      configStore,
		Persistent: ok,
	}, nil
}

type configCheckResult struct {
	Status             string   `json:"status"`
	ConfigPath         string   `json:"config_path"`
	PersistentOverride bool     `json:"persistent_override"`
	PersistentPath     string   `json:"persistent_path"`
	Workers            int      `json:"workers"`
	DataDir            string   `json:"data_dir"`
	DataDirAbsolute    string   `json:"data_dir_absolute"`
	StorageWritable    bool     `json:"storage_writable"`
	Warnings           []string `json:"warnings,omitempty"`
}

func runConfigCheck(configPath string, out io.Writer) error {
	loaded, err := loadEffectiveConfig(configPath)
	if err != nil {
		return err
	}
	if err := checkWritableDir(loaded.Paths.DataDir); err != nil {
		return fmt.Errorf("storage data_dir %q is not writable: %w", loaded.Paths.DataDir, err)
	}
	absDir, err := filepath.Abs(loaded.Paths.DataDir)
	if err != nil {
		absDir = loaded.Paths.DataDir
	}
	res := configCheckResult{
		Status:             "ok",
		ConfigPath:         configPath,
		PersistentOverride: loaded.Persistent,
		PersistentPath:     loaded.Store.Path(),
		Workers:            len(loaded.Config.Workers),
		DataDir:            loaded.Paths.DataDir,
		DataDirAbsolute:    absDir,
		StorageWritable:    true,
		Warnings:           configWarnings(loaded.Config),
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(res)
}

func checkWritableDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".gateway-preflight-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.WriteString("ok\n"); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func configWarnings(cfg *config.Config) []string {
	var warnings []string
	if control := strings.TrimSpace(cfg.Native.ControlWorker); control != "" {
		found := false
		for _, w := range cfg.Workers {
			if w.Name == control {
				found = true
				break
			}
		}
		if !found {
			warnings = append(warnings, fmt.Sprintf("native.control_worker %q does not match any configured worker; management requests will fall back to another healthy worker", control))
		}
	}
	for _, w := range cfg.Workers {
		if !w.LocalSystemStats {
			continue
		}
		u, err := url.Parse(w.URL)
		if err != nil {
			continue
		}
		host := u.Hostname()
		if host == "" || isLoopbackHost(host) {
			continue
		}
		warnings = append(warnings, fmt.Sprintf("worker %q has local_system_stats=true but URL host %q is remote; local system stats describe the gateway host, not that worker (use telemetry_url or disable local_system_stats)", w.Name, host))
	}
	if cfg.Auth.IPBypassUseForwardedIP && len(cfg.Auth.IPBypass) > 0 {
		warnings = append(warnings, "auth.ip_bypass_use_forwarded_ip=true allows X-Forwarded-For-derived addresses to satisfy credential-free IP bypass; use only with tightly restricted trusted_proxies and network ACLs")
	}
	for _, raw := range cfg.Auth.TrustedProxies {
		if broadTrustedProxyCIDR(raw) {
			warnings = append(warnings, fmt.Sprintf("auth.trusted_proxies contains broad CIDR %q; any directly reachable peer in that range can influence X-Forwarded-For-derived client_ip, so prefer exact proxy addresses", raw))
		}
	}
	if cfg.UI.Enabled && !cfg.UI.OIDC.Enabled && len(cfg.Auth.APIKeys) == 0 {
		warnings = append(warnings, "UI is enabled without UI OIDC and without bootstrap API keys; remote UI login depends entirely on IP bypass rules")
	}
	if cfg.UI.Enabled && !cfg.UI.SecureCookies && cfg.UI.OIDC.Enabled {
		warnings = append(warnings, "ui.secure_cookies=false while UI OIDC is enabled; enable secure cookies when the browser reaches the gateway over HTTPS")
	}
	if cfg.Server.MetricsPublic {
		warnings = append(warnings, "server.metrics_public=true exposes gateway metrics without authentication")
	}
	if cfg.ModelCapabilities.Context.MaxRequestedTokens == -1 {
		warnings = append(warnings, "model_capabilities.context.max_requested_tokens=-1 removes the gateway-side context cap; large native num_ctx requests can cause substantial KV-cache/VRAM pressure")
	}
	if cfg.ModelCapabilities.Context.DefaultWorkerTokens == -1 {
		warnings = append(warnings, "model_capabilities.context.default_worker_tokens=-1 falls back to the theoretical model maximum for unloaded models without Modelfile num_ctx; configure an explicit worker default for predictable memory use")
	}
	if !filepath.IsAbs(cfg.Storage.DataDir) {
		warnings = append(warnings, fmt.Sprintf("storage.data_dir %q is relative and therefore depends on the process working directory", cfg.Storage.DataDir))
	}
	return warnings
}

func broadTrustedProxyCIDR(raw string) bool {
	ip, n, err := net.ParseCIDR(strings.TrimSpace(raw))
	if err != nil || ip == nil || n == nil || ip.IsLoopback() {
		return false
	}
	ones, bits := n.Mask.Size()
	if bits == 32 {
		return ones < 24
	}
	if bits == 128 {
		return ones < 64
	}
	return false
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func runProbe(rawURL string, timeout time.Duration) error {
	if timeout <= 0 {
		return fmt.Errorf("probe timeout must be > 0")
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("invalid probe URL %q", rawURL)
	}
	client := &http.Client{Timeout: timeout}
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("probe returned HTTP %d", resp.StatusCode)
	}
	return nil
}
