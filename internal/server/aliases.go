package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/proxy"
)

type configOverrideLoader interface {
	LoadWithBootstrap(*config.Config) (*config.Config, error)
}

func cloneModelAliases(in map[string]config.ModelAliasConfig) map[string]config.ModelAliasConfig {
	out := make(map[string]config.ModelAliasConfig, len(in))
	for name, a := range in {
		a.Models = append([]string(nil), a.Models...)
		a.RequiredCapabilities = append([]string(nil), a.RequiredCapabilities...)
		if a.Visible != nil {
			v := *a.Visible
			a.Visible = &v
		}
		out[name] = a
	}
	return out
}

func (s *Server) aliasSnapshot() map[string]config.ModelAliasConfig {
	if s == nil {
		return map[string]config.ModelAliasConfig{}
	}
	v := s.aliases.Load()
	if v == nil {
		return cloneModelAliases(s.cfg.ModelAliases)
	}
	return cloneModelAliases(v.(map[string]config.ModelAliasConfig))
}

func (s *Server) aliasConfig(name string) (config.ModelAliasConfig, bool) {
	if s == nil {
		return config.ModelAliasConfig{}, false
	}
	v := s.aliases.Load()
	if v == nil {
		a, ok := s.cfg.ModelAliases[name]
		return a, ok
	}
	a, ok := v.(map[string]config.ModelAliasConfig)[name]
	return a, ok
}

func (s *Server) storeAliases(next map[string]config.ModelAliasConfig) error {
	if s.configStore == nil {
		return errors.New("persistent configuration store unavailable")
	}
	base := s.cfg
	if loader, ok := s.configStore.(configOverrideLoader); ok {
		if loaded, err := loader.LoadWithBootstrap(s.cfg); err == nil {
			base = loaded
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	b, err := json.Marshal(base)
	if err != nil {
		return err
	}
	var candidate config.Config
	if err := json.Unmarshal(b, &candidate); err != nil {
		return err
	}
	candidate.ModelAliases = cloneModelAliases(next)
	if err := validateAliasSet(candidate.ModelAliases); err != nil {
		return err
	}
	if err := s.configStore.Save(&candidate); err != nil {
		return err
	}
	// Publish only after persistence succeeds. Readers never observe a map that
	// would be lost on restart, and the stored map is never mutated in place.
	s.aliases.Store(cloneModelAliases(next))
	return nil
}

func validateAliasSet(aliases map[string]config.ModelAliasConfig) error {
	for name, a := range aliases {
		if strings.TrimSpace(name) == "" {
			return errors.New("model alias name is required")
		}
		if len(a.Models) == 0 {
			return fmt.Errorf("model alias %q requires at least one model", name)
		}
		for _, model := range a.Models {
			if strings.TrimSpace(model) == "" {
				return fmt.Errorf("model alias %q contains an empty model", name)
			}
		}
	}
	return nil
}

func (s *Server) uiModelAliases(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if r.URL.Path == "/gateway/ui-api/model-aliases" {
		if r.Method != http.MethodGet {
			proxy.WriteJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "GET required")
			return
		}
		aliases := s.aliasSnapshot()
		names := make([]string, 0, len(aliases))
		for name := range aliases {
			names = append(names, name)
		}
		sort.Strings(names)
		writeJSON(w, http.StatusOK, map[string]any{"aliases": aliases, "names": names, "runtime": true, "persistent": s.configStore != nil})
		return
	}

	raw := strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/model-aliases/")
	name, err := url.PathUnescape(raw)
	name = strings.TrimSpace(name)
	if err != nil || name == "" || strings.Contains(name, "/") || len(name) > 256 {
		proxy.WriteJSONError(w, http.StatusBadRequest, "bad_alias", "invalid model alias name")
		return
	}
	if s.configStore == nil {
		proxy.WriteJSONError(w, http.StatusServiceUnavailable, "config_store", "persistent configuration store unavailable")
		return
	}

	s.aliasMu.Lock()
	defer s.aliasMu.Unlock()
	next := s.aliasSnapshot()
	switch r.Method {
	case http.MethodPut:
		var in config.ModelAliasConfig
		if err := decodeJSON(r, &in, 128<<10); err != nil {
			proxy.WriteJSONError(w, http.StatusBadRequest, "bad_alias", err.Error())
			return
		}
		in.Models = cleanStrings(in.Models)
		in.RequiredCapabilities = cleanStrings(in.RequiredCapabilities)
		next[name] = in
		if err := s.storeAliases(next); err != nil {
			proxy.WriteJSONError(w, http.StatusBadRequest, "alias_store", err.Error())
			return
		}
		s.log.Info("model alias saved", "alias", name, "models", len(in.Models), "admin_subject", actor.Subject, "admin_auth_type", actor.AuthType)
		writeJSON(w, http.StatusOK, map[string]any{"name": name, "alias": in, "restart_required": false})
	case http.MethodDelete:
		if _, ok := next[name]; !ok {
			proxy.WriteJSONError(w, http.StatusNotFound, "alias_not_found", fmt.Sprintf("model alias %q not found", name))
			return
		}
		delete(next, name)
		if err := s.storeAliases(next); err != nil {
			proxy.WriteJSONError(w, http.StatusBadRequest, "alias_store", err.Error())
			return
		}
		s.log.Info("model alias deleted", "alias", name, "admin_subject", actor.Subject, "admin_auth_type", actor.AuthType)
		writeJSON(w, http.StatusOK, map[string]any{"deleted": true, "name": name, "restart_required": false})
	default:
		proxy.WriteJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "PUT or DELETE required")
	}
}

func cleanStrings(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
