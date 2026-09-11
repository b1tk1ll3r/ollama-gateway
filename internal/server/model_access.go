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

func cloneModelAccess(in config.ModelAccessConfig) config.ModelAccessConfig {
	cloneRule := func(r config.ModelAccessRule) config.ModelAccessRule {
		r.AllowedModels = append([]string(nil), r.AllowedModels...)
		r.DeniedModels = append([]string(nil), r.DeniedModels...)
		return r
	}
	out := config.ModelAccessConfig{Default: cloneRule(in.Default), Tenants: make(map[string]config.ModelAccessRule, len(in.Tenants))}
	for name, r := range in.Tenants {
		out.Tenants[name] = cloneRule(r)
	}
	return out
}

func (s *Server) modelAccessSnapshot() config.ModelAccessConfig {
	if s == nil {
		return config.ModelAccessConfig{Tenants: map[string]config.ModelAccessRule{}}
	}
	if v := s.modelAccess.Load(); v != nil {
		return cloneModelAccess(v.(config.ModelAccessConfig))
	}
	return cloneModelAccess(s.cfg.ModelAccess)
}

func (s *Server) runtimeTenantModelAccessRule(tenant string) config.ModelAccessRule {
	m := s.modelAccessSnapshot()
	if r, ok := m.Tenants[tenant]; ok {
		if r.Mode == "" {
			r.Mode = "allow_all"
		}
		return r
	}
	r := m.Default
	if r.Mode == "" {
		r.Mode = "allow_all"
	}
	return r
}

func normalizeAccessRule(r config.ModelAccessRule) config.ModelAccessRule {
	r.Mode = strings.TrimSpace(r.Mode)
	if r.Mode == "" {
		r.Mode = "allow_all"
	}
	r.AllowedModels = cleanStrings(r.AllowedModels)
	r.DeniedModels = cleanStrings(r.DeniedModels)
	return r
}

func validateModelAccessConfig(m config.ModelAccessConfig) error {
	if err := config.ValidateModelAccessRule(normalizeAccessRule(m.Default)); err != nil {
		return fmt.Errorf("default model access: %w", err)
	}
	for tenant, r := range m.Tenants {
		if strings.TrimSpace(tenant) == "" {
			return errors.New("tenant name is required")
		}
		if err := config.ValidateModelAccessRule(normalizeAccessRule(r)); err != nil {
			return fmt.Errorf("tenant %q model access: %w", tenant, err)
		}
	}
	return nil
}

func (s *Server) storeModelAccess(next config.ModelAccessConfig) error {
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
	next = cloneModelAccess(next)
	if next.Tenants == nil {
		next.Tenants = map[string]config.ModelAccessRule{}
	}
	candidate.ModelAccess = next
	if err := validateModelAccessConfig(next); err != nil {
		return err
	}
	if err := s.configStore.Save(&candidate); err != nil {
		return err
	}
	s.modelAccess.Store(cloneModelAccess(next))
	return nil
}

func (s *Server) uiModelAccess(w http.ResponseWriter, r *http.Request, actor auth.Identity) {
	if r.URL.Path == "/gateway/ui-api/model-access" {
		if r.Method != http.MethodGet {
			proxy.WriteJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "GET required")
			return
		}
		m := s.modelAccessSnapshot()
		names := make([]string, 0, len(m.Tenants))
		for n := range m.Tenants {
			names = append(names, n)
		}
		sort.Strings(names)
		writeJSON(w, http.StatusOK, map[string]any{"default": m.Default, "tenants": m.Tenants, "tenant_names": names, "runtime": true, "persistent": s.configStore != nil})
		return
	}
	raw := strings.TrimPrefix(r.URL.Path, "/gateway/ui-api/model-access/")
	tenant, err := url.PathUnescape(raw)
	tenant = strings.TrimSpace(tenant)
	if err != nil || tenant == "" || strings.Contains(tenant, "/") || len(tenant) > 256 {
		proxy.WriteJSONError(w, 400, "bad_tenant", "invalid tenant")
		return
	}
	if s.configStore == nil {
		proxy.WriteJSONError(w, 503, "config_store", "persistent configuration store unavailable")
		return
	}
	s.modelAccessMu.Lock()
	defer s.modelAccessMu.Unlock()
	next := s.modelAccessSnapshot()
	switch r.Method {
	case http.MethodPut:
		var in config.ModelAccessRule
		if err := decodeJSON(r, &in, 128<<10); err != nil {
			proxy.WriteJSONError(w, 400, "bad_model_access", err.Error())
			return
		}
		in = normalizeAccessRule(in)
		if err := config.ValidateModelAccessRule(in); err != nil {
			proxy.WriteJSONError(w, 400, "bad_model_access", err.Error())
			return
		}
		next.Tenants[tenant] = in
		if err := s.storeModelAccess(next); err != nil {
			proxy.WriteJSONError(w, 400, "model_access_store", err.Error())
			return
		}
		s.log.Info("tenant model access saved", "tenant", tenant, "mode", in.Mode, "admin_subject", actor.Subject)
		writeJSON(w, 200, map[string]any{"tenant": tenant, "rule": in, "restart_required": false})
	case http.MethodDelete:
		if _, ok := next.Tenants[tenant]; !ok {
			proxy.WriteJSONError(w, 404, "model_access_not_found", fmt.Sprintf("tenant model access %q not found", tenant))
			return
		}
		delete(next.Tenants, tenant)
		if err := s.storeModelAccess(next); err != nil {
			proxy.WriteJSONError(w, 400, "model_access_store", err.Error())
			return
		}
		s.log.Info("tenant model access reset", "tenant", tenant, "admin_subject", actor.Subject)
		writeJSON(w, 200, map[string]any{"deleted": true, "tenant": tenant, "restart_required": false})
	default:
		proxy.WriteJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed", "PUT or DELETE required")
	}
}
