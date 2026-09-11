package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

var ErrModelAccessDenied = errors.New("model access denied")
var ErrAliasUnavailable = errors.New("model alias has no routable target")

func (s *Server) tenantModelAccessRule(id auth.Identity) config.ModelAccessRule {
	return s.runtimeTenantModelAccessRule(id.Tenant)
}

func (s *Server) modelAllowed(id auth.Identity, model string) bool {
	if strings.TrimSpace(model) == "" {
		return true
	}
	// Tenant policy is the outer security boundary. An API-key ACL may narrow
	// that boundary, but must never widen it.
	if !config.ModelAccessAllowed(s.tenantModelAccessRule(id), model) {
		return false
	}
	if id.ModelACLSet && !config.ModelAccessAllowed(id.ModelAccess, model) {
		return false
	}
	return true
}

func (s *Server) resolveModel(ctx context.Context, id auth.Identity, requested string) (string, string, error) {
	requested = strings.TrimSpace(requested)
	if requested == "" {
		return "", "", nil
	}
	if !s.modelAllowed(id, requested) {
		return "", "", fmt.Errorf("%w: model %q is not allowed for this identity", ErrModelAccessDenied, requested)
	}
	a, ok := s.aliasConfig(requested)
	if !ok {
		return requested, "", nil
	}
	for _, m := range a.Models {
		m = strings.TrimSpace(m)
		if m == "" || !s.workers.CanRoute(m) {
			continue
		}
		if len(a.RequiredCapabilities) > 0 {
			meta, _, err := s.workers.Metadata(ctx, m)
			if err != nil {
				continue
			}
			okCaps := true
			for _, capName := range a.RequiredCapabilities {
				if !worker.HasCapability(meta, capName) {
					okCaps = false
					break
				}
			}
			if !okCaps {
				continue
			}
		}
		return m, requested, nil
	}
	return "", requested, fmt.Errorf("%w: alias %q has no healthy/eligible installed target", ErrAliasUnavailable, requested)
}

func rewriteModelBody(body []byte, model string) ([]byte, error) {
	if len(body) == 0 || strings.TrimSpace(model) == "" {
		return body, nil
	}
	var v map[string]any
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}
	v["model"] = model
	if _, ok := v["name"]; ok {
		v["name"] = model
	}
	return json.Marshal(v)
}

func (s *Server) visibleAliases(ctx context.Context, id auth.Identity) []string {
	aliases := s.aliasSnapshot()
	out := make([]string, 0, len(aliases))
	for name, a := range aliases {
		if a.Visible != nil && !*a.Visible {
			continue
		}
		if !s.modelAllowed(id, name) {
			continue
		}
		if _, _, err := s.resolveModel(ctx, id, name); err == nil {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
