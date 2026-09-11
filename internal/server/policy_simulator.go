package server

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/example/ollama-fair-gateway/internal/auth"
	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type policySimulationRequest struct {
	Tenant               string   `json:"tenant"`
	APIKeyID             string   `json:"api_key_id,omitempty"`
	APIKeyName           string   `json:"api_key_name,omitempty"`
	Model                string   `json:"model"`
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	InputTokens          int64    `json:"input_tokens,omitempty"`
	OutputTokens         int64    `json:"output_tokens,omitempty"`
	ServiceClass         string   `json:"service_class,omitempty"`
}

type accessExplanation struct {
	TenantAllowed bool                   `json:"tenant_allowed"`
	KeyApplied    bool                   `json:"key_applied"`
	KeyAllowed    bool                   `json:"key_allowed"`
	Allowed       bool                   `json:"allowed"`
	TenantRule    config.ModelAccessRule `json:"tenant_rule"`
	KeyRule       config.ModelAccessRule `json:"key_rule,omitempty"`
}

type aliasCandidateExplanation struct {
	Model        string   `json:"model"`
	Routable     bool     `json:"routable"`
	Capabilities []string `json:"capabilities,omitempty"`
	Reason       string   `json:"reason,omitempty"`
}

type policySimulationResult struct {
	RequestedModel       string                      `json:"requested_model"`
	ResolvedModel        string                      `json:"resolved_model,omitempty"`
	Alias                string                      `json:"alias,omitempty"`
	AliasCandidates      []aliasCandidateExplanation `json:"alias_candidates,omitempty"`
	Access               accessExplanation           `json:"access"`
	CapabilitiesRequired []string                    `json:"capabilities_required,omitempty"`
	CapabilitiesKnown    []string                    `json:"capabilities_known,omitempty"`
	CapabilitiesOK       bool                        `json:"capabilities_ok"`
	ContextLength        int64                       `json:"context_length,omitempty"`
	ContextEffectiveMax  int64                       `json:"context_effective_max,omitempty"`
	ContextRequested     int64                       `json:"context_requested,omitempty"`
	ContextOK            bool                        `json:"context_ok"`
	EstimatedCredits     float64                     `json:"estimated_credits"`
	CostRate             config.ModelRate            `json:"cost_rate"`
	TenantPolicy         config.TenantPolicy         `json:"tenant_policy"`
	ServiceClass         string                      `json:"service_class"`
	ServiceClassConfig   config.ServiceClassConfig   `json:"service_class_config"`
	Workers              []worker.RoutingExplanation `json:"workers"`
	SelectedWorker       string                      `json:"selected_worker,omitempty"`
	Decision             string                      `json:"decision"`
	Errors               []string                    `json:"errors,omitempty"`
}

func (s *Server) simulatedIdentity(in policySimulationRequest) (auth.Identity, *auth.APIKeyInfo, error) {
	tenant := strings.TrimSpace(in.Tenant)
	var selected *auth.APIKeyInfo
	if strings.TrimSpace(in.APIKeyID) != "" || strings.TrimSpace(in.APIKeyName) != "" {
		for _, k := range s.auth.APIKeys() {
			idMatch := in.APIKeyID != "" && k.ID == in.APIKeyID
			nameMatch := in.APIKeyID == "" && in.APIKeyName != "" && k.Name == in.APIKeyName && (tenant == "" || k.Tenant == tenant)
			if idMatch || nameMatch {
				kk := k
				selected = &kk
				break
			}
		}
		if selected == nil {
			return auth.Identity{}, nil, fmt.Errorf("API key not found")
		}
		if tenant == "" {
			tenant = selected.Tenant
		} else if selected.Tenant != tenant {
			return auth.Identity{}, nil, fmt.Errorf("API key belongs to tenant %q, not %q", selected.Tenant, tenant)
		}
	}
	if tenant == "" {
		tenant = "default"
	}
	id := auth.Identity{Tenant: tenant, Subject: "policy-simulator", Application: "admin-ui", AuthType: "simulation", Scopes: map[string]bool{}}
	if selected != nil {
		id.Subject = selected.Subject
		id.Application = selected.Application
		id.ServiceClass = selected.ServiceClass
		if len(selected.AllowedModels) > 0 || len(selected.DeniedModels) > 0 {
			id.ModelACLSet = true
			id.ModelAccess = config.ModelAccessRule{Mode: "allow_all", AllowedModels: append([]string(nil), selected.AllowedModels...), DeniedModels: append([]string(nil), selected.DeniedModels...)}
		}
		for _, scope := range selected.Scopes {
			id.Scopes[scope] = true
		}
	}
	return id, selected, nil
}

func (s *Server) simulatePolicy(ctx context.Context, in policySimulationRequest) (policySimulationResult, error) {
	in.Model = strings.TrimSpace(in.Model)
	if in.Model == "" {
		return policySimulationResult{}, fmt.Errorf("model is required")
	}
	if in.InputTokens < 0 || in.OutputTokens < 0 {
		return policySimulationResult{}, fmt.Errorf("token estimates must be >= 0")
	}
	id, _, err := s.simulatedIdentity(in)
	if err != nil {
		return policySimulationResult{}, err
	}
	result := policySimulationResult{RequestedModel: in.Model, CapabilitiesOK: true, ContextOK: true}
	tenantRule := s.tenantModelAccessRule(id)
	result.Access = accessExplanation{TenantAllowed: config.ModelAccessAllowed(tenantRule, in.Model), TenantRule: tenantRule, KeyAllowed: true}
	if id.ModelACLSet {
		result.Access.KeyApplied = true
		result.Access.KeyRule = id.ModelAccess
		result.Access.KeyAllowed = config.ModelAccessAllowed(id.ModelAccess, in.Model)
	}
	result.Access.Allowed = result.Access.TenantAllowed && result.Access.KeyAllowed
	if !result.Access.Allowed {
		result.Decision = "denied_model_access"
		return result, nil
	}

	resolved := in.Model
	if alias, ok := s.aliasConfig(in.Model); ok {
		result.Alias = in.Model
		for _, candidate := range alias.Models {
			candidate = strings.TrimSpace(candidate)
			if candidate == "" {
				continue
			}
			x := aliasCandidateExplanation{Model: candidate, Routable: s.workers.CanRoute(candidate)}
			if !x.Routable {
				x.Reason = "no_eligible_worker"
			}
			if len(alias.RequiredCapabilities) > 0 {
				meta, _, metaErr := s.workers.Metadata(ctx, candidate)
				if metaErr != nil {
					x.Reason = "metadata_unavailable"
				} else {
					x.Capabilities = append([]string(nil), meta.Capabilities...)
					for _, c := range alias.RequiredCapabilities {
						if !worker.HasCapability(meta, c) {
							x.Routable = false
							x.Reason = "missing_alias_capability:" + c
							break
						}
					}
				}
			}
			result.AliasCandidates = append(result.AliasCandidates, x)
			if resolved == in.Model && x.Routable {
				resolved = candidate
			}
		}
		if resolved == in.Model {
			result.Decision = "alias_unavailable"
			return result, nil
		}
	}
	result.ResolvedModel = resolved

	requiredMap := map[string]bool{}
	for _, c := range in.RequiredCapabilities {
		if c = strings.TrimSpace(c); c != "" {
			requiredMap[c] = true
		}
	}
	if alias, ok := s.aliasConfig(in.Model); ok {
		for _, c := range alias.RequiredCapabilities {
			if c = strings.TrimSpace(c); c != "" {
				requiredMap[c] = true
			}
		}
	}
	for c := range requiredMap {
		result.CapabilitiesRequired = append(result.CapabilitiesRequired, c)
	}
	sort.Strings(result.CapabilitiesRequired)
	if len(result.CapabilitiesRequired) > 0 || in.InputTokens+in.OutputTokens > 0 {
		meta, _, metaErr := s.workers.Metadata(ctx, resolved)
		if metaErr != nil {
			result.Errors = append(result.Errors, "model metadata unavailable: "+metaErr.Error())
		} else {
			result.CapabilitiesKnown = append([]string(nil), meta.Capabilities...)
			result.ContextLength = meta.ContextLength
			for _, c := range result.CapabilitiesRequired {
				if !worker.HasCapability(meta, c) {
					result.CapabilitiesOK = false
					result.Errors = append(result.Errors, "missing capability: "+c)
				}
			}
		}
	}
	margin := int64(0)
	if pct := s.cfg.ModelCapabilities.Context.EstimationMarginPercent; pct > 0 && in.InputTokens > 0 {
		margin = int64(math.Ceil(float64(in.InputTokens) * pct / 100))
	}
	result.ContextRequested = in.InputTokens + margin + in.OutputTokens
	contextAllowed := map[string]bool{}
	for _, cw := range s.workers.ContextWindows(ctx, resolved) {
		effective := cw.EffectiveTokens
		if cap := s.cfg.ModelCapabilities.Context.MaxRequestedTokens; cap > 0 {
			effective = minPositive64(effective, cap)
		}
		if effective > result.ContextEffectiveMax {
			result.ContextEffectiveMax = effective
		}
		if effective > 0 && result.ContextRequested <= effective {
			contextAllowed[cw.Worker] = true
		}
	}
	if cap := s.cfg.ModelCapabilities.Context.MaxRequestedTokens; cap > 0 && result.ContextRequested > cap {
		result.ContextOK = false
		result.Errors = append(result.Errors, fmt.Sprintf("context %d exceeds gateway cap %d", result.ContextRequested, cap))
	} else if result.ContextEffectiveMax > 0 && result.ContextRequested > result.ContextEffectiveMax {
		result.ContextOK = false
		result.Errors = append(result.Errors, fmt.Sprintf("context %d exceeds effective worker context %d", result.ContextRequested, result.ContextEffectiveMax))
	} else if result.ContextLength > 0 && result.ContextRequested > result.ContextLength {
		result.ContextOK = false
		result.Errors = append(result.Errors, fmt.Sprintf("context %d exceeds model limit %d", result.ContextRequested, result.ContextLength))
	}

	rate := s.estimator.Rate(resolved)
	result.CostRate = rate
	result.EstimatedCredits = float64(in.InputTokens)/1000*rate.InputCreditsPer1K + float64(in.OutputTokens)/1000*rate.OutputCreditsPer1K
	if rate.ComputeCreditsPerSecond > 0 {
		sec := 0.0
		if rate.ExpectedPromptTokensPerSecond > 0 {
			sec += float64(in.InputTokens) / rate.ExpectedPromptTokensPerSecond
		}
		if rate.ExpectedOutputTokensPerSecond > 0 {
			sec += float64(in.OutputTokens) / rate.ExpectedOutputTokensPerSecond
		}
		result.EstimatedCredits += sec * rate.ComputeCreditsPerSecond
	}
	result.TenantPolicy = s.policyFor(ctx, id.Tenant)
	className := strings.TrimSpace(in.ServiceClass)
	if className == "" {
		className = strings.TrimSpace(id.ServiceClass)
	}
	if className == "" {
		className = s.cfg.ServiceClasses.Default
	}
	if className == "" {
		className = "interactive"
	}
	result.ServiceClass = className
	if sc, ok := s.cfg.ServiceClasses.Classes[className]; ok {
		result.ServiceClassConfig = sc
	} else {
		result.Errors = append(result.Errors, "unknown service class: "+className)
	}

	if len(contextAllowed) == 0 {
		contextAllowed = nil
	}
	result.Workers = s.workers.ExplainRoutingAllowed(resolved, contextAllowed, 0)
	for _, w := range result.Workers {
		if w.Eligible {
			result.SelectedWorker = w.Worker
			break
		}
	}
	if !result.CapabilitiesOK {
		result.Decision = "denied_capability"
	} else if !result.ContextOK {
		result.Decision = "denied_context"
	} else if result.SelectedWorker == "" {
		result.Decision = "no_eligible_worker"
	} else {
		result.Decision = "would_route"
	}
	return result, nil
}
