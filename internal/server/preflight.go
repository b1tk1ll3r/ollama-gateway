package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/example/ollama-fair-gateway/internal/cost"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type requestRequirements struct {
	Model            string
	Capabilities     []string
	RequestedContext int64
}

type modelPreflight struct {
	AllowedWorkers   map[string]bool
	RequestedContext int64
	RequiredContext  int64
}

func requirementsFor(path string, body []byte) (requestRequirements, error) {
	var doc map[string]json.RawMessage
	_ = json.Unmarshal(body, &doc)
	var model string
	_ = json.Unmarshal(doc["model"], &model)
	req := requestRequirements{Model: model}
	caps := map[string]bool{}
	if strings.Contains(path, "embed") {
		caps["embedding"] = true
	}
	if strings.Contains(path, "chat") || strings.Contains(path, "completion") || strings.Contains(path, "responses") || strings.Contains(path, "messages") || strings.Contains(path, "generate") {
		caps["completion"] = true
	}
	if nonEmptyJSONList(doc["tools"]) {
		caps["tools"] = true
	}
	if thinkingRequested(doc) {
		caps["thinking"] = true
	}
	if containsVision(body) {
		caps["vision"] = true
	}
	if raw := bytes.TrimSpace(doc["options"]); len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
		var opts map[string]json.RawMessage
		if err := json.Unmarshal(raw, &opts); err != nil {
			return req, fmt.Errorf("options must be a JSON object: %w", err)
		}
		if rawNum, ok := opts["num_ctx"]; ok {
			if !strings.HasPrefix(path, "/api/") {
				return req, fmt.Errorf("options.num_ctx is only supported on native Ollama /api endpoints")
			}
			n, err := strictPositiveInt64(rawNum)
			if err != nil {
				return req, fmt.Errorf("options.num_ctx must be a positive integer: %w", err)
			}
			req.RequestedContext = n
		}
	}
	for c := range caps {
		req.Capabilities = append(req.Capabilities, c)
	}
	sort.Strings(req.Capabilities)
	return req, nil
}

func strictPositiveInt64(raw json.RawMessage) (int64, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) || raw[0] == '"' {
		return 0, fmt.Errorf("not an integer")
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || n <= 0 {
		if err == nil {
			err = fmt.Errorf("must be greater than zero")
		}
		return 0, err
	}
	return n, nil
}

func nonEmptyJSONList(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 2 && !bytes.Equal(raw, []byte("null")) && !bytes.Equal(raw, []byte("[]"))
}

func thinkingRequested(doc map[string]json.RawMessage) bool {
	if raw := bytes.TrimSpace(doc["think"]); len(raw) > 0 && !bytes.Equal(raw, []byte("false")) && !bytes.Equal(raw, []byte("null")) && !bytes.Equal(raw, []byte(`"none"`)) {
		return true
	}
	var effort string
	_ = json.Unmarshal(doc["reasoning_effort"], &effort)
	if effort != "" && !strings.EqualFold(effort, "none") {
		return true
	}
	if raw := doc["thinking"]; len(raw) > 0 {
		var x struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(raw, &x) == nil && strings.EqualFold(x.Type, "enabled") {
			return true
		}
	}
	if raw := doc["reasoning"]; len(raw) > 0 {
		var x struct {
			Effort string `json:"effort"`
		}
		if json.Unmarshal(raw, &x) == nil && x.Effort != "" && !strings.EqualFold(x.Effort, "none") {
			return true
		}
	}
	return false
}

func countVisionInputs(body []byte) int64 {
	var v any
	if json.Unmarshal(body, &v) != nil {
		return 0
	}
	var walk func(any) int64
	walk = func(x any) int64 {
		switch z := x.(type) {
		case []any:
			var n int64
			for _, item := range z {
				n += walk(item)
			}
			return n
		case map[string]any:
			var n int64
			if imgs, ok := z["images"].([]any); ok {
				n += int64(len(imgs))
			}
			typ, _ := z["type"].(string)
			isImageObject := typ == "image_url" || typ == "input_image" || typ == "image"
			if isImageObject {
				n++
			}
			for k, item := range z {
				// Payloads can be very large. The image object/array itself already
				// reserves context tokens, so never recurse into base64/URLs.
				if k == "image_url" || k == "images" || (isImageObject && (k == "url" || k == "data" || k == "image")) {
					continue
				}
				n += walk(item)
			}
			return n
		}
		return 0
	}
	return walk(v)
}

func containsVision(body []byte) bool { return countVisionInputs(body) > 0 }

func requiredContextTokens(est cost.Estimate, body []byte, visionReserve int64, marginPercent float64) (required, vision, margin int64) {
	images := countVisionInputs(body)
	if images > 0 && visionReserve > 0 {
		vision = images * visionReserve
	}
	base := est.InputTokens + vision
	if marginPercent > 0 && base > 0 {
		margin = int64(math.Ceil(float64(base) * marginPercent / 100))
	}
	return base + margin + est.OutputTokens, vision, margin
}

func minPositive64(values ...int64) int64 {
	var out int64
	for _, v := range values {
		if v <= 0 {
			continue
		}
		if out == 0 || v < out {
			out = v
		}
	}
	return out
}

func contextWindowsSummary(windows []worker.ContextWindow) string {
	parts := make([]string, 0, len(windows))
	for _, x := range windows {
		if x.EffectiveTokens > 0 {
			parts = append(parts, fmt.Sprintf("%s=%d(%s)", x.Worker, x.EffectiveTokens, x.EffectiveSource))
		} else {
			parts = append(parts, x.Worker+"=unknown")
		}
	}
	return strings.Join(parts, ", ")
}

// preflightModel validates capability-sensitive requests and derives the set of
// workers that can actually serve the request's context budget. The model
// maximum from /api/show is only one input: loaded /api/ps context_length,
// Modelfile num_ctx, worker defaults and per-worker caps are stronger runtime
// evidence for OpenAI-compatible requests that cannot set num_ctx themselves.
func (s *Server) preflightModel(w http.ResponseWriter, r *http.Request, body []byte, est cost.Estimate) (modelPreflight, bool) {
	out := modelPreflight{}
	cfg := s.cfg.ModelCapabilities
	if cfg.Mode == "off" || strings.TrimSpace(est.Model) == "" {
		return out, true
	}
	req, reqErr := requirementsFor(r.URL.Path, body)
	if reqErr != nil {
		writeProtocolError(w, r, http.StatusBadRequest, "invalid_num_ctx", reqErr.Error())
		return out, false
	}
	out.RequestedContext = req.RequestedContext
	meta, metaWorker, err := s.workers.Metadata(r.Context(), est.Model)
	if err != nil {
		w.Header().Set("X-Gateway-Model-Metadata", "unavailable")
		s.log.Warn("model metadata unavailable; capability checks are best-effort", "model", est.Model, "error", err)
	} else {
		if metaWorker != "" {
			w.Header().Set("X-Gateway-Model-Metadata-Worker", metaWorker)
		}
		if len(meta.Capabilities) > 0 {
			w.Header().Set("X-Gateway-Model-Capabilities", strings.Join(meta.Capabilities, ","))
		}
		if meta.ContextLength > 0 {
			w.Header().Set("X-Gateway-Model-Context", strconv.FormatInt(meta.ContextLength, 10))
		}
		if meta.ConfiguredContextLength > 0 {
			w.Header().Set("X-Gateway-Model-Configured-Context", strconv.FormatInt(meta.ConfiguredContextLength, 10))
		}
	}

	unsupported := make([]string, 0)
	if err == nil && len(meta.Capabilities) > 0 {
		for _, capability := range req.Capabilities {
			if !worker.HasCapability(meta, capability) {
				unsupported = append(unsupported, capability)
			}
		}
	}
	if len(unsupported) > 0 {
		msg := fmt.Sprintf("model %s does not support required capability: %s", est.Model, strings.Join(unsupported, ", "))
		if cfg.Mode == "enforce" {
			writeProtocolError(w, r, http.StatusBadRequest, "unsupported_capability", msg)
			return out, false
		}
		w.Header().Set("X-Gateway-Capability-Warning", strings.Join(unsupported, ","))
		s.log.Warn("unsupported model capability observed", "model", est.Model, "required", unsupported)
	}

	if cfg.ContextGuard == "off" {
		return out, true
	}

	required, visionReserve, margin := requiredContextTokens(est, body, cfg.Context.VisionReserveTokensPerImage, cfg.Context.EstimationMarginPercent)
	out.RequiredContext = required
	w.Header().Set("X-Gateway-Context-Required", strconv.FormatInt(required, 10))
	if req.RequestedContext > 0 {
		w.Header().Set("X-Gateway-Requested-Context", strconv.FormatInt(req.RequestedContext, 10))
	}
	if visionReserve > 0 {
		w.Header().Set("X-Gateway-Context-Vision-Reserve", strconv.FormatInt(visionReserve, 10))
	}
	if margin > 0 {
		w.Header().Set("X-Gateway-Context-Margin", strconv.FormatInt(margin, 10))
	}

	var warning string
	gatewayCap := cfg.Context.MaxRequestedTokens
	if gatewayCap > 0 && req.RequestedContext > gatewayCap {
		warning = fmt.Sprintf("requested num_ctx %d exceeds gateway context cap %d", req.RequestedContext, gatewayCap)
	} else if gatewayCap > 0 && required > gatewayCap {
		warning = fmt.Sprintf("estimated request context %d tokens exceeds gateway context cap %d", required, gatewayCap)
	} else if req.RequestedContext > 0 && meta.ContextLength > 0 && req.RequestedContext > meta.ContextLength {
		warning = fmt.Sprintf("requested num_ctx %d exceeds model context length %d", req.RequestedContext, meta.ContextLength)
	} else if req.RequestedContext > 0 && required > req.RequestedContext {
		warning = fmt.Sprintf("estimated request context %d tokens exceeds explicit num_ctx %d", required, req.RequestedContext)
	}

	windows := s.workers.ContextWindows(r.Context(), est.Model)
	allowed := make(map[string]bool, len(windows))
	var maxEffective int64
	for _, x := range windows {
		eligible := false
		if req.RequestedContext > 0 {
			// Explicit native num_ctx can resize/reload the model, so current loaded
			// context does not hard-block the worker. The theoretical model maximum
			// and the operator's per-worker context cap still do.
			capacity := minPositive64(x.ModelMaxTokens, x.WorkerLimitTokens)
			if gatewayCap > 0 {
				capacity = minPositive64(capacity, gatewayCap)
			}
			if capacity > maxEffective {
				maxEffective = capacity
			}
			eligible = (x.ModelMaxTokens <= 0 || req.RequestedContext <= x.ModelMaxTokens) &&
				(x.WorkerLimitTokens <= 0 || req.RequestedContext <= x.WorkerLimitTokens)
		} else {
			effective := x.EffectiveTokens
			if gatewayCap > 0 {
				effective = minPositive64(effective, gatewayCap)
			}
			if effective > maxEffective {
				maxEffective = effective
			}
			eligible = effective > 0 && required <= effective
		}
		if eligible {
			allowed[x.Worker] = true
		}
	}
	if maxEffective > 0 {
		w.Header().Set("X-Gateway-Effective-Context-Max", strconv.FormatInt(maxEffective, 10))
	}
	if len(windows) > 0 {
		w.Header().Set("X-Gateway-Context-Eligible-Workers", fmt.Sprintf("%d/%d", len(allowed), len(windows)))
	}
	if warning == "" && len(windows) > 0 && len(allowed) == 0 {
		if req.RequestedContext > 0 {
			warning = fmt.Sprintf("requested num_ctx %d cannot be served by any eligible worker; contexts: %s", req.RequestedContext, contextWindowsSummary(windows))
		} else {
			warning = fmt.Sprintf("estimated request context %d tokens exceeds every effective worker context; contexts: %s", required, contextWindowsSummary(windows))
		}
	}

	if warning != "" {
		if cfg.ContextGuard == "reject" {
			writeProtocolError(w, r, http.StatusBadRequest, "context_window_exceeded", warning)
			return out, false
		}
		w.Header().Set("X-Gateway-Context-Warning", warning)
		s.log.Warn("context guard warning", "model", est.Model, "warning", warning)
	}
	// Even in warn mode, prefer context-suitable workers when at least one exists.
	// If none exists, warn mode intentionally preserves legacy routing behavior.
	if len(allowed) > 0 {
		out.AllowedWorkers = allowed
	}
	return out, true
}
