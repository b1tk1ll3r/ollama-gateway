package cost

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"

	"github.com/example/ollama-fair-gateway/internal/config"
)

type Estimate struct {
	Model        string
	InputTokens  int64
	OutputTokens int64
	Credits      float64
}

type Usage struct {
	PromptTokens       int64 `json:"prompt_tokens"`
	CompletionTokens   int64 `json:"completion_tokens"`
	CachedPromptTokens int64 `json:"cached_prompt_tokens,omitempty"`
	PromptEvalNS       int64 `json:"prompt_eval_ns,omitempty"`
	EvalNS             int64 `json:"eval_ns,omitempty"`
	LoadNS             int64 `json:"load_ns,omitempty"`
	TotalNS            int64 `json:"total_ns,omitempty"`
	Approximate        bool  `json:"approximate,omitempty"`
}

type Estimator struct{ cfg config.CostConfig }

func New(cfg config.CostConfig) *Estimator { return &Estimator{cfg: cfg} }

func (e *Estimator) Rate(model string) config.ModelRate {
	if r, ok := e.cfg.Models[model]; ok {
		return fillRate(r, e.cfg.Default)
	}
	// permit convenient prefix entries such as "qwen3:*"
	best := ""
	var out config.ModelRate
	for p, r := range e.cfg.Models {
		if strings.HasSuffix(p, "*") && strings.HasPrefix(model, strings.TrimSuffix(p, "*")) && len(p) > len(best) {
			best = p
			out = r
		}
	}
	if best != "" {
		return fillRate(out, e.cfg.Default)
	}
	return e.cfg.Default
}
func fillRate(r, d config.ModelRate) config.ModelRate {
	if r.InputCreditsPer1K <= 0 {
		r.InputCreditsPer1K = d.InputCreditsPer1K
	}
	if r.OutputCreditsPer1K <= 0 {
		r.OutputCreditsPer1K = d.OutputCreditsPer1K
	}
	if r.CachedInputFactor <= 0 {
		r.CachedInputFactor = d.CachedInputFactor
		if r.CachedInputFactor <= 0 {
			r.CachedInputFactor = 1
		}
	}
	if r.ComputeCreditsPerSecond < 0 {
		r.ComputeCreditsPerSecond = 0
	}
	return r
}

type wireReq struct {
	Model        string          `json:"model"`
	Prompt       string          `json:"prompt"`
	Suffix       string          `json:"suffix"`
	System       json.RawMessage `json:"system"`
	Instructions json.RawMessage `json:"instructions"`
	Input        json.RawMessage `json:"input"`
	Tools        json.RawMessage `json:"tools"`
	Messages     []struct {
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	MaxTokens           int `json:"max_tokens"`
	MaxCompletionTokens int `json:"max_completion_tokens"`
	MaxOutputTokens     int `json:"max_output_tokens"`
	Options             struct {
		NumPredict int `json:"num_predict"`
		NumCtx     int `json:"num_ctx"`
	} `json:"options"`
}

func (e *Estimator) Estimate(path string, body []byte) Estimate {
	var r wireReq
	_ = json.Unmarshal(body, &r)
	textBytes := len(r.Prompt) + len(r.Suffix) + textualBytes(r.System)
	textBytes += textualBytes(r.Instructions)
	textBytes += textualBytes(r.Input)
	// Tool schemas are injected into the model context. Counting their raw JSON
	// bytes gives a conservative pre-run estimate without attempting to tokenize
	// provider-specific JSON schema dialects.
	if raw := bytes.TrimSpace(r.Tools); len(raw) > 2 && !bytes.Equal(raw, []byte("[]")) && !bytes.Equal(raw, []byte("null")) {
		textBytes += len(raw)
	}
	for _, m := range r.Messages {
		textBytes += textualBytes(m.Content)
	}
	in := roughTokens(textBytes)
	if in < 1 { // bounded fallback for unusual request shapes
		n := len(body)
		if n > 64<<10 {
			n = 64 << 10
		}
		in = roughTokens(n)
	}
	// Use the largest explicitly requested output budget. Different compatible
	// APIs use different field names, and choosing the largest is conservative
	// when a client sends more than one of them.
	out := maxInt(r.MaxCompletionTokens, r.MaxOutputTokens, r.MaxTokens, r.Options.NumPredict)
	if out <= 0 {
		out = e.cfg.DefaultMaxOutputTokens
	}
	if strings.Contains(path, "embeddings") || strings.Contains(path, "/embed") {
		out = 0
	}
	rate := e.Rate(r.Model)
	credits := creditsFor(rate, in, 0, int64(out), 0)
	if rate.ComputeCreditsPerSecond > 0 {
		var sec float64
		if rate.ExpectedPromptTokensPerSecond > 0 {
			sec += float64(in) / rate.ExpectedPromptTokensPerSecond
		}
		if out > 0 && rate.ExpectedOutputTokensPerSecond > 0 {
			sec += float64(out) / rate.ExpectedOutputTokensPerSecond
		}
		credits += sec * rate.ComputeCreditsPerSecond
	}
	if credits <= 0 {
		credits = .001
	}
	return Estimate{Model: r.Model, InputTokens: in, OutputTokens: int64(out), Credits: credits}
}

func (e *Estimator) Actual(model string, u Usage) float64 {
	r := e.Rate(model)
	sec := float64(u.PromptEvalNS+u.EvalNS) / 1e9
	return creditsFor(r, u.PromptTokens, u.CachedPromptTokens, u.CompletionTokens, sec)
}
func creditsFor(r config.ModelRate, in, cached, out int64, sec float64) float64 {
	if cached < 0 {
		cached = 0
	}
	if cached > in {
		cached = in
	}
	uncached := in - cached
	input := (float64(uncached) + float64(cached)*r.CachedInputFactor) / 1000 * r.InputCreditsPer1K
	return input + float64(out)/1000*r.OutputCreditsPer1K + sec*r.ComputeCreditsPerSecond
}
func maxInt(values ...int) int {
	m := 0
	for _, v := range values {
		if v > m {
			m = v
		}
	}
	return m
}

func roughTokens(n int) int64 {
	if n <= 0 {
		return 0
	}
	return int64(math.Ceil(float64(n) / 4))
}

func textualBytes(raw json.RawMessage) int {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0
	}
	var s string
	if raw[0] == '"' && json.Unmarshal(raw, &s) == nil {
		return len(s)
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	if dec.Decode(&v) != nil {
		return 0
	}
	return walkText(v)
}
func walkText(v any) int {
	switch x := v.(type) {
	case string:
		return len(x)
	case []any:
		n := 0
		for _, z := range x {
			n += walkText(z)
		}
		return n
	case map[string]any:
		// Follow only known textual fields. URLs/base64 image payloads are
		// deliberately excluded from the pre-run token estimate.
		n := 0
		if v, ok := x["text"]; ok {
			n += walkText(v)
		}
		if v, ok := x["content"]; ok {
			n += walkText(v)
		}
		return n
	}
	return 0
}
