package cost

import (
	"github.com/example/ollama-fair-gateway/internal/config"
	"strings"
	"testing"
)

func TestEstimateIgnoresImagePayload(t *testing.T) {
	e := New(config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 100})
	b := []byte(`{"model":"m","messages":[{"content":[{"type":"text","text":"hello world"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AAAAAAAAAAAAAAAAAAAAAAAA"}}]}],"max_tokens":10}`)
	x := e.Estimate("/v1/chat/completions", b)
	if x.InputTokens > 20 {
		t.Fatalf("image data inflated estimate: %d", x.InputTokens)
	}
	if x.OutputTokens != 10 {
		t.Fatalf("output=%d", x.OutputTokens)
	}
}

func TestActualDiscountsCachedPromptTokens(t *testing.T) {
	e := New(config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 2, OutputCreditsPer1K: 4, CachedInputFactor: 0.25}})
	got := e.Actual("m", Usage{PromptTokens: 1000, CachedPromptTokens: 800, CompletionTokens: 100})
	// input: (200 + 800*0.25)/1000*2 = 0.8; output: 0.1*4 = 0.4
	if got < 1.199999 || got > 1.200001 {
		t.Fatalf("credits=%f, want 1.2", got)
	}
}

func TestEstimateCountsResponsesInstructionsAndMaxOutputTokens(t *testing.T) {
	e := New(config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 100})
	b := []byte(`{"model":"m","instructions":"` + strings.Repeat("i", 400) + `","input":"hello","max_output_tokens":8192}`)
	x := e.Estimate("/v1/responses", b)
	if x.InputTokens < 100 {
		t.Fatalf("instructions not counted, input=%d", x.InputTokens)
	}
	if x.OutputTokens != 8192 {
		t.Fatalf("output=%d, want 8192", x.OutputTokens)
	}
}

func TestEstimateCountsGenerateSuffixAndLargestOutputBudget(t *testing.T) {
	e := New(config.CostConfig{Default: config.ModelRate{InputCreditsPer1K: 1, OutputCreditsPer1K: 3}, DefaultMaxOutputTokens: 100})
	b := []byte(`{"model":"m","prompt":"abc","suffix":"` + strings.Repeat("s", 400) + `","max_tokens":12,"max_completion_tokens":24,"max_output_tokens":48,"options":{"num_predict":36}}`)
	x := e.Estimate("/api/generate", b)
	if x.InputTokens < 100 {
		t.Fatalf("suffix not counted, input=%d", x.InputTokens)
	}
	if x.OutputTokens != 48 {
		t.Fatalf("output=%d, want 48", x.OutputTokens)
	}
}
