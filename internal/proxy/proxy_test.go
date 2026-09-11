package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/example/ollama-fair-gateway/internal/cost"
)

func TestNativeMeter(t *testing.T) {
	m := newMeter("ollama", 0)
	m.Feed([]byte("{\"response\":\"x\",\"done\":false}\n{\"done\":true,\"prompt_eval_count\":12,\"eval_count\":4,\"eval_duration\":100}\n"))
	u := m.Finish(100)
	if u.PromptTokens != 12 || u.CompletionTokens != 4 || u.EvalNS != 100 || u.Approximate {
		t.Fatalf("bad usage %#v", u)
	}
}
func TestOpenAIMeter(t *testing.T) {
	m := newMeter("openai", 9)
	m.Feed([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n"))
	u := m.Finish(100)
	if u.PromptTokens != 10 || u.CompletionTokens != 2 || u.Approximate {
		t.Fatalf("bad usage %#v", u)
	}
}

func TestAnthropicMeter(t *testing.T) {
	m := newMeter("anthropic", 0)
	m.Feed([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":11,\"output_tokens\":0}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n"))
	u := m.Finish(100)
	if u.PromptTokens != 11 || u.CompletionTokens != 5 || u.Approximate {
		t.Fatalf("bad anthropic usage %#v", u)
	}
}

func TestForwardProgressObserver(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"message\":{\"content\":\"hi\"},\"done\":false}\n")
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = io.WriteString(w, "{\"done\":true,\"prompt_eval_count\":10,\"eval_count\":2}\n")
	}))
	defer backend.Close()
	target, _ := url.Parse(backend.URL)
	in := httptest.NewRequest(http.MethodPost, "http://gateway/api/chat", strings.NewReader(`{"model":"x"}`))
	out := httptest.NewRecorder()
	var calls int
	var lastBytes int64
	var last cost.Usage
	res := New().Forward(context.Background(), out, in, target, strings.NewReader(`{"model":"x"}`), "ollama", 0, func(n int64, u cost.Usage) {
		calls++
		lastBytes = n
		last = u
	})
	if res.Status != http.StatusOK || calls == 0 || lastBytes == 0 || last.PromptTokens != 10 || last.CompletionTokens != 2 {
		t.Fatalf("status=%d calls=%d bytes=%d usage=%#v", res.Status, calls, lastBytes, last)
	}
}
