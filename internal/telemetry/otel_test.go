package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func TestOTLPHTTPJSONExportIsMetadataOnly(t *testing.T) {
	bodyCh := make(chan []byte, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			t.Fatalf("path=%s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("content-type=%s", r.Header.Get("Content-Type"))
		}
		b, _ := io.ReadAll(r.Body)
		bodyCh <- b
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	e := New(config.OpenTelemetryConfig{Enabled: true, Endpoint: ts.URL, ServiceName: "gateway-test", SampleRatio: 1, BatchSize: 1, FlushInterval: config.Duration(10 * time.Millisecond)})
	ctx := e.NewTrace("00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	start := time.Now().Add(-time.Second)
	e.Record(Record{Trace: ctx, RequestID: "req-1", API: "openai", Path: "/v1/chat/completions", Tenant: "t", Actor: "u", ServiceClass: "interactive", Model: "qwen3:8b", Worker: "gpu", Started: start, Finished: time.Now(), FirstByte: 100 * time.Millisecond, Status: 200, PromptTokens: 12, OutputTokens: 7, Credits: 2.5, Intervals: []Interval{{Name: "gateway.queue", Start: start, End: start.Add(20 * time.Millisecond)}}})

	var b []byte
	select {
	case b = <-bodyCh:
	case <-time.After(2 * time.Second):
		t.Fatal("no OTLP export")
	}
	if strings.Contains(string(b), "secret prompt") {
		t.Fatal("content leaked")
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc["resourceSpans"].([]any)) != 1 {
		t.Fatalf("bad resourceSpans: %s", b)
	}
	if !strings.Contains(string(b), "gen_ai.usage.input_tokens") || !strings.Contains(string(b), "gen_ai.response.time_to_first_chunk") {
		t.Fatalf("missing GenAI attrs: %s", b)
	}
	if !strings.Contains(string(b), "0123456789abcdef0123456789abcdef") {
		t.Fatalf("incoming trace id not propagated: %s", b)
	}
	cctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Close(cctx); err != nil {
		t.Fatal(err)
	}
	if e.Exported() == 0 {
		t.Fatal("exported counter not incremented")
	}
}
