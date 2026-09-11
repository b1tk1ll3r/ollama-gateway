package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	d := []time.Duration{10 * time.Millisecond, 50 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 30 * time.Millisecond}
	s := stats(d)
	if s.P50 != 30 || s.P95 != 40 || s.P99 != 40 || s.Max != 50 {
		t.Fatalf("unexpected stats: %+v", s)
	}
}

func TestOneCapturesStatusUsageBytesAndHeaders(t *testing.T) {
	var gotAuth, gotClass string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotClass = r.Header.Get("X-Gateway-Service-Class")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":9,"completion_tokens":4}}`)
	}))
	defer ts.Close()

	cfg := benchConfig{Base: ts.URL + "/", Key: "secret", Model: "m", ServiceClass: "batch", Timeout: time.Second}
	body, err := requestBody(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := one(context.Background(), newClient(cfg), cfg, body)
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if r.Status != http.StatusOK || r.Prompt != 9 || r.Completion != 4 || r.Bytes == 0 || r.Latency <= 0 || r.TTFB <= 0 {
		t.Fatalf("unexpected result: %+v", r)
	}
	if gotAuth != "Bearer secret" || gotClass != "batch" {
		t.Fatalf("headers auth=%q class=%q", gotAuth, gotClass)
	}
}

func TestSummarizeSeparatesErrors(t *testing.T) {
	cfg := benchConfig{Base: "http://gateway", Model: "m", Concurrency: 2, Requests: 3, Warmup: 1}
	start := time.Unix(1, 0).UTC()
	finish := start.Add(time.Second)
	s := summarize(cfg, start, finish, []result{
		{Latency: 10 * time.Millisecond, TTFB: 5 * time.Millisecond, Prompt: 8, Completion: 2, Bytes: 100, Status: 200},
		{Latency: 20 * time.Millisecond, TTFB: 7 * time.Millisecond, Prompt: 8, Completion: 3, Bytes: 110, Status: 200},
		{Latency: 2 * time.Millisecond, Status: 503, Err: fmt.Errorf("HTTP 503: unavailable")},
	})
	if s.Successful != 2 || s.Errors != 1 || s.StatusCounts["200"] != 2 || s.StatusCounts["503"] != 1 {
		t.Fatalf("unexpected counts: %+v", s)
	}
	if s.ThroughputRPS != 2 || s.PromptTokens != 16 || s.CompletionTokens != 5 || s.BytesReceived != 210 {
		t.Fatalf("unexpected throughput/usage: %+v", s)
	}
}

func TestStringsTrimRightSlash(t *testing.T) {
	if got := stringsTrimRightSlash("http://x///"); got != "http://x" {
		t.Fatalf("got %q", got)
	}
}
