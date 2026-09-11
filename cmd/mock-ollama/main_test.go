package main

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestOpenAIChatNonStreaming(t *testing.T) {
	s := &server{model: "mock:latest", responseBytes: 12, promptTokens: 7, outputTokens: 3}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":false}`))
	rr := httptest.NewRecorder()
	s.openAIChat(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	var doc struct {
		Model   string `json:"model"`
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
		Usage struct {
			Prompt int64 `json:"prompt_tokens"`
			Output int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Model != "mock:latest" || len(doc.Choices) != 1 || len(doc.Choices[0].Message.Content) != 12 {
		t.Fatalf("unexpected response: %+v", doc)
	}
	if doc.Usage.Prompt != 7 || doc.Usage.Output != 3 {
		t.Fatalf("unexpected usage: %+v", doc.Usage)
	}
}

func TestOpenAIChatStreaming(t *testing.T) {
	s := &server{model: "mock:latest", responseBytes: 16}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	rr := httptest.NewRecorder()
	s.openAIChat(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type=%q", ct)
	}
	if !strings.Contains(rr.Body.String(), "data: [DONE]") {
		t.Fatalf("missing done marker: %s", rr.Body.String())
	}
}

func TestNativeStreamingEndsWithUsage(t *testing.T) {
	s := &server{model: "mock:latest", responseBytes: 8, promptTokens: 11, outputTokens: 5}
	req := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(`{"stream":true}`))
	rr := httptest.NewRecorder()
	s.nativeChat(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	scanner := bufio.NewScanner(strings.NewReader(rr.Body.String()))
	var last map[string]any
	for scanner.Scan() {
		if err := json.Unmarshal(scanner.Bytes(), &last); err != nil {
			t.Fatal(err)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if done, _ := last["done"].(bool); !done {
		t.Fatalf("last chunk not done: %#v", last)
	}
	if last["prompt_eval_count"] != float64(11) || last["eval_count"] != float64(5) {
		t.Fatalf("unexpected usage: %#v", last)
	}
}

func TestShouldFailConcurrent(t *testing.T) {
	const total = 1000
	s := &server{failEvery: 5}
	var wg sync.WaitGroup
	failures := make(chan struct{}, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.shouldFail() {
				failures <- struct{}{}
			}
		}()
	}
	wg.Wait()
	close(failures)
	if got, want := len(failures), total/5; got != want {
		t.Fatalf("failures=%d want=%d", got, want)
	}
	if got := s.requests.Load(); got != total {
		t.Fatalf("requests=%d want=%d", got, total)
	}
}
