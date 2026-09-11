package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type server struct {
	model         string
	delay         time.Duration
	streamDelay   time.Duration
	responseBytes int
	promptTokens  int64
	outputTokens  int64
	failEvery     int64
	requests      atomic.Int64
}

func main() {
	listen := flag.String("listen", "127.0.0.1:11435", "listen address")
	model := flag.String("model", "qwen3:8b", "mock model name")
	delay := flag.Duration("delay", 0, "delay before response headers")
	streamDelay := flag.Duration("stream-delay", 0, "delay between streaming chunks")
	responseBytes := flag.Int("response-bytes", 128, "approximate generated content bytes")
	promptTokens := flag.Int64("prompt-tokens", 16, "reported prompt token count")
	outputTokens := flag.Int64("output-tokens", 8, "reported completion token count")
	failEvery := flag.Int64("fail-every", 0, "return HTTP 503 for every Nth inference request; 0 disables")
	flag.Parse()
	if *responseBytes < 0 || *promptTokens < 0 || *outputTokens < 0 || *failEvery < 0 {
		log.Fatal("numeric flags must be non-negative")
	}
	s := &server{model: *model, delay: *delay, streamDelay: *streamDelay, responseBytes: *responseBytes, promptTokens: *promptTokens, outputTokens: *outputTokens, failEvery: *failEvery}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/ps", s.ps)
	mux.HandleFunc("/api/tags", s.tags)
	mux.HandleFunc("/api/show", s.show)
	mux.HandleFunc("/api/chat", s.nativeChat)
	mux.HandleFunc("/api/generate", s.nativeGenerate)
	mux.HandleFunc("/v1/chat/completions", s.openAIChat)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 2 * time.Minute}
	log.Printf("mock Ollama listening on http://%s model=%s delay=%s response_bytes=%d", *listen, *model, *delay, *responseBytes)
	log.Fatal(h.ListenAndServe())
}

func (s *server) ps(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"models": []map[string]any{{"name": s.model, "model": s.model, "size": 1 << 30, "size_vram": 1 << 30, "context_length": 32768}}})
}

func (s *server) tags(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"models": []map[string]any{{"name": s.model, "model": s.model, "size": 1 << 30, "details": map[string]any{"parameter_size": "8B", "quantization_level": "Q4_K_M"}}}})
}

func (s *server) show(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]any{"capabilities": []string{"completion", "tools", "thinking", "vision"}, "model_info": map[string]any{"mock.context_length": 32768}})
}

func (s *server) shouldFail() bool {
	n := s.requests.Add(1)
	return s.failEvery > 0 && n%s.failEvery == 0
}

func (s *server) openAIChat(w http.ResponseWriter, r *http.Request) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.shouldFail() {
		writeJSONStatus(w, http.StatusServiceUnavailable, map[string]any{"error": map[string]any{"message": "mock failure", "type": "server_error"}})
		return
	}
	var req struct {
		Stream bool `json:"stream"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req)
	content := strings.Repeat("x", s.responseBytes)
	if req.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		f, _ := w.(http.Flusher)
		parts := split(content, 4)
		for i, p := range parts {
			chunk := map[string]any{"id": "chatcmpl-mock", "object": "chat.completion.chunk", "choices": []map[string]any{{"index": 0, "delta": map[string]any{"content": p}, "finish_reason": nil}}}
			if i == len(parts)-1 {
				chunk["choices"] = []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}
			}
			b, _ := json.Marshal(chunk)
			fmt.Fprintf(w, "data: %s\n\n", b)
			if f != nil {
				f.Flush()
			}
			if s.streamDelay > 0 {
				time.Sleep(s.streamDelay)
			}
		}
		io.WriteString(w, "data: [DONE]\n\n")
		if f != nil {
			f.Flush()
		}
		return
	}
	writeJSON(w, map[string]any{
		"id": "chatcmpl-mock", "object": "chat.completion", "created": time.Now().Unix(), "model": s.model,
		"choices": []map[string]any{{"index": 0, "message": map[string]any{"role": "assistant", "content": content}, "finish_reason": "stop"}},
		"usage":   map[string]any{"prompt_tokens": s.promptTokens, "completion_tokens": s.outputTokens, "total_tokens": s.promptTokens + s.outputTokens},
	})
}

func (s *server) nativeChat(w http.ResponseWriter, r *http.Request) {
	s.native(w, r, "message")
}

func (s *server) nativeGenerate(w http.ResponseWriter, r *http.Request) {
	s.native(w, r, "response")
}

func (s *server) native(w http.ResponseWriter, r *http.Request, field string) {
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.shouldFail() {
		writeJSONStatus(w, http.StatusServiceUnavailable, map[string]any{"error": "mock failure"})
		return
	}
	var req map[string]any
	_ = json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req)
	stream, _ := req["stream"].(bool)
	content := strings.Repeat("x", s.responseBytes)
	w.Header().Set("Content-Type", "application/x-ndjson")
	bw := bufio.NewWriter(w)
	if !stream {
		doc := map[string]any{"model": s.model, "done": true, "prompt_eval_count": s.promptTokens, "eval_count": s.outputTokens}
		if field == "message" {
			doc[field] = map[string]any{"role": "assistant", "content": content}
		} else {
			doc[field] = content
		}
		_ = json.NewEncoder(bw).Encode(doc)
		_ = bw.Flush()
		return
	}
	f, _ := w.(http.Flusher)
	for _, p := range split(content, 4) {
		doc := map[string]any{"model": s.model, "done": false}
		if field == "message" {
			doc[field] = map[string]any{"role": "assistant", "content": p}
		} else {
			doc[field] = p
		}
		_ = json.NewEncoder(bw).Encode(doc)
		_ = bw.Flush()
		if f != nil {
			f.Flush()
		}
		if s.streamDelay > 0 {
			time.Sleep(s.streamDelay)
		}
	}
	_ = json.NewEncoder(bw).Encode(map[string]any{"model": s.model, "done": true, "prompt_eval_count": s.promptTokens, "eval_count": s.outputTokens})
	_ = bw.Flush()
	if f != nil {
		f.Flush()
	}
}

func split(s string, n int) []string {
	if n <= 1 || len(s) == 0 {
		return []string{s}
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		start := len(s) * i / n
		end := len(s) * (i + 1) / n
		out = append(out, s[start:end])
	}
	return out
}

func writeJSON(w http.ResponseWriter, v any) { writeJSONStatus(w, http.StatusOK, v) }
func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
