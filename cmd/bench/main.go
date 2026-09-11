package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"sort"
	"sync"
	"time"
)

type result struct {
	Latency    time.Duration
	TTFB       time.Duration
	Prompt     int64
	Completion int64
	Bytes      int64
	Status     int
	Err        error
}

type durationStats struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

type summary struct {
	StartedAt        time.Time      `json:"started_at"`
	FinishedAt       time.Time      `json:"finished_at"`
	BaseURL          string         `json:"base_url"`
	Model            string         `json:"model"`
	Concurrency      int            `json:"concurrency"`
	Requests         int            `json:"requests"`
	WarmupRequests   int            `json:"warmup_requests"`
	Successful       int            `json:"successful"`
	Errors           int            `json:"errors"`
	StatusCounts     map[string]int `json:"status_counts"`
	WallMS           float64        `json:"wall_ms"`
	ThroughputRPS    float64        `json:"throughput_rps"`
	Latency          durationStats  `json:"latency"`
	TTFB             durationStats  `json:"ttfb"`
	PromptTokens     int64          `json:"prompt_tokens"`
	CompletionTokens int64          `json:"completion_tokens"`
	CompletionTokPS  float64        `json:"completion_tokens_per_second"`
	BytesReceived    int64          `json:"bytes_received"`
	BytesPerSecond   float64        `json:"bytes_per_second"`
	Stream           bool           `json:"stream"`
	KeepAlive        bool           `json:"keep_alive"`
	ServiceClass     string         `json:"service_class,omitempty"`
	ErrorsByMessage  map[string]int `json:"errors_by_message,omitempty"`
}

type benchConfig struct {
	Base             string
	Key              string
	Model            string
	Concurrency      int
	Requests         int
	Warmup           int
	MaxTokens        int
	Prompt           string
	Timeout          time.Duration
	Stream           bool
	DisableKeepAlive bool
	ServiceClass     string
	JSONOut          string
}

func main() {
	base := flag.String("base-url", "http://127.0.0.1:8080", "gateway base URL")
	key := flag.String("api-key", "", "API key; can also use GATEWAY_BENCH_API_KEY")
	model := flag.String("model", "qwen3:8b", "model name")
	conc := flag.Int("concurrency", 4, "parallel clients")
	n := flag.Int("requests", 20, "total measured requests")
	warmup := flag.Int("warmup", 0, "warmup requests before measurement")
	maxTokens := flag.Int("max-tokens", 128, "max completion tokens")
	prompt := flag.String("prompt", "Explain in three concise paragraphs why fair scheduling matters for shared LLM inference.", "prompt")
	timeout := flag.Duration("timeout", 2*time.Minute, "per-request HTTP timeout")
	stream := flag.Bool("stream", false, "request OpenAI streaming responses")
	disableKeepAlive := flag.Bool("disable-keepalive", false, "disable HTTP connection reuse")
	serviceClass := flag.String("service-class", "", "optional X-Gateway-Service-Class override")
	jsonOut := flag.String("json-out", "", "optional path for machine-readable JSON summary; '-' writes JSON to stdout")
	flag.Parse()
	if *key == "" {
		*key = os.Getenv("GATEWAY_BENCH_API_KEY")
	}
	cfg := benchConfig{Base: *base, Key: *key, Model: *model, Concurrency: *conc, Requests: *n, Warmup: *warmup, MaxTokens: *maxTokens, Prompt: *prompt, Timeout: *timeout, Stream: *stream, DisableKeepAlive: *disableKeepAlive, ServiceClass: *serviceClass, JSONOut: *jsonOut}
	if cfg.Concurrency < 1 || cfg.Requests < 1 || cfg.Warmup < 0 {
		fmt.Fprintln(os.Stderr, "concurrency and requests must be positive; warmup must be non-negative")
		os.Exit(2)
	}
	if cfg.Timeout <= 0 {
		fmt.Fprintln(os.Stderr, "timeout must be positive")
		os.Exit(2)
	}

	client := newClient(cfg)
	body, err := requestBody(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "request body:", err)
		os.Exit(2)
	}
	ctx := context.Background()
	if cfg.Warmup > 0 {
		warm := run(ctx, client, cfg, body, cfg.Warmup)
		for _, r := range warm {
			if r.Err != nil {
				fmt.Fprintln(os.Stderr, "warmup error:", r.Err)
			}
		}
	}

	started := time.Now().UTC()
	results := run(ctx, client, cfg, body, cfg.Requests)
	finished := time.Now().UTC()
	s := summarize(cfg, started, finished, results)
	printHuman(s)
	if cfg.JSONOut != "" {
		if err := writeJSONSummary(cfg.JSONOut, s); err != nil {
			fmt.Fprintln(os.Stderr, "write JSON summary:", err)
			os.Exit(1)
		}
	}
	if s.Successful == 0 {
		os.Exit(1)
	}
}

func newClient(cfg benchConfig) *http.Client {
	idle := max(256, cfg.Concurrency*2)
	return &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        idle,
			MaxIdleConnsPerHost: idle,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   cfg.DisableKeepAlive,
		},
	}
}

func requestBody(cfg benchConfig) ([]byte, error) {
	return json.Marshal(map[string]any{
		"model":      cfg.Model,
		"messages":   []map[string]string{{"role": "user", "content": cfg.Prompt}},
		"max_tokens": cfg.MaxTokens,
		"stream":     cfg.Stream,
	})
}

func run(ctx context.Context, client *http.Client, cfg benchConfig, body []byte, count int) []result {
	jobs := make(chan struct{})
	results := make(chan result, count)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				results <- one(ctx, client, cfg, body)
			}
		}()
	}
	go func() {
		for i := 0; i < count; i++ {
			jobs <- struct{}{}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()
	out := make([]result, 0, count)
	for r := range results {
		out = append(out, r)
	}
	return out
}

func one(ctx context.Context, client *http.Client, cfg benchConfig, body []byte) result {
	started := time.Now()
	var firstByte time.Time
	trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { firstByte = time.Now() }}
	reqCtx := httptrace.WithClientTrace(ctx, trace)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, stringsTrimRightSlash(cfg.Base)+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return result{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Key != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.Key)
	}
	if cfg.ServiceClass != "" {
		req.Header.Set("X-Gateway-Service-Class", cfg.ServiceClass)
	}
	resp, err := client.Do(req)
	if err != nil {
		return result{Latency: time.Since(started), Err: err}
	}
	b, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	latency := time.Since(started)
	ttfb := time.Duration(0)
	if !firstByte.IsZero() {
		ttfb = firstByte.Sub(started)
	}
	r := result{Latency: latency, TTFB: ttfb, Bytes: int64(len(b)), Status: resp.StatusCode}
	if readErr != nil {
		r.Err = readErr
		return r
	}
	if closeErr != nil {
		r.Err = closeErr
		return r
	}
	if resp.StatusCode/100 != 2 {
		r.Err = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(b), 240))
		return r
	}
	if !cfg.Stream {
		var doc struct {
			Usage struct {
				Prompt     int64 `json:"prompt_tokens"`
				Completion int64 `json:"completion_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(b, &doc); err == nil {
			r.Prompt = doc.Usage.Prompt
			r.Completion = doc.Usage.Completion
		}
	}
	return r
}

func summarize(cfg benchConfig, started, finished time.Time, results []result) summary {
	wall := finished.Sub(started)
	latencies := make([]time.Duration, 0, len(results))
	ttfbs := make([]time.Duration, 0, len(results))
	statusCounts := map[string]int{}
	errorsByMessage := map[string]int{}
	var prompt, completion, received int64
	errs := 0
	for _, r := range results {
		if r.Status != 0 {
			statusCounts[fmt.Sprintf("%d", r.Status)]++
		}
		if r.Err != nil {
			errs++
			errorsByMessage[truncate(r.Err.Error(), 180)]++
			continue
		}
		latencies = append(latencies, r.Latency)
		if r.TTFB > 0 {
			ttfbs = append(ttfbs, r.TTFB)
		}
		prompt += r.Prompt
		completion += r.Completion
		received += r.Bytes
	}
	success := len(latencies)
	seconds := wall.Seconds()
	if seconds <= 0 {
		seconds = 1e-9
	}
	return summary{
		StartedAt: started, FinishedAt: finished, BaseURL: cfg.Base, Model: cfg.Model,
		Concurrency: cfg.Concurrency, Requests: cfg.Requests, WarmupRequests: cfg.Warmup,
		Successful: success, Errors: errs, StatusCounts: statusCounts, WallMS: float64(wall) / float64(time.Millisecond),
		ThroughputRPS: float64(success) / seconds, Latency: stats(latencies), TTFB: stats(ttfbs),
		PromptTokens: prompt, CompletionTokens: completion, CompletionTokPS: float64(completion) / seconds,
		BytesReceived: received, BytesPerSecond: float64(received) / seconds, Stream: cfg.Stream,
		KeepAlive: !cfg.DisableKeepAlive, ServiceClass: cfg.ServiceClass, ErrorsByMessage: errorsByMessage,
	}
}

func stats(ds []time.Duration) durationStats {
	if len(ds) == 0 {
		return durationStats{}
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	pct := func(p float64) time.Duration {
		idx := int(float64(len(ds)-1) * p)
		return ds[idx]
	}
	return durationStats{P50: msFloat(pct(.50)), P95: msFloat(pct(.95)), P99: msFloat(pct(.99)), Max: msFloat(ds[len(ds)-1])}
}

func printHuman(s summary) {
	fmt.Printf("successful=%d errors=%d concurrency=%d wall=%s keepalive=%t stream=%t\n", s.Successful, s.Errors, s.Concurrency, time.Duration(s.WallMS*float64(time.Millisecond)).Round(time.Millisecond), s.KeepAlive, s.Stream)
	fmt.Printf("latency p50=%s p95=%s p99=%s max=%s\n", fmtMS(s.Latency.P50), fmtMS(s.Latency.P95), fmtMS(s.Latency.P99), fmtMS(s.Latency.Max))
	fmt.Printf("ttfb    p50=%s p95=%s p99=%s max=%s\n", fmtMS(s.TTFB.P50), fmtMS(s.TTFB.P95), fmtMS(s.TTFB.P99), fmtMS(s.TTFB.Max))
	fmt.Printf("throughput=%.2f req/s bytes=%d bytes/s=%.0f prompt_tokens=%d completion_tokens=%d completion_tok/s=%.2f\n", s.ThroughputRPS, s.BytesReceived, s.BytesPerSecond, s.PromptTokens, s.CompletionTokens, s.CompletionTokPS)
	if len(s.StatusCounts) > 0 {
		b, _ := json.Marshal(s.StatusCounts)
		fmt.Printf("status=%s\n", b)
	}
	if len(s.ErrorsByMessage) > 0 {
		for msg, n := range s.ErrorsByMessage {
			fmt.Fprintf(os.Stderr, "error x%d: %s\n", n, msg)
		}
	}
}

func writeJSONSummary(path string, s summary) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if path == "-" {
		_, err = os.Stdout.Write(b)
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func msFloat(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
func fmtMS(v float64) string {
	return (time.Duration(v * float64(time.Millisecond))).Round(time.Microsecond).String()
}
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
func stringsTrimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
