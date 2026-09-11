package telemetry

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

type Context struct {
	TraceID      string
	RootSpanID   string
	ParentSpanID string
	Sampled      bool
}

type Interval struct {
	Name  string
	Start time.Time
	End   time.Time
	Attrs map[string]any
}

type Record struct {
	Trace        Context
	RequestID    string
	API          string
	Path         string
	Tenant       string
	Actor        string
	Application  string
	ServiceClass string
	Model        string
	Alias        string
	Worker       string
	Started      time.Time
	Finished     time.Time
	FirstByte    time.Duration
	Status       int
	PromptTokens int64
	OutputTokens int64
	CachedTokens int64
	Credits      float64
	Error        string
	Intervals    []Interval
}

type Exporter struct {
	cfg      config.OpenTelemetryConfig
	client   *http.Client
	ch       chan []span
	stop     chan struct{}
	done     chan struct{}
	closed   atomic.Bool
	dropped  atomic.Uint64
	failed   atomic.Uint64
	exported atomic.Uint64
}

type span struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	Kind         int
	Start        time.Time
	End          time.Time
	Attrs        map[string]any
	Error        string
}

func New(cfg config.OpenTelemetryConfig) *Exporter {
	if !cfg.Enabled {
		return nil
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 128
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = config.Duration(2 * time.Second)
	}
	e := &Exporter{cfg: cfg, client: &http.Client{Timeout: 10 * time.Second}, ch: make(chan []span, cfg.BatchSize*4), stop: make(chan struct{}), done: make(chan struct{})}
	go e.loop()
	return e
}

func (e *Exporter) NewTrace(traceparent string) Context {
	if e == nil {
		return Context{}
	}
	ratio := e.cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	sampled := ratio >= 1 || randomFraction() < ratio
	traceID, parent := parseTraceParent(traceparent)
	if traceID == "" {
		traceID = randomHex(16)
	}
	return Context{TraceID: traceID, RootSpanID: randomHex(8), ParentSpanID: parent, Sampled: sampled}
}
func (e *Exporter) TraceParent(c Context) string {
	if e == nil || !c.Sampled {
		return ""
	}
	return "00-" + c.TraceID + "-" + c.RootSpanID + "-01"
}
func (e *Exporter) Dropped() uint64 {
	if e == nil {
		return 0
	}
	return e.dropped.Load()
}
func (e *Exporter) Failed() uint64 {
	if e == nil {
		return 0
	}
	return e.failed.Load()
}
func (e *Exporter) Exported() uint64 {
	if e == nil {
		return 0
	}
	return e.exported.Load()
}

func (e *Exporter) Record(r Record) {
	if e == nil || !r.Trace.Sampled || e.closed.Load() {
		return
	}
	if r.Finished.IsZero() {
		r.Finished = time.Now().UTC()
	}
	attrs := map[string]any{
		"gen_ai.operation.name":                operationName(r.Path),
		"gen_ai.provider.name":                 "ollama",
		"gen_ai.request.model":                 r.Model,
		"gen_ai.usage.input_tokens":            r.PromptTokens,
		"gen_ai.usage.output_tokens":           r.OutputTokens,
		"gen_ai.usage.cache_read.input_tokens": r.CachedTokens,
		"http.request.method":                  "POST",
		"http.response.status_code":            r.Status,
		"server.address":                       r.Worker,
		"ollama.gateway.request_id":            r.RequestID,
		"ollama.gateway.api":                   r.API,
		"ollama.gateway.tenant":                r.Tenant,
		"ollama.gateway.actor":                 r.Actor,
		"ollama.gateway.application":           r.Application,
		"ollama.gateway.service_class":         r.ServiceClass,
		"ollama.gateway.credits":               r.Credits,
	}
	if r.Alias != "" {
		attrs["ollama.gateway.model_alias"] = r.Alias
	}
	if r.FirstByte > 0 {
		attrs["gen_ai.response.time_to_first_chunk"] = r.FirstByte.Seconds()
	}
	root := span{TraceID: r.Trace.TraceID, SpanID: r.Trace.RootSpanID, ParentSpanID: r.Trace.ParentSpanID, Name: operationName(r.Path) + " " + r.Model, Kind: 2, Start: r.Started, End: r.Finished, Attrs: attrs, Error: r.Error}
	spans := []span{root}
	for _, iv := range r.Intervals {
		if iv.Start.IsZero() || iv.End.IsZero() || iv.End.Before(iv.Start) {
			continue
		}
		a := map[string]any{"ollama.gateway.request_id": r.RequestID}
		for k, v := range iv.Attrs {
			a[k] = v
		}
		spans = append(spans, span{TraceID: r.Trace.TraceID, SpanID: randomHex(8), ParentSpanID: r.Trace.RootSpanID, Name: iv.Name, Kind: 1, Start: iv.Start, End: iv.End, Attrs: a})
	}
	select {
	case e.ch <- spans:
	default:
		e.dropped.Add(uint64(len(spans)))
	}
}

func (e *Exporter) Close(ctx context.Context) error {
	if e == nil || !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(e.stop)
	select {
	case <-e.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Exporter) loop() {
	defer close(e.done)
	tick := time.NewTicker(e.cfg.FlushInterval.Value())
	defer tick.Stop()
	batch := make([]span, 0, e.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := e.export(batch); err != nil {
			e.failed.Add(uint64(len(batch)))
		} else {
			e.exported.Add(uint64(len(batch)))
		}
		batch = batch[:0]
	}
	for {
		select {
		case xs := <-e.ch:
			batch = append(batch, xs...)
			if len(batch) >= e.cfg.BatchSize {
				flush()
			}
		case <-tick.C:
			flush()
		case <-e.stop:
			for {
				select {
				case xs := <-e.ch:
					batch = append(batch, xs...)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (e *Exporter) export(spans []span) error {
	endpoint := strings.TrimSpace(e.cfg.Endpoint)
	if endpoint == "" {
		return fmt.Errorf("empty OTLP endpoint")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return err
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/traces"
	} else if !strings.HasSuffix(u.Path, "/v1/traces") {
		u.Path = strings.TrimRight(u.Path, "/") + "/v1/traces"
	}
	payload := map[string]any{"resourceSpans": []any{map[string]any{"resource": map[string]any{"attributes": attrsToOTLP(map[string]any{"service.name": e.cfg.ServiceName, "service.version": e.cfg.ServiceVersion, "telemetry.sdk.name": "ollama-fair-gateway", "telemetry.sdk.language": "go"})}, "scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "ollama-fair-gateway"}, "spans": spansToOTLP(spans)}}}}}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range e.cfg.Headers {
		req.Header.Set(k, v)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("OTLP HTTP %d", resp.StatusCode)
	}
	return nil
}

func spansToOTLP(in []span) []any {
	out := make([]any, 0, len(in))
	for _, s := range in {
		x := map[string]any{"traceId": s.TraceID, "spanId": s.SpanID, "name": s.Name, "kind": s.Kind, "startTimeUnixNano": fmt.Sprintf("%d", s.Start.UnixNano()), "endTimeUnixNano": fmt.Sprintf("%d", s.End.UnixNano()), "attributes": attrsToOTLP(s.Attrs)}
		if s.ParentSpanID != "" {
			x["parentSpanId"] = s.ParentSpanID
		}
		if s.Error != "" {
			x["status"] = map[string]any{"code": 2, "message": s.Error}
		}
		out = append(out, x)
	}
	return out
}
func attrsToOTLP(in map[string]any) []any {
	keys := make([]string, 0, len(in))
	for k, v := range in {
		if !empty(v) {
			keys = append(keys, k)
		}
	}
	sortStrings(keys)
	out := make([]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{"key": k, "value": otlpValue(in[k])})
	}
	return out
}
func otlpValue(v any) map[string]any {
	switch x := v.(type) {
	case string:
		return map[string]any{"stringValue": x}
	case bool:
		return map[string]any{"boolValue": x}
	case int:
		return map[string]any{"intValue": fmt.Sprintf("%d", x)}
	case int64:
		return map[string]any{"intValue": fmt.Sprintf("%d", x)}
	case uint64:
		return map[string]any{"intValue": fmt.Sprintf("%d", x)}
	case float64:
		return map[string]any{"doubleValue": x}
	default:
		return map[string]any{"stringValue": fmt.Sprint(x)}
	}
}
func empty(v any) bool {
	switch x := v.(type) {
	case string:
		return x == ""
	case int:
		return x == 0
	case int64:
		return x == 0
	case uint64:
		return x == 0
	case float64:
		return x == 0
	}
	return false
}
func operationName(path string) string {
	p := strings.ToLower(path)
	switch {
	case strings.Contains(p, "embed"):
		return "embeddings"
	case strings.Contains(p, "chat") || strings.Contains(p, "messages"):
		return "chat"
	default:
		return "text_completion"
	}
}
func randomHex(n int) string { b := make([]byte, n); _, _ = rand.Read(b); return hex.EncodeToString(b) }
func randomFraction() float64 {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	var x uint64
	for _, z := range b {
		x = x<<8 | uint64(z)
	}
	return float64(x>>11) / float64(uint64(1)<<53)
}
func parseTraceParent(v string) (string, string) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		return "", ""
	}
	if _, e := hex.DecodeString(parts[1]); e != nil {
		return "", ""
	}
	if _, e := hex.DecodeString(parts[2]); e != nil {
		return "", ""
	}
	return strings.ToLower(parts[1]), strings.ToLower(parts[2])
}
func sortStrings(x []string) {
	for i := 1; i < len(x); i++ {
		for j := i; j > 0 && x[j] < x[j-1]; j-- {
			x[j], x[j-1] = x[j-1], x[j]
		}
	}
}
