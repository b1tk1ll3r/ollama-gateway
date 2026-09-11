package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/cost"
)

type Proxy struct {
	client  *http.Client
	buffers sync.Pool
}
type ProgressFunc func(bytesOut int64, usage cost.Usage)

type Result struct {
	Status           int
	BytesIn          int64
	BytesOut         int64
	Usage            cost.Usage
	Err              error
	Started          bool
	FirstByte        time.Duration
	Captured         []byte
	CaptureTruncated bool
}

func New() *Proxy {
	tr := &http.Transport{Proxy: http.ProxyFromEnvironment, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: false, MaxIdleConns: 1024, MaxIdleConnsPerHost: 256, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ExpectContinueTimeout: time.Second, DisableCompression: true}
	p := &Proxy{client: &http.Client{Transport: tr}}
	p.buffers.New = func() any { b := make([]byte, 32<<10); return &b }
	return p
}

func (p *Proxy) Forward(ctx context.Context, w http.ResponseWriter, in *http.Request, target *url.URL, body io.Reader, api string, estimatedInput int64, progress ...ProgressFunc) Result {
	return p.forward(ctx, w, in, target, body, api, estimatedInput, 0, progress...)
}

// ForwardCapture behaves like Forward but retains up to captureLimit bytes of
// the upstream response for post-response control-plane processing. It is used
// only by explicitly enabled content-bearing features such as conversation
// persistence; the normal inference path keeps response bodies uncaptured.
func (p *Proxy) ForwardCapture(ctx context.Context, w http.ResponseWriter, in *http.Request, target *url.URL, body io.Reader, api string, estimatedInput int64, captureLimit int64, progress ...ProgressFunc) Result {
	return p.forward(ctx, w, in, target, body, api, estimatedInput, captureLimit, progress...)
}

func (p *Proxy) forward(ctx context.Context, w http.ResponseWriter, in *http.Request, target *url.URL, body io.Reader, api string, estimatedInput int64, captureLimit int64, progress ...ProgressFunc) Result {
	u := *target
	u.Path = singleJoiningSlash(target.Path, in.URL.Path)
	u.RawQuery = in.URL.RawQuery
	cr := &countingReader{r: body}
	var requestBody io.Reader
	if body != nil {
		requestBody = cr
	}
	req, err := http.NewRequestWithContext(ctx, in.Method, u.String(), requestBody)
	if err != nil {
		return Result{Status: 502, BytesIn: cr.n, Err: err}
	}
	if body != nil && in.ContentLength >= 0 {
		req.ContentLength = in.ContentLength
	}
	copyHeader(req.Header, in.Header)
	stripHop(req.Header)
	req.Header.Del("Authorization")
	req.Header.Del("X-API-Key")
	req.Header.Del("X-Gateway-Service-Class")
	req.Host = target.Host
	if ip, _, e := net.SplitHostPort(in.RemoteAddr); e == nil {
		prior := req.Header.Get("X-Forwarded-For")
		if prior != "" {
			req.Header.Set("X-Forwarded-For", prior+", "+ip)
		} else {
			req.Header.Set("X-Forwarded-For", ip)
		}
	}
	if in.TLS != nil {
		req.Header.Set("X-Forwarded-Proto", "https")
	} else {
		req.Header.Set("X-Forwarded-Proto", "http")
	}
	requestStarted := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		return Result{Status: 502, BytesIn: cr.n, Err: err}
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	stripHop(w.Header())
	w.WriteHeader(resp.StatusCode)
	meter := newMeter(api, estimatedInput)
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	stream := strings.Contains(ct, "event-stream") || strings.Contains(ct, "ndjson") || strings.Contains(ct, "stream")
	bp := p.buffers.Get().(*[]byte)
	defer p.buffers.Put(bp)
	buf := *bp
	var out int64
	var firstByte time.Duration
	var captured []byte
	var captureTruncated bool
	if captureLimit > 0 {
		capHint := captureLimit
		if capHint > 1<<20 {
			capHint = 1 << 20
		}
		captured = make([]byte, 0, int(capHint))
	}
	var observer ProgressFunc
	if len(progress) > 0 {
		observer = progress[0]
	}
	var lastProgress time.Time
	notifyProgress := func(force bool, usage cost.Usage) {
		if observer == nil {
			return
		}
		now := time.Now()
		if force || lastProgress.IsZero() || now.Sub(lastProgress) >= 200*time.Millisecond {
			observer(out, usage)
			lastProgress = now
		}
	}
	for {
		n, re := resp.Body.Read(buf)
		if n > 0 {
			if firstByte == 0 {
				firstByte = time.Since(requestStarted)
			}
			chunk := buf[:n]
			meter.Feed(chunk)
			if captureLimit > 0 {
				remain := captureLimit - int64(len(captured))
				if remain > 0 {
					take := int64(len(chunk))
					if take > remain {
						take = remain
					}
					captured = append(captured, chunk[:int(take)]...)
				}
				if int64(len(chunk)) > remain {
					captureTruncated = true
				}
			}
			wn, we := w.Write(chunk)
			out += int64(wn)
			if stream {
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
			}
			notifyProgress(false, meter.usage)
			if we != nil {
				u := meter.Finish(out)
				notifyProgress(true, u)
				return Result{Status: resp.StatusCode, BytesIn: cr.n, BytesOut: out, Usage: u, Err: we, Started: true, FirstByte: firstByte, Captured: captured, CaptureTruncated: captureTruncated}
			}
		}
		if re != nil {
			if re == io.EOF {
				break
			}
			u := meter.Finish(out)
			notifyProgress(true, u)
			return Result{Status: resp.StatusCode, BytesIn: cr.n, BytesOut: out, Usage: u, Err: re, Started: true, FirstByte: firstByte, Captured: captured, CaptureTruncated: captureTruncated}
		}
	}
	finalUsage := meter.Finish(out)
	notifyProgress(true, finalUsage)
	return Result{Status: resp.StatusCode, BytesIn: cr.n, BytesOut: out, Usage: finalUsage, Started: true, FirstByte: firstByte, Captured: captured, CaptureTruncated: captureTruncated}
}
func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

var hopHeaders = []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func stripHop(h http.Header) {
	if c := h.Get("Connection"); c != "" {
		for _, f := range strings.Split(c, ",") {
			h.Del(strings.TrimSpace(f))
		}
	}
	for _, k := range hopHeaders {
		h.Del(k)
	}
}
func singleJoiningSlash(a, b string) string {
	as := strings.HasSuffix(a, "/")
	bs := strings.HasPrefix(b, "/")
	switch {
	case as && bs:
		return a + b[1:]
	case !as && !bs:
		return a + "/" + b
	default:
		return a + b
	}
}

type meter struct {
	api            string
	line           []byte
	usage          cost.Usage
	found          bool
	outTextBytes   int64
	estimatedInput int64
}

func newMeter(api string, estimatedInput int64) *meter {
	return &meter{api: api, estimatedInput: estimatedInput}
}
func (m *meter) Feed(p []byte) {
	m.line = append(m.line, p...)
	for {
		idx := bytes.IndexByte(m.line, '\n')
		if idx < 0 {
			if len(m.line) > 2<<20 {
				m.line = append([]byte(nil), m.line[len(m.line)-(1<<20):]...)
			}
			return
		}
		line := bytes.TrimSpace(m.line[:idx])
		m.process(line)
		m.line = append(m.line[:0], m.line[idx+1:]...)
	}
}
func (m *meter) Finish(bytesOut int64) cost.Usage {
	if len(bytes.TrimSpace(m.line)) > 0 {
		m.process(bytes.TrimSpace(m.line))
	}
	if !m.found {
		m.usage.PromptTokens = m.estimatedInput
		if m.usage.CompletionTokens == 0 {
			if m.outTextBytes > 0 {
				m.usage.CompletionTokens = (m.outTextBytes + 3) / 4
			} else if bytesOut > 0 {
				m.usage.CompletionTokens = (bytesOut + 15) / 16
			}
		}
		m.usage.Approximate = true
	}
	return m.usage
}
func (m *meter) process(line []byte) {
	if len(line) == 0 {
		return
	}
	if bytes.HasPrefix(line, []byte("data:")) {
		line = bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if bytes.Equal(line, []byte("[DONE]")) {
			return
		}
	}
	var v map[string]any
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return
	}
	if m.api == "ollama" {
		m.parseNative(v)
	} else if m.api == "anthropic" {
		m.parseAnthropic(v)
	} else {
		m.parseOpenAI(v)
	}
}
func (m *meter) parseNative(v map[string]any) {
	u := cost.Usage{PromptTokens: i64(v["prompt_eval_count"]), CachedPromptTokens: i64(v["prompt_eval_cached_count"]), CompletionTokens: i64(v["eval_count"]), PromptEvalNS: i64(v["prompt_eval_duration"]), EvalNS: i64(v["eval_duration"]), LoadNS: i64(v["load_duration"]), TotalNS: i64(v["total_duration"])}
	if u.PromptTokens > 0 || u.CompletionTokens > 0 || u.TotalNS > 0 {
		m.usage = u
		m.found = true
	}
}
func (m *meter) parseOpenAI(v map[string]any) {
	if u := findUsage(v); u != nil {
		pt := firstI64(u, "prompt_tokens", "input_tokens")
		ct := firstI64(u, "completion_tokens", "output_tokens")
		cached := int64(0)
		if d, ok := u["prompt_tokens_details"].(map[string]any); ok {
			cached = i64(d["cached_tokens"])
		}
		m.usage.PromptTokens = pt
		m.usage.CompletionTokens = ct
		m.usage.CachedPromptTokens = cached
		m.found = true
	}
	m.outTextBytes += deltaTextBytes(v)
}
func (m *meter) parseAnthropic(v map[string]any) {
	if u := findUsage(v); u != nil {
		pt := firstI64(u, "input_tokens", "prompt_tokens")
		ct := firstI64(u, "output_tokens", "completion_tokens")
		cached := firstI64(u, "cache_read_input_tokens", "cached_tokens")
		if pt > 0 {
			m.usage.PromptTokens = pt
		}
		if ct > 0 {
			m.usage.CompletionTokens = ct
		}
		if cached > 0 {
			m.usage.CachedPromptTokens = cached
		}
		if pt > 0 || ct > 0 {
			m.found = true
		}
	}
	if d, ok := v["delta"].(map[string]any); ok {
		if text, ok := d["text"].(string); ok {
			m.outTextBytes += int64(len(text))
		}
	}
	if cb, ok := v["content_block"].(map[string]any); ok {
		if text, ok := cb["text"].(string); ok {
			m.outTextBytes += int64(len(text))
		}
	}
}

func findUsage(v any) map[string]any {
	switch x := v.(type) {
	case map[string]any:
		if u, ok := x["usage"].(map[string]any); ok {
			return u
		}
		for _, key := range []string{"response", "message"} {
			if r, ok := x[key]; ok {
				if u := findUsage(r); u != nil {
					return u
				}
			}
		}
	case []any:
		for _, z := range x {
			if u := findUsage(z); u != nil {
				return u
			}
		}
	}
	return nil
}
func deltaTextBytes(v map[string]any) int64 {
	var n int64
	if s, ok := v["delta"].(string); ok {
		n += int64(len(s))
	}
	if choices, ok := v["choices"].([]any); ok {
		for _, c := range choices {
			cm, _ := c.(map[string]any)
			d, _ := cm["delta"].(map[string]any)
			if s, ok := d["content"].(string); ok {
				n += int64(len(s))
			}
		}
	}
	return n
}
func i64(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		n, _ := x.Int64()
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}
func firstI64(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if n := i64(m[k]); n != 0 {
			return n
		}
	}
	return 0
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	if c.r == nil {
		return 0, io.EOF
	}
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func WriteJSONError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": msg, "type": "gateway_error"}})
}
func FormatRetryAfter(d time.Duration) string {
	if d <= 0 {
		return "1"
	}
	return strconv.Itoa(max(1, int(d.Round(time.Second)/time.Second)))
}
func BackendError(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("Ollama backend error: %v", err)
}
