package autotune

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/hoststats"
	"github.com/example/ollama-fair-gateway/internal/state"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type Sample struct {
	TTFTMS           float64 `json:"ttft_ms"`
	ServiceMS        float64 `json:"service_ms"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	PromptTPS        float64 `json:"prompt_tps"`
	OutputTPS        float64 `json:"output_tps"`
	Status           int     `json:"status"`
	Error            string  `json:"error,omitempty"`
}

type Level struct {
	Concurrency        int      `json:"concurrency"`
	Requests           int      `json:"requests"`
	Successful         int      `json:"successful"`
	TTFTP50MS          float64  `json:"ttft_p50_ms"`
	TTFTP95MS          float64  `json:"ttft_p95_ms"`
	MeanServiceMS      float64  `json:"mean_service_ms"`
	MeanPromptTPS      float64  `json:"mean_prompt_tps"`
	MeanOutputTPS      float64  `json:"mean_output_tps"`
	AggregateOutputTPS float64  `json:"aggregate_output_tps"`
	PeakVRAMBytes      int64    `json:"peak_vram_bytes,omitempty"`
	PeakGPUPercent     float64  `json:"peak_gpu_percent,omitempty"`
	Score              float64  `json:"score"`
	Samples            []Sample `json:"samples,omitempty"`
}

type Profile struct {
	ID                     string    `json:"id"`
	Worker                 string    `json:"worker"`
	Model                  string    `json:"model"`
	Status                 string    `json:"status"` // queued|running|completed|failed|cancelled
	StartedAt              time.Time `json:"started_at"`
	FinishedAt             time.Time `json:"finished_at,omitempty"`
	Levels                 []Level   `json:"levels,omitempty"`
	RecommendedConcurrency int       `json:"recommended_concurrency,omitempty"`
	Applied                bool      `json:"applied,omitempty"`
	AppliedAt              time.Time `json:"applied_at,omitempty"`
	Error                  string    `json:"error,omitempty"`
}

type persistentState struct {
	Version  int                       `json:"version"`
	SavedAt  time.Time                 `json:"saved_at"`
	Profiles []Profile                 `json:"profiles"`
	Applied  map[string]map[string]int `json:"applied"`
}

type StartRequest struct {
	Worker          string `json:"worker"`
	Model           string `json:"model"`
	MaxConcurrency  int    `json:"max_concurrency,omitempty"`
	SamplesPerLevel int    `json:"samples_per_level,omitempty"`
	MaxTokens       int    `json:"max_tokens,omitempty"`
	Prompt          string `json:"prompt,omitempty"`
}

type Manager struct {
	mu       sync.RWMutex
	cfg      config.AutoTuningConfig
	workers  *worker.Pool
	path     string
	client   *http.Client
	profiles map[string]Profile
	order    []string
	applied  map[string]map[string]int
	cancel   map[string]context.CancelFunc
}

func New(cfg config.AutoTuningConfig, workers *worker.Pool, path string) (*Manager, error) {
	m := &Manager{cfg: cfg, workers: workers, path: path, client: &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, MaxIdleConns: 64, MaxIdleConnsPerHost: 32, IdleConnTimeout: 30 * time.Second}}, profiles: map[string]Profile{}, applied: map[string]map[string]int{}, cancel: map[string]context.CancelFunc{}}
	var ps persistentState
	if path != "" {
		err := (state.AtomicJSON{Path: path, Mode: 0640}).Load(&ps)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	for _, p := range ps.Profiles {
		if p.ID == "" {
			continue
		}
		if p.Status == "running" || p.Status == "queued" {
			p.Status = "failed"
			p.Error = "gateway restarted while benchmark was active"
			p.FinishedAt = time.Now().UTC()
		}
		m.profiles[p.ID] = p
		m.order = append(m.order, p.ID)
	}
	if ps.Applied != nil {
		m.applied = ps.Applied
	}
	return m, nil
}

func (m *Manager) saveLocked() error {
	if m.path == "" {
		return nil
	}
	profiles := make([]Profile, 0, len(m.order))
	for _, id := range m.order {
		if p, ok := m.profiles[id]; ok {
			profiles = append(profiles, p)
		}
	}
	ps := persistentState{Version: 1, SavedAt: time.Now().UTC(), Profiles: profiles, Applied: m.applied}
	return (state.AtomicJSON{Path: m.path, Mode: 0640}).Save(ps)
}

func (m *Manager) Applied() map[string]map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]map[string]int{}
	for w, mm := range m.applied {
		out[w] = map[string]int{}
		for model, n := range mm {
			out[w][model] = n
		}
	}
	return out
}

func (m *Manager) List() []Profile {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Profile, 0, len(m.order))
	for i := len(m.order) - 1; i >= 0; i-- {
		if p, ok := m.profiles[m.order[i]]; ok {
			out = append(out, cloneProfile(p))
		}
	}
	return out
}
func (m *Manager) Get(id string) (Profile, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.profiles[id]
	return cloneProfile(p), ok
}

func cloneProfile(p Profile) Profile {
	p.Levels = append([]Level(nil), p.Levels...)
	for i := range p.Levels {
		p.Levels[i].Samples = append([]Sample(nil), p.Levels[i].Samples...)
	}
	return p
}

func (m *Manager) Start(parent context.Context, in StartRequest) (Profile, error) {
	if !m.cfg.Enabled {
		return Profile{}, errors.New("auto tuning is disabled")
	}
	in.Worker = strings.TrimSpace(in.Worker)
	in.Model = strings.TrimSpace(in.Model)
	if in.Worker == "" || in.Model == "" {
		return Profile{}, errors.New("worker and model are required")
	}
	wc, ok := m.workers.WorkerConfig(in.Worker)
	if !ok {
		return Profile{}, fmt.Errorf("unknown worker %q", in.Worker)
	}
	if in.MaxConcurrency <= 0 {
		in.MaxConcurrency = m.cfg.MaxConcurrency
	}
	in.MaxConcurrency = min(in.MaxConcurrency, wc.MaxConcurrent)
	if in.MaxConcurrency < 1 {
		in.MaxConcurrency = 1
	}
	if in.SamplesPerLevel <= 0 {
		in.SamplesPerLevel = m.cfg.SamplesPerLevel
	}
	if in.SamplesPerLevel > 20 {
		in.SamplesPerLevel = 20
	}
	if in.MaxTokens <= 0 {
		in.MaxTokens = m.cfg.MaxTokens
	}
	if in.MaxTokens > 4096 {
		in.MaxTokens = 4096
	}
	if strings.TrimSpace(in.Prompt) == "" {
		in.Prompt = m.cfg.Prompt
	}
	id := fmt.Sprintf("tune-%d", time.Now().UnixNano())
	p := Profile{ID: id, Worker: in.Worker, Model: in.Model, Status: "queued", StartedAt: time.Now().UTC()}
	m.mu.Lock()
	m.profiles[id] = p
	m.order = append(m.order, id)
	_ = m.saveLocked()
	m.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), m.cfg.Timeout.Value())
	m.mu.Lock()
	m.cancel[id] = cancel
	m.mu.Unlock()
	go m.run(ctx, id, in, wc)
	return p, nil
}

func (m *Manager) Cancel(id string) bool {
	m.mu.RLock()
	c := m.cancel[id]
	m.mu.RUnlock()
	if c == nil {
		return false
	}
	c()
	return true
}

func (m *Manager) Apply(id string) (Profile, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.profiles[id]
	if !ok {
		return Profile{}, errors.New("benchmark profile not found")
	}
	if p.Status != "completed" || p.RecommendedConcurrency <= 0 {
		return Profile{}, errors.New("benchmark has no applicable recommendation")
	}
	if m.applied[p.Worker] == nil {
		m.applied[p.Worker] = map[string]int{}
	}
	m.applied[p.Worker][p.Model] = p.RecommendedConcurrency
	p.Applied = true
	p.AppliedAt = time.Now().UTC()
	m.profiles[id] = p
	if err := m.saveLocked(); err != nil {
		return Profile{}, err
	}
	return cloneProfile(p), nil
}
func (m *Manager) Reset(workerName, model string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mm := m.applied[workerName]; mm != nil {
		delete(mm, model)
		if len(mm) == 0 {
			delete(m.applied, workerName)
		}
	}
	return m.saveLocked()
}

func (m *Manager) run(ctx context.Context, id string, in StartRequest, wc config.WorkerConfig) {
	m.update(id, func(p *Profile) { p.Status = "running" })
	levels := make([]Level, 0, in.MaxConcurrency)
	for c := 1; c <= in.MaxConcurrency; c++ {
		if ctx.Err() != nil {
			m.finishCancelled(id, ctx.Err())
			return
		}
		lvl := m.runLevel(ctx, wc, in.Model, in.Prompt, in.MaxTokens, c, in.SamplesPerLevel)
		levels = append(levels, lvl)
		m.update(id, func(p *Profile) { p.Levels = append([]Level(nil), levels...) })
	}
	if len(levels) == 0 {
		m.finishFailed(id, "no benchmark levels completed")
		return
	}
	maxAgg, maxP95 := 0.0, 0.0
	for _, l := range levels {
		if l.AggregateOutputTPS > maxAgg {
			maxAgg = l.AggregateOutputTPS
		}
		if l.TTFTP95MS > maxP95 {
			maxP95 = l.TTFTP95MS
		}
	}
	best, bestScore := 1, -math.MaxFloat64
	for i := range levels {
		throughput := 0.0
		if maxAgg > 0 {
			throughput = levels[i].AggregateOutputTPS / maxAgg
		}
		latency := 0.0
		if maxP95 > 0 {
			latency = levels[i].TTFTP95MS / maxP95
		}
		failure := 1.0 - float64(levels[i].Successful)/float64(max(1, levels[i].Requests))
		levels[i].Score = m.cfg.ThroughputWeight*throughput - m.cfg.TTFTWeight*latency - failure
		if levels[i].Successful == levels[i].Requests && levels[i].Score > bestScore {
			bestScore = levels[i].Score
			best = levels[i].Concurrency
		}
	}
	m.mu.Lock()
	p := m.profiles[id]
	p.Status = "completed"
	p.Levels = levels
	p.RecommendedConcurrency = best
	p.FinishedAt = time.Now().UTC()
	m.profiles[id] = p
	delete(m.cancel, id)
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *Manager) update(id string, fn func(*Profile)) {
	m.mu.Lock()
	p := m.profiles[id]
	fn(&p)
	m.profiles[id] = p
	_ = m.saveLocked()
	m.mu.Unlock()
}
func (m *Manager) finishCancelled(id string, err error) {
	m.mu.Lock()
	p := m.profiles[id]
	p.Status = "cancelled"
	if err != nil {
		p.Error = err.Error()
	}
	p.FinishedAt = time.Now().UTC()
	m.profiles[id] = p
	delete(m.cancel, id)
	_ = m.saveLocked()
	m.mu.Unlock()
}
func (m *Manager) finishFailed(id, msg string) {
	m.mu.Lock()
	p := m.profiles[id]
	p.Status = "failed"
	p.Error = msg
	p.FinishedAt = time.Now().UTC()
	m.profiles[id] = p
	delete(m.cancel, id)
	_ = m.saveLocked()
	m.mu.Unlock()
}

func (m *Manager) runLevel(ctx context.Context, wc config.WorkerConfig, model, prompt string, maxTokens, concurrency, repeats int) Level {
	lvl := Level{Concurrency: concurrency, Requests: concurrency * repeats}
	var samples []Sample
	var totalTokens int64
	var totalWall time.Duration
	var peakVRAM int64
	var peakGPU float64
	var peakMu sync.Mutex
	for rep := 0; rep < repeats; rep++ {
		start := time.Now()
		ch := make(chan Sample, concurrency)
		var wg sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); ch <- m.one(ctx, wc.URL, model, prompt, maxTokens) }()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(ch); close(done) }()
		var telemetryWG sync.WaitGroup
		if wc.NVIDIASMI {
			telemetryWG.Add(1)
			go func() {
				defer telemetryWG.Done()
				t := time.NewTicker(200 * time.Millisecond)
				defer t.Stop()
				for {
					select {
					case <-done:
						return
					case <-ctx.Done():
						return
					case <-t.C:
						c, cancel := context.WithTimeout(ctx, time.Second)
						n, e := hoststats.ReadNVIDIA(c, wc.NVIDIAGPU)
						cancel()
						if e == nil {
							peakMu.Lock()
							if n.MemoryUsedBytes > peakVRAM {
								peakVRAM = n.MemoryUsedBytes
							}
							if n.UtilizationPercent > peakGPU {
								peakGPU = n.UtilizationPercent
							}
							peakMu.Unlock()
						}
					}
				}
			}()
		}
		for s := range ch {
			samples = append(samples, s)
			if s.Status >= 200 && s.Status < 300 && s.Error == "" {
				lvl.Successful++
				totalTokens += s.CompletionTokens
			}
		}
		telemetryWG.Wait()
		totalWall += time.Since(start)
	}
	lvl.Samples = samples
	lvl.PeakVRAMBytes = peakVRAM
	lvl.PeakGPUPercent = peakGPU
	var ttfts, services []float64
	var pTPS, oTPS float64
	for _, s := range samples {
		if s.Error != "" || s.Status < 200 || s.Status >= 300 {
			continue
		}
		ttfts = append(ttfts, s.TTFTMS)
		services = append(services, s.ServiceMS)
		pTPS += s.PromptTPS
		oTPS += s.OutputTPS
	}
	lvl.TTFTP50MS = percentile(ttfts, .50)
	lvl.TTFTP95MS = percentile(ttfts, .95)
	lvl.MeanServiceMS = mean(services)
	if lvl.Successful > 0 {
		lvl.MeanPromptTPS = pTPS / float64(lvl.Successful)
		lvl.MeanOutputTPS = oTPS / float64(lvl.Successful)
	}
	if totalWall > 0 {
		lvl.AggregateOutputTPS = float64(totalTokens) / totalWall.Seconds()
	}
	return lvl
}

func (m *Manager) one(ctx context.Context, base, model, prompt string, maxTokens int) Sample {
	u, err := url.Parse(strings.TrimRight(base, "/") + "/api/generate")
	if err != nil {
		return Sample{Error: err.Error()}
	}
	b, _ := json.Marshal(map[string]any{"model": model, "prompt": prompt, "stream": true, "keep_alive": "5m", "options": map[string]any{"num_predict": maxTokens, "temperature": 0}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return Sample{Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := m.client.Do(req)
	if err != nil {
		return Sample{Error: err.Error(), ServiceMS: float64(time.Since(start).Microseconds()) / 1000}
	}
	defer resp.Body.Close()
	out := Sample{Status: resp.StatusCode}
	scan := bufio.NewScanner(resp.Body)
	buf := make([]byte, 64*1024)
	scan.Buffer(buf, 4<<20)
	first := true
	for scan.Scan() {
		if first {
			out.TTFTMS = float64(time.Since(start).Microseconds()) / 1000
			first = false
		}
		var v map[string]any
		if json.Unmarshal(scan.Bytes(), &v) != nil {
			continue
		}
		if n := asI64(v["prompt_eval_count"]); n > 0 {
			out.PromptTokens = n
		}
		if n := asI64(v["eval_count"]); n > 0 {
			out.CompletionTokens = n
		}
		pn := asI64(v["prompt_eval_duration"])
		en := asI64(v["eval_duration"])
		if pn > 0 && out.PromptTokens > 0 {
			out.PromptTPS = float64(out.PromptTokens) / (float64(pn) / 1e9)
		}
		if en > 0 && out.CompletionTokens > 0 {
			out.OutputTPS = float64(out.CompletionTokens) / (float64(en) / 1e9)
		}
	}
	if err := scan.Err(); err != nil {
		out.Error = err.Error()
	}
	out.ServiceMS = float64(time.Since(start).Microseconds()) / 1000
	return out
}
func asI64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case int64:
		return x
	case int:
		return int64(x)
	}
	return 0
}
func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}
func percentile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	ys := append([]float64(nil), xs...)
	sort.Float64s(ys)
	i := int(math.Ceil(p*float64(len(ys)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(ys) {
		i = len(ys) - 1
	}
	return ys[i]
}
