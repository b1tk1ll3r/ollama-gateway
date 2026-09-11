package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/hoststats"
)

var (
	ErrModelPlacementBlocked = errors.New("model blocked by placement policy")
	ErrModelNotInstalled     = errors.New("model not installed on eligible worker")
)

type state struct {
	cfg                      config.WorkerConfig
	url                      *url.URL
	active                   atomic.Int64
	healthy                  atomic.Bool
	mu                       sync.RWMutex
	models                   map[string]bool
	installed                map[string]bool
	installedKnown           bool
	installedModels          []string
	inventoryError           string
	modelActive              map[string]int
	modelMaintenance         map[string]bool
	metadata                 map[string]metadataEntry
	performance              map[string]performanceState
	baselinePlacement        config.ModelPlacementRule
	baselineModelConcurrency map[string]int
	placement                config.ModelPlacementRule
	placementOverride        bool
	loadedModels             []LoadedModel
	telemetry                ResourceTelemetry
	lastError                string
	lastCheck                time.Time
	maintenance              string
	circuitState             string
	circuitFailures          int
	circuitOpenUntil         time.Time
	halfOpenInFlight         bool
	lastCircuitError         string
}

type LoadedModel struct {
	Name          string    `json:"name"`
	Model         string    `json:"model,omitempty"`
	Size          int64     `json:"size,omitempty"`
	SizeVRAM      int64     `json:"size_vram,omitempty"`
	ContextLength int64     `json:"context_length,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
}

type ResourceTelemetry struct {
	MemoryUsedBytes   int64     `json:"memory_used_bytes,omitempty"`
	MemoryTotalBytes  int64     `json:"memory_total_bytes,omitempty"`
	VRAMUsedBytes     int64     `json:"vram_used_bytes,omitempty"`
	VRAMTotalBytes    int64     `json:"vram_total_bytes,omitempty"`
	GPUUtilizationPct float64   `json:"gpu_utilization_percent,omitempty"`
	GPUTemperatureC   float64   `json:"gpu_temperature_c,omitempty"`
	GPUPowerWatts     float64   `json:"gpu_power_watts,omitempty"`
	Source            string    `json:"source,omitempty"`
	UpdatedAt         time.Time `json:"updated_at,omitempty"`
	Error             string    `json:"error,omitempty"`
}

// externalTelemetry uses pointers so an omitted field does not accidentally
// overwrite a previously collected local value with the JSON zero value.
type externalTelemetry struct {
	MemoryUsedBytes   *int64     `json:"memory_used_bytes"`
	MemoryTotalBytes  *int64     `json:"memory_total_bytes"`
	VRAMUsedBytes     *int64     `json:"vram_used_bytes"`
	VRAMTotalBytes    *int64     `json:"vram_total_bytes"`
	GPUUtilizationPct *float64   `json:"gpu_utilization_percent"`
	GPUTemperatureC   *float64   `json:"gpu_temperature_c"`
	GPUPowerWatts     *float64   `json:"gpu_power_watts"`
	Source            string     `json:"source,omitempty"`
	UpdatedAt         *time.Time `json:"updated_at,omitempty"`
	Error             string     `json:"error,omitempty"`
}

type ModelMetadata struct {
	Model                   string       `json:"model"`
	Capabilities            []string     `json:"capabilities,omitempty"`
	ContextLength           int64        `json:"context_length,omitempty"`
	ConfiguredContextLength int64        `json:"configured_context_length,omitempty"`
	Details                 ModelDetails `json:"details,omitempty"`
	UpdatedAt               time.Time    `json:"updated_at,omitempty"`
	Error                   string       `json:"error,omitempty"`
}

type ContextWindow struct {
	Worker              string `json:"worker"`
	Model               string `json:"model"`
	ModelMaxTokens      int64  `json:"model_max_tokens,omitempty"`
	ConfiguredTokens    int64  `json:"configured_tokens,omitempty"`
	LoadedTokens        int64  `json:"loaded_tokens,omitempty"`
	WorkerDefaultTokens int64  `json:"worker_default_tokens,omitempty"`
	WorkerLimitTokens   int64  `json:"worker_limit_tokens,omitempty"`
	EffectiveTokens     int64  `json:"effective_tokens,omitempty"`
	EffectiveSource     string `json:"effective_source,omitempty"`
	MetadataError       string `json:"metadata_error,omitempty"`
}

type metadataEntry struct {
	Data      ModelMetadata
	FetchedAt time.Time
}

type ModelPerformance struct {
	Model     string  `json:"model"`
	PromptTPS float64 `json:"prompt_tps,omitempty"`
	OutputTPS float64 `json:"output_tps,omitempty"`
	Samples   int64   `json:"samples"`
}

type performanceState struct {
	PromptTPS float64
	OutputTPS float64
	Samples   int64
}

type Snapshot struct {
	Name                string                    `json:"name"`
	URL                 string                    `json:"url"`
	Healthy             bool                      `json:"healthy"`
	Active              int64                     `json:"active"`
	MaxConcurrent       int                       `json:"max_concurrent"`
	Models              []string                  `json:"models"`
	LoadedModels        []LoadedModel             `json:"loaded_models,omitempty"`
	ResidentBytes       int64                     `json:"resident_bytes,omitempty"`
	VRAMBytes           int64                     `json:"vram_bytes,omitempty"`
	MemoryCapacityBytes int64                     `json:"memory_capacity_bytes,omitempty"`
	VRAMCapacityBytes   int64                     `json:"vram_capacity_bytes,omitempty"`
	MemoryUsedBytes     int64                     `json:"memory_used_bytes,omitempty"`
	MemoryTotalBytes    int64                     `json:"memory_total_bytes,omitempty"`
	VRAMUsedBytes       int64                     `json:"vram_used_bytes,omitempty"`
	VRAMTotalBytes      int64                     `json:"vram_total_bytes,omitempty"`
	GPUUtilizationPct   float64                   `json:"gpu_utilization_percent,omitempty"`
	GPUTemperatureC     float64                   `json:"gpu_temperature_c,omitempty"`
	GPUPowerWatts       float64                   `json:"gpu_power_watts,omitempty"`
	ModelActive         map[string]int            `json:"model_active,omitempty"`
	ModelLimits         map[string]int            `json:"model_limits,omitempty"`
	Performance         []ModelPerformance        `json:"performance,omitempty"`
	ModelPlacement      config.ModelPlacementRule `json:"model_placement"`
	PlacementOverride   bool                      `json:"placement_override,omitempty"`
	Maintenance         string                    `json:"maintenance"`
	AcceptingNew        bool                      `json:"accepting_new"`
	CircuitState        string                    `json:"circuit_state"`
	CircuitFailures     int                       `json:"circuit_failures,omitempty"`
	CircuitOpenUntil    time.Time                 `json:"circuit_open_until,omitempty"`
	LastCircuitError    string                    `json:"last_circuit_error,omitempty"`
	TelemetrySource     string                    `json:"telemetry_source,omitempty"`
	TelemetryError      string                    `json:"telemetry_error,omitempty"`
	Labels              map[string]string         `json:"labels,omitempty"`
	LastError           string                    `json:"last_error,omitempty"`
	LastCheck           time.Time                 `json:"last_check"`
}

type PlacementDecision struct {
	Allowed       bool   `json:"allowed"`
	Source        string `json:"source"`
	Pattern       string `json:"pattern,omitempty"`
	ExactOverride bool   `json:"exact_override,omitempty"`
}

type PlacementSnapshot struct {
	Worker          string                    `json:"worker"`
	Baseline        config.ModelPlacementRule `json:"baseline"`
	Effective       config.ModelPlacementRule `json:"effective"`
	Override        bool                      `json:"override"`
	InventoryKnown  bool                      `json:"inventory_known"`
	InstalledModels []string                  `json:"installed_models,omitempty"`
	LoadedModels    []string                  `json:"loaded_models,omitempty"`
	InventoryError  string                    `json:"inventory_error,omitempty"`
}

type Pool struct {
	workers     []*state
	byName      map[string]*state
	client      *http.Client
	notify      chan struct{}
	control     string
	routing     config.RoutingConfig
	capCfg      config.ModelCapabilitiesConfig
	reliability config.ReliabilityConfig
}

type Lease struct {
	State *state
	model string
	once  sync.Once
	pool  *Pool
}

func (l *Lease) Name() string  { return l.State.cfg.Name }
func (l *Lease) URL() *url.URL { return l.State.url }
func (l *Lease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.State.abandonCircuitProbe()
		l.State.active.Add(-1)
		if l.model != "" {
			l.State.mu.Lock()
			if l.State.modelActive[l.model] > 1 {
				l.State.modelActive[l.model]--
			} else {
				delete(l.State.modelActive, l.model)
			}
			l.State.mu.Unlock()
		}
		l.pool.signal()
	})
}

func New(cfgs []config.WorkerConfig, control string) *Pool {
	p := &Pool{byName: map[string]*state{}, notify: make(chan struct{}, 1), control: control, client: &http.Client{Timeout: 4 * time.Second}}
	p.SetRoutingConfig(config.RoutingConfig{})
	p.SetModelCapabilitiesConfig(config.ModelCapabilitiesConfig{})
	for _, c := range cfgs {
		u, _ := url.Parse(strings.TrimRight(c.URL, "/"))
		placement := normalizePlacementRule(c.ModelPlacement)
		baselineConc := map[string]int{}
		for k, v := range c.ModelConcurrency {
			baselineConc[k] = v
		}
		s := &state{cfg: c, url: u, models: map[string]bool{}, installed: map[string]bool{}, modelActive: map[string]int{}, modelMaintenance: map[string]bool{}, metadata: map[string]metadataEntry{}, performance: map[string]performanceState{}, baselinePlacement: placement, baselineModelConcurrency: baselineConc, placement: placement, maintenance: "active", circuitState: "closed"}
		s.healthy.Store(true)
		p.workers = append(p.workers, s)
		p.byName[c.Name] = s
	}
	return p
}

func (p *Pool) SetRoutingConfig(c config.RoutingConfig) {
	if c.LoadedBonus == 0 {
		c.LoadedBonus = 60
	}
	if c.InstalledBonus == 0 {
		c.InstalledBonus = 30
	}
	if c.ThroughputBonus == 0 {
		c.ThroughputBonus = 20
	}
	if c.VRAMPressurePenalty == 0 {
		c.VRAMPressurePenalty = 35
	}
	if c.GPUUtilizationPenalty == 0 {
		c.GPUUtilizationPenalty = 10
	}
	if c.AvoidVRAMPercent == 0 {
		c.AvoidVRAMPercent = 97
	}
	p.routing = c
}

func (p *Pool) SetModelCapabilitiesConfig(c config.ModelCapabilitiesConfig) {
	if c.Mode == "" {
		c.Mode = "enforce"
	}
	if c.CacheTTL == 0 {
		c.CacheTTL = config.Duration(10 * time.Minute)
	}
	if c.ContextGuard == "" {
		c.ContextGuard = "reject"
	}
	if c.Context.MaxRequestedTokens == 0 {
		c.Context.MaxRequestedTokens = 32768
	}
	if c.Context.DefaultWorkerTokens == 0 {
		c.Context.DefaultWorkerTokens = 4096
	}
	if c.Context.EstimationMarginPercent == 0 {
		c.Context.EstimationMarginPercent = 15
	}
	if c.Context.VisionReserveTokensPerImage == 0 {
		c.Context.VisionReserveTokensPerImage = 2048
	}
	p.capCfg = c
}

func (p *Pool) SetReliabilityConfig(c config.ReliabilityConfig) {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 3
	}
	if c.OpenDuration == 0 {
		c.OpenDuration = config.Duration(30 * time.Second)
	}
	p.reliability = c
}

func (p *Pool) Start(ctx context.Context) {
	for _, w := range p.workers {
		p.refresh(ctx, w)
		go p.healthLoop(ctx, w)
	}
}
func (p *Pool) Health(context.Context) error {
	for _, w := range p.workers {
		if w.healthy.Load() {
			return nil
		}
	}
	return fmt.Errorf("no healthy Ollama workers")
}
func (p *Pool) signal() {
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

func (p *Pool) Acquire(ctx context.Context, model string) (*Lease, error) {
	return p.AcquireExcluding(ctx, model, nil)
}

func (p *Pool) AcquireExcluding(ctx context.Context, model string, excluded map[string]bool) (*Lease, error) {
	return p.AcquireAllowedExcluding(ctx, model, nil, excluded, 0)
}

// AcquireAllowed limits worker selection to an admission-approved set. A nil
// set preserves the legacy behavior. requestedContext is used only to avoid a
// misleading loaded-model bonus when an explicit num_ctx would require Ollama
// to reload the model with a larger KV cache.
func (p *Pool) AcquireAllowed(ctx context.Context, model string, allowed map[string]bool, requestedContext int64) (*Lease, error) {
	return p.AcquireAllowedExcluding(ctx, model, allowed, nil, requestedContext)
}

func (p *Pool) AcquireAllowedExcluding(ctx context.Context, model string, allowed, excluded map[string]bool, requestedContext int64) (*Lease, error) {
	for {
		candidates := p.candidatesExcludingAllowed(model, excluded, allowed, requestedContext)
		for _, w := range candidates {
			for {
				cur := w.active.Load()
				if cur >= int64(w.cfg.MaxConcurrent) {
					break
				}
				if !w.active.CompareAndSwap(cur, cur+1) {
					continue
				}
				if !w.claimCircuitProbe(p.reliability) {
					w.active.Add(-1)
					continue
				}
				if model != "" && !w.acquireModelSlot(model) {
					w.abandonCircuitProbe()
					w.active.Add(-1)
					break
				}
				return &Lease{State: w, model: canonicalModel(model), pool: p}, nil
			}
		}
		if len(candidates) == 0 {
			return nil, p.noCandidateError(model)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.notify:
		case <-time.After(50 * time.Millisecond):
		}
	}
}
func (p *Pool) Control() (*url.URL, string, error) {
	if p.control != "" {
		if w := p.byName[p.control]; w != nil && w.healthy.Load() {
			return w.url, w.cfg.Name, nil
		}
	}
	for _, w := range p.workers {
		if w.healthy.Load() {
			return w.url, w.cfg.Name, nil
		}
	}
	return nil, "", fmt.Errorf("no healthy control worker")
}
func (p *Pool) ControlForModel(model string) (*url.URL, string, error) {
	if model != "" {
		candidates := p.candidates(model)
		if len(candidates) == 0 {
			return nil, "", p.noCandidateError(model)
		}
		return candidates[0].url, candidates[0].cfg.Name, nil
	}
	return p.Control()
}

func (p *Pool) noCandidateError(model string) error {
	healthy, allowed, knownInventories := 0, 0, 0
	for _, w := range p.workers {
		if !w.healthy.Load() {
			continue
		}
		healthy++
		if model != "" && !w.placementDecision(model).Allowed {
			continue
		}
		allowed++
		w.mu.RLock()
		if w.installedKnown {
			knownInventories++
		}
		w.mu.RUnlock()
	}
	if healthy == 0 {
		return fmt.Errorf("no healthy Ollama workers")
	}
	if model != "" && allowed == 0 {
		return fmt.Errorf("%w: model %q is blocked on all healthy workers", ErrModelPlacementBlocked, model)
	}
	if model != "" && allowed > 0 && knownInventories == allowed {
		return fmt.Errorf("%w: model %q is not installed on any eligible worker", ErrModelNotInstalled, model)
	}
	return fmt.Errorf("no eligible Ollama worker for model %q", model)
}

func (p *Pool) Snapshots() []Snapshot {
	out := make([]Snapshot, 0, len(p.workers))
	for _, w := range p.workers {
		w.mu.RLock()
		models := make([]string, 0, len(w.models))
		for m := range w.models {
			models = append(models, m)
		}
		sort.Strings(models)
		loaded := append([]LoadedModel(nil), w.loadedModels...)
		tel := w.telemetry
		var resident, vram int64
		for _, m := range loaded {
			resident += m.Size
			vram += m.SizeVRAM
		}
		memoryTotal := tel.MemoryTotalBytes
		if memoryTotal <= 0 {
			memoryTotal = w.cfg.MemoryCapacityBytes
		}
		vramTotal := tel.VRAMTotalBytes
		if vramTotal <= 0 {
			vramTotal = w.cfg.VRAMCapacityBytes
		}
		activeByModel := make(map[string]int, len(w.modelActive))
		for k, v := range w.modelActive {
			activeByModel[k] = v
		}
		limits := make(map[string]int, len(w.cfg.ModelConcurrency))
		for k, v := range w.cfg.ModelConcurrency {
			limits[k] = v
		}
		perf := performanceSnapshotLocked(w)
		maintenance := w.maintenance
		circuitState := w.circuitState
		if circuitState == "open" && !w.circuitOpenUntil.IsZero() && !time.Now().Before(w.circuitOpenUntil) {
			circuitState = "half_open"
		}
		accepting := maintenance == "active" && circuitState != "open"
		x := Snapshot{Name: w.cfg.Name, URL: w.url.String(), Healthy: w.healthy.Load(), Active: w.active.Load(), MaxConcurrent: w.cfg.MaxConcurrent, Models: models, LoadedModels: loaded, ResidentBytes: resident, VRAMBytes: vram, MemoryCapacityBytes: w.cfg.MemoryCapacityBytes, VRAMCapacityBytes: w.cfg.VRAMCapacityBytes, MemoryUsedBytes: tel.MemoryUsedBytes, MemoryTotalBytes: memoryTotal, VRAMUsedBytes: tel.VRAMUsedBytes, VRAMTotalBytes: vramTotal, GPUUtilizationPct: tel.GPUUtilizationPct, GPUTemperatureC: tel.GPUTemperatureC, GPUPowerWatts: tel.GPUPowerWatts, ModelActive: activeByModel, ModelLimits: limits, Performance: perf, ModelPlacement: clonePlacementRule(w.placement), PlacementOverride: w.placementOverride, Maintenance: maintenance, AcceptingNew: accepting, CircuitState: circuitState, CircuitFailures: w.circuitFailures, CircuitOpenUntil: w.circuitOpenUntil, LastCircuitError: w.lastCircuitError, TelemetrySource: tel.Source, TelemetryError: tel.Error, Labels: w.cfg.Labels, LastError: w.lastError, LastCheck: w.lastCheck}
		w.mu.RUnlock()
		out = append(out, x)
	}
	return out
}
func (p *Pool) candidates(model string) []*state { return p.candidatesExcluding(model, nil) }
func (p *Pool) candidatesExcluding(model string, excluded map[string]bool) []*state {
	return p.candidatesExcludingAllowed(model, excluded, nil, 0)
}
func (p *Pool) candidatesExcludingAllowed(model string, excluded, allowed map[string]bool, requestedContext int64) []*state {
	eligible := make([]*state, 0, len(p.workers))
	maxTPS := 0.0
	for _, w := range p.workers {
		if !w.healthy.Load() || (excluded != nil && excluded[w.cfg.Name]) || (allowed != nil && !allowed[w.cfg.Name]) || !w.acceptingNew(p.reliability) {
			continue
		}
		if model != "" && !w.placementDecision(model).Allowed {
			continue
		}
		if model != "" && w.modelMaintenanceActive(model) {
			continue
		}
		eligible = append(eligible, w)
		if t := w.outputTPS(model); t > maxTPS {
			maxTPS = t
		}
	}
	out := eligible
	if model != "" {
		installed := make([]*state, 0, len(eligible))
		unknown := make([]*state, 0, len(eligible))
		for _, w := range eligible {
			known, has := w.inventoryState(model)
			if !known {
				unknown = append(unknown, w)
				continue
			}
			if has {
				installed = append(installed, w)
			}
		}
		if len(installed) > 0 {
			out = installed
		} else if len(unknown) > 0 {
			out = unknown
		} else {
			out = nil
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return p.scoreForContext(out[i], model, maxTPS, requestedContext) < p.scoreForContext(out[j], model, maxTPS, requestedContext)
	})
	return out
}

func clonePlacementRule(r config.ModelPlacementRule) config.ModelPlacementRule {
	r.AllowedModels = append([]string(nil), r.AllowedModels...)
	r.DeniedModels = append([]string(nil), r.DeniedModels...)
	return r
}

func normalizePlacementRule(r config.ModelPlacementRule) config.ModelPlacementRule {
	if strings.TrimSpace(r.Mode) == "" {
		r.Mode = "allow_all"
	}
	r.Mode = strings.TrimSpace(r.Mode)
	norm := func(in []string) []string {
		out := make([]string, 0, len(in))
		seen := map[string]bool{}
		for _, x := range in {
			x = strings.TrimSpace(x)
			if x == "" || seen[x] {
				continue
			}
			seen[x] = true
			out = append(out, x)
		}
		sort.Strings(out)
		return out
	}
	r.AllowedModels = norm(r.AllowedModels)
	r.DeniedModels = norm(r.DeniedModels)
	return r
}

func modelPatternSpecificity(pattern, model string) (int, bool, bool) {
	pattern = strings.TrimSpace(pattern)
	model = strings.TrimSpace(model)
	plain := canonicalModel(model)
	if pattern == "" {
		return 0, false, false
	}
	if pattern == model || pattern == plain {
		return 100000 + len(pattern), true, true
	}
	if pattern == "*" {
		return 0, true, false
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		if strings.HasPrefix(model, prefix) || strings.HasPrefix(plain, prefix) {
			return len(prefix), true, false
		}
	}
	return 0, false, false
}

func evaluatePlacement(r config.ModelPlacementRule, model string) PlacementDecision {
	r = normalizePlacementRule(r)
	bestAllow, bestDeny := -1, -1
	allowPattern, denyPattern := "", ""
	allowExact, denyExact := false, false
	for _, p := range r.AllowedModels {
		if spec, ok, exact := modelPatternSpecificity(p, model); ok && spec > bestAllow {
			bestAllow, allowPattern, allowExact = spec, p, exact
		}
	}
	for _, p := range r.DeniedModels {
		if spec, ok, exact := modelPatternSpecificity(p, model); ok && spec > bestDeny {
			bestDeny, denyPattern, denyExact = spec, p, exact
		}
	}
	if bestAllow >= 0 || bestDeny >= 0 {
		if bestAllow > bestDeny {
			return PlacementDecision{Allowed: true, Source: "allow_rule", Pattern: allowPattern, ExactOverride: allowExact}
		}
		return PlacementDecision{Allowed: false, Source: "deny_rule", Pattern: denyPattern, ExactOverride: denyExact}
	}
	if r.Mode == "whitelist" {
		return PlacementDecision{Allowed: false, Source: "whitelist_default"}
	}
	return PlacementDecision{Allowed: true, Source: "allow_all_default"}
}

func (w *state) placementDecision(model string) PlacementDecision {
	w.mu.RLock()
	r := clonePlacementRule(w.placement)
	w.mu.RUnlock()
	return evaluatePlacement(r, model)
}

func (w *state) inventoryState(model string) (known, has bool) {
	if model == "" {
		return true, true
	}
	plain := canonicalModel(model)
	w.mu.RLock()
	known = w.installedKnown
	has = w.installed[model] || w.installed[plain] || w.models[model] || w.models[plain]
	w.mu.RUnlock()
	return known, has
}

func (p *Pool) PlacementDecision(workerName, model string) (PlacementDecision, bool) {
	w := p.byName[workerName]
	if w == nil {
		return PlacementDecision{}, false
	}
	return w.placementDecision(model), true
}

func (p *Pool) PlacementSnapshots() []PlacementSnapshot {
	out := make([]PlacementSnapshot, 0, len(p.workers))
	for _, w := range p.workers {
		w.mu.RLock()
		loaded := make([]string, 0, len(w.loadedModels))
		seenLoaded := map[string]bool{}
		for _, m := range w.loadedModels {
			name := strings.TrimSpace(m.Model)
			if name == "" {
				name = strings.TrimSpace(m.Name)
			}
			if name != "" && !seenLoaded[name] {
				seenLoaded[name] = true
				loaded = append(loaded, name)
			}
		}
		sort.Strings(loaded)
		out = append(out, PlacementSnapshot{Worker: w.cfg.Name, Baseline: clonePlacementRule(w.baselinePlacement), Effective: clonePlacementRule(w.placement), Override: w.placementOverride, InventoryKnown: w.installedKnown, InstalledModels: append([]string(nil), w.installedModels...), LoadedModels: loaded, InventoryError: w.inventoryError})
		w.mu.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Worker < out[j].Worker })
	return out
}

func (p *Pool) SetPlacement(workerName string, rule config.ModelPlacementRule, override bool) error {
	if err := config.ValidateModelPlacementRule(rule); err != nil {
		return err
	}
	w := p.byName[workerName]
	if w == nil {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	rule = normalizePlacementRule(rule)
	w.mu.Lock()
	w.placement = rule
	w.placementOverride = override
	w.mu.Unlock()
	p.signal()
	return nil
}

func (p *Pool) ResetPlacement(workerName string) error {
	w := p.byName[workerName]
	if w == nil {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	w.mu.Lock()
	w.placement = clonePlacementRule(w.baselinePlacement)
	w.placementOverride = false
	w.mu.Unlock()
	p.signal()
	return nil
}

func normalizeMaintenance(mode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "active"
	}
	switch mode {
	case "active", "draining", "disabled":
		return mode, nil
	}
	return "", fmt.Errorf("maintenance mode must be active, draining, or disabled")
}

func (p *Pool) SetMaintenance(workerName, mode string) error {
	w := p.byName[workerName]
	if w == nil {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	m, err := normalizeMaintenance(mode)
	if err != nil {
		return err
	}
	w.mu.Lock()
	w.maintenance = m
	w.mu.Unlock()
	p.signal()
	return nil
}

func (p *Pool) Maintenance(workerName string) (string, bool) {
	w := p.byName[workerName]
	if w == nil {
		return "", false
	}
	w.mu.RLock()
	m := w.maintenance
	w.mu.RUnlock()
	return m, true
}

func (w *state) acceptingNew(rel config.ReliabilityConfig) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.maintenance != "active" {
		return false
	}
	if !rel.Enabled {
		return true
	}
	if w.circuitState == "open" && time.Now().Before(w.circuitOpenUntil) {
		return false
	}
	if w.circuitState == "half_open" && w.halfOpenInFlight {
		return false
	}
	return true
}

func (w *state) claimCircuitProbe(rel config.ReliabilityConfig) bool {
	if !rel.Enabled {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.maintenance != "active" {
		return false
	}
	now := time.Now()
	if w.circuitState == "open" {
		if now.Before(w.circuitOpenUntil) {
			return false
		}
		if w.halfOpenInFlight {
			return false
		}
		w.circuitState = "half_open"
		w.halfOpenInFlight = true
		return true
	}
	if w.circuitState == "half_open" {
		if w.halfOpenInFlight {
			return false
		}
		w.halfOpenInFlight = true
		return true
	}
	return true
}

func (w *state) abandonCircuitProbe() {
	w.mu.Lock()
	if w.circuitState == "half_open" && w.halfOpenInFlight {
		w.halfOpenInFlight = false
	}
	w.mu.Unlock()
}

func (p *Pool) ReportResult(workerName string, failed bool, errText string) bool {
	w := p.byName[workerName]
	if w == nil || !p.reliability.Enabled {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !failed {
		w.circuitFailures = 0
		w.circuitState = "closed"
		w.circuitOpenUntil = time.Time{}
		w.halfOpenInFlight = false
		w.lastCircuitError = ""
		return false
	}
	wasOpen := w.circuitState == "open"
	w.circuitFailures++
	w.lastCircuitError = errText
	if w.circuitState == "half_open" || w.circuitFailures >= p.reliability.FailureThreshold {
		w.circuitState = "open"
		w.circuitOpenUntil = time.Now().Add(p.reliability.OpenDuration.Value())
	}
	w.halfOpenInFlight = false
	p.signal()
	return !wasOpen && w.circuitState == "open"
}

func (p *Pool) CircuitReset(workerName string) error {
	w := p.byName[workerName]
	if w == nil {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	w.mu.Lock()
	w.circuitState = "closed"
	w.circuitFailures = 0
	w.circuitOpenUntil = time.Time{}
	w.halfOpenInFlight = false
	w.lastCircuitError = ""
	w.mu.Unlock()
	p.signal()
	return nil
}

func (p *Pool) CanRoute(model string) bool { return len(p.candidates(model)) > 0 }

func (p *Pool) SetModelConcurrency(workerName, model string, limit int) error {
	w := p.byName[workerName]
	if w == nil {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	model = strings.TrimSpace(model)
	if model == "" || limit <= 0 {
		return fmt.Errorf("model and positive limit are required")
	}
	w.mu.Lock()
	if w.cfg.ModelConcurrency == nil {
		w.cfg.ModelConcurrency = map[string]int{}
	}
	w.cfg.ModelConcurrency[model] = minInt(limit, w.cfg.MaxConcurrent)
	w.mu.Unlock()
	p.signal()
	return nil
}

func (p *Pool) ResetModelConcurrency(workerName, model string) error {
	w := p.byName[workerName]
	if w == nil {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return fmt.Errorf("model required")
	}
	w.mu.Lock()
	if v, ok := w.baselineModelConcurrency[model]; ok {
		if w.cfg.ModelConcurrency == nil {
			w.cfg.ModelConcurrency = map[string]int{}
		}
		w.cfg.ModelConcurrency[model] = v
	} else {
		delete(w.cfg.ModelConcurrency, model)
	}
	w.mu.Unlock()
	p.signal()
	return nil
}

func (p *Pool) WorkerConfig(workerName string) (config.WorkerConfig, bool) {
	w := p.byName[workerName]
	if w == nil {
		return config.WorkerConfig{}, false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	c := w.cfg
	c.ModelConcurrency = map[string]int{}
	for k, v := range w.cfg.ModelConcurrency {
		c.ModelConcurrency[k] = v
	}
	c.ContextLimits = map[string]int64{}
	for k, v := range w.cfg.ContextLimits {
		c.ContextLimits[k] = v
	}
	return c, true
}

func canonicalModel(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	return strings.TrimSuffix(model, ":latest")
}

func (w *state) hasModel(model string) bool {
	if model == "" {
		return true
	}
	plain := canonicalModel(model)
	w.mu.RLock()
	ok := w.installed[model] || w.installed[plain] || w.models[model] || w.models[plain]
	w.mu.RUnlock()
	return ok
}

func (w *state) loadedContext(model string) int64 {
	plain := canonicalModel(model)
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, m := range w.loadedModels {
		id := strings.TrimSpace(m.Model)
		if id == "" {
			id = strings.TrimSpace(m.Name)
		}
		if id == model || canonicalModel(id) == plain {
			return m.ContextLength
		}
	}
	return 0
}

func (w *state) contextLimit(model string) int64 {
	w.mu.RLock()
	limits := make(map[string]int64, len(w.cfg.ContextLimits))
	for pattern, limit := range w.cfg.ContextLimits {
		limits[pattern] = limit
	}
	w.mu.RUnlock()
	bestSpec := -1
	var best int64
	for pattern, limit := range limits {
		if spec, ok, _ := modelPatternSpecificity(pattern, model); ok && spec > bestSpec {
			bestSpec, best = spec, limit
		}
	}
	return best
}

func minPositive(values ...int64) int64 {
	var out int64
	for _, v := range values {
		if v <= 0 {
			continue
		}
		if out == 0 || v < out {
			out = v
		}
	}
	return out
}

func (p *Pool) contextWindowForWorker(ctx context.Context, w *state, model string) ContextWindow {
	workerDefault := w.cfg.DefaultContextTokens
	if workerDefault == 0 {
		workerDefault = p.capCfg.Context.DefaultWorkerTokens
	}
	x := ContextWindow{Worker: w.cfg.Name, Model: model, WorkerDefaultTokens: workerDefault, WorkerLimitTokens: w.contextLimit(model)}
	meta, err := p.metadataForWorker(ctx, w, model)
	if err != nil {
		x.MetadataError = err.Error()
	} else {
		x.ModelMaxTokens = meta.ContextLength
		x.ConfiguredTokens = meta.ConfiguredContextLength
	}
	x.LoadedTokens = w.loadedContext(model)
	// The loaded context is the strongest evidence because it is what Ollama is
	// actually using right now. Otherwise prefer a Modelfile num_ctx, then the
	// operator/default worker context. -1 means explicitly fall back to the
	// theoretical model maximum.
	switch {
	case x.LoadedTokens > 0:
		x.EffectiveTokens, x.EffectiveSource = x.LoadedTokens, "loaded"
	case x.ConfiguredTokens > 0:
		x.EffectiveTokens, x.EffectiveSource = x.ConfiguredTokens, "modelfile"
	case x.WorkerDefaultTokens > 0:
		x.EffectiveTokens, x.EffectiveSource = x.WorkerDefaultTokens, "worker_default"
	case x.WorkerDefaultTokens == -1 && x.ModelMaxTokens > 0:
		x.EffectiveTokens, x.EffectiveSource = x.ModelMaxTokens, "model_max"
	}
	x.EffectiveTokens = minPositive(x.EffectiveTokens, x.ModelMaxTokens, x.WorkerLimitTokens)
	if x.EffectiveTokens == 0 && x.EffectiveSource != "" {
		x.EffectiveSource = ""
	}
	return x
}

// ContextWindows returns the effective context evidence for every currently
// routable worker that owns the model. It is intentionally independent of
// concurrency so admission can reject an impossible context before waiting in
// the fair scheduler queue.
func (p *Pool) ContextWindows(ctx context.Context, model string) []ContextWindow {
	candidates := p.candidates(model)
	out := make([]ContextWindow, 0, len(candidates))
	for _, w := range candidates {
		out = append(out, p.contextWindowForWorker(ctx, w, model))
	}
	return out
}

func (w *state) modelLimit(model string) int {
	raw := strings.TrimSpace(model)
	plain := canonicalModel(raw)
	limit := w.cfg.MaxConcurrent
	best := ""
	for pattern, n := range w.cfg.ModelConcurrency {
		if pattern == raw || pattern == plain {
			return minInt(n, w.cfg.MaxConcurrent)
		}
		if pattern == "*" && best == "" {
			best = pattern
			limit = n
			continue
		}
		if strings.HasSuffix(pattern, "*") {
			prefix := strings.TrimSuffix(pattern, "*")
			if (strings.HasPrefix(raw, prefix) || strings.HasPrefix(plain, prefix)) && len(pattern) > len(best) {
				best = pattern
				limit = n
			}
		}
	}
	if limit <= 0 {
		limit = w.cfg.MaxConcurrent
	}
	return minInt(limit, w.cfg.MaxConcurrent)
}

func (w *state) acquireModelSlot(model string) bool {
	key := canonicalModel(model)
	limit := w.modelLimit(model)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.modelMaintenance[key] || w.modelActive[key] >= limit {
		return false
	}
	w.modelActive[key]++
	return true
}

func (w *state) modelMaintenanceActive(model string) bool {
	key := canonicalModel(model)
	w.mu.RLock()
	busy := w.modelMaintenance[key]
	w.mu.RUnlock()
	return busy
}

// BeginModelMaintenance reserves a worker/model pair for a short control-plane
// operation such as preload or unload. New inference leases for the same model
// are excluded until the returned release function is called. This closes the
// race where an idle-unload could otherwise start exactly as inference arrives.
func (p *Pool) BeginModelMaintenance(workerName, model string) (func(), error) {
	w := p.byName[workerName]
	if w == nil {
		return nil, fmt.Errorf("unknown worker %q", workerName)
	}
	key := canonicalModel(model)
	if key == "" {
		return nil, errors.New("model required")
	}
	if !w.healthy.Load() {
		return nil, fmt.Errorf("worker %s is unhealthy", workerName)
	}
	w.mu.Lock()
	if w.maintenance != "active" {
		w.mu.Unlock()
		return nil, fmt.Errorf("worker %s is %s", workerName, w.maintenance)
	}
	if w.circuitState != "" && w.circuitState != "closed" {
		w.mu.Unlock()
		return nil, fmt.Errorf("worker %s circuit is %s", workerName, w.circuitState)
	}
	if w.modelMaintenance[key] {
		w.mu.Unlock()
		return nil, fmt.Errorf("model %s already has a maintenance operation on worker %s", model, workerName)
	}
	if w.modelActive[key] > 0 {
		w.mu.Unlock()
		return nil, fmt.Errorf("model %s is active on worker %s", model, workerName)
	}
	w.modelMaintenance[key] = true
	w.mu.Unlock()
	p.signal()
	var once sync.Once
	return func() {
		once.Do(func() {
			w.mu.Lock()
			delete(w.modelMaintenance, key)
			w.mu.Unlock()
			p.signal()
		})
	}, nil
}

func (w *state) outputTPS(model string) float64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if p, ok := w.performance[canonicalModel(model)]; ok {
		return p.OutputTPS
	}
	return 0
}

func (p *Pool) score(w *state, model string, maxTPS float64) float64 {
	return p.scoreForContext(w, model, maxTPS, 0)
}

func (p *Pool) scoreForContext(w *state, model string, maxTPS float64, requestedContext int64) float64 {
	active := float64(w.active.Load()) / float64(max(1, w.cfg.MaxConcurrent))
	s := active * 100
	w.mu.RLock()
	plain := canonicalModel(model)
	loaded := w.models[model] || w.models[plain]
	installed := w.installed[model] || w.installed[plain]
	loadedContext := int64(0)
	if requestedContext > 0 && loaded {
		for _, m := range w.loadedModels {
			id := strings.TrimSpace(m.Model)
			if id == "" {
				id = strings.TrimSpace(m.Name)
			}
			if id == model || canonicalModel(id) == plain {
				loadedContext = m.ContextLength
				break
			}
		}
	}
	tel := w.telemetry
	modelActive := w.modelActive[plain]
	perf := w.performance[plain]
	w.mu.RUnlock()
	if requestedContext > 0 && loadedContext > 0 && loadedContext < requestedContext {
		// An explicit larger num_ctx can still be routed here, but Ollama must
		// reload/resize the model, so do not pretend the current loaded state is
		// a warm-context advantage.
		loaded = false
		installed = true
	}
	if loaded {
		s -= p.routing.LoadedBonus
	} else if installed {
		s -= p.routing.InstalledBonus
	}
	if lim := w.modelLimit(model); lim > 0 {
		s += 25 * float64(modelActive) / float64(lim)
	}
	if maxTPS > 0 && perf.OutputTPS > 0 {
		s -= p.routing.ThroughputBonus * math.Min(1, perf.OutputTPS/maxTPS)
	}
	vramTotal := tel.VRAMTotalBytes
	if vramTotal <= 0 {
		vramTotal = w.cfg.VRAMCapacityBytes
	}
	if vramTotal > 0 && tel.VRAMUsedBytes > 0 {
		pct := 100 * float64(tel.VRAMUsedBytes) / float64(vramTotal)
		s += p.routing.VRAMPressurePenalty * math.Min(1, pct/100)
		if !loaded && p.routing.AvoidVRAMPercent > 0 && pct >= p.routing.AvoidVRAMPercent {
			s += 500
		}
	}
	if tel.GPUUtilizationPct > 0 {
		s += p.routing.GPUUtilizationPenalty * math.Min(1, tel.GPUUtilizationPct/100)
	}
	return s
}

// RoutingExplanation describes why a worker is or is not eligible for a model.
// It is intended for the admin policy simulator and never acquires a worker slot.
type RoutingExplanation struct {
	Worker                string             `json:"worker"`
	Eligible              bool               `json:"eligible"`
	Reason                string             `json:"reason,omitempty"`
	Healthy               bool               `json:"healthy"`
	Maintenance           string             `json:"maintenance"`
	CircuitState          string             `json:"circuit_state"`
	Placement             PlacementDecision  `json:"placement"`
	InventoryKnown        bool               `json:"inventory_known"`
	Installed             bool               `json:"installed"`
	Loaded                bool               `json:"loaded"`
	Active                int64              `json:"active"`
	MaxConcurrent         int                `json:"max_concurrent"`
	ModelActive           int                `json:"model_active"`
	ModelLimit            int                `json:"model_limit"`
	OutputTPS             float64            `json:"output_tps,omitempty"`
	VRAMPercent           float64            `json:"vram_percent,omitempty"`
	GPUUtilizationPercent float64            `json:"gpu_utilization_percent,omitempty"`
	Score                 float64            `json:"score,omitempty"`
	ScoreComponents       map[string]float64 `json:"score_components,omitempty"`
}

// ExplainRouting snapshots the exact eligibility gates and adaptive score used
// for a model without reserving scheduler or worker capacity.
func (p *Pool) ExplainRouting(model string) []RoutingExplanation {
	return p.ExplainRoutingAllowed(model, nil, 0)
}

// ExplainRoutingAllowed additionally applies an admission-approved context
// worker set so the policy simulator can mirror real context-aware routing.
func (p *Pool) ExplainRoutingAllowed(model string, allowed map[string]bool, requestedContext int64) []RoutingExplanation {
	plain := canonicalModel(model)
	maxTPS := 0.0
	for _, w := range p.workers {
		if t := w.outputTPS(model); t > maxTPS {
			maxTPS = t
		}
	}
	out := make([]RoutingExplanation, 0, len(p.workers))
	for _, w := range p.workers {
		w.mu.RLock()
		tel := w.telemetry
		loaded := w.models[model] || w.models[plain]
		installed := w.installed[model] || w.installed[plain] || loaded
		loadedContext := int64(0)
		if requestedContext > 0 && loaded {
			for _, lm := range w.loadedModels {
				id := strings.TrimSpace(lm.Model)
				if id == "" {
					id = strings.TrimSpace(lm.Name)
				}
				if id == model || canonicalModel(id) == plain {
					loadedContext = lm.ContextLength
					break
				}
			}
		}
		known := w.installedKnown
		modelActive := w.modelActive[plain]
		perf := w.performance[plain]
		maintenance := w.maintenance
		circuit := w.circuitState
		openUntil := w.circuitOpenUntil
		w.mu.RUnlock()
		if requestedContext > 0 && loadedContext > 0 && loadedContext < requestedContext {
			loaded = false
			installed = true
		}
		if circuit == "open" && !openUntil.IsZero() && !time.Now().Before(openUntil) {
			circuit = "half_open"
		}
		pl := w.placementDecision(model)
		e := RoutingExplanation{Worker: w.cfg.Name, Healthy: w.healthy.Load(), Maintenance: maintenance, CircuitState: circuit, Placement: pl, InventoryKnown: known, Installed: installed, Loaded: loaded, Active: w.active.Load(), MaxConcurrent: w.cfg.MaxConcurrent, ModelActive: modelActive, ModelLimit: w.modelLimit(model), OutputTPS: perf.OutputTPS, GPUUtilizationPercent: tel.GPUUtilizationPct}
		total := tel.VRAMTotalBytes
		if total <= 0 {
			total = w.cfg.VRAMCapacityBytes
		}
		if total > 0 && tel.VRAMUsedBytes > 0 {
			e.VRAMPercent = 100 * float64(tel.VRAMUsedBytes) / float64(total)
		}
		switch {
		case allowed != nil && !allowed[w.cfg.Name]:
			e.Reason = "context_window"
		case !e.Healthy:
			e.Reason = "worker_unhealthy"
		case maintenance != "active":
			e.Reason = "worker_" + maintenance
		case circuit == "open":
			e.Reason = "circuit_open"
		case !pl.Allowed:
			e.Reason = "placement_denied"
		case known && !installed:
			e.Reason = "model_not_installed"
		case e.Active >= int64(max(1, w.cfg.MaxConcurrent)):
			e.Reason = "worker_concurrency_full"
		case e.ModelActive >= e.ModelLimit:
			e.Reason = "model_concurrency_full"
		default:
			e.Eligible = true
		}
		if e.Eligible {
			components := map[string]float64{}
			components["worker_load"] = float64(e.Active) / float64(max(1, w.cfg.MaxConcurrent)) * 100
			if loaded {
				components["loaded_bonus"] = -p.routing.LoadedBonus
			} else if installed {
				components["installed_bonus"] = -p.routing.InstalledBonus
			}
			if e.ModelLimit > 0 {
				components["model_load"] = 25 * float64(e.ModelActive) / float64(e.ModelLimit)
			}
			if maxTPS > 0 && perf.OutputTPS > 0 {
				components["throughput_bonus"] = -p.routing.ThroughputBonus * math.Min(1, perf.OutputTPS/maxTPS)
			}
			if e.VRAMPercent > 0 {
				components["vram_pressure"] = p.routing.VRAMPressurePenalty * math.Min(1, e.VRAMPercent/100)
				if !loaded && p.routing.AvoidVRAMPercent > 0 && e.VRAMPercent >= p.routing.AvoidVRAMPercent {
					components["vram_avoid"] = 500
				}
			}
			if tel.GPUUtilizationPct > 0 {
				components["gpu_pressure"] = p.routing.GPUUtilizationPenalty * math.Min(1, tel.GPUUtilizationPct/100)
			}
			for _, v := range components {
				e.Score += v
			}
			e.ScoreComponents = components
		}
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Eligible != out[j].Eligible {
			return out[i].Eligible
		}
		if out[i].Eligible && out[i].Score != out[j].Score {
			return out[i].Score < out[j].Score
		}
		return out[i].Worker < out[j].Worker
	})
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (p *Pool) healthLoop(ctx context.Context, w *state) {
	t := time.NewTicker(w.cfg.HealthInterval.Value())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.refresh(ctx, w)
		}
	}
}
func (p *Pool) refresh(ctx context.Context, w *state) {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, w.url.String()+"/api/ps", nil)
	resp, err := p.client.Do(req)
	if err != nil {
		p.setHealth(w, false, err.Error(), nil, nil)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		p.setHealth(w, false, fmt.Sprintf("HTTP %d", resp.StatusCode), nil, nil)
		return
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		p.setHealth(w, false, err.Error(), nil, nil)
		return
	}
	var doc struct {
		Models []LoadedModel `json:"models"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		p.setHealth(w, false, err.Error(), nil, nil)
		return
	}
	models := map[string]bool{}
	for _, m := range doc.Models {
		if m.Name != "" {
			models[m.Name] = true
			models[strings.TrimSuffix(m.Name, ":latest")] = true
		}
		if m.Model != "" {
			models[m.Model] = true
			models[strings.TrimSuffix(m.Model, ":latest")] = true
		}
	}
	p.setHealth(w, true, "", models, doc.Models)
	p.refreshInstalled(ctx, w)
	p.refreshTelemetry(ctx, w)
}
func (p *Pool) refreshInstalled(ctx context.Context, w *state) {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodGet, w.url.String()+"/api/tags", nil)
	resp, err := p.client.Do(req)
	if err != nil {
		w.mu.Lock()
		w.inventoryError = err.Error()
		w.mu.Unlock()
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		w.mu.Lock()
		w.inventoryError = fmt.Sprintf("HTTP %d", resp.StatusCode)
		w.mu.Unlock()
		return
	}
	var doc struct {
		Models []ModelInfo `json:"models"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&doc); err != nil {
		w.mu.Lock()
		w.inventoryError = err.Error()
		w.mu.Unlock()
		return
	}
	installed := make(map[string]bool, len(doc.Models)*2)
	display := make([]string, 0, len(doc.Models))
	seenDisplay := map[string]bool{}
	for _, m := range doc.Models {
		preferred := strings.TrimSpace(m.Model)
		if preferred == "" {
			preferred = strings.TrimSpace(m.Name)
		}
		if preferred != "" && !seenDisplay[preferred] {
			seenDisplay[preferred] = true
			display = append(display, preferred)
		}
		for _, id := range []string{m.Model, m.Name} {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			installed[id] = true
			installed[strings.TrimSuffix(id, ":latest")] = true
		}
	}
	sort.Strings(display)
	w.mu.Lock()
	w.installed = installed
	w.installedKnown = true
	w.installedModels = display
	w.inventoryError = ""
	w.mu.Unlock()
}

func (p *Pool) refreshTelemetry(ctx context.Context, w *state) {
	if !w.cfg.LocalSystemStats && !w.cfg.NVIDIASMI && w.cfg.TelemetryURL == "" {
		return
	}
	t := ResourceTelemetry{UpdatedAt: time.Now().UTC()}
	if w.cfg.LocalSystemStats {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		m, err := hoststats.ReadMemory(cctx)
		cancel()
		if err != nil {
			t.Error = err.Error()
		} else {
			t.MemoryTotalBytes = m.TotalBytes
			t.MemoryUsedBytes = m.UsedBytes
			t.Source = "local-system"
		}
	}
	if w.cfg.NVIDIASMI {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		n, err := hoststats.ReadNVIDIA(cctx, w.cfg.NVIDIAGPU)
		cancel()
		if err != nil {
			if t.Error == "" {
				t.Error = err.Error()
			} else {
				t.Error += "; " + err.Error()
			}
		} else {
			t.VRAMUsedBytes = n.MemoryUsedBytes
			t.VRAMTotalBytes = n.MemoryTotalBytes
			t.GPUUtilizationPct = n.UtilizationPercent
			t.GPUTemperatureC = n.TemperatureC
			t.GPUPowerWatts = n.PowerWatts
			if t.Source == "" {
				t.Source = "nvidia-smi"
			} else {
				t.Source += "+nvidia-smi"
			}
		}
	}
	if w.cfg.TelemetryURL != "" {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		req, _ := http.NewRequestWithContext(cctx, http.MethodGet, w.cfg.TelemetryURL, nil)
		resp, err := p.client.Do(req)
		if err != nil {
			appendTelemetryError(&t, err.Error())
		} else {
			func() {
				defer resp.Body.Close()
				if resp.StatusCode/100 != 2 {
					appendTelemetryError(&t, fmt.Sprintf("telemetry HTTP %d", resp.StatusCode))
					return
				}
				var ext externalTelemetry
				if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&ext); err != nil {
					appendTelemetryError(&t, err.Error())
					return
				}
				if err := validateExternalTelemetryTimestamp(time.Now().UTC(), ext.UpdatedAt, telemetryMaxAge(w.cfg.HealthInterval.Value())); err != nil {
					appendTelemetryError(&t, err.Error())
					return
				}
				mergeExternalTelemetry(&t, ext)
			}()
		}
		cancel()
	}
	w.mu.Lock()
	w.telemetry = t
	w.mu.Unlock()
}

func telemetryMaxAge(healthInterval time.Duration) time.Duration {
	maxAge := 6 * healthInterval
	if maxAge < 30*time.Second {
		maxAge = 30 * time.Second
	}
	return maxAge
}

func validateExternalTelemetryTimestamp(now time.Time, updatedAt *time.Time, maxAge time.Duration) error {
	// updated_at was optional before checkpoint 21. Preserve compatibility with
	// existing exporters, while the shipped agent always supplies it.
	if updatedAt == nil {
		return nil
	}
	if maxAge <= 0 {
		maxAge = 30 * time.Second
	}
	age := now.Sub(updatedAt.UTC())
	if age < -30*time.Second {
		return fmt.Errorf("telemetry timestamp is %.0fs in the future", -age.Seconds())
	}
	if age > maxAge {
		return fmt.Errorf("telemetry sample is stale: age %s exceeds %s", age.Round(time.Second), maxAge)
	}
	return nil
}

func mergeExternalTelemetry(t *ResourceTelemetry, ext externalTelemetry) {
	if ext.MemoryTotalBytes != nil {
		t.MemoryTotalBytes = *ext.MemoryTotalBytes
	}
	if ext.MemoryUsedBytes != nil {
		t.MemoryUsedBytes = *ext.MemoryUsedBytes
	}
	if ext.VRAMTotalBytes != nil {
		t.VRAMTotalBytes = *ext.VRAMTotalBytes
	}
	if ext.VRAMUsedBytes != nil {
		t.VRAMUsedBytes = *ext.VRAMUsedBytes
	}
	if ext.GPUUtilizationPct != nil {
		t.GPUUtilizationPct = *ext.GPUUtilizationPct
	}
	if ext.GPUTemperatureC != nil {
		t.GPUTemperatureC = *ext.GPUTemperatureC
	}
	if ext.GPUPowerWatts != nil {
		t.GPUPowerWatts = *ext.GPUPowerWatts
	}
	source := "telemetry-url"
	if extSource := strings.TrimSpace(ext.Source); extSource != "" {
		source += ":" + extSource
	}
	if t.Source == "" {
		t.Source = source
	} else {
		t.Source += "+" + source
	}
	if strings.TrimSpace(ext.Error) != "" {
		appendTelemetryError(t, ext.Error)
	}
}

func appendTelemetryError(t *ResourceTelemetry, message string) {
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	if t.Error == "" {
		t.Error = message
	} else {
		t.Error += "; " + message
	}
}

// Observe updates an in-memory EWMA of model throughput for adaptive routing.
// Exact Ollama eval durations are preferred; wall time is only used when an
// OpenAI-compatible response did not expose token evaluation timing.
func (p *Pool) Observe(workerName, model string, promptTokens, completionTokens, promptEvalNS, evalNS int64, service time.Duration) {
	w := p.byName[workerName]
	model = canonicalModel(model)
	if w == nil || model == "" {
		return
	}
	promptTPS, outputTPS := 0.0, 0.0
	if promptTokens > 0 && promptEvalNS > 0 {
		promptTPS = float64(promptTokens) / (float64(promptEvalNS) / 1e9)
	}
	if completionTokens > 0 {
		den := time.Duration(evalNS)
		if den <= 0 {
			den = service
		}
		if den > 0 {
			outputTPS = float64(completionTokens) / den.Seconds()
		}
	}
	if promptTPS <= 0 && outputTPS <= 0 {
		return
	}
	w.mu.Lock()
	ps := w.performance[model]
	const alpha = 0.25
	if ps.Samples == 0 {
		ps.PromptTPS, ps.OutputTPS = promptTPS, outputTPS
	} else {
		if promptTPS > 0 {
			ps.PromptTPS = alpha*promptTPS + (1-alpha)*ps.PromptTPS
		}
		if outputTPS > 0 {
			ps.OutputTPS = alpha*outputTPS + (1-alpha)*ps.OutputTPS
		}
	}
	ps.Samples++
	w.performance[model] = ps
	w.mu.Unlock()
}

func performanceSnapshotLocked(w *state) []ModelPerformance {
	out := make([]ModelPerformance, 0, len(w.performance))
	for model, p := range w.performance {
		out = append(out, ModelPerformance{Model: model, PromptTPS: p.PromptTPS, OutputTPS: p.OutputTPS, Samples: p.Samples})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

func (p *Pool) setHealth(w *state, ok bool, errText string, models map[string]bool, loaded []LoadedModel) {
	was := w.healthy.Swap(ok)
	w.mu.Lock()
	w.lastCheck = time.Now()
	w.lastError = errText
	if models != nil {
		w.models = models
		w.loadedModels = append([]LoadedModel(nil), loaded...)
	}
	w.mu.Unlock()
	if was != ok {
		p.signal()
	}
}
func newID() string { b := make([]byte, 12); _, _ = rand.Read(b); return hex.EncodeToString(b) }

type ModelDetails struct {
	ParentModel       string   `json:"parent_model,omitempty"`
	Format            string   `json:"format,omitempty"`
	Family            string   `json:"family,omitempty"`
	Families          []string `json:"families,omitempty"`
	ParameterSize     string   `json:"parameter_size,omitempty"`
	QuantizationLevel string   `json:"quantization_level,omitempty"`
}

type ModelInfo struct {
	Name                    string       `json:"name"`
	Model                   string       `json:"model,omitempty"`
	ModifiedAt              time.Time    `json:"modified_at,omitempty"`
	Size                    int64        `json:"size,omitempty"`
	Digest                  string       `json:"digest,omitempty"`
	Details                 ModelDetails `json:"details"`
	Loaded                  bool         `json:"loaded"`
	Capabilities            []string     `json:"capabilities,omitempty"`
	ContextLength           int64        `json:"context_length,omitempty"`
	ConfiguredContextLength int64        `json:"configured_context_length,omitempty"`
	LoadedContextLength     int64        `json:"loaded_context_length,omitempty"`
	MetadataError           string       `json:"metadata_error,omitempty"`
}

// Metadata returns cached /api/show metadata for a model from a healthy
// worker that has the model installed. The second return value is the worker
// used for discovery.
func (p *Pool) Metadata(ctx context.Context, model string) (ModelMetadata, string, error) {
	if p.capCfg.Mode == "off" || strings.TrimSpace(model) == "" {
		return ModelMetadata{Model: model}, "", nil
	}
	for _, w := range p.candidates(model) {
		if !w.hasModel(model) {
			continue
		}
		m, err := p.metadataForWorker(ctx, w, model)
		return m, w.cfg.Name, err
	}
	return ModelMetadata{Model: model}, "", fmt.Errorf("model %q is not installed on a healthy worker", model)
}

func (p *Pool) metadataForWorker(ctx context.Context, w *state, model string) (ModelMetadata, error) {
	key := canonicalModel(model)
	w.mu.RLock()
	entry, ok := w.metadata[key]
	w.mu.RUnlock()
	if ok {
		ttl := p.capCfg.CacheTTL.Value()
		if entry.Data.Error != "" && ttl > 15*time.Second {
			ttl = 15 * time.Second
		}
		if ttl > 0 && time.Since(entry.FetchedAt) < ttl {
			if entry.Data.Error != "" {
				return entry.Data, errors.New(entry.Data.Error)
			}
			return entry.Data, nil
		}
	}
	cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	body, _ := json.Marshal(map[string]any{"model": model, "verbose": false})
	req, _ := http.NewRequestWithContext(cctx, http.MethodPost, w.url.String()+"/api/show", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		m := ModelMetadata{Model: model, Error: err.Error(), UpdatedAt: time.Now().UTC()}
		w.mu.Lock()
		w.metadata[key] = metadataEntry{Data: m, FetchedAt: time.Now()}
		w.mu.Unlock()
		return m, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<10))
		err := fmt.Errorf("/api/show HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		m := ModelMetadata{Model: model, Error: err.Error(), UpdatedAt: time.Now().UTC()}
		w.mu.Lock()
		w.metadata[key] = metadataEntry{Data: m, FetchedAt: time.Now()}
		w.mu.Unlock()
		return m, err
	}
	var doc struct {
		Capabilities []string        `json:"capabilities"`
		Details      ModelDetails    `json:"details"`
		ModelInfo    map[string]any  `json:"model_info"`
		Parameters   json.RawMessage `json:"parameters"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&doc); err != nil {
		return ModelMetadata{Model: model}, err
	}
	caps := make([]string, 0, len(doc.Capabilities))
	seen := map[string]bool{}
	for _, c := range doc.Capabilities {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" && !seen[c] {
			seen[c] = true
			caps = append(caps, c)
		}
	}
	sort.Strings(caps)
	var contextLength int64
	for k, v := range doc.ModelInfo {
		if k != "context_length" && !strings.HasSuffix(k, ".context_length") {
			continue
		}
		var n int64
		switch x := v.(type) {
		case float64:
			n = int64(x)
		case int64:
			n = x
		case json.Number:
			n, _ = x.Int64()
		}
		if n > contextLength {
			contextLength = n
		}
	}
	m := ModelMetadata{Model: model, Capabilities: caps, ContextLength: contextLength, ConfiguredContextLength: parseNumCtxParameter(doc.Parameters), Details: doc.Details, UpdatedAt: time.Now().UTC()}
	w.mu.Lock()
	w.metadata[key] = metadataEntry{Data: m, FetchedAt: time.Now()}
	w.mu.Unlock()
	return m, nil
}

func parseNumCtxParameter(raw json.RawMessage) int64 {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return 0
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		for _, line := range strings.Split(text, "\n") {
			fields := strings.Fields(strings.TrimSpace(line))
			if len(fields) < 2 || !strings.EqualFold(fields[0], "num_ctx") {
				continue
			}
			n, err := strconv.ParseInt(strings.Trim(fields[1], "\"'"), 10, 64)
			if err == nil && n > 0 {
				return n
			}
		}
		return 0
	}
	var obj map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&obj) != nil {
		return 0
	}
	v, ok := obj["num_ctx"]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case json.Number:
		n, _ := x.Int64()
		if n > 0 {
			return n
		}
	case float64:
		if x > 0 {
			return int64(x)
		}
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if n > 0 {
			return n
		}
	}
	return 0
}

func HasCapability(m ModelMetadata, capability string) bool {
	capability = strings.ToLower(strings.TrimSpace(capability))
	for _, c := range m.Capabilities {
		if strings.EqualFold(c, capability) {
			return true
		}
	}
	return false
}

type Inventory struct {
	Worker string      `json:"worker"`
	URL    string      `json:"url"`
	Models []ModelInfo `json:"models"`
	Error  string      `json:"error,omitempty"`
}

func (p *Pool) URLFor(name string) (*url.URL, bool) {
	w := p.byName[name]
	if w == nil {
		return nil, false
	}
	u := *w.url
	return &u, true
}

// TagModel is the native Ollama /api/tags model shape used for client-facing
// discovery. It intentionally excludes gateway-only fields such as Loaded.
type TagModel struct {
	Name       string       `json:"name"`
	Model      string       `json:"model"`
	ModifiedAt time.Time    `json:"modified_at,omitempty"`
	Size       int64        `json:"size,omitempty"`
	Digest     string       `json:"digest,omitempty"`
	Details    ModelDetails `json:"details"`
}

// Tags queries every healthy worker, merges installed models and normalizes the
// modern Ollama discovery contract so clients can rely on both name and model.
// A failed worker does not hide models served by the remaining healthy workers.
func (p *Pool) Tags(ctx context.Context) ([]TagModel, []string) {
	inventories := p.Inventories(ctx)
	byID := make(map[string]TagModel)
	errs := make([]string, 0)
	for _, inv := range inventories {
		if inv.Error != "" {
			errs = append(errs, inv.Worker+": "+inv.Error)
			continue
		}
		for _, m := range inv.Models {
			name := strings.TrimSpace(m.Name)
			model := strings.TrimSpace(m.Model)
			if model == "" {
				model = name
			}
			if name == "" {
				name = model
			}
			if model == "" {
				continue
			}
			if decision, ok := p.PlacementDecision(inv.Worker, model); ok && !decision.Allowed {
				continue
			}
			cur, exists := byID[model]
			if !exists || m.ModifiedAt.After(cur.ModifiedAt) {
				byID[model] = TagModel{Name: name, Model: model, ModifiedAt: m.ModifiedAt, Size: m.Size, Digest: m.Digest, Details: m.Details}
			}
		}
	}
	out := make([]TagModel, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	sort.Strings(errs)
	return out, errs
}

// Loaded returns a de-duplicated native Ollama /api/ps view across workers.
func (p *Pool) Loaded() []LoadedModel {
	byID := make(map[string]LoadedModel)
	for _, snap := range p.Snapshots() {
		if !snap.Healthy {
			continue
		}
		for _, m := range snap.LoadedModels {
			id := strings.TrimSpace(m.Model)
			if id == "" {
				id = strings.TrimSpace(m.Name)
			}
			if id == "" {
				continue
			}
			if m.Model == "" {
				m.Model = id
			}
			if m.Name == "" {
				m.Name = id
			}
			if cur, ok := byID[id]; !ok || m.SizeVRAM > cur.SizeVRAM {
				byID[id] = m
			}
		}
	}
	out := make([]LoadedModel, 0, len(byID))
	for _, m := range byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Model < out[j].Model })
	return out
}

func (p *Pool) Inventories(ctx context.Context) []Inventory {
	out := make([]Inventory, len(p.workers))
	var wg sync.WaitGroup
	for i, w := range p.workers {
		wg.Add(1)
		go func(i int, w *state) {
			defer wg.Done()
			inv := Inventory{Worker: w.cfg.Name, URL: w.url.String()}
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(cctx, http.MethodGet, w.url.String()+"/api/tags", nil)
			resp, err := p.client.Do(req)
			if err != nil {
				inv.Error = err.Error()
				out[i] = inv
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode/100 != 2 {
				inv.Error = fmt.Sprintf("HTTP %d", resp.StatusCode)
				out[i] = inv
				return
			}
			var doc struct {
				Models []ModelInfo `json:"models"`
			}
			if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&doc); err != nil {
				inv.Error = err.Error()
				out[i] = inv
				return
			}
			installed := make(map[string]bool, len(doc.Models)*2)
			for _, m := range doc.Models {
				for _, id := range []string{m.Model, m.Name} {
					id = strings.TrimSpace(id)
					if id != "" {
						installed[id] = true
						installed[strings.TrimSuffix(id, ":latest")] = true
					}
				}
			}
			w.mu.Lock()
			w.installed = installed
			w.installedKnown = true
			display := make([]string, 0, len(doc.Models))
			seenDisplay := map[string]bool{}
			for _, m := range doc.Models {
				name := strings.TrimSpace(m.Model)
				if name == "" {
					name = strings.TrimSpace(m.Name)
				}
				if name != "" && !seenDisplay[name] {
					seenDisplay[name] = true
					display = append(display, name)
				}
			}
			sort.Strings(display)
			w.installedModels = display
			w.inventoryError = ""
			w.mu.Unlock()
			w.mu.RLock()
			loaded := make(map[string]bool, len(w.models))
			loadedCtx := make(map[string]int64, len(w.loadedModels)*2)
			for k, v := range w.models {
				loaded[k] = v
			}
			for _, lm := range w.loadedModels {
				id := strings.TrimSpace(lm.Model)
				if id == "" {
					id = strings.TrimSpace(lm.Name)
				}
				if id != "" && lm.ContextLength > 0 {
					loadedCtx[id] = lm.ContextLength
					loadedCtx[canonicalModel(id)] = lm.ContextLength
				}
			}
			w.mu.RUnlock()
			for j := range doc.Models {
				name := doc.Models[j].Name
				if name == "" {
					name = doc.Models[j].Model
				}
				doc.Models[j].Loaded = loaded[name] || loaded[strings.TrimSuffix(name, ":latest")]
				doc.Models[j].LoadedContextLength = loadedCtx[name]
				if doc.Models[j].LoadedContextLength == 0 {
					doc.Models[j].LoadedContextLength = loadedCtx[canonicalModel(name)]
				}
			}
			if p.capCfg.Mode != "off" {
				sem := make(chan struct{}, 4)
				var mwg sync.WaitGroup
				for j := range doc.Models {
					j := j
					model := doc.Models[j].Model
					if model == "" {
						model = doc.Models[j].Name
					}
					mwg.Add(1)
					go func() {
						defer mwg.Done()
						select {
						case sem <- struct{}{}:
						case <-ctx.Done():
							return
						}
						defer func() { <-sem }()
						m, err := p.metadataForWorker(ctx, w, model)
						if err != nil {
							doc.Models[j].MetadataError = err.Error()
							return
						}
						doc.Models[j].Capabilities = append([]string(nil), m.Capabilities...)
						doc.Models[j].ContextLength = m.ContextLength
						doc.Models[j].ConfiguredContextLength = m.ConfiguredContextLength
					}()
				}
				mwg.Wait()
			}
			sort.Slice(doc.Models, func(a, b int) bool { return doc.Models[a].Name < doc.Models[b].Name })
			inv.Models = doc.Models
			out[i] = inv
		}(i, w)
	}
	wg.Wait()
	return out
}
