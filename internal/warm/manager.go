package warm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/state"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type Action struct {
	Time    time.Time `json:"time"`
	Type    string    `json:"type"`
	Worker  string    `json:"worker"`
	Model   string    `json:"model"`
	Policy  string    `json:"policy,omitempty"`
	Status  string    `json:"status"`
	Message string    `json:"message,omitempty"`
}

type Suggestion struct {
	Worker      string    `json:"worker"`
	Model       string    `json:"model"`
	Class       string    `json:"class"`
	Reason      string    `json:"reason"`
	LastUsed    time.Time `json:"last_used,omitempty"`
	VRAMPercent float64   `json:"vram_percent,omitempty"`
}

type Status struct {
	Enabled       bool                              `json:"enabled"`
	Override      bool                              `json:"override"`
	Baseline      map[string]config.WarmModelPolicy `json:"baseline"`
	Policies      map[string]config.WarmModelPolicy `json:"policies"`
	Actions       []Action                          `json:"actions"`
	Suggestions   []Suggestion                      `json:"eviction_suggestions"`
	LastReconcile time.Time                         `json:"last_reconcile,omitempty"`
	LastError     string                            `json:"last_error,omitempty"`
}

type persistentFile struct {
	Override bool                              `json:"override"`
	Policies map[string]config.WarmModelPolicy `json:"policies,omitempty"`
	Actions  []Action                          `json:"actions,omitempty"`
}

type Manager struct {
	mu sync.RWMutex

	cfg      config.WarmModelsConfig
	pool     *worker.Pool
	file     state.AtomicJSON
	client   *http.Client
	baseline map[string]config.WarmModelPolicy
	policies map[string]config.WarmModelPolicy
	override bool

	lastUse       map[string]time.Time
	seenLoaded    map[string]bool
	inFlight      map[string]bool
	actions       []Action
	suggestions   []Suggestion
	lastReconcile time.Time
	lastError     string
	wake          chan struct{}
	sem           chan struct{}
	wg            sync.WaitGroup
}

func New(cfg config.WarmModelsConfig, pool *worker.Pool, path string) (*Manager, error) {
	m := &Manager{
		cfg:        cfg,
		pool:       pool,
		file:       state.AtomicJSON{Path: path, Mode: 0600},
		client:     &http.Client{Timeout: cfg.OperationTimeout.Value()},
		baseline:   clonePolicies(cfg.Policies),
		policies:   clonePolicies(cfg.Policies),
		lastUse:    map[string]time.Time{},
		seenLoaded: map[string]bool{},
		inFlight:   map[string]bool{},
		wake:       make(chan struct{}, 1),
		sem:        make(chan struct{}, 2),
	}
	var pf persistentFile
	if err := m.file.Load(&pf); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else if err == nil {
		if pf.Override {
			if err := config.ValidateWarmModelPolicies(pf.Policies, workerNames(pool)); err != nil {
				return nil, fmt.Errorf("load warm model policies: %w", err)
			}
			m.override = true
			m.policies = clonePolicies(pf.Policies)
		}
		m.actions = append([]Action(nil), pf.Actions...)
		if len(m.actions) > 200 {
			m.actions = m.actions[len(m.actions)-200:]
		}
	}
	return m, nil
}

func workerNames(p *worker.Pool) map[string]bool {
	out := map[string]bool{}
	if p == nil {
		return out
	}
	for _, s := range p.Snapshots() {
		out[s.Name] = true
	}
	return out
}

func clonePolicies(in map[string]config.WarmModelPolicy) map[string]config.WarmModelPolicy {
	out := make(map[string]config.WarmModelPolicy, len(in))
	for k, v := range in {
		v.Workers = append([]string(nil), v.Workers...)
		out[k] = v
	}
	return out
}

func (m *Manager) Start(ctx context.Context) {
	if m == nil || !m.cfg.Enabled {
		return
	}
	go func() {
		m.Reconcile(ctx)
		t := time.NewTicker(m.cfg.ReconcileInterval.Value())
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				m.Reconcile(ctx)
			case <-m.wake:
				m.Reconcile(ctx)
			}
		}
	}()
}

// Wait blocks until currently scheduled preload/unload actions have finished.
// It is primarily useful for graceful shutdowns and deterministic tests.
func (m *Manager) Wait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) Wake() {
	if m == nil {
		return
	}
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) Touch(workerName, model string) {
	if m == nil || workerName == "" || model == "" {
		return
	}
	m.mu.Lock()
	m.lastUse[key(workerName, model)] = time.Now().UTC()
	m.mu.Unlock()
}

func (m *Manager) Status() Status {
	if m == nil {
		return Status{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	a := append([]Action(nil), m.actions...)
	sort.Slice(a, func(i, j int) bool { return a[i].Time.After(a[j].Time) })
	if len(a) > 100 {
		a = a[:100]
	}
	return Status{Enabled: m.cfg.Enabled, Override: m.override, Baseline: clonePolicies(m.baseline), Policies: clonePolicies(m.policies), Actions: a, Suggestions: append([]Suggestion(nil), m.suggestions...), LastReconcile: m.lastReconcile, LastError: m.lastError}
}

func (m *Manager) SetPolicies(p map[string]config.WarmModelPolicy) error {
	if m == nil {
		return errors.New("warm manager unavailable")
	}
	if err := config.ValidateWarmModelPolicies(p, workerNames(m.pool)); err != nil {
		return err
	}
	m.mu.Lock()
	old, oldOverride := m.policies, m.override
	m.policies, m.override = clonePolicies(p), true
	err := m.saveLocked()
	if err != nil {
		m.policies, m.override = old, oldOverride
	}
	m.mu.Unlock()
	if err == nil {
		m.Wake()
	}
	return err
}

func (m *Manager) Reset() error {
	if m == nil {
		return errors.New("warm manager unavailable")
	}
	m.mu.Lock()
	old, oldOverride := m.policies, m.override
	m.policies, m.override = clonePolicies(m.baseline), false
	err := m.saveLocked()
	if err != nil {
		m.policies, m.override = old, oldOverride
	}
	m.mu.Unlock()
	if err == nil {
		m.Wake()
	}
	return err
}

func (m *Manager) Reconcile(ctx context.Context) {
	if m == nil || !m.cfg.Enabled || m.pool == nil {
		return
	}
	placements := m.pool.PlacementSnapshots()
	snaps := m.pool.Snapshots()
	pByName := map[string]worker.PlacementSnapshot{}
	for _, p := range placements {
		pByName[p.Worker] = p
	}
	now := time.Now().UTC()

	m.mu.Lock()
	policies := clonePolicies(m.policies)
	for _, s := range snaps {
		for _, lm := range s.LoadedModels {
			model := loadedName(lm)
			if model == "" {
				continue
			}
			k := key(s.Name, model)
			if !m.seenLoaded[k] {
				m.seenLoaded[k] = true
				if m.lastUse[k].IsZero() {
					m.lastUse[k] = now
				}
			}
		}
	}
	lastUse := make(map[string]time.Time, len(m.lastUse))
	for k, v := range m.lastUse {
		lastUse[k] = v
	}
	m.mu.Unlock()

	models := installedModels(placements)
	for _, model := range models {
		pattern, pol, ok := selectPolicy(policies, model)
		if !ok {
			continue
		}
		eligible := eligibleWorkers(model, pol, snaps, pByName, m.pool)
		if pol.Class == "hot" || (pol.Class == "warm" && pol.Preload) {
			n := pol.Replicas
			if n <= 0 {
				n = 1
			}
			if n > len(eligible) {
				n = len(eligible)
			}
			for i := 0; i < n; i++ {
				if !isLoaded(eligible[i], model) {
					m.startAction(ctx, "preload", eligible[i].Name, model, pattern)
				}
			}
		}
		if pol.Class == "warm" || pol.Class == "cold" {
			idle := pol.IdleTimeout.Value()
			for _, s := range snaps {
				if s.Maintenance != "active" || !policyTargetsWorker(pol, s.Name) || !isLoaded(s, model) || modelActive(s, model) > 0 {
					continue
				}
				lu := lastUse[key(s.Name, model)]
				if lu.IsZero() {
					lu = now
				}
				if idle <= 0 || now.Sub(lu) >= idle {
					m.startAction(ctx, "unload", s.Name, model, pattern)
				}
			}
		}
	}

	suggestions := evictionSuggestions(snaps, policies, lastUse)
	m.mu.Lock()
	m.suggestions = suggestions
	m.lastReconcile = now
	m.lastError = ""
	m.mu.Unlock()
}

func (m *Manager) startAction(parent context.Context, action, workerName, model, pattern string) {
	k := action + "\x00" + workerName + "\x00" + model
	m.mu.Lock()
	if m.inFlight[k] {
		m.mu.Unlock()
		return
	}
	m.inFlight[k] = true
	a := Action{Time: time.Now().UTC(), Type: action, Worker: workerName, Model: model, Policy: pattern, Status: "running"}
	m.actions = append(m.actions, a)
	m.trimActionsLocked()
	_ = m.saveLocked()
	m.mu.Unlock()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.sem <- struct{}{}
		defer func() { <-m.sem }()
		ctx, cancel := context.WithTimeout(parent, m.cfg.OperationTimeout.Value())
		defer cancel()
		release, err := m.pool.BeginModelMaintenance(workerName, model)
		if err == nil {
			defer release()
			err = m.modelAction(ctx, action, workerName, model)
		}
		m.mu.Lock()
		delete(m.inFlight, k)
		status, msg := "completed", "success"
		if err != nil {
			status, msg = "failed", err.Error()
			m.lastError = err.Error()
		}
		for i := len(m.actions) - 1; i >= 0; i-- {
			if m.actions[i].Type == action && m.actions[i].Worker == workerName && m.actions[i].Model == model && m.actions[i].Status == "running" {
				m.actions[i].Status = status
				m.actions[i].Message = msg
				m.actions[i].Time = time.Now().UTC()
				break
			}
		}
		if action == "preload" && err == nil {
			m.lastUse[key(workerName, model)] = time.Now().UTC()
		}
		m.trimActionsLocked()
		_ = m.saveLocked()
		m.mu.Unlock()
		if err == nil {
			m.Wake()
		}
	}()
}

func (m *Manager) modelAction(ctx context.Context, action, workerName, model string) error {
	base, ok := m.pool.URLFor(workerName)
	if !ok {
		return fmt.Errorf("unknown worker %q", workerName)
	}
	if mode, ok := m.pool.Maintenance(workerName); !ok || mode != "active" {
		return fmt.Errorf("worker %s is not active", workerName)
	}
	if action == "preload" {
		pd, ok := m.pool.PlacementDecision(workerName, model)
		if !ok || !pd.Allowed {
			return fmt.Errorf("model %s is blocked by placement on worker %s", model, workerName)
		}
		for _, ps := range m.pool.PlacementSnapshots() {
			if ps.Worker != workerName || !ps.InventoryKnown {
				continue
			}
			found := false
			for _, installed := range ps.InstalledModels {
				if sameModel(installed, model) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("model %s is not installed on worker %s", model, workerName)
			}
		}
	}
	keep := any(-1)
	if action == "unload" {
		keep = 0
	}
	body, _ := json.Marshal(map[string]any{"model": model, "prompt": "", "keep_alive": keep, "stream": false})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base.String(), "/")+"/api/generate", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return fmt.Errorf("Ollama HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

func (m *Manager) saveLocked() error {
	return m.file.Save(persistentFile{Override: m.override, Policies: clonePolicies(m.policies), Actions: append([]Action(nil), m.actions...)})
}
func (m *Manager) trimActionsLocked() {
	if len(m.actions) > 200 {
		m.actions = append([]Action(nil), m.actions[len(m.actions)-200:]...)
	}
}

func loadedName(m worker.LoadedModel) string {
	if strings.TrimSpace(m.Model) != "" {
		return strings.TrimSpace(m.Model)
	}
	return strings.TrimSpace(m.Name)
}
func key(workerName, model string) string { return workerName + "\x00" + model }
func canonical(s string) string           { return strings.TrimSuffix(strings.TrimSpace(s), ":latest") }
func sameModel(a, b string) bool          { return a == b || canonical(a) == canonical(b) }
func isLoaded(s worker.Snapshot, model string) bool {
	for _, lm := range s.LoadedModels {
		if sameModel(loadedName(lm), model) {
			return true
		}
	}
	return false
}
func modelActive(s worker.Snapshot, model string) int {
	if n := s.ModelActive[model]; n > 0 {
		return n
	}
	for k, n := range s.ModelActive {
		if sameModel(k, model) {
			return n
		}
	}
	return 0
}

func installedModels(ps []worker.PlacementSnapshot) []string {
	set := map[string]bool{}
	for _, p := range ps {
		for _, m := range p.InstalledModels {
			if strings.TrimSpace(m) != "" {
				set[m] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

func match(pattern, model string) (int, bool) {
	pattern = strings.TrimSpace(pattern)
	if pattern == "*" {
		return 0, true
	}
	if strings.HasSuffix(pattern, "*") {
		p := strings.TrimSuffix(pattern, "*")
		if strings.HasPrefix(model, p) {
			return len(p), true
		}
		return -1, false
	}
	if sameModel(pattern, model) {
		return 100000 + len(pattern), true
	}
	return -1, false
}
func selectPolicy(ps map[string]config.WarmModelPolicy, model string) (string, config.WarmModelPolicy, bool) {
	best := -1
	var bp string
	var out config.WarmModelPolicy
	for p, v := range ps {
		if sp, ok := match(p, model); ok && sp > best {
			best, bp, out = sp, p, v
		}
	}
	return bp, out, best >= 0
}

func policyTargetsWorker(pol config.WarmModelPolicy, workerName string) bool {
	if len(pol.Workers) == 0 {
		return true
	}
	for _, w := range pol.Workers {
		if w == workerName {
			return true
		}
	}
	return false
}

func eligibleWorkers(model string, pol config.WarmModelPolicy, snaps []worker.Snapshot, placements map[string]worker.PlacementSnapshot, pool *worker.Pool) []worker.Snapshot {
	allowedNames := map[string]bool{}
	if len(pol.Workers) > 0 {
		for _, w := range pol.Workers {
			allowedNames[w] = true
		}
	}
	out := []worker.Snapshot{}
	for _, s := range snaps {
		if !s.Healthy || s.Maintenance != "active" || s.CircuitState == "open" {
			continue
		}
		if len(allowedNames) > 0 && !allowedNames[s.Name] {
			continue
		}
		pd, ok := pool.PlacementDecision(s.Name, model)
		if !ok || !pd.Allowed {
			continue
		}
		p := placements[s.Name]
		if p.InventoryKnown {
			found := false
			for _, m := range p.InstalledModels {
				if sameModel(m, model) {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		li, lj := isLoaded(out[i], model), isLoaded(out[j], model)
		if li != lj {
			return li
		}
		ri := float64(out[i].Active) / float64(max(1, out[i].MaxConcurrent))
		rj := float64(out[j].Active) / float64(max(1, out[j].MaxConcurrent))
		if ri != rj {
			return ri < rj
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func evictionSuggestions(snaps []worker.Snapshot, policies map[string]config.WarmModelPolicy, last map[string]time.Time) []Suggestion {
	out := []Suggestion{}
	for _, s := range snaps {
		if s.VRAMTotalBytes <= 0 || s.VRAMUsedBytes <= 0 {
			continue
		}
		pct := 100 * float64(s.VRAMUsedBytes) / float64(s.VRAMTotalBytes)
		if pct < 90 {
			continue
		}
		for _, lm := range s.LoadedModels {
			m := loadedName(lm)
			_, p, ok := selectPolicy(policies, m)
			if !ok || p.Class == "hot" || modelActive(s, m) > 0 {
				continue
			}
			out = append(out, Suggestion{Worker: s.Name, Model: m, Class: p.Class, Reason: "VRAM pressure; inactive non-hot model", LastUsed: last[key(s.Name, m)], VRAMPercent: pct})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Class != out[j].Class {
			return out[i].Class == "cold"
		}
		return out[i].LastUsed.Before(out[j].LastUsed)
	})
	if len(out) > 50 {
		out = out[:50]
	}
	return out
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
