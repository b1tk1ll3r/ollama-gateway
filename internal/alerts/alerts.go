package alerts

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/state"
)

type Worker struct {
	Name             string
	Healthy          bool
	CircuitState     string
	LastCircuitError string
	VRAMUsedBytes    int64
	VRAMTotalBytes   int64
}

type Snapshot struct {
	QueueDepth   int
	QueueWait    time.Duration
	Workers      []Worker
	StorageBytes int64
}

type Provider func() Snapshot

type Event struct {
	ID         string         `json:"id"`
	Key        string         `json:"key"`
	Type       string         `json:"type"`
	Severity   string         `json:"severity"`
	State      string         `json:"state"` // firing | resolved
	Message    string         `json:"message"`
	Worker     string         `json:"worker,omitempty"`
	Tenant     string         `json:"tenant,omitempty"`
	Actor      string         `json:"actor,omitempty"`
	Value      float64        `json:"value,omitempty"`
	Threshold  float64        `json:"threshold,omitempty"`
	StartedAt  time.Time      `json:"started_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	ResolvedAt time.Time      `json:"resolved_at,omitempty"`
	Meta       map[string]any `json:"meta,omitempty"`
}

type Delivery struct {
	Time       time.Time `json:"time"`
	EventID    string    `json:"event_id"`
	Webhook    string    `json:"webhook"`
	Attempt    int       `json:"attempt,omitempty"`
	StatusCode int       `json:"status_code,omitempty"`
	Error      string    `json:"error,omitempty"`
}

type deliveryTask struct {
	Event   Event
	Webhook config.WebhookConfig
}

type Status struct {
	Enabled      bool                   `json:"enabled"`
	Active       []Event                `json:"active"`
	History      []Event                `json:"history"`
	Deliveries   []Delivery             `json:"deliveries"`
	Webhooks     []map[string]any       `json:"webhooks"`
	Thresholds   config.AlertThresholds `json:"thresholds"`
	LastEvaluate time.Time              `json:"last_evaluate,omitempty"`
	LastError    string                 `json:"last_error,omitempty"`
}

type persistent struct {
	Active     map[string]Event     `json:"active"`
	History    []Event              `json:"history"`
	Deliveries []Delivery           `json:"deliveries"`
	LastSent   map[string]time.Time `json:"last_sent"`
}

type Manager struct {
	mu           sync.RWMutex
	cfg          config.AlertsConfig
	file         state.AtomicJSON
	provider     Provider
	client       *http.Client
	active       map[string]Event
	history      []Event
	deliveries   []Delivery
	lastSent     map[string]time.Time
	downSince    map[string]time.Time
	lastEvaluate time.Time
	lastError    string

	deliveryQ   chan deliveryTask
	persistWake chan struct{}
	started     atomic.Bool
}

func New(cfg config.AlertsConfig, path string, provider Provider) (*Manager, error) {
	if cfg.WebhookTimeout.Value() <= 0 {
		cfg.WebhookTimeout = config.Duration(5 * time.Second)
	}
	if cfg.WebhookMaxConcurrent <= 0 {
		cfg.WebhookMaxConcurrent = 4
	}
	if cfg.WebhookQueue <= 0 {
		cfg.WebhookQueue = 1024
	}
	if cfg.WebhookRetryAttempts <= 0 {
		cfg.WebhookRetryAttempts = 3
	}
	if cfg.WebhookRetryBackoff.Value() < 0 {
		cfg.WebhookRetryBackoff = 0
	}
	m := &Manager{cfg: cfg, file: state.AtomicJSON{Path: path, Mode: 0600}, provider: provider, client: &http.Client{Timeout: cfg.WebhookTimeout.Value()}, active: map[string]Event{}, lastSent: map[string]time.Time{}, downSince: map[string]time.Time{}, deliveryQ: make(chan deliveryTask, cfg.WebhookQueue), persistWake: make(chan struct{}, 1)}
	var p persistent
	if err := m.file.Load(&p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	} else if err == nil {
		if p.Active != nil {
			m.active = p.Active
		}
		m.history = p.History
		m.deliveries = p.Deliveries
		if p.LastSent != nil {
			m.lastSent = p.LastSent
		}
		m.trimLocked()
	}
	return m, nil
}

func (m *Manager) Start(ctx context.Context) {
	if m == nil || !m.cfg.Enabled || !m.started.CompareAndSwap(false, true) {
		return
	}
	for i := 0; i < m.cfg.WebhookMaxConcurrent; i++ {
		go m.deliveryWorker(ctx)
	}
	go m.persistenceWorker(ctx)
	go func() {
		m.Evaluate()
		t := time.NewTicker(m.cfg.EvaluationInterval.Value())
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				_ = m.persistNow()
				return
			case <-t.C:
				m.Evaluate()
			}
		}
	}()
}

func (m *Manager) Evaluate() {
	if m == nil || !m.cfg.Enabled || m.provider == nil {
		return
	}
	s := m.provider()
	now := time.Now().UTC()
	present := map[string]Event{}
	th := m.cfg.Thresholds
	if th.QueueDepth > 0 && s.QueueDepth >= th.QueueDepth {
		e := newEvent("queue_depth", "warning", fmt.Sprintf("queue depth %d exceeds threshold %d", s.QueueDepth, th.QueueDepth))
		e.Value = float64(s.QueueDepth)
		e.Threshold = float64(th.QueueDepth)
		present[e.Key] = e
	}
	if th.QueueWait.Value() > 0 && s.QueueWait >= th.QueueWait.Value() {
		e := newEvent("queue_wait", "warning", fmt.Sprintf("oldest queued request has waited %s (threshold %s)", s.QueueWait.Round(time.Millisecond), th.QueueWait.Value()))
		e.Value = s.QueueWait.Seconds()
		e.Threshold = th.QueueWait.Value().Seconds()
		present[e.Key] = e
	}
	if th.StorageBytes > 0 && s.StorageBytes >= th.StorageBytes {
		e := newEvent("storage_growth", "warning", fmt.Sprintf("gateway storage %d bytes exceeds threshold %d", s.StorageBytes, th.StorageBytes))
		e.Value = float64(s.StorageBytes)
		e.Threshold = float64(th.StorageBytes)
		present[e.Key] = e
	}
	m.mu.Lock()
	for _, w := range s.Workers {
		if !w.Healthy {
			if m.downSince[w.Name].IsZero() {
				m.downSince[w.Name] = now
			}
			if th.WorkerDownFor.Value() <= 0 || now.Sub(m.downSince[w.Name]) >= th.WorkerDownFor.Value() {
				e := newEvent("worker_down:"+w.Name, "critical", fmt.Sprintf("worker %s is unhealthy", w.Name))
				e.Type = "worker_down"
				e.Worker = w.Name
				present[e.Key] = e
			}
		} else {
			delete(m.downSince, w.Name)
		}
		if th.CircuitOpen && w.CircuitState == "open" {
			e := newEvent("circuit_open:"+w.Name, "warning", fmt.Sprintf("worker %s circuit breaker is open", w.Name))
			e.Type = "circuit_open"
			e.Worker = w.Name
			present[e.Key] = e
		}
		if th.VRAMPercent > 0 && w.VRAMTotalBytes > 0 {
			pct := 100 * float64(w.VRAMUsedBytes) / float64(w.VRAMTotalBytes)
			if pct >= th.VRAMPercent {
				e := newEvent("vram_pressure:"+w.Name, "warning", fmt.Sprintf("worker %s VRAM usage %.1f%% exceeds %.1f%%", w.Name, pct, th.VRAMPercent))
				e.Type = "vram_pressure"
				e.Worker = w.Name
				e.Value = pct
				e.Threshold = th.VRAMPercent
				present[e.Key] = e
			}
		}
		if th.OOM && looksOOM(w.LastCircuitError) {
			e := newEvent("oom:"+w.Name, "critical", fmt.Sprintf("worker %s reported an out-of-memory failure", w.Name))
			e.Type = "oom"
			e.Worker = w.Name
			e.Meta = map[string]any{"last_error": w.LastCircuitError}
			present[e.Key] = e
		}
	}
	toSend := m.reconcileLocked(present, now)
	m.lastEvaluate = now
	m.mu.Unlock()
	m.schedulePersist()
	for _, e := range toSend {
		m.deliver(e)
	}
}

func newEvent(key, severity, msg string) Event {
	now := time.Now().UTC()
	return Event{ID: eventID(key, now), Key: key, Type: key, Severity: severity, State: "firing", Message: msg, StartedAt: now, UpdatedAt: now}
}

func (m *Manager) reconcileLocked(present map[string]Event, now time.Time) []Event {
	toSend := []Event{}
	for k, next := range present {
		if cur, ok := m.active[k]; ok {
			next.ID = cur.ID
			next.StartedAt = cur.StartedAt
			next.UpdatedAt = now
			m.active[k] = next
			if m.cooldownReadyLocked(k, now) {
				toSend = append(toSend, next)
				m.lastSent[k] = now
			}
			continue
		}
		m.active[k] = next
		m.history = append(m.history, next)
		toSend = append(toSend, next)
		m.lastSent[k] = now
	}
	for k, cur := range m.active {
		if _, ok := present[k]; ok {
			continue
		}
		cur.State = "resolved"
		cur.UpdatedAt = now
		cur.ResolvedAt = now
		m.history = append(m.history, cur)
		delete(m.active, k)
		toSend = append(toSend, cur)
		m.lastSent[k] = now
	}
	m.trimLocked()
	return toSend
}
func (m *Manager) cooldownReadyLocked(k string, now time.Time) bool {
	d := m.cfg.Cooldown.Value()
	return d <= 0 || m.lastSent[k].IsZero() || now.Sub(m.lastSent[k]) >= d
}

// ObserveQuota feeds the latest token-bucket remainder into alerting without
// putting webhook delivery on the request path. Delivery always happens in a goroutine.
func (m *Manager) ObserveQuota(tenant, actor string, remainingActor, remainingTenant, actorCap, tenantCap float64) {
	if m == nil || !m.cfg.Enabled || m.cfg.Thresholds.QuotaRemainingPct <= 0 {
		return
	}
	threshold := m.cfg.Thresholds.QuotaRemainingPct
	now := time.Now().UTC()
	events := []Event{}
	check := func(kind, name string, remaining, cap float64) {
		if cap <= 0 {
			return
		}
		pct := 100 * remaining / cap
		key := "quota_" + kind + ":" + name
		m.mu.Lock()
		if pct <= threshold {
			e := newEvent(key, "warning", fmt.Sprintf("%s quota %s has %.1f%% credits remaining", kind, name, pct))
			e.Type = "quota_near_exhaustion"
			e.Tenant = tenant
			if kind == "actor" {
				e.Actor = actor
			}
			e.Value = pct
			e.Threshold = threshold
			if cur, ok := m.active[key]; ok {
				e.ID = cur.ID
				e.StartedAt = cur.StartedAt
			}
			m.active[key] = e
			if _, seen := m.lastSent[key]; !seen || m.cooldownReadyLocked(key, now) {
				m.history = append(m.history, e)
				m.lastSent[key] = now
				events = append(events, e)
			}
		} else if cur, ok := m.active[key]; ok {
			cur.State = "resolved"
			cur.UpdatedAt = now
			cur.ResolvedAt = now
			m.history = append(m.history, cur)
			delete(m.active, key)
			events = append(events, cur)
		}
		m.trimLocked()
		m.mu.Unlock()
		m.schedulePersist()
	}
	check("actor", tenant+"/"+actor, remainingActor, actorCap)
	check("tenant", tenant, remainingTenant, tenantCap)
	for _, e := range events {
		go m.deliver(e)
	}
}

func (m *Manager) Status() Status {
	if m == nil {
		return Status{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	active := make([]Event, 0, len(m.active))
	for _, e := range m.active {
		active = append(active, e)
	}
	sort.Slice(active, func(i, j int) bool { return active[i].StartedAt.After(active[j].StartedAt) })
	hist := append([]Event(nil), m.history...)
	sort.Slice(hist, func(i, j int) bool { return hist[i].UpdatedAt.After(hist[j].UpdatedAt) })
	if len(hist) > m.cfg.HistoryLimit {
		hist = hist[:m.cfg.HistoryLimit]
	}
	ds := append([]Delivery(nil), m.deliveries...)
	sort.Slice(ds, func(i, j int) bool { return ds[i].Time.After(ds[j].Time) })
	if len(ds) > 100 {
		ds = ds[:100]
	}
	wh := make([]map[string]any, 0, len(m.cfg.Webhooks))
	for _, w := range m.cfg.Webhooks {
		wh = append(wh, map[string]any{"name": w.Name, "url": w.URL, "enabled": w.Enabled, "signed": w.Secret != ""})
	}
	return Status{Enabled: m.cfg.Enabled, Active: active, History: hist, Deliveries: ds, Webhooks: wh, Thresholds: m.cfg.Thresholds, LastEvaluate: m.lastEvaluate, LastError: m.lastError}
}

func (m *Manager) TestWebhook(name string) error {
	if m == nil {
		return errors.New("alerts unavailable")
	}
	e := newEvent("webhook_test", "info", "Ollama Gateway webhook test")
	e.Type = "test"
	return m.deliverToNamed(e, name)
}

func (m *Manager) deliver(e Event) {
	for _, w := range m.cfg.Webhooks {
		if !w.Enabled {
			continue
		}
		task := deliveryTask{Event: e, Webhook: w}
		if !m.started.Load() {
			// A manager that has not been Start()ed has no owned worker lifecycle
			// to join at shutdown. Deliver synchronously in that uncommon/test-only
			// mode so no orphan goroutine can write persistence after its caller
			// has torn down the state directory. Production managers use deliveryQ.
			_ = m.deliverWithRetry(context.Background(), task)
			continue
		}
		select {
		case m.deliveryQ <- task:
		default:
			m.recordDelivery(Delivery{Time: time.Now().UTC(), EventID: e.ID, Webhook: w.Name, Error: "webhook delivery queue full"})
		}
	}
}

func (m *Manager) deliverToNamed(e Event, name string) error {
	for _, w := range m.cfg.Webhooks {
		if w.Enabled && (name == "" || w.Name == name) {
			return m.deliverWithRetry(context.Background(), deliveryTask{Event: e, Webhook: w})
		}
	}
	return fmt.Errorf("enabled webhook %q not found", name)
}

func (m *Manager) deliveryWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-m.deliveryQ:
			_ = m.deliverWithRetry(ctx, task)
		}
	}
}

func (m *Manager) deliverWithRetry(ctx context.Context, task deliveryTask) error {
	attempts := m.cfg.WebhookRetryAttempts
	if attempts <= 0 {
		attempts = 1
	}
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		retry, err := m.deliverOne(ctx, task.Event, task.Webhook, attempt)
		if err == nil {
			return nil
		}
		last = err
		if !retry || attempt == attempts {
			break
		}
		d := m.cfg.WebhookRetryBackoff.Value()
		if d > 0 {
			// Bounded exponential backoff keeps retry storms away from downstreams.
			for i := 1; i < attempt && d < 30*time.Second; i++ {
				d *= 2
			}
			if d > 30*time.Second {
				d = 30 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
	}
	return last
}

// deliverOne returns retry=true only for transient failures. Event IDs remain
// stable across attempts so webhook consumers can deduplicate deliveries.
func (m *Manager) deliverOne(ctx context.Context, e Event, w config.WebhookConfig, attempt int) (retry bool, err error) {
	payload := map[string]any{"version": "1", "source": "ollama-fair-gateway", "event": e}
	b, _ := json.Marshal(payload)
	reqCtx := ctx
	var cancel context.CancelFunc
	if timeout := m.cfg.WebhookTimeout.Value(); timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, w.URL, bytes.NewReader(b))
	if err != nil {
		m.recordDelivery(Delivery{Time: time.Now().UTC(), EventID: e.ID, Webhook: w.Name, Attempt: attempt, Error: err.Error()})
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "ollama-fair-gateway/alerts")
	ts := fmt.Sprint(time.Now().Unix())
	req.Header.Set("X-Ollama-Gateway-Timestamp", ts)
	req.Header.Set("X-Ollama-Gateway-Event-ID", e.ID)
	req.Header.Set("X-Ollama-Gateway-Delivery-Attempt", fmt.Sprint(attempt))
	if w.Secret != "" {
		mac := hmac.New(sha256.New, []byte(w.Secret))
		_, _ = mac.Write([]byte(ts + "."))
		_, _ = mac.Write(b)
		req.Header.Set("X-Ollama-Gateway-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := m.client.Do(req)
	d := Delivery{Time: time.Now().UTC(), EventID: e.ID, Webhook: w.Name, Attempt: attempt}
	if err != nil {
		d.Error = err.Error()
		m.recordDelivery(d)
		return true, err
	}
	d.StatusCode = resp.StatusCode
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		err = fmt.Errorf("webhook HTTP %d", resp.StatusCode)
		d.Error = err.Error()
		m.recordDelivery(d)
		return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500, err
	}
	m.recordDelivery(d)
	return false, nil
}

func (m *Manager) recordDelivery(d Delivery) {
	m.mu.Lock()
	m.deliveries = append(m.deliveries, d)
	if d.Error != "" {
		m.lastError = d.Error
	}
	m.trimLocked()
	m.mu.Unlock()
	m.schedulePersist()
}

func (m *Manager) schedulePersist() {
	if m == nil {
		return
	}
	if !m.started.Load() {
		_ = m.persistNow()
		return
	}
	select {
	case m.persistWake <- struct{}{}:
	default:
	}
}

func (m *Manager) persistenceWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.persistWake:
			if err := m.persistNow(); err != nil {
				m.mu.Lock()
				m.lastError = err.Error()
				m.mu.Unlock()
			}
		}
	}
}

func (m *Manager) persistNow() error {
	m.mu.RLock()
	active := make(map[string]Event, len(m.active))
	for k, v := range m.active {
		active[k] = v
	}
	history := append([]Event(nil), m.history...)
	deliveries := append([]Delivery(nil), m.deliveries...)
	lastSent := make(map[string]time.Time, len(m.lastSent))
	for k, v := range m.lastSent {
		lastSent[k] = v
	}
	m.mu.RUnlock()
	return m.file.Save(persistent{Active: active, History: history, Deliveries: deliveries, LastSent: lastSent})
}

func (m *Manager) trimLocked() {
	limit := m.cfg.HistoryLimit
	if limit <= 0 {
		limit = 500
	}
	if len(m.history) > limit {
		m.history = append([]Event(nil), m.history[len(m.history)-limit:]...)
	}
	if len(m.deliveries) > 200 {
		m.deliveries = append([]Delivery(nil), m.deliveries[len(m.deliveries)-200:]...)
	}
}
func looksOOM(s string) bool {
	s = strings.ToLower(s)
	return strings.Contains(s, "out of memory") || strings.Contains(s, "oom") || strings.Contains(s, "cuda error: out of memory")
}
func eventID(k string, t time.Time) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", k, t.UnixNano())))
	return hex.EncodeToString(h[:8])
}

func DirSize(root string) int64 {
	var total int64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}
