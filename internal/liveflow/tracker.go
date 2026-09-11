package liveflow

import (
	"sort"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/cost"
)

const (
	StateQueued    = "queued"
	StateRouting   = "routing"
	StateRunning   = "running"
	StateStreaming = "streaming"
	StateCompleted = "completed"
	StateCancelled = "cancelled"
	StateFailed    = "failed"
)

type Request struct {
	ID                    string     `json:"id"`
	Tenant                string     `json:"tenant"`
	Actor                 string     `json:"actor"`
	Application           string     `json:"application,omitempty"`
	ServiceClass          string     `json:"service_class,omitempty"`
	API                   string     `json:"api"`
	Path                  string     `json:"path"`
	Model                 string     `json:"model,omitempty"`
	Worker                string     `json:"worker,omitempty"`
	State                 string     `json:"state"`
	EstimatedCredits      float64    `json:"estimated_credits"`
	ActualCredits         float64    `json:"actual_credits,omitempty"`
	EstimatedPromptTokens int64      `json:"estimated_prompt_tokens,omitempty"`
	PromptTokens          int64      `json:"prompt_tokens,omitempty"`
	CompletionTokens      int64      `json:"completion_tokens,omitempty"`
	BytesOut              int64      `json:"bytes_out,omitempty"`
	Status                int        `json:"status,omitempty"`
	QueuedAt              time.Time  `json:"queued_at"`
	StartedAt             *time.Time `json:"started_at,omitempty"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
	UpdatedAt             time.Time  `json:"updated_at"`
	QueueMS               int64      `json:"queue_ms,omitempty"`
	ServiceMS             int64      `json:"service_ms,omitempty"`
}

type Counts struct {
	Total     int `json:"total"`
	Active    int `json:"active"`
	Queued    int `json:"queued"`
	Routing   int `json:"routing"`
	Running   int `json:"running"`
	Streaming int `json:"streaming"`
	Completed int `json:"completed"`
	Cancelled int `json:"cancelled"`
	Failed    int `json:"failed"`
}

type Snapshot struct {
	GeneratedAt time.Time `json:"generated_at"`
	Version     uint64    `json:"version"`
	Counts      Counts    `json:"counts"`
	Truncated   bool      `json:"truncated,omitempty"`
	Requests    []Request `json:"requests"`
}

type Tracker struct {
	mu        sync.RWMutex
	active    map[string]Request
	recent    []Request
	recentTTL time.Duration
	maxRecent int
	version   uint64
	notify    chan struct{}
}

func New(recentTTL time.Duration, maxRecent int) *Tracker {
	if recentTTL <= 0 {
		recentTTL = 8 * time.Second
	}
	if maxRecent <= 0 {
		maxRecent = 256
	}
	return &Tracker{active: make(map[string]Request), recentTTL: recentTTL, maxRecent: maxRecent, notify: make(chan struct{})}
}

func (t *Tracker) Begin(r Request) {
	now := time.Now().UTC()
	r.State = StateQueued
	r.QueuedAt = now
	r.UpdatedAt = now
	t.mu.Lock()
	t.active[r.ID] = r
	t.bumpLocked()
	t.mu.Unlock()
}

func (t *Tracker) MarkRouting(id, worker string, queue time.Duration) {
	t.update(id, func(r *Request, now time.Time) {
		r.State = StateRouting
		r.Worker = worker
		r.QueueMS = queue.Milliseconds()
		r.UpdatedAt = now
	})
}

func (t *Tracker) MarkRunning(id string) {
	t.update(id, func(r *Request, now time.Time) {
		r.State = StateRunning
		if r.StartedAt == nil {
			x := now
			r.StartedAt = &x
		}
		r.UpdatedAt = now
	})
}

func (t *Tracker) Progress(id string, bytesOut int64, u cost.Usage) {
	t.update(id, func(r *Request, now time.Time) {
		if bytesOut > 0 {
			r.BytesOut = bytesOut
			if r.State == StateRunning || r.State == StateRouting {
				r.State = StateStreaming
			}
		}
		if u.PromptTokens > 0 {
			r.PromptTokens = u.PromptTokens
		}
		if u.CompletionTokens > 0 {
			r.CompletionTokens = u.CompletionTokens
		} else if bytesOut > 0 {
			// Purely for live visualization. Final accounting still comes from
			// Ollama/OpenAI usage metadata in the normal request recorder.
			approx := (bytesOut + 15) / 16
			if approx > r.CompletionTokens {
				r.CompletionTokens = approx
			}
		}
		r.UpdatedAt = now
	})
}

func (t *Tracker) Finish(id string, status int, actualCredits float64, u cost.Usage, service time.Duration) {
	now := time.Now().UTC()
	t.mu.Lock()
	r, ok := t.active[id]
	if !ok {
		t.mu.Unlock()
		return
	}
	delete(t.active, id)
	if status >= 200 && status < 400 {
		r.State = StateCompleted
	} else {
		r.State = StateFailed
	}
	r.Status = status
	r.ActualCredits = actualCredits
	r.ServiceMS = service.Milliseconds()
	if u.PromptTokens > 0 {
		r.PromptTokens = u.PromptTokens
	}
	if u.CompletionTokens > 0 {
		r.CompletionTokens = u.CompletionTokens
	}
	x := now
	r.FinishedAt = &x
	r.UpdatedAt = now
	t.pruneLocked(now)
	t.recent = append(t.recent, r)
	if len(t.recent) > t.maxRecent {
		t.recent = append([]Request(nil), t.recent[len(t.recent)-t.maxRecent:]...)
	}
	t.bumpLocked()
	t.mu.Unlock()
}

func (t *Tracker) Cancel(id string, status int, actualCredits float64, u cost.Usage, service time.Duration) {
	now := time.Now().UTC()
	t.mu.Lock()
	r, ok := t.active[id]
	if !ok {
		t.mu.Unlock()
		return
	}
	delete(t.active, id)
	r.State = StateCancelled
	r.Status = status
	r.ActualCredits = actualCredits
	r.ServiceMS = service.Milliseconds()
	if u.PromptTokens > 0 {
		r.PromptTokens = u.PromptTokens
	}
	if u.CompletionTokens > 0 {
		r.CompletionTokens = u.CompletionTokens
	}
	x := now
	r.FinishedAt = &x
	r.UpdatedAt = now
	t.pruneLocked(now)
	t.recent = append(t.recent, r)
	if len(t.recent) > t.maxRecent {
		t.recent = append([]Request(nil), t.recent[len(t.recent)-t.maxRecent:]...)
	}
	t.bumpLocked()
	t.mu.Unlock()
}

func (t *Tracker) Drop(id string, status int) {
	t.Finish(id, status, 0, cost.Usage{}, 0)
}

func (t *Tracker) Snapshot() Snapshot {
	now := time.Now().UTC()
	t.mu.Lock()
	t.pruneLocked(now)
	out := make([]Request, 0, len(t.active)+len(t.recent))
	counts := Counts{}
	for _, r := range t.active {
		out = append(out, r)
		addCount(&counts, r.State)
	}
	for _, r := range t.recent {
		out = append(out, r)
		addCount(&counts, r.State)
	}
	counts.Total = len(t.active) + len(t.recent)
	counts.Active = len(t.active)
	version := t.version
	limit := t.maxRecent
	t.mu.Unlock()

	truncated := limit > 0 && len(out) > limit
	if truncated {
		sort.SliceStable(out, func(i, j int) bool {
			pi, pj := statePriority(out[i].State), statePriority(out[j].State)
			if pi != pj {
				return pi < pj
			}
			if out[i].State == StateCompleted || out[i].State == StateCancelled || out[i].State == StateFailed {
				return out[i].UpdatedAt.After(out[j].UpdatedAt)
			}
			return out[i].QueuedAt.Before(out[j].QueuedAt)
		})
		out = out[:limit]
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].QueuedAt.Equal(out[j].QueuedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].QueuedAt.Before(out[j].QueuedAt)
	})
	return Snapshot{GeneratedAt: now, Version: version, Counts: counts, Truncated: truncated, Requests: out}
}

func addCount(c *Counts, state string) {
	switch state {
	case StateQueued:
		c.Queued++
	case StateRouting:
		c.Routing++
	case StateRunning:
		c.Running++
	case StateStreaming:
		c.Streaming++
	case StateCompleted:
		c.Completed++
	case StateCancelled:
		c.Cancelled++
	case StateFailed:
		c.Failed++
	}
}

func statePriority(state string) int {
	switch state {
	case StateStreaming:
		return 0
	case StateRunning:
		return 1
	case StateRouting:
		return 2
	case StateQueued:
		return 3
	case StateCompleted:
		return 4
	case StateCancelled:
		return 5
	default:
		return 5
	}
}

func (t *Tracker) Changed() <-chan struct{} {
	t.mu.RLock()
	ch := t.notify
	t.mu.RUnlock()
	return ch
}

func (t *Tracker) update(id string, fn func(*Request, time.Time)) {
	now := time.Now().UTC()
	t.mu.Lock()
	r, ok := t.active[id]
	if ok {
		fn(&r, now)
		t.active[id] = r
		t.bumpLocked()
	}
	t.mu.Unlock()
}

func (t *Tracker) pruneLocked(now time.Time) {
	if len(t.recent) == 0 {
		return
	}
	cutoff := now.Add(-t.recentTTL)
	first := 0
	for first < len(t.recent) {
		r := t.recent[first]
		if r.FinishedAt == nil || r.FinishedAt.After(cutoff) {
			break
		}
		first++
	}
	if first > 0 {
		t.recent = append([]Request(nil), t.recent[first:]...)
	}
}

func (t *Tracker) bumpLocked() {
	t.version++
	close(t.notify)
	t.notify = make(chan struct{})
}
