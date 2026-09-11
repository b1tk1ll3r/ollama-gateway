package server

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/liveflow"
)

var errJobCancelled = errors.New("job cancelled by administrator")

type jobEntry struct {
	ID           string     `json:"id"`
	Tenant       string     `json:"tenant"`
	Actor        string     `json:"actor"`
	Application  string     `json:"application,omitempty"`
	ServiceClass string     `json:"service_class,omitempty"`
	Model        string     `json:"model,omitempty"`
	Path         string     `json:"path"`
	API          string     `json:"api"`
	Worker       string     `json:"worker,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	CancelledAt  *time.Time `json:"cancelled_at,omitempty"`
	Cancelling   bool       `json:"cancelling,omitempty"`
}

type jobView struct {
	liveflow.Request
	Cancellable bool `json:"cancellable"`
	Cancelling  bool `json:"cancelling,omitempty"`
}

type jobManager struct {
	mu     sync.RWMutex
	jobs   map[string]jobEntry
	cancel map[string]context.CancelCauseFunc
}

func newJobManager() *jobManager {
	return &jobManager{jobs: make(map[string]jobEntry), cancel: make(map[string]context.CancelCauseFunc)}
}

func (m *jobManager) register(j jobEntry, cancel context.CancelCauseFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	m.cancel[j.ID] = cancel
}

func (m *jobManager) setWorker(id, worker string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return
	}
	j.Worker = worker
	m.jobs[id] = j
}

func (m *jobManager) finish(id string) {
	m.mu.Lock()
	delete(m.jobs, id)
	delete(m.cancel, id)
	m.mu.Unlock()
}

func (m *jobManager) cancelJob(id string) bool {
	m.mu.Lock()
	cancel := m.cancel[id]
	j, ok := m.jobs[id]
	if cancel == nil || !ok {
		m.mu.Unlock()
		return false
	}
	if !j.Cancelling {
		now := time.Now().UTC()
		j.Cancelling = true
		j.CancelledAt = &now
		m.jobs[id] = j
	}
	m.mu.Unlock()
	cancel(errJobCancelled)
	return true
}

func (m *jobManager) list() []jobEntry {
	m.mu.RLock()
	out := make([]jobEntry, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, j)
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

func isAdminJobCancel(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), errJobCancelled)
}
