package policy

import (
	"context"
	"sort"
	"sync"

	"github.com/example/ollama-fair-gateway/internal/config"
)

type Store interface {
	Get(context.Context, string) (config.TenantPolicy, bool, error)
	Put(context.Context, string, config.TenantPolicy) error
	Delete(context.Context, string) error
	List(context.Context) (map[string]config.TenantPolicy, error)
	Health(context.Context) error
}

type Memory struct {
	mu sync.RWMutex
	m  map[string]config.TenantPolicy
}

func NewMemory() *Memory { return &Memory{m: map[string]config.TenantPolicy{}} }

func (m *Memory) Get(_ context.Context, tenant string) (config.TenantPolicy, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p, ok := m.m[tenant]
	return p, ok, nil
}
func (m *Memory) Put(_ context.Context, tenant string, p config.TenantPolicy) error {
	m.mu.Lock()
	m.m[tenant] = p
	m.mu.Unlock()
	return nil
}
func (m *Memory) Delete(_ context.Context, tenant string) error {
	m.mu.Lock()
	delete(m.m, tenant)
	m.mu.Unlock()
	return nil
}
func (m *Memory) List(context.Context) (map[string]config.TenantPolicy, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]config.TenantPolicy, len(m.m))
	for k, v := range m.m {
		out[k] = v
	}
	return out, nil
}
func (m *Memory) Health(context.Context) error { return nil }

// Names returns the stable sorted tenant names in a policy map. Kept here so
// both API and UI can present deterministic policy tables.
func Names(m map[string]config.TenantPolicy) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
