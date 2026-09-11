package state

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/example/ollama-fair-gateway/internal/config"
)

type PolicyStore struct {
	mu   sync.RWMutex
	file AtomicJSON
	m    map[string]config.TenantPolicy
}

type policyFile struct {
	Policies map[string]config.TenantPolicy `json:"policies"`
}

func NewPolicyStore(path string) (*PolicyStore, error) {
	s := &PolicyStore{file: AtomicJSON{Path: path, Mode: 0600}, m: map[string]config.TenantPolicy{}}
	var f policyFile
	if err := s.file.Load(&f); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for k, v := range f.Policies {
		s.m[k] = v
	}
	return s, nil
}
func (s *PolicyStore) Get(_ context.Context, tenant string) (config.TenantPolicy, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[tenant]
	return p, ok, nil
}
func (s *PolicyStore) Put(_ context.Context, tenant string, p config.TenantPolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.m[tenant]
	s.m[tenant] = p
	if err := s.saveLocked(); err != nil {
		if had {
			s.m[tenant] = old
		} else {
			delete(s.m, tenant)
		}
		return err
	}
	return nil
}
func (s *PolicyStore) Delete(_ context.Context, tenant string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.m[tenant]
	delete(s.m, tenant)
	if err := s.saveLocked(); err != nil {
		if had {
			s.m[tenant] = old
		}
		return err
	}
	return nil
}
func (s *PolicyStore) List(context.Context) (map[string]config.TenantPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]config.TenantPolicy, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return out, nil
}
func (s *PolicyStore) Health(context.Context) error { return nil }
func (s *PolicyStore) saveLocked() error {
	out := make(map[string]config.TenantPolicy, len(s.m))
	for k, v := range s.m {
		out[k] = v
	}
	return s.file.Save(policyFile{Policies: out})
}
