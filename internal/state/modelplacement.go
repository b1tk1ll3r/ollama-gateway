package state

import (
	"context"
	"errors"
	"os"
	"sync"

	"github.com/example/ollama-fair-gateway/internal/config"
)

// ModelPlacementStore persists only runtime overrides. Bootstrap rules remain
// part of config.json and can be restored per worker by deleting an override.
type ModelPlacementStore struct {
	mu   sync.RWMutex
	file AtomicJSON
	m    map[string]config.ModelPlacementRule
}

type modelPlacementFile struct {
	Workers map[string]config.ModelPlacementRule `json:"workers"`
}

func NewModelPlacementStore(path string) (*ModelPlacementStore, error) {
	s := &ModelPlacementStore{file: AtomicJSON{Path: path, Mode: 0600}, m: map[string]config.ModelPlacementRule{}}
	var f modelPlacementFile
	if err := s.file.Load(&f); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for name, rule := range f.Workers {
		if err := config.ValidateModelPlacementRule(rule); err != nil {
			return nil, err
		}
		s.m[name] = clonePlacementRule(rule)
	}
	return s, nil
}

func (s *ModelPlacementStore) Get(_ context.Context, worker string) (config.ModelPlacementRule, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.m[worker]
	return clonePlacementRule(r), ok, nil
}

func (s *ModelPlacementStore) List(context.Context) (map[string]config.ModelPlacementRule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]config.ModelPlacementRule, len(s.m))
	for k, v := range s.m {
		out[k] = clonePlacementRule(v)
	}
	return out, nil
}

func (s *ModelPlacementStore) Put(_ context.Context, worker string, r config.ModelPlacementRule) error {
	if err := config.ValidateModelPlacementRule(r); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.m[worker]
	s.m[worker] = clonePlacementRule(r)
	if err := s.saveLocked(); err != nil {
		if had {
			s.m[worker] = old
		} else {
			delete(s.m, worker)
		}
		return err
	}
	return nil
}

func (s *ModelPlacementStore) Delete(_ context.Context, worker string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.m[worker]
	delete(s.m, worker)
	if err := s.saveLocked(); err != nil {
		if had {
			s.m[worker] = old
		}
		return err
	}
	return nil
}

func (s *ModelPlacementStore) Health(context.Context) error { return nil }

func (s *ModelPlacementStore) saveLocked() error {
	out := make(map[string]config.ModelPlacementRule, len(s.m))
	for k, v := range s.m {
		out[k] = clonePlacementRule(v)
	}
	return s.file.Save(modelPlacementFile{Workers: out})
}

func clonePlacementRule(r config.ModelPlacementRule) config.ModelPlacementRule {
	r.AllowedModels = append([]string(nil), r.AllowedModels...)
	r.DeniedModels = append([]string(nil), r.DeniedModels...)
	return r
}
