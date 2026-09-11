package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/example/ollama-fair-gateway/internal/auth"
)

type APIKeyStore struct {
	mu      sync.Mutex
	file    AtomicJSON
	records map[string]auth.StoredAPIKey
}

type apiKeyFile struct {
	Keys []auth.StoredAPIKey `json:"keys"`
}

func NewAPIKeyStore(path string) (*APIKeyStore, error) {
	s := &APIKeyStore{file: AtomicJSON{Path: path, Mode: 0600}, records: map[string]auth.StoredAPIKey{}}
	var f apiKeyFile
	if err := s.file.Load(&f); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, r := range f.Keys {
		if r.ID != "" {
			s.records[r.ID] = r
		}
	}
	return s, nil
}
func (s *APIKeyStore) Load() ([]auth.StoredAPIKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]auth.StoredAPIKey, 0, len(s.records))
	for _, r := range s.records {
		r.Scopes = append([]string(nil), r.Scopes...)
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
func (s *APIKeyStore) Put(r auth.StoredAPIKey) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[r.ID]; ok {
		return errors.New("API key id already exists")
	}
	s.records[r.ID] = r
	if err := s.saveLocked(); err != nil {
		delete(s.records, r.ID)
		return err
	}
	return nil
}
func (s *APIKeyStore) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.records[id]
	if !ok {
		return nil
	}
	delete(s.records, id)
	if err := s.saveLocked(); err != nil {
		s.records[id] = old
		return err
	}
	return nil
}
func (s *APIKeyStore) saveLocked() error {
	a := make([]auth.StoredAPIKey, 0, len(s.records))
	for _, r := range s.records {
		a = append(a, r)
	}
	sort.Slice(a, func(i, j int) bool { return a[i].CreatedAt.Before(a[j].CreatedAt) })
	return s.file.Save(apiKeyFile{Keys: a})
}
func (s *APIKeyStore) Health(context.Context) error {
	if err := os.MkdirAll(filepath.Dir(s.file.Path), 0750); err != nil {
		return err
	}
	f, err := os.OpenFile(s.file.Path+".health", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_ = f.Close()
	return os.Remove(s.file.Path + ".health")
}
