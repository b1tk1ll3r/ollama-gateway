package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type WorkerRuntimeStore struct {
	mu    sync.Mutex
	file  AtomicJSON
	modes map[string]string
}

type workerRuntimeFile struct {
	Workers map[string]string `json:"workers"`
}

func NewWorkerRuntimeStore(path string) (*WorkerRuntimeStore, error) {
	s := &WorkerRuntimeStore{file: AtomicJSON{Path: path, Mode: 0600}, modes: map[string]string{}}
	var f workerRuntimeFile
	if err := s.file.Load(&f); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for k, v := range f.Workers {
		if k != "" {
			s.modes[k] = v
		}
	}
	return s, nil
}
func (s *WorkerRuntimeStore) List(context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]string{}
	for k, v := range s.modes {
		out[k] = v
	}
	return out, nil
}
func (s *WorkerRuntimeStore) Put(_ context.Context, name, mode string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.modes[name]
	s.modes[name] = mode
	if err := s.saveLocked(); err != nil {
		if had {
			s.modes[name] = old
		} else {
			delete(s.modes, name)
		}
		return err
	}
	return nil
}
func (s *WorkerRuntimeStore) Delete(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.modes[name]
	delete(s.modes, name)
	if err := s.saveLocked(); err != nil {
		if had {
			s.modes[name] = old
		}
		return err
	}
	return nil
}
func (s *WorkerRuntimeStore) saveLocked() error {
	keys := make([]string, 0, len(s.modes))
	for k := range s.modes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m := map[string]string{}
	for _, k := range keys {
		m[k] = s.modes[k]
	}
	return s.file.Save(workerRuntimeFile{Workers: m})
}
func (s *WorkerRuntimeStore) Health(context.Context) error {
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
func (s *WorkerRuntimeStore) Path() string { return s.file.Path }
