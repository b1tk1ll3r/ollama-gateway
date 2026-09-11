package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"time"
)

var ErrNotFound = errors.New("session not found")

type Store interface {
	Create(context.Context, string, time.Duration) (string, error)
	Get(context.Context, string) (string, error)
	Delete(context.Context, string) error
	Health(context.Context) error
}

type memoryEntry struct {
	token string
	exp   time.Time
}
type Memory struct {
	mu sync.Mutex
	m  map[string]memoryEntry
}

func NewMemory() *Memory { return &Memory{m: map[string]memoryEntry{}} }
func (m *Memory) Create(_ context.Context, token string, ttl time.Duration) (string, error) {
	id := newID()
	m.mu.Lock()
	m.m[id] = memoryEntry{token: token, exp: time.Now().Add(ttl)}
	if len(m.m) > 4096 {
		now := time.Now()
		for k, v := range m.m {
			if now.After(v.exp) {
				delete(m.m, k)
			}
		}
	}
	m.mu.Unlock()
	return id, nil
}
func (m *Memory) Get(_ context.Context, id string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.m[id]
	if !ok || time.Now().After(e.exp) {
		delete(m.m, id)
		return "", ErrNotFound
	}
	return e.token, nil
}
func (m *Memory) Delete(_ context.Context, id string) error {
	m.mu.Lock()
	delete(m.m, id)
	m.mu.Unlock()
	return nil
}
func (m *Memory) Health(context.Context) error { return nil }

func newID() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
