package quota

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/example/ollama-fair-gateway/internal/state"
)

type BucketState struct {
	Balance float64   `json:"balance"`
	Updated time.Time `json:"updated"`
}

type PersistentState struct {
	Version int                    `json:"version"`
	SavedAt time.Time              `json:"saved_at"`
	Buckets map[string]BucketState `json:"buckets"`
}

func (m *Memory) SnapshotPersistent() PersistentState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := PersistentState{Version: 1, SavedAt: time.Now().UTC(), Buckets: make(map[string]BucketState, len(m.buckets))}
	for k, b := range m.buckets {
		out.Buckets[k] = BucketState{Balance: b.balance, Updated: b.updated}
	}
	return out
}

func (m *Memory) RestorePersistent(s PersistentState) {
	if s.Version != 1 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets = make(map[string]bucket, len(s.Buckets))
	for k, b := range s.Buckets {
		m.buckets[k] = bucket{balance: b.Balance, updated: b.Updated}
	}
}

func (m *Memory) LoadPersistent(path string) error {
	var s PersistentState
	err := (state.AtomicJSON{Path: path, Mode: 0600}).Load(&s)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	m.RestorePersistent(s)
	return nil
}

func (m *Memory) SavePersistent(path string) error {
	return (state.AtomicJSON{Path: path, Mode: 0600}).Save(m.SnapshotPersistent())
}

func (m *Memory) StartPersistence(ctx context.Context, path string, interval time.Duration, onError func(error)) {
	if interval < time.Second {
		interval = 10 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		defer func() {
			if err := m.SavePersistent(path); err != nil && onError != nil {
				onError(err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := m.SavePersistent(path); err != nil && onError != nil {
					onError(err)
				}
			}
		}
	}()
}
