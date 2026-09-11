package worker

import (
	"context"
	"errors"
	"os"
	"time"

	persiststate "github.com/example/ollama-fair-gateway/internal/state"
)

type PersistentPerformanceState struct {
	Version int                                    `json:"version"`
	SavedAt time.Time                              `json:"saved_at"`
	Workers map[string]map[string]ModelPerformance `json:"workers"`
}

func (p *Pool) SnapshotPerformance() PersistentPerformanceState {
	out := PersistentPerformanceState{Version: 1, SavedAt: time.Now().UTC(), Workers: map[string]map[string]ModelPerformance{}}
	for _, w := range p.workers {
		w.mu.RLock()
		models := make(map[string]ModelPerformance, len(w.performance))
		for model, perf := range w.performance {
			models[model] = ModelPerformance{Model: model, PromptTPS: perf.PromptTPS, OutputTPS: perf.OutputTPS, Samples: perf.Samples}
		}
		w.mu.RUnlock()
		if len(models) > 0 {
			out.Workers[w.cfg.Name] = models
		}
	}
	return out
}

func (p *Pool) RestorePerformance(s PersistentPerformanceState) {
	if s.Version != 1 {
		return
	}
	for workerName, models := range s.Workers {
		w := p.byName[workerName]
		if w == nil {
			continue
		}
		w.mu.Lock()
		for model, perf := range models {
			if model == "" || perf.Samples <= 0 {
				continue
			}
			w.performance[canonicalModel(model)] = performanceState{PromptTPS: perf.PromptTPS, OutputTPS: perf.OutputTPS, Samples: perf.Samples}
		}
		w.mu.Unlock()
	}
}

func (p *Pool) LoadPerformance(path string) error {
	var s PersistentPerformanceState
	err := (persiststate.AtomicJSON{Path: path, Mode: 0640}).Load(&s)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	p.RestorePerformance(s)
	return nil
}

func (p *Pool) SavePerformance(path string) error {
	return (persiststate.AtomicJSON{Path: path, Mode: 0640}).Save(p.SnapshotPerformance())
}

func (p *Pool) StartPerformancePersistence(ctx context.Context, path string, interval time.Duration, onError func(error)) {
	if interval < time.Second {
		interval = 10 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		defer func() {
			if err := p.SavePerformance(path); err != nil && onError != nil {
				onError(err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := p.SavePerformance(path); err != nil && onError != nil {
					onError(err)
				}
			}
		}
	}()
}
