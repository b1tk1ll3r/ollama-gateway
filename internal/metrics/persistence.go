package metrics

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"time"

	"github.com/example/ollama-fair-gateway/internal/state"
)

type histogramState struct {
	Buckets []float64 `json:"buckets"`
	Counts  []uint64  `json:"counts"`
	Sum     float64   `json:"sum"`
	Total   uint64    `json:"total"`
}

type PersistentState struct {
	Version          int               `json:"version"`
	SavedAt          time.Time         `json:"saved_at"`
	Requests         map[string]uint64 `json:"requests"`
	Errors           map[string]uint64 `json:"errors"`
	Queue            histogramState    `json:"queue"`
	Service          histogramState    `json:"service"`
	Prompt           uint64            `json:"prompt_tokens"`
	Completion       uint64            `json:"completion_tokens"`
	Credits          float64           `json:"credits"`
	BytesIn          uint64            `json:"bytes_in"`
	BytesOut         uint64            `json:"bytes_out"`
	UsageDropped     uint64            `json:"usage_dropped"`
	UpstreamFailures map[string]uint64 `json:"upstream_failures,omitempty"`
	Retries          map[string]uint64 `json:"retries,omitempty"`
	CircuitOpens     map[string]uint64 `json:"circuit_opens,omitempty"`
	CircuitResets    map[string]uint64 `json:"circuit_resets,omitempty"`
}

func histogramSnapshot(h *histogram) histogramState {
	x := histogramState{Buckets: append([]float64(nil), h.buckets...), Counts: make([]uint64, len(h.counts)), Sum: atomicLoadFloat(&h.sum), Total: h.total.Load()}
	for i := range h.counts {
		x.Counts[i] = h.counts[i].Load()
	}
	return x
}
func (r *Registry) SnapshotPersistent() PersistentState {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := PersistentState{Version: 1, SavedAt: time.Now().UTC(), Requests: map[string]uint64{}, Errors: map[string]uint64{}, Queue: histogramSnapshot(r.queue), Service: histogramSnapshot(r.service), Prompt: r.prompt.Load(), Completion: r.completion.Load(), Credits: atomicLoadFloat(&r.creditsBits), BytesIn: r.bytesIn.Load(), BytesOut: r.bytesOut.Load(), UsageDropped: r.usageDropped.Load(), UpstreamFailures: map[string]uint64{}, Retries: map[string]uint64{}, CircuitOpens: map[string]uint64{}, CircuitResets: map[string]uint64{}}
	for k, v := range r.requests {
		s.Requests[k] = v.Load()
	}
	for k, v := range r.errors {
		s.Errors[k] = v.Load()
	}
	for k, v := range r.upstreamFailures {
		s.UpstreamFailures[k] = v.Load()
	}
	for k, v := range r.retries {
		s.Retries[k] = v.Load()
	}
	for k, v := range r.circuitOpens {
		s.CircuitOpens[k] = v.Load()
	}
	for k, v := range r.circuitResets {
		s.CircuitResets[k] = v.Load()
	}
	return s
}
func restoreHistogram(dst *histogram, src histogramState) {
	if len(src.Counts) != len(dst.counts) {
		return
	}
	for i := range dst.counts {
		dst.counts[i].Store(src.Counts[i])
	}
	dst.total.Store(src.Total)
	dst.sum.Store(mathFloat64bits(src.Sum))
}
func (r *Registry) RestorePersistent(s PersistentState) {
	if s.Version != 1 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = map[string]*atomic.Uint64{}
	for k, v := range s.Requests {
		x := &atomic.Uint64{}
		x.Store(v)
		r.requests[k] = x
	}
	r.errors = map[string]*atomic.Uint64{}
	for k, v := range s.Errors {
		x := &atomic.Uint64{}
		x.Store(v)
		r.errors[k] = x
	}
	restoreMap := func(src map[string]uint64) map[string]*atomic.Uint64 {
		out := map[string]*atomic.Uint64{}
		for k, v := range src {
			x := &atomic.Uint64{}
			x.Store(v)
			out[k] = x
		}
		return out
	}
	r.upstreamFailures = restoreMap(s.UpstreamFailures)
	r.retries = restoreMap(s.Retries)
	r.circuitOpens = restoreMap(s.CircuitOpens)
	r.circuitResets = restoreMap(s.CircuitResets)
	restoreHistogram(r.queue, s.Queue)
	restoreHistogram(r.service, s.Service)
	r.prompt.Store(s.Prompt)
	r.completion.Store(s.Completion)
	r.creditsBits.Store(mathFloat64bits(s.Credits))
	r.bytesIn.Store(s.BytesIn)
	r.bytesOut.Store(s.BytesOut)
	r.usageDropped.Store(s.UsageDropped)
}

func (r *Registry) LoadPersistent(path string) error {
	var s PersistentState
	err := (state.AtomicJSON{Path: path, Mode: 0640}).Load(&s)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	r.RestorePersistent(s)
	return nil
}
func (r *Registry) SavePersistent(path string) error {
	return (state.AtomicJSON{Path: path, Mode: 0640}).Save(r.SnapshotPersistent())
}

func (r *Registry) StartPersistence(ctx context.Context, path string, interval time.Duration, onError func(error)) {
	if interval < time.Second {
		interval = 10 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		defer func() {
			if err := r.SavePersistent(path); err != nil && onError != nil {
				onError(err)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := r.SavePersistent(path); err != nil && onError != nil {
					onError(err)
				}
			}
		}
	}()
}
