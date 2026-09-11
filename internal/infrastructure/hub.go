package infrastructure

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
	"github.com/example/ollama-fair-gateway/internal/liveflow"
	"github.com/example/ollama-fair-gateway/internal/scheduler"
	"github.com/example/ollama-fair-gateway/internal/worker"
)

type Gateway struct {
	NodeID     string    `json:"node_id"`
	NodeName   string    `json:"node_name"`
	Hostname   string    `json:"hostname"`
	StartedAt  time.Time `json:"started_at"`
	LastSeen   time.Time `json:"last_seen"`
	Queued     int64     `json:"queued"`
	Running    int64     `json:"running"`
	LiveActive int       `json:"live_active"`
	Workers    int       `json:"workers"`
	Healthy    bool      `json:"healthy"`
}

type Request struct {
	liveflow.Request
	GatewayID   string `json:"gateway_id"`
	GatewayName string `json:"gateway_name"`
}

type Worker struct {
	worker.Snapshot
	Gateways []string `json:"gateways"`
}

type Counts struct {
	Gateways  int `json:"gateways"`
	Workers   int `json:"workers"`
	Models    int `json:"models"`
	Active    int `json:"active"`
	Queued    int `json:"queued"`
	Routing   int `json:"routing"`
	Running   int `json:"running"`
	Streaming int `json:"streaming"`
}

type Snapshot struct {
	GeneratedAt time.Time `json:"generated_at"`
	Version     uint64    `json:"version"`
	Mode        string    `json:"mode"`
	Queue       int64     `json:"queue"`
	Running     int64     `json:"running"`
	Counts      Counts    `json:"counts"`
	Gateways    []Gateway `json:"gateways"`
	Workers     []Worker  `json:"workers"`
	Requests    []Request `json:"requests"`
}

type Hub struct {
	cfg       config.InfrastructureConfig
	live      *liveflow.Tracker
	sched     scheduler.Scheduler
	workers   *worker.Pool
	startedAt time.Time
	nodeID    string
	nodeName  string
	hostname  string

	mu      sync.RWMutex
	version uint64
	notify  chan struct{}
}

func New(cfg config.InfrastructureConfig, live *liveflow.Tracker, sched scheduler.Scheduler, workers *worker.Pool) *Hub {
	host, _ := os.Hostname()
	if host == "" {
		host = "gateway"
	}
	id := cfg.NodeID
	if id == "" {
		var b [6]byte
		_, _ = rand.Read(b[:])
		id = host + "-" + hex.EncodeToString(b[:])
	}
	name := cfg.NodeName
	if name == "" {
		name = host
	}
	return &Hub{cfg: cfg, live: live, sched: sched, workers: workers, startedAt: time.Now().UTC(), nodeID: id, nodeName: name, hostname: host, notify: make(chan struct{})}
}

func (h *Hub) NodeID() string   { return h.nodeID }
func (h *Hub) NodeName() string { return h.nodeName }

func (h *Hub) Start(ctx context.Context) {
	interval := h.cfg.RefreshInterval.Value()
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		changed := h.live.Changed()
		for {
			select {
			case <-ctx.Done():
				return
			case <-changed:
				changed = h.live.Changed()
				h.bump()
			case <-t.C:
				h.bump()
			}
		}
	}()
}

func (h *Hub) bump() {
	h.mu.Lock()
	h.version++
	close(h.notify)
	h.notify = make(chan struct{})
	h.mu.Unlock()
}

func (h *Hub) Changed() <-chan struct{} {
	h.mu.RLock()
	ch := h.notify
	h.mu.RUnlock()
	return ch
}

func (h *Hub) Snapshot() Snapshot {
	now := time.Now().UTC()
	live := h.live.Snapshot()
	if max := h.cfg.MaxRequests; max > 0 && len(live.Requests) > max {
		active := make([]liveflow.Request, 0, max)
		recent := make([]liveflow.Request, 0, max)
		for _, r := range live.Requests {
			if r.State == liveflow.StateCompleted || r.State == liveflow.StateFailed {
				recent = append(recent, r)
			} else {
				active = append(active, r)
			}
		}
		if len(active) >= max {
			live.Requests = active[:max]
		} else {
			need := max - len(active)
			if need > len(recent) {
				need = len(recent)
			}
			live.Requests = append(active, recent[len(recent)-need:]...)
		}
	}
	st := h.sched.Stats(context.Background())
	ws := h.workers.Snapshots()
	h.mu.RLock()
	version := h.version
	h.mu.RUnlock()

	out := Snapshot{GeneratedAt: now, Version: version, Mode: "in-memory", Queue: st.Queued, Running: st.Running}
	out.Gateways = []Gateway{{NodeID: h.nodeID, NodeName: h.nodeName, Hostname: h.hostname, StartedAt: h.startedAt, LastSeen: now, Queued: st.Queued, Running: st.Running, LiveActive: live.Counts.Active, Workers: len(ws), Healthy: true}}
	modelSet := map[string]bool{}
	for _, w := range ws {
		out.Workers = append(out.Workers, Worker{Snapshot: w, Gateways: []string{h.nodeName}})
		for _, m := range w.LoadedModels {
			if m.Name != "" {
				modelSet[m.Name] = true
			}
		}
	}
	for _, r := range live.Requests {
		out.Requests = append(out.Requests, Request{Request: r, GatewayID: h.nodeID, GatewayName: h.nodeName})
		switch r.State {
		case liveflow.StateQueued:
			out.Counts.Queued++
		case liveflow.StateRouting:
			out.Counts.Routing++
		case liveflow.StateRunning:
			out.Counts.Running++
		case liveflow.StateStreaming:
			out.Counts.Streaming++
		}
		if r.State != liveflow.StateCompleted && r.State != liveflow.StateFailed {
			out.Counts.Active++
		}
	}
	sort.Slice(out.Workers, func(i, j int) bool { return out.Workers[i].Name < out.Workers[j].Name })
	sort.Slice(out.Requests, func(i, j int) bool { return out.Requests[i].QueuedAt.Before(out.Requests[j].QueuedAt) })
	out.Counts.Gateways = 1
	out.Counts.Workers = len(out.Workers)
	out.Counts.Models = len(modelSet)
	return out
}
