package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

var ErrQueueFull = errors.New("scheduler queue full")
var ErrActorQueueFull = errors.New("actor queue full")

type Request struct {
	Tenant, Actor             string
	Cost                      float64
	TenantWeight, ActorWeight float64
	Timeout                   time.Duration
	ServiceClass              string
	ClassWeight               float64
	ClassMaxConcurrent        int
}

type ClassStats struct {
	Queued  int64 `json:"queued"`
	Running int64 `json:"running"`
}

type Stats struct {
	Queued     int64                 `json:"queued"`
	Running    int64                 `json:"running"`
	OldestWait time.Duration         `json:"oldest_wait,omitempty"`
	Classes    map[string]ClassStats `json:"classes,omitempty"`
}

type Scheduler interface {
	Acquire(context.Context, Request) (*Lease, error)
	Stats(context.Context) Stats
	Health(context.Context) error
}

type Lease struct {
	Wait    time.Duration
	release func()
	once    sync.Once
}

func (l *Lease) Release() {
	if l != nil && l.release != nil {
		l.once.Do(l.release)
	}
}

// Local implements two-level hierarchical weighted fair queueing. Tenants are
// the root fairness boundary. Within a tenant, actor virtual finish time is
// adjusted by an optional service-class weight. Class concurrency caps are
// global across tenants; saturated classes are skipped instead of causing
// head-of-line blocking for other classes.
type localTicket struct {
	ctx         context.Context
	req         Request
	enqueued    time.Time
	actorFinish float64
	cancelled   bool
	dispatched  bool
	ready       chan struct{}
	index       int
}

type ticketHeap []*localTicket

func (h ticketHeap) Len() int { return len(h) }
func (h ticketHeap) Less(i, j int) bool {
	if h[i].actorFinish == h[j].actorFinish {
		return h[i].enqueued.Before(h[j].enqueued)
	}
	return h[i].actorFinish < h[j].actorFinish
}
func (h ticketHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *ticketHeap) Push(x any)   { t := x.(*localTicket); t.index = len(*h); *h = append(*h, t) }
func (h *ticketHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	t.index = -1
	*h = old[:n-1]
	return t
}

type tenantState struct {
	queue     ticketHeap
	service   float64
	rootScore float64
	active    bool
}

type Local struct {
	mu                   sync.Mutex
	tenants              map[string]*tenantState
	lastActor            map[string]float64
	actorQueued          map[string]int
	classQueued          map[string]int
	classRunning         map[string]int
	queued, running      int
	maxRunning, maxQueue int
	maxActor             int
	virtualTime          float64
	wake                 chan struct{}
}

func NewLocal(maxRunning, maxQueue, maxActor int) *Local {
	l := &Local{maxRunning: maxRunning, maxQueue: maxQueue, maxActor: maxActor, tenants: map[string]*tenantState{}, lastActor: map[string]float64{}, actorQueued: map[string]int{}, classQueued: map[string]int{}, classRunning: map[string]int{}, wake: make(chan struct{}, 1)}
	go l.loop()
	return l
}
func (l *Local) Health(context.Context) error { return nil }
func (l *Local) Stats(context.Context) Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	classes := map[string]ClassStats{}
	for k, q := range l.classQueued {
		classes[k] = ClassStats{Queued: int64(q), Running: int64(l.classRunning[k])}
	}
	for k, r := range l.classRunning {
		if _, ok := classes[k]; !ok {
			classes[k] = ClassStats{Running: int64(r)}
		}
	}
	var oldest time.Time
	for _, ts := range l.tenants {
		for _, ticket := range ts.queue {
			if ticket == nil || ticket.index < 0 {
				continue
			}
			if oldest.IsZero() || ticket.enqueued.Before(oldest) {
				oldest = ticket.enqueued
			}
		}
	}
	oldestWait := time.Duration(0)
	if !oldest.IsZero() {
		oldestWait = time.Since(oldest)
	}
	return Stats{Queued: int64(l.queued), Running: int64(l.running), OldestWait: oldestWait, Classes: classes}
}
func (l *Local) Acquire(ctx context.Context, r Request) (*Lease, error) {
	normalize(&r)
	t := &localTicket{ctx: ctx, req: r, enqueued: time.Now(), ready: make(chan struct{})}
	actorKey := r.Tenant + "\x00" + r.Actor
	l.mu.Lock()
	if l.queued >= l.maxQueue {
		l.mu.Unlock()
		return nil, ErrQueueFull
	}
	if l.actorQueued[actorKey] >= l.maxActor {
		l.mu.Unlock()
		return nil, ErrActorQueueFull
	}
	ts := l.tenants[r.Tenant]
	if ts == nil {
		ts = &tenantState{}
		heap.Init(&ts.queue)
		l.tenants[r.Tenant] = ts
	}
	base := math.Max(l.lastActor[actorKey], math.Max(ts.service, l.virtualTime))
	t.actorFinish = base + r.Cost/(r.ActorWeight*r.ClassWeight)
	l.lastActor[actorKey] = t.actorFinish
	heap.Push(&ts.queue, t)
	if !ts.active {
		ts.rootScore = math.Max(ts.service, l.virtualTime)
		ts.active = true
	}
	l.queued++
	l.actorQueued[actorKey]++
	l.classQueued[r.ServiceClass]++
	l.mu.Unlock()
	l.signal()

	var timer *time.Timer
	var timeout <-chan time.Time
	if r.Timeout > 0 {
		timer = time.NewTimer(r.Timeout)
		timeout = timer.C
		defer timer.Stop()
	}
	select {
	case <-t.ready:
		return &Lease{Wait: time.Since(t.enqueued), release: func() {
			l.mu.Lock()
			if l.running > 0 {
				l.running--
			}
			if l.classRunning[r.ServiceClass] > 0 {
				l.classRunning[r.ServiceClass]--
			}
			l.mu.Unlock()
			l.signal()
		}}, nil
	case <-ctx.Done():
		l.cancel(t)
		return nil, ctx.Err()
	case <-timeout:
		l.cancel(t)
		return nil, context.DeadlineExceeded
	}
}
func (l *Local) cancel(t *localTicket) {
	l.mu.Lock()
	if t.cancelled {
		l.mu.Unlock()
		return
	}
	t.cancelled = true
	if t.index >= 0 {
		if ts := l.tenants[t.req.Tenant]; ts != nil {
			heap.Remove(&ts.queue, t.index)
			l.decQueuedLocked(t)
			if ts.queue.Len() == 0 {
				ts.active = false
				delete(l.tenants, t.req.Tenant)
			}
		}
	} else if t.dispatched {
		// Acquire selected ctx.Done after dispatch but before consuming ready.
		// Reclaim the slot here because no Lease will be returned to release it.
		if l.running > 0 {
			l.running--
		}
		if l.classRunning[t.req.ServiceClass] > 0 {
			l.classRunning[t.req.ServiceClass]--
		}
	}
	l.mu.Unlock()
	l.signal()
}
func (l *Local) decQueuedLocked(t *localTicket) {
	if l.queued > 0 {
		l.queued--
	}
	actorKey := t.req.Tenant + "\x00" + t.req.Actor
	if l.actorQueued[actorKey] > 0 {
		l.actorQueued[actorKey]--
	}
	if l.classQueued[t.req.ServiceClass] > 0 {
		l.classQueued[t.req.ServiceClass]--
	}
}
func (l *Local) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}
func (l *Local) loop() {
	for range l.wake {
		for {
			l.mu.Lock()
			if l.running >= l.maxRunning || l.queued == 0 {
				l.mu.Unlock()
				break
			}
			name, ts := l.nextTenantLocked()
			if ts == nil {
				l.mu.Unlock()
				break
			}
			t := l.nextTicketLocked(name, ts)
			if t == nil {
				l.mu.Unlock()
				continue
			}
			base := math.Max(ts.rootScore, l.virtualTime)
			ts.service = base + t.req.Cost/t.req.TenantWeight
			l.virtualTime = math.Max(l.virtualTime, base)
			if ts.queue.Len() > 0 {
				ts.rootScore = ts.service
			} else {
				ts.active = false
			}
			t.dispatched = true
			l.running++
			l.classRunning[t.req.ServiceClass]++
			close(t.ready)
			l.mu.Unlock()
		}
	}
}
func (l *Local) classEligibleLocked(t *localTicket) bool {
	if t.cancelled || t.ctx.Err() != nil {
		return false
	}
	return t.req.ClassMaxConcurrent <= 0 || l.classRunning[t.req.ServiceClass] < t.req.ClassMaxConcurrent
}
func (l *Local) bestEligibleIndexLocked(ts *tenantState) int {
	best := -1
	for i, t := range ts.queue {
		if !l.classEligibleLocked(t) {
			continue
		}
		if best < 0 || ts.queue.Less(i, best) {
			best = i
		}
	}
	return best
}
func (l *Local) nextTenantLocked() (string, *tenantState) {
	var bestName string
	var best *tenantState
	var bestTicket *localTicket
	for name, ts := range l.tenants {
		if !ts.active || ts.queue.Len() == 0 {
			continue
		}
		idx := l.bestEligibleIndexLocked(ts)
		if idx < 0 {
			continue
		}
		t := ts.queue[idx]
		if best == nil || ts.rootScore < best.rootScore || (ts.rootScore == best.rootScore && t.enqueued.Before(bestTicket.enqueued)) {
			bestName, best, bestTicket = name, ts, t
		}
	}
	return bestName, best
}
func (l *Local) nextTicketLocked(name string, ts *tenantState) *localTicket {
	for ts.queue.Len() > 0 {
		idx := l.bestEligibleIndexLocked(ts)
		if idx < 0 {
			return nil
		}
		t := heap.Remove(&ts.queue, idx).(*localTicket)
		l.decQueuedLocked(t)
		if t.cancelled || t.ctx.Err() != nil {
			continue
		}
		return t
	}
	ts.active = false
	delete(l.tenants, name)
	return nil
}

func normalize(r *Request) {
	if r.TenantWeight <= 0 {
		r.TenantWeight = 1
	}
	if r.ActorWeight <= 0 {
		r.ActorWeight = 1
	}
	if r.ClassWeight <= 0 {
		r.ClassWeight = 1
	}
	if r.ServiceClass == "" {
		r.ServiceClass = "default"
	}
	if r.Cost <= 0 {
		r.Cost = .001
	}
}
