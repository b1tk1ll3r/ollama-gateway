package usage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/example/ollama-fair-gateway/internal/cost"
)

type Event struct {
	ID               string     `json:"id"`
	Time             time.Time  `json:"time"`
	Tenant           string     `json:"tenant"`
	Subject          string     `json:"subject"`
	Actor            string     `json:"actor"`
	Application      string     `json:"application,omitempty"`
	ServiceClass     string     `json:"service_class,omitempty"`
	AuthType         string     `json:"auth_type"`
	ClientIP         string     `json:"client_ip,omitempty"`
	API              string     `json:"api"`
	Path             string     `json:"path"`
	Model            string     `json:"model,omitempty"`
	Worker           string     `json:"worker,omitempty"`
	Status           int        `json:"status"`
	QueueMS          int64      `json:"queue_ms"`
	ServiceMS        int64      `json:"service_ms"`
	EstimatedCredits float64    `json:"estimated_credits"`
	ActualCredits    float64    `json:"actual_credits"`
	Usage            cost.Usage `json:"usage"`
	BytesIn          int64      `json:"bytes_in"`
	BytesOut         int64      `json:"bytes_out"`
}

type Summary struct {
	Requests         uint64    `json:"requests"`
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	Credits          float64   `json:"credits"`
	QueueMS          int64     `json:"queue_ms"`
	ServiceMS        int64     `json:"service_ms"`
	LastRequest      time.Time `json:"last_request"`
}

type Recorder struct {
	dir       string
	journalCh chan Event
	flush     time.Duration
	drop      func()
	mu        sync.RWMutex
	byActor   map[string]Summary
	byTenant  map[string]Summary
	global    Summary
	recent    []Event
	recentCap int

	retention       RetentionConfig
	compactMu       sync.Mutex
	retentionMu     sync.RWMutex
	retentionStatus RetentionStatus
	rollupMu        sync.RWMutex
	dailyAgg        map[string]RollupData
	monthlyAgg      map[string]RollupData
	loaded          bool

	closeOnce sync.Once
	closeCh   chan struct{}
	flushCh   chan chan error
	wg        sync.WaitGroup
}

func New(dir string, buffer int, flush time.Duration, drop func()) (*Recorder, error) {
	return NewWithRetention(dir, buffer, flush, RetentionConfig{}, drop)
}

func NewWithRetention(dir string, buffer int, flush time.Duration, retention RetentionConfig, drop func()) (*Recorder, error) {
	retention = normalizeRetention(retention)
	r := &Recorder{dir: dir, journalCh: make(chan Event, buffer), flush: flush, drop: drop, byActor: map[string]Summary{}, byTenant: map[string]Summary{}, recentCap: 10000, closeCh: make(chan struct{}), flushCh: make(chan chan error), retention: retention, dailyAgg: map[string]RollupData{}, monthlyAgg: map[string]RollupData{}}
	if dir != "" {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return nil, err
		}
		// Compact before replay so startup does not load request-level history that
		// is already outside the configured detail retention window.
		if _, err := r.compactDisk(context.Background(), time.Now().UTC()); err != nil {
			return nil, fmt.Errorf("usage retention startup compaction: %w", err)
		}
		if err := r.loadRollupsForReplay(); err != nil {
			return nil, fmt.Errorf("replay usage rollups: %w", err)
		}
		if err := r.replayExisting(); err != nil {
			return nil, err
		}
		r.loaded = true
		r.wg.Add(2)
		go r.journalLoop()
		go r.retentionLoop()
	}
	return r, nil
}
func (r *Recorder) apply(e Event) {
	actor := eventActor(e)
	ak := e.Tenant + "\x00" + actor
	r.byActor[ak] = add(r.byActor[ak], e)
	r.byTenant[e.Tenant] = add(r.byTenant[e.Tenant], e)
	r.global = add(r.global, e)
	day := e.Time.UTC().Format("2006-01-02")
	r.rollupMu.Lock()
	d := r.dailyAgg[day]
	if d.Tenants == nil {
		d = newRollupData()
	}
	d.addEvent(e)
	r.dailyAgg[day] = d
	r.rollupMu.Unlock()
	if r.recentCap > 0 {
		if len(r.recent) >= r.recentCap {
			copy(r.recent, r.recent[len(r.recent)-r.recentCap+1:])
			r.recent = r.recent[:r.recentCap-1]
		}
		r.recent = append(r.recent, e)
	}
}

func (r *Recorder) Record(e Event) {
	r.mu.Lock()
	r.apply(e)
	r.mu.Unlock()
	if r.dir != "" {
		select {
		case r.journalCh <- e:
		default:
			r.dropped()
		}
	}
}
func (r *Recorder) dropped() {
	if r.drop != nil {
		r.drop()
	}
}
func eventActor(e Event) string {
	if e.Actor != "" {
		return e.Actor
	}
	return e.Subject
}

func add(s Summary, e Event) Summary {
	s.Requests++
	s.PromptTokens += e.Usage.PromptTokens
	s.CompletionTokens += e.Usage.CompletionTokens
	s.Credits += e.ActualCredits
	s.QueueMS += e.QueueMS
	s.ServiceMS += e.ServiceMS
	s.LastRequest = e.Time
	return s
}
func (r *Recorder) localActor(tenant, subject string) Summary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byActor[tenant+"\x00"+subject]
}
func (r *Recorder) localTenant(tenant string) Summary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byTenant[tenant]
}
func (r *Recorder) Actor(_ context.Context, tenant, subject string) Summary {
	return r.localActor(tenant, subject)
}
func (r *Recorder) Tenant(_ context.Context, tenant string) Summary {
	return r.localTenant(tenant)
}

type NamedSummary struct {
	Name    string  `json:"name"`
	Summary Summary `json:"summary"`
}

func (r *Recorder) SetRecentCapacity(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n < 0 {
		n = 0
	}
	r.recentCap = n
	if n == 0 {
		r.recent = nil
	} else if len(r.recent) > n {
		r.recent = append([]Event(nil), r.recent[len(r.recent)-n:]...)
	}
}

func (r *Recorder) Recent(limit int) []Event {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if limit <= 0 || limit > len(r.recent) {
		limit = len(r.recent)
	}
	out := make([]Event, limit)
	for i := 0; i < limit; i++ {
		out[i] = r.recent[len(r.recent)-1-i]
	}
	return out
}

func (r *Recorder) Global() Summary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.global
}

func (r *Recorder) LocalTenants() []NamedSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NamedSummary, 0, len(r.byTenant))
	for name, summary := range r.byTenant {
		out = append(out, NamedSummary{Name: name, Summary: summary})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Summary.Credits == out[j].Summary.Credits {
			return out[i].Name < out[j].Name
		}
		return out[i].Summary.Credits > out[j].Summary.Credits
	})
	return out
}

func (r *Recorder) LocalActors(tenant string) []NamedSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	prefix := tenant + "\x00"
	out := []NamedSummary{}
	for key, summary := range r.byActor {
		if strings.HasPrefix(key, prefix) {
			out = append(out, NamedSummary{Name: strings.TrimPrefix(key, prefix), Summary: summary})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Summary.Credits == out[j].Summary.Credits {
			return out[i].Name < out[j].Name
		}
		return out[i].Summary.Credits > out[j].Summary.Credits
	})
	return out
}

func (r *Recorder) journalLoop() {
	defer r.wg.Done()
	var f *os.File
	var bw *bufio.Writer
	day := ""
	ticker := time.NewTicker(r.flush)
	defer ticker.Stop()
	open := func(now time.Time) error {
		d := now.UTC().Format("2006-01-02")
		if d == day && f != nil {
			return nil
		}
		if bw != nil {
			_ = bw.Flush()
		}
		if f != nil {
			_ = f.Sync()
			_ = f.Close()
		}
		path := filepath.Join(r.dir, "usage-"+d+".jsonl")
		nf, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
		if err != nil {
			return err
		}
		f = nf
		bw = bufio.NewWriterSize(f, 256<<10)
		day = d
		return nil
	}
	flushClose := func() {
		if bw != nil {
			_ = bw.Flush()
		}
		if f != nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}
	defer flushClose()
	for {
		select {
		case e := <-r.journalCh:
			if err := open(e.Time); err != nil {
				r.dropped()
				continue
			}
			b, _ := json.Marshal(e)
			_, _ = bw.Write(b)
			_ = bw.WriteByte('\n')
		case <-ticker.C:
			if bw != nil {
				_ = bw.Flush()
			}
			if f != nil {
				_ = f.Sync()
			}
		case ack := <-r.flushCh:
			// A flush is a barrier for events that were already queued before the
			// request. Since journalCh and flushCh are independent channels, drain
			// the event queue explicitly before flushing the buffered writer.
			for draining := true; draining; {
				select {
				case e := <-r.journalCh:
					if err := open(e.Time); err != nil {
						r.dropped()
						continue
					}
					b, _ := json.Marshal(e)
					_, _ = bw.Write(b)
					_ = bw.WriteByte('\n')
				default:
					draining = false
				}
			}
			var err error
			if bw != nil {
				err = bw.Flush()
			}
			if err == nil && f != nil {
				err = f.Sync()
			}
			ack <- err
		case <-r.closeCh:
			for {
				select {
				case e := <-r.journalCh:
					if err := open(e.Time); err == nil {
						b, _ := json.Marshal(e)
						_, _ = bw.Write(b)
						_ = bw.WriteByte('\n')
					}
				default:
					return
				}
			}
		}
	}
}
func (r *Recorder) replayExisting() error {
	paths, err := filepath.Glob(filepath.Join(r.dir, "usage-*.jsonl"))
	if err != nil {
		return err
	}
	sort.Strings(paths)
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("replay usage journal %s: %w", path, err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64<<10), 2<<20)
		for sc.Scan() {
			var e Event
			if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
				continue
			} // tolerate a partial/crashed final line
			r.apply(e)
		}
		err = sc.Err()
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("replay usage journal %s: %w", path, err)
		}
	}
	return nil
}

func (r *Recorder) Flush(ctx context.Context) error {
	if r == nil || r.dir == "" {
		return nil
	}
	ack := make(chan error, 1)
	select {
	case r.flushCh <- ack:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Recorder) Close() {
	if r == nil || r.dir == "" {
		return
	}
	r.closeOnce.Do(func() { close(r.closeCh); r.wg.Wait() })
}

func (r *Recorder) Health(context.Context) error {
	if r.dir == "" {
		return nil
	}
	test := filepath.Join(r.dir, ".health")
	f, err := os.OpenFile(test, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("usage journal: %w", err)
	}
	_ = f.Close()
	_ = os.Remove(test)
	return nil
}
