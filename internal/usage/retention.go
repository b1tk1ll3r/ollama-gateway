package usage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// RetentionConfig controls the tiered usage history. DetailDays keeps raw
// per-request JSONL journals. Older detail is converted to daily rollups.
// DailyDays is the maximum age of daily rollups; older days are folded into
// idempotent monthly rollups. MonthlyMonths=0 keeps monthly rollups forever.
type RetentionConfig struct {
	DetailDays         int
	DailyDays          int
	MonthlyMonths      int
	CompactionInterval time.Duration
}

type Aggregate struct {
	Requests           uint64    `json:"requests"`
	Errors             uint64    `json:"errors,omitempty"`
	PromptTokens       int64     `json:"prompt_tokens"`
	CompletionTokens   int64     `json:"completion_tokens"`
	CachedPromptTokens int64     `json:"cached_prompt_tokens,omitempty"`
	Credits            float64   `json:"credits"`
	QueueMS            int64     `json:"queue_ms"`
	ServiceMS          int64     `json:"service_ms"`
	PromptEvalNS       int64     `json:"prompt_eval_ns,omitempty"`
	EvalNS             int64     `json:"eval_ns,omitempty"`
	BytesIn            int64     `json:"bytes_in,omitempty"`
	BytesOut           int64     `json:"bytes_out,omitempty"`
	LastRequest        time.Time `json:"last_request,omitempty"`
}

func (a Aggregate) PromptTPS() float64 {
	if a.PromptEvalNS <= 0 {
		return 0
	}
	return float64(a.PromptTokens) / (float64(a.PromptEvalNS) / 1e9)
}
func (a Aggregate) OutputTPS() float64 {
	if a.EvalNS <= 0 {
		return 0
	}
	return float64(a.CompletionTokens) / (float64(a.EvalNS) / 1e9)
}
func (a *Aggregate) addEvent(e Event) {
	a.Requests++
	if e.Status >= 400 {
		a.Errors++
	}
	a.PromptTokens += e.Usage.PromptTokens
	a.CompletionTokens += e.Usage.CompletionTokens
	a.CachedPromptTokens += e.Usage.CachedPromptTokens
	a.Credits += e.ActualCredits
	a.QueueMS += e.QueueMS
	a.ServiceMS += e.ServiceMS
	a.PromptEvalNS += e.Usage.PromptEvalNS
	a.EvalNS += e.Usage.EvalNS
	a.BytesIn += e.BytesIn
	a.BytesOut += e.BytesOut
	if e.Time.After(a.LastRequest) {
		a.LastRequest = e.Time
	}
}
func (a *Aggregate) merge(b Aggregate) {
	a.Requests += b.Requests
	a.Errors += b.Errors
	a.PromptTokens += b.PromptTokens
	a.CompletionTokens += b.CompletionTokens
	a.CachedPromptTokens += b.CachedPromptTokens
	a.Credits += b.Credits
	a.QueueMS += b.QueueMS
	a.ServiceMS += b.ServiceMS
	a.PromptEvalNS += b.PromptEvalNS
	a.EvalNS += b.EvalNS
	a.BytesIn += b.BytesIn
	a.BytesOut += b.BytesOut
	if b.LastRequest.After(a.LastRequest) {
		a.LastRequest = b.LastRequest
	}
}

type RollupData struct {
	Global       Aggregate            `json:"global"`
	Tenants      map[string]Aggregate `json:"tenants,omitempty"`
	Actors       map[string]Aggregate `json:"actors,omitempty"`
	Applications map[string]Aggregate `json:"applications,omitempty"`
	Models       map[string]Aggregate `json:"models,omitempty"`
	Workers      map[string]Aggregate `json:"workers,omitempty"`
}

func newRollupData() RollupData {
	return RollupData{Tenants: map[string]Aggregate{}, Actors: map[string]Aggregate{}, Applications: map[string]Aggregate{}, Models: map[string]Aggregate{}, Workers: map[string]Aggregate{}}
}
func addMap(m map[string]Aggregate, key string, e Event) {
	if key == "" {
		return
	}
	x := m[key]
	x.addEvent(e)
	m[key] = x
}
func mergeMap(dst map[string]Aggregate, src map[string]Aggregate) {
	for k, v := range src {
		x := dst[k]
		x.merge(v)
		dst[k] = x
	}
}
func (d *RollupData) addEvent(e Event) {
	d.Global.addEvent(e)
	addMap(d.Tenants, e.Tenant, e)
	actor := eventActor(e)
	if e.Tenant != "" && actor != "" {
		addMap(d.Actors, e.Tenant+"\x00"+actor, e)
	}
	addMap(d.Applications, e.Application, e)
	addMap(d.Models, e.Model, e)
	addMap(d.Workers, e.Worker, e)
}
func (d *RollupData) merge(o RollupData) {
	d.Global.merge(o.Global)
	mergeMap(d.Tenants, o.Tenants)
	mergeMap(d.Actors, o.Actors)
	mergeMap(d.Applications, o.Applications)
	mergeMap(d.Models, o.Models)
	mergeMap(d.Workers, o.Workers)
}

type DailyRollup struct {
	Version     int        `json:"version"`
	Granularity string     `json:"granularity"`
	Day         string     `json:"day"`
	GeneratedAt time.Time  `json:"generated_at"`
	Data        RollupData `json:"data"`
}

type MonthlyRollup struct {
	Version     int       `json:"version"`
	Granularity string    `json:"granularity"`
	Month       string    `json:"month"`
	GeneratedAt time.Time `json:"generated_at"`
	// Days makes the monthly update idempotent if the process crashes after
	// writing the month but before removing the source daily file.
	Days map[string]RollupData `json:"days"`
}

func (m MonthlyRollup) Total() RollupData {
	out := newRollupData()
	keys := make([]string, 0, len(m.Days))
	for k := range m.Days {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out.merge(m.Days[k])
	}
	return out
}

type RollupPoint struct {
	Period           string  `json:"period"`
	Requests         uint64  `json:"requests"`
	Errors           uint64  `json:"errors"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Credits          float64 `json:"credits"`
	QueueMS          int64   `json:"queue_ms"`
	ServiceMS        int64   `json:"service_ms"`
	PromptTPS        float64 `json:"prompt_tps"`
	OutputTPS        float64 `json:"output_tps"`
}

func point(period string, a Aggregate) RollupPoint {
	return RollupPoint{Period: period, Requests: a.Requests, Errors: a.Errors, PromptTokens: a.PromptTokens, CompletionTokens: a.CompletionTokens, Credits: a.Credits, QueueMS: a.QueueMS, ServiceMS: a.ServiceMS, PromptTPS: a.PromptTPS(), OutputTPS: a.OutputTPS()}
}

type RetentionStatus struct {
	Enabled            bool      `json:"enabled"`
	DetailDays         int       `json:"detail_days"`
	DailyDays          int       `json:"daily_days"`
	MonthlyMonths      int       `json:"monthly_months"`
	CompactionInterval string    `json:"compaction_interval"`
	RawFiles           int       `json:"raw_files"`
	RawBytes           int64     `json:"raw_bytes"`
	DailyFiles         int       `json:"daily_files"`
	DailyBytes         int64     `json:"daily_bytes"`
	MonthlyFiles       int       `json:"monthly_files"`
	MonthlyBytes       int64     `json:"monthly_bytes"`
	LastCompaction     time.Time `json:"last_compaction,omitempty"`
	LastError          string    `json:"last_error,omitempty"`
	LastReclaimedBytes int64     `json:"last_reclaimed_bytes,omitempty"`
	LastRawCompacted   int       `json:"last_raw_compacted,omitempty"`
	LastDailyCompacted int       `json:"last_daily_compacted,omitempty"`
}

func normalizeRetention(c RetentionConfig) RetentionConfig {
	if c.DetailDays <= 0 {
		c.DetailDays = 30
	}
	if c.DailyDays <= 0 {
		c.DailyDays = 400
	}
	if c.DailyDays < c.DetailDays {
		c.DailyDays = c.DetailDays
	}
	if c.CompactionInterval <= 0 {
		c.CompactionInterval = 6 * time.Hour
	}
	return c
}
func (r *Recorder) rollupDirs() (string, string) {
	return filepath.Join(r.dir, "rollups", "daily"), filepath.Join(r.dir, "rollups", "monthly")
}

func atomicJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0640); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	cerr := f.Close()
	if err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	if d, e := os.Open(filepath.Dir(path)); e == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func parseDayFromRaw(path string) (time.Time, bool) {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "usage-") || !strings.HasSuffix(base, ".jsonl") {
		return time.Time{}, false
	}
	s := strings.TrimSuffix(strings.TrimPrefix(base, "usage-"), ".jsonl")
	t, err := time.Parse("2006-01-02", s)
	return t, err == nil
}
func parseDayFromDaily(path string) (time.Time, bool) {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "rollup-daily-") || !strings.HasSuffix(base, ".json") {
		return time.Time{}, false
	}
	s := strings.TrimSuffix(strings.TrimPrefix(base, "rollup-daily-"), ".json")
	t, err := time.Parse("2006-01-02", s)
	return t, err == nil
}
func cutoffDays(now time.Time, days int) time.Time {
	today := time.Date(now.UTC().Year(), now.UTC().Month(), now.UTC().Day(), 0, 0, 0, 0, time.UTC)
	return today.AddDate(0, 0, -days+1)
}
func cutoffMonths(now time.Time, months int) time.Time {
	n := now.UTC()
	first := time.Date(n.Year(), n.Month(), 1, 0, 0, 0, 0, time.UTC)
	return first.AddDate(0, -months+1, 0)
}

func aggregateRaw(path string) (DailyRollup, error) {
	day, ok := parseDayFromRaw(path)
	if !ok {
		return DailyRollup{}, fmt.Errorf("invalid journal filename %s", path)
	}
	d := newRollupData()
	f, err := os.Open(path)
	if err != nil {
		return DailyRollup{}, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 2<<20)
	for sc.Scan() {
		var e Event
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			d.addEvent(e)
		}
	}
	if err := sc.Err(); err != nil {
		return DailyRollup{}, err
	}
	return DailyRollup{Version: 1, Granularity: "daily", Day: day.Format("2006-01-02"), GeneratedAt: time.Now().UTC(), Data: d}, nil
}

func (r *Recorder) compactDisk(ctx context.Context, now time.Time) (RetentionStatus, error) {
	if r.dir == "" {
		return RetentionStatus{}, nil
	}
	r.compactMu.Lock()
	defer r.compactMu.Unlock()
	c := r.retention
	dailyDir, monthlyDir := r.rollupDirs()
	_ = os.MkdirAll(dailyDir, 0750)
	_ = os.MkdirAll(monthlyDir, 0750)
	before := dirSize(r.dir)
	stat := RetentionStatus{Enabled: true, DetailDays: c.DetailDays, DailyDays: c.DailyDays, MonthlyMonths: c.MonthlyMonths, CompactionInterval: c.CompactionInterval.String()}
	detailCut := cutoffDays(now, c.DetailDays)
	raws, _ := filepath.Glob(filepath.Join(r.dir, "usage-*.jsonl"))
	sort.Strings(raws)
	for _, p := range raws {
		select {
		case <-ctx.Done():
			return stat, ctx.Err()
		default:
		}
		day, ok := parseDayFromRaw(p)
		if !ok || !day.Before(detailCut) {
			continue
		}
		roll, err := aggregateRaw(p)
		if err != nil {
			return stat, err
		}
		out := filepath.Join(dailyDir, "rollup-daily-"+roll.Day+".json")
		if err := atomicJSON(out, roll); err != nil {
			return stat, err
		}
		if err := os.Remove(p); err != nil {
			return stat, err
		}
		stat.LastRawCompacted++
	}
	dailyCut := cutoffDays(now, c.DailyDays)
	ds, _ := filepath.Glob(filepath.Join(dailyDir, "rollup-daily-*.json"))
	sort.Strings(ds)
	for _, p := range ds {
		select {
		case <-ctx.Done():
			return stat, ctx.Err()
		default:
		}
		day, ok := parseDayFromDaily(p)
		if !ok || !day.Before(dailyCut) {
			continue
		}
		var d DailyRollup
		if err := readJSON(p, &d); err != nil {
			return stat, err
		}
		month := day.Format("2006-01")
		mp := filepath.Join(monthlyDir, "rollup-monthly-"+month+".json")
		m := MonthlyRollup{Version: 1, Granularity: "monthly", Month: month, GeneratedAt: time.Now().UTC(), Days: map[string]RollupData{}}
		if err := readJSON(mp, &m); err != nil && !errors.Is(err, os.ErrNotExist) {
			return stat, err
		}
		if m.Days == nil {
			m.Days = map[string]RollupData{}
		}
		m.Version = 1
		m.Granularity = "monthly"
		m.Month = month
		m.GeneratedAt = time.Now().UTC()
		m.Days[d.Day] = d.Data
		if err := atomicJSON(mp, m); err != nil {
			return stat, err
		}
		if err := os.Remove(p); err != nil {
			return stat, err
		}
		stat.LastDailyCompacted++
	}
	if c.MonthlyMonths > 0 {
		cut := cutoffMonths(now, c.MonthlyMonths)
		ms, _ := filepath.Glob(filepath.Join(monthlyDir, "rollup-monthly-*.json"))
		for _, p := range ms {
			base := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(p), "rollup-monthly-"), ".json")
			mt, err := time.Parse("2006-01", base)
			if err != nil || !mt.Before(cut) {
				continue
			}
			var expired MonthlyRollup
			_ = readJSON(p, &expired)
			if err := os.Remove(p); err != nil {
				return stat, err
			}
			if r.loaded && expired.Days != nil {
				r.removeAggregate(expired.Total())
			}
		}
	}
	after := dirSize(r.dir)
	if before > after {
		stat.LastReclaimedBytes = before - after
	}
	stat.LastCompaction = time.Now().UTC()
	r.setRetentionStatus(stat)
	return stat, nil
}

func dirSize(dir string) int64 {
	var n int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info.Mode().IsRegular() {
			n += info.Size()
		}
		return nil
	})
	return n
}
func fileCounts(pattern string) (int, int64) {
	ps, _ := filepath.Glob(pattern)
	var n int64
	for _, p := range ps {
		if s, e := os.Stat(p); e == nil && s.Mode().IsRegular() {
			n += s.Size()
		}
	}
	return len(ps), n
}
func (r *Recorder) setRetentionStatus(s RetentionStatus) {
	r.retentionMu.Lock()
	r.retentionStatus = s
	r.retentionMu.Unlock()
}
func (r *Recorder) RetentionStatus() RetentionStatus {
	r.retentionMu.RLock()
	s := r.retentionStatus
	r.retentionMu.RUnlock()
	if r.dir == "" {
		return s
	}
	d, m := r.rollupDirs()
	s.RawFiles, s.RawBytes = fileCounts(filepath.Join(r.dir, "usage-*.jsonl"))
	s.DailyFiles, s.DailyBytes = fileCounts(filepath.Join(d, "rollup-daily-*.json"))
	s.MonthlyFiles, s.MonthlyBytes = fileCounts(filepath.Join(m, "rollup-monthly-*.json"))
	return s
}
func (r *Recorder) Compact(ctx context.Context) (RetentionStatus, error) {
	if r == nil || r.dir == "" {
		return RetentionStatus{}, nil
	}
	if err := r.Flush(ctx); err != nil {
		return r.RetentionStatus(), err
	}
	stat, err := r.compactDisk(ctx, time.Now().UTC())
	if err != nil {
		stat.LastError = err.Error()
		r.setRetentionStatus(stat)
		return stat, err
	}
	// Refresh only the rollup index used for historical charts. All-time
	// counters are already correct in memory and must not be applied twice.
	_ = r.reloadRollupIndex()
	return r.RetentionStatus(), nil
}
func (r *Recorder) retentionLoop() {
	defer r.wg.Done()
	t := time.NewTicker(r.retention.CompactionInterval)
	defer t.Stop()
	for {
		select {
		case <-r.closeCh:
			return
		case <-t.C:
			ctx, c := context.WithTimeout(context.Background(), 30*time.Minute)
			_, err := r.compactDisk(ctx, time.Now().UTC())
			if err == nil {
				_ = r.reloadRollupIndex()
			}
			c()
			if err != nil {
				st := r.RetentionStatus()
				st.LastError = err.Error()
				r.setRetentionStatus(st)
			}
		}
	}
}

func (r *Recorder) loadRollupsForReplay() error {
	dailyDir, monthlyDir := r.rollupDirs()
	ms, _ := filepath.Glob(filepath.Join(monthlyDir, "rollup-monthly-*.json"))
	sort.Strings(ms)
	for _, p := range ms {
		var m MonthlyRollup
		if err := readJSON(p, &m); err != nil {
			return err
		}
		d := m.Total()
		r.applyAggregate(d)
		r.monthlyAgg[m.Month] = d
	}
	ds, _ := filepath.Glob(filepath.Join(dailyDir, "rollup-daily-*.json"))
	sort.Strings(ds)
	for _, p := range ds {
		var d DailyRollup
		if err := readJSON(p, &d); err != nil {
			return err
		}
		r.applyAggregate(d.Data)
		r.dailyAgg[d.Day] = d.Data
	}
	return nil
}
func (r *Recorder) reloadRollupIndex() error {
	daily := map[string]RollupData{}
	monthly := map[string]RollupData{}
	dd, md := r.rollupDirs()
	ms, _ := filepath.Glob(filepath.Join(md, "rollup-monthly-*.json"))
	for _, p := range ms {
		var m MonthlyRollup
		if readJSON(p, &m) == nil {
			monthly[m.Month] = m.Total()
		}
	}
	ds, _ := filepath.Glob(filepath.Join(dd, "rollup-daily-*.json"))
	for _, p := range ds {
		var d DailyRollup
		if readJSON(p, &d) == nil {
			daily[d.Day] = d.Data
		}
	}
	// Preserve in-memory raw/detail days; replace only days that are now on disk rollups.
	r.rollupMu.Lock()
	for day, v := range r.dailyAgg {
		if _, ok := daily[day]; !ok {
			if t, e := time.Parse("2006-01-02", day); e == nil && !t.Before(cutoffDays(time.Now().UTC(), r.retention.DetailDays)) {
				daily[day] = v
			}
		}
	}
	r.dailyAgg = daily
	r.monthlyAgg = monthly
	r.rollupMu.Unlock()
	return nil
}
func (r *Recorder) applyAggregate(d RollupData) {
	r.global = mergeSummary(r.global, d.Global)
	for k, a := range d.Tenants {
		r.byTenant[k] = mergeSummary(r.byTenant[k], a)
	}
	for k, a := range d.Actors {
		r.byActor[k] = mergeSummary(r.byActor[k], a)
	}
}
func (r *Recorder) removeAggregate(d RollupData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.global = subtractSummary(r.global, d.Global)
	for k, a := range d.Tenants {
		x := subtractSummary(r.byTenant[k], a)
		if x.Requests == 0 {
			delete(r.byTenant, k)
		} else {
			r.byTenant[k] = x
		}
	}
	for k, a := range d.Actors {
		x := subtractSummary(r.byActor[k], a)
		if x.Requests == 0 {
			delete(r.byActor, k)
		} else {
			r.byActor[k] = x
		}
	}
}
func subtractSummary(s Summary, a Aggregate) Summary {
	if a.Requests >= s.Requests {
		s.Requests = 0
	} else {
		s.Requests -= a.Requests
	}
	s.PromptTokens -= a.PromptTokens
	if s.PromptTokens < 0 {
		s.PromptTokens = 0
	}
	s.CompletionTokens -= a.CompletionTokens
	if s.CompletionTokens < 0 {
		s.CompletionTokens = 0
	}
	s.Credits -= a.Credits
	if s.Credits < 0 {
		s.Credits = 0
	}
	s.QueueMS -= a.QueueMS
	if s.QueueMS < 0 {
		s.QueueMS = 0
	}
	s.ServiceMS -= a.ServiceMS
	if s.ServiceMS < 0 {
		s.ServiceMS = 0
	}
	if !a.LastRequest.IsZero() && a.LastRequest.Equal(s.LastRequest) {
		s.LastRequest = time.Time{}
	}
	return s
}
func mergeSummary(s Summary, a Aggregate) Summary {
	s.Requests += a.Requests
	s.PromptTokens += a.PromptTokens
	s.CompletionTokens += a.CompletionTokens
	s.Credits += a.Credits
	s.QueueMS += a.QueueMS
	s.ServiceMS += a.ServiceMS
	if a.LastRequest.After(s.LastRequest) {
		s.LastRequest = a.LastRequest
	}
	return s
}

func chooseAggregate(d RollupData, dimension, name string) Aggregate {
	switch dimension {
	case "tenant":
		return d.Tenants[name]
	case "actor":
		return d.Actors[name]
	case "application":
		return d.Applications[name]
	case "model":
		return d.Models[name]
	case "worker":
		return d.Workers[name]
	default:
		return d.Global
	}
}
func (r *Recorder) Series(granularity, dimension, name string, limit int) []RollupPoint {
	if limit <= 0 {
		limit = 90
	}
	if limit > 2000 {
		limit = 2000
	}
	r.rollupMu.RLock()
	defer r.rollupMu.RUnlock()
	src := r.dailyAgg
	if granularity == "monthly" {
		src = r.monthlyAgg
	}
	keys := make([]string, 0, len(src))
	for k := range src {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[len(keys)-limit:]
	}
	out := make([]RollupPoint, 0, len(keys))
	for _, k := range keys {
		out = append(out, point(k, chooseAggregate(src[k], dimension, name)))
	}
	return out
}

// CompactAt is intentionally exported for deterministic retention tests.
func (r *Recorder) CompactAt(ctx context.Context, now time.Time) (RetentionStatus, error) {
	if err := r.Flush(ctx); err != nil {
		return r.RetentionStatus(), err
	}
	s, e := r.compactDisk(ctx, now)
	if e == nil {
		_ = r.reloadRollupIndex()
	}
	return s, e
}
