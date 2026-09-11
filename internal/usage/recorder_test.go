package usage

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/cost"
)

func TestRecorderAggregatesByEffectiveActor(t *testing.T) {
	r, err := New("", 8, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Record(Event{Time: time.Now(), Tenant: "t", Subject: "shared-key-subject", Actor: "app:svc-a", ActualCredits: 2})
	if got := r.Actor(context.Background(), "t", "app:svc-a"); got.Requests != 1 || got.Credits != 2 {
		t.Fatalf("actor summary=%#v", got)
	}
	if got := r.Actor(context.Background(), "t", "shared-key-subject"); got.Requests != 0 {
		t.Fatalf("subject unexpectedly used as actor: %#v", got)
	}
}

func TestRecorderReplaysPersistentJournal(t *testing.T) {
	dir := t.TempDir()
	r, err := New(dir, 32, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	r.Record(Event{ID: "a", Time: now, Tenant: "team", Subject: "user", Actor: "user", ActualCredits: 3.5, Usage: cost.Usage{PromptTokens: 100, CompletionTokens: 20}})
	r.Close()
	r2, err := New(dir, 32, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	s := r2.Global()
	if s.Requests != 1 || s.PromptTokens != 100 || s.CompletionTokens != 20 || s.Credits != 3.5 {
		t.Fatalf("summary=%#v", s)
	}
	recent := r2.Recent(10)
	if len(recent) != 1 || recent[0].ID != "a" {
		t.Fatalf("recent=%#v", recent)
	}
}

func TestFlushPersistsBufferedEvent(t *testing.T) {
	dir := t.TempDir()
	r, err := New(dir, 8, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	r.Record(Event{ID: "flush-1", Time: time.Now().UTC(), Tenant: "t", Subject: "s", Actor: "s", Status: 200})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "usage-*.jsonl"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("journal paths: %v %v", paths, err)
	}
	b, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"id":"flush-1"`)) {
		t.Fatalf("event not flushed: %s", b)
	}
}

func TestRetentionCompactsDetailToDailyAndPreservesTotals(t *testing.T) {
	dir := t.TempDir()
	r, err := NewWithRetention(dir, 32, 10*time.Millisecond, RetentionConfig{DetailDays: 30, DailyDays: 400, CompactionInterval: time.Hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().AddDate(0, 0, -5).Truncate(24 * time.Hour)
	r.Record(Event{ID: "old-a", Time: base, Tenant: "team-a", Actor: "user-a", Application: "openwebui", Model: "qwen3:8b", Worker: "gpu-a", Status: 200, ActualCredits: 4.5, QueueMS: 12, ServiceMS: 500, Usage: cost.Usage{PromptTokens: 1000, CompletionTokens: 200, PromptEvalNS: int64(time.Second), EvalNS: int64(2 * time.Second)}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	future := base.AddDate(0, 0, 40)
	st, err := r.CompactAt(ctx, future)
	if err != nil {
		t.Fatal(err)
	}
	if st.LastRawCompacted != 1 {
		t.Fatalf("raw compacted=%d", st.LastRawCompacted)
	}
	if paths, _ := filepath.Glob(filepath.Join(dir, "usage-*.jsonl")); len(paths) != 0 {
		t.Fatalf("raw journals remain: %v", paths)
	}
	daily, _ := filepath.Glob(filepath.Join(dir, "rollups", "daily", "rollup-daily-*.json"))
	if len(daily) != 1 {
		t.Fatalf("daily rollups=%v", daily)
	}
	if got := r.Global(); got.Requests != 1 || got.PromptTokens != 1000 || got.Credits != 4.5 {
		t.Fatalf("live summary after compaction=%#v", got)
	}
	r.Close()

	// A restart must reconstruct all-time totals from the rollup even though the
	// request-level journal no longer exists.
	r2, err := NewWithRetention(dir, 32, time.Second, RetentionConfig{DetailDays: 30, DailyDays: 400, CompactionInterval: time.Hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	got := r2.Global()
	if got.Requests != 1 || got.PromptTokens != 1000 || got.CompletionTokens != 200 || got.Credits != 4.5 {
		t.Fatalf("replayed summary=%#v", got)
	}
	if recent := r2.Recent(10); len(recent) != 0 {
		t.Fatalf("compacted detail unexpectedly replayed: %#v", recent)
	}
	pts := r2.Series("daily", "tenant", "team-a", 30)
	if len(pts) != 1 || pts[0].Requests != 1 || pts[0].OutputTPS != 100 {
		t.Fatalf("daily points=%#v", pts)
	}
}

func TestRetentionFoldsDailyIntoIdempotentMonthlyRollup(t *testing.T) {
	dir := t.TempDir()
	r, err := NewWithRetention(dir, 32, 10*time.Millisecond, RetentionConfig{DetailDays: 2, DailyDays: 4, CompactionInterval: time.Hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
	r.Record(Event{ID: "m-a", Time: base, Tenant: "t", Actor: "a", Model: "m", Worker: "w", Status: 500, ActualCredits: 2, Usage: cost.Usage{PromptTokens: 50, CompletionTokens: 10, EvalNS: int64(time.Second)}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	future := base.AddDate(0, 0, 10)
	if _, err := r.CompactAt(ctx, future); err != nil {
		t.Fatal(err)
	}
	monthly, _ := filepath.Glob(filepath.Join(dir, "rollups", "monthly", "rollup-monthly-*.json"))
	if len(monthly) != 1 {
		t.Fatalf("monthly rollups=%v", monthly)
	}
	if daily, _ := filepath.Glob(filepath.Join(dir, "rollups", "daily", "*.json")); len(daily) != 0 {
		t.Fatalf("daily remains=%v", daily)
	}
	// Running compaction again must not duplicate the monthly contribution.
	if _, err := r.CompactAt(ctx, future); err != nil {
		t.Fatal(err)
	}
	r.Close()
	r2, err := NewWithRetention(dir, 32, time.Second, RetentionConfig{DetailDays: 2, DailyDays: 4, CompactionInterval: time.Hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if got := r2.Global(); got.Requests != 1 || got.Credits != 2 {
		t.Fatalf("monthly replay duplicated: %#v", got)
	}
	pts := r2.Series("monthly", "global", "", 12)
	if len(pts) != 1 || pts[0].Requests != 1 || pts[0].Errors != 1 {
		t.Fatalf("monthly points=%#v", pts)
	}
}

func TestRetentionExpiresMonthlyRollupsAndUpdatesAllTimeTotals(t *testing.T) {
	dir := t.TempDir()
	cfg := RetentionConfig{DetailDays: 1, DailyDays: 2, MonthlyMonths: 2, CompactionInterval: time.Hour}
	r, err := NewWithRetention(dir, 32, 10*time.Millisecond, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	r.Record(Event{ID: "jan", Time: base, Tenant: "team", Actor: "alice", Application: "ui", Model: "qwen3:8b", Worker: "gpu", Status: 200, ActualCredits: 5, Usage: cost.Usage{PromptTokens: 100, CompletionTokens: 20}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// By April, January is older than the configured two-month monthly window.
	if _, err := r.CompactAt(ctx, time.Date(2026, 4, 20, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(dir, "rollups", "monthly", "rollup-monthly-*.json")); len(files) != 0 {
		t.Fatalf("expired monthly rollup still present: %v", files)
	}
	if got := r.Global(); got.Requests != 0 || got.PromptTokens != 0 || got.Credits != 0 {
		t.Fatalf("expired monthly data still counted in all-time totals: %#v", got)
	}
	r.Close()

	r2, err := NewWithRetention(dir, 32, time.Second, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if got := r2.Global(); got.Requests != 0 || got.Credits != 0 {
		t.Fatalf("expired monthly data replayed after restart: %#v", got)
	}
}

func TestRetentionSeriesDimensions(t *testing.T) {
	dir := t.TempDir()
	r, err := NewWithRetention(dir, 32, 10*time.Millisecond, RetentionConfig{DetailDays: 1, DailyDays: 100, CompactionInterval: time.Hour}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	base := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	r.Record(Event{ID: "dims", Time: base, Tenant: "tenant-a", Actor: "actor-a", Application: "openwebui", Model: "gemma4:e4b", Worker: "rtx-4090", Status: 200, ActualCredits: 3, Usage: cost.Usage{PromptTokens: 200, CompletionTokens: 50, PromptEvalNS: int64(time.Second), EvalNS: int64(time.Second)}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CompactAt(ctx, base.AddDate(0, 0, 10)); err != nil {
		t.Fatal(err)
	}
	checks := []struct{ dim, name string }{
		{"tenant", "tenant-a"},
		{"actor", "tenant-a\x00actor-a"},
		{"application", "openwebui"},
		{"model", "gemma4:e4b"},
		{"worker", "rtx-4090"},
	}
	for _, tc := range checks {
		pts := r.Series("daily", tc.dim, tc.name, 10)
		if len(pts) != 1 || pts[0].Requests != 1 || pts[0].Credits != 3 {
			t.Fatalf("series %s/%q = %#v", tc.dim, tc.name, pts)
		}
	}
}
