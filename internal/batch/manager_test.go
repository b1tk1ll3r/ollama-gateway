package batch

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/config"
)

func batchTestConfig() config.BatchJobsConfig {
	return config.BatchJobsConfig{Enabled: true, Retention: config.Duration(time.Hour), MaxJobs: 100, MaxConcurrent: 1, MaxInputBytes: 1 << 20}
}

func waitState(t *testing.T, m *Manager, id, state string) Job {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		j, ok := m.Get(id, "t", "u", false)
		if ok && j.State == state {
			return j
		}
		time.Sleep(5 * time.Millisecond)
	}
	j, _ := m.Get(id, "t", "u", false)
	t.Fatalf("job %s did not reach %s; got %#v", id, state, j)
	return Job{}
}

func TestCreateRunPersistAndOutput(t *testing.T) {
	dir := t.TempDir()
	meta := filepath.Join(dir, "batch-jobs.json")
	spool := filepath.Join(dir, "batch")
	m, err := New(batchTestConfig(), meta, spool)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx, func(ctx context.Context, j Job, in io.Reader, out io.Writer) RunResult {
		b, _ := io.ReadAll(in)
		if string(b) != `{"model":"m","input":"x"}` {
			t.Errorf("input=%s", b)
		}
		io.WriteString(out, `{"id":"ok"}`)
		return RunResult{HTTPStatus: 200, ResponseContentType: "application/json", RequestID: "req-1"}
	})
	j, err := m.Create(IdentitySnapshot{Tenant: "t", Subject: "u", Actor: "u", AuthType: "oidc"}, "/v1/responses", "m", []byte(`{"model":"m","input":"x"}`))
	if err != nil {
		t.Fatal(err)
	}
	j = waitState(t, m, j.ID, StateCompleted)
	if j.OutputRef == "" || j.ExecutionRequestID != "req-1" || j.HTTPStatus != 200 {
		t.Fatalf("job=%#v", j)
	}
	f, _, err := m.OpenOutput(j.ID, "t", "u", false)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(f)
	f.Close()
	if string(b) != `{"id":"ok"}` {
		t.Fatalf("output=%s", b)
	}

	m2, err := New(batchTestConfig(), meta, spool)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := m2.Get(j.ID, "t", "u", false)
	if !ok || got.State != StateCompleted {
		t.Fatalf("reloaded=%#v ok=%v", got, ok)
	}
	if _, ok := m2.Get(j.ID, "t", "other", false); ok {
		t.Fatal("cross-actor job lookup must be hidden")
	}
}

func TestPauseResumeAndCancel(t *testing.T) {
	dir := t.TempDir()
	m, err := New(batchTestConfig(), filepath.Join(dir, "jobs.json"), filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var attempts atomic.Int32
	m.Start(ctx, func(ctx context.Context, j Job, in io.Reader, out io.Writer) RunResult {
		n := attempts.Add(1)
		if n == 1 {
			<-ctx.Done()
			return RunResult{HTTPStatus: 499, Error: context.Cause(ctx).Error()}
		}
		io.WriteString(out, "done")
		return RunResult{HTTPStatus: 200, RequestID: "req-done"}
	})
	j, err := m.Create(IdentitySnapshot{Tenant: "t", Subject: "u", Actor: "u", AuthType: "oidc"}, "/api/chat", "m", []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, m, j.ID, StateRunning)
	if _, err := m.Pause(j.ID, "t", "u", false); err != nil {
		t.Fatal(err)
	}
	waitState(t, m, j.ID, StatePaused)
	if _, err := m.Resume(j.ID, "t", "u", false); err != nil {
		t.Fatal(err)
	}
	waitState(t, m, j.ID, StateCompleted)
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}

	j2, err := m.Create(IdentitySnapshot{Tenant: "t", Subject: "u", Actor: "u", AuthType: "oidc"}, "/api/chat", "m", []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	// The runner completes quickly on later attempts, so cancel while queued by
	// first pausing it synchronously.
	if _, err := m.Pause(j2.ID, "t", "u", false); err != nil && err != ErrInvalidState {
		t.Fatal(err)
	}
	cur, _ := m.Get(j2.ID, "t", "u", false)
	if cur.State == StatePaused {
		if _, err := m.Cancel(j2.ID, "t", "u", false); err != nil {
			t.Fatal(err)
		}
		waitState(t, m, j2.ID, StateCancelled)
	}
}

func TestRetentionDeletesContentFiles(t *testing.T) {
	dir := t.TempDir()
	cfg := batchTestConfig()
	cfg.Retention = config.Duration(time.Minute)
	m, err := New(cfg, filepath.Join(dir, "jobs.json"), filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }
	j, err := m.Create(IdentitySnapshot{Tenant: "t", Subject: "u", Actor: "u", AuthType: "oidc"}, "/api/chat", "m", []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Cancel(j.ID, "t", "u", false); err != nil {
		t.Fatal(err)
	}
	job, _ := m.Get(j.ID, "t", "u", false)
	inputPath := m.refPath(job.InputRef)
	if _, err := os.Stat(inputPath); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if err := m.Compact(); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get(j.ID, "t", "u", false); ok {
		t.Fatal("expired job retained")
	}
	if _, err := os.Stat(inputPath); !os.IsNotExist(err) {
		t.Fatalf("input still exists: %v", err)
	}
}

func TestInputLimit(t *testing.T) {
	dir := t.TempDir()
	cfg := batchTestConfig()
	cfg.MaxInputBytes = 4
	m, _ := New(cfg, filepath.Join(dir, "jobs.json"), filepath.Join(dir, "spool"))
	_, err := m.Create(IdentitySnapshot{Tenant: "t", Actor: "u"}, "/api/chat", "m", []byte(strings.Repeat("x", 5)))
	if err != ErrInputTooLarge {
		t.Fatalf("err=%v", err)
	}
}

func TestWaitPersistsRestartSafeStateAfterShutdown(t *testing.T) {
	dir := t.TempDir()
	m, err := New(batchTestConfig(), filepath.Join(dir, "jobs.json"), filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithCancel(context.Background())
	m.Start(root, func(ctx context.Context, j Job, in io.Reader, out io.Writer) RunResult {
		<-ctx.Done()
		return RunResult{HTTPStatus: 499, Error: context.Cause(ctx).Error()}
	})
	j, err := m.Create(IdentitySnapshot{Tenant: "t", Subject: "u", Actor: "u", AuthType: "oidc"}, "/api/chat", "m", []byte(`{"model":"m"}`))
	if err != nil {
		t.Fatal(err)
	}
	waitState(t, m, j.ID, StateRunning)
	cancel()
	wctx, wcancel := context.WithTimeout(context.Background(), time.Second)
	defer wcancel()
	if err := m.Wait(wctx); err != nil {
		t.Fatal(err)
	}
	got, ok := m.Get(j.ID, "t", "u", false)
	if !ok || got.State != StateQueued || !strings.Contains(got.Error, "shutdown") {
		t.Fatalf("restart-safe state not persisted: %#v ok=%v", got, ok)
	}
	m2, err := New(batchTestConfig(), filepath.Join(dir, "jobs.json"), filepath.Join(dir, "spool"))
	if err != nil {
		t.Fatal(err)
	}
	reloaded, ok := m2.Get(j.ID, "t", "u", false)
	if !ok || reloaded.State != StateQueued {
		t.Fatalf("reloaded state=%#v ok=%v", reloaded, ok)
	}
}
