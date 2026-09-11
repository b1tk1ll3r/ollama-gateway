package liveflow

import (
	"testing"
	"time"

	"github.com/example/ollama-fair-gateway/internal/cost"
)

func TestLifecycle(t *testing.T) {
	tr := New(time.Second, 10)
	tr.Begin(Request{ID: "r1", Tenant: "a", Actor: "u", Model: "m", EstimatedCredits: 2})
	snap := tr.Snapshot()
	if len(snap.Requests) != 1 || snap.Requests[0].State != StateQueued {
		t.Fatalf("unexpected initial snapshot: %#v", snap)
	}
	tr.MarkRouting("r1", "w1", 25*time.Millisecond)
	tr.MarkRunning("r1")
	tr.Progress("r1", 160, cost.Usage{PromptTokens: 12})
	snap = tr.Snapshot()
	if got := snap.Requests[0]; got.State != StateStreaming || got.Worker != "w1" || got.PromptTokens != 12 || got.CompletionTokens == 0 {
		t.Fatalf("unexpected progress: %#v", got)
	}
	tr.Finish("r1", 200, 1.25, cost.Usage{PromptTokens: 12, CompletionTokens: 8}, 50*time.Millisecond)
	snap = tr.Snapshot()
	if got := snap.Requests[0]; got.State != StateCompleted || got.Status != 200 || got.ActualCredits != 1.25 || got.CompletionTokens != 8 {
		t.Fatalf("unexpected final: %#v", got)
	}
}

func TestChangedNotifies(t *testing.T) {
	tr := New(time.Second, 10)
	ch := tr.Changed()
	tr.Begin(Request{ID: "r"})
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("tracker did not notify")
	}
}

func TestSnapshotIsBoundedButCountsRemainExact(t *testing.T) {
	tr := New(time.Second, 2)
	tr.Begin(Request{ID: "a"})
	tr.Begin(Request{ID: "b"})
	tr.Begin(Request{ID: "c"})
	s := tr.Snapshot()
	if !s.Truncated || len(s.Requests) != 2 || s.Counts.Active != 3 || s.Counts.Queued != 3 {
		t.Fatalf("unexpected bounded snapshot: %#v", s)
	}
}
