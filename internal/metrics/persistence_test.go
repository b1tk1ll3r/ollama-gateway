package metrics

import (
	"path/filepath"
	"testing"
	"time"
)

func TestPersistentMetricsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.json")
	a := New()
	a.Record("ollama", 200, 25*time.Millisecond, 2*time.Second, 123, 45, 7.25, 1000, 2000)
	a.Record("openai", 500, time.Second, 3*time.Second, 5, 1, 1.5, 10, 20)
	a.DropUsage()
	if err := a.SavePersistent(path); err != nil {
		t.Fatal(err)
	}
	b := New()
	if err := b.LoadPersistent(path); err != nil {
		t.Fatal(err)
	}
	x := b.SnapshotPersistent()
	if x.Prompt != 128 || x.Completion != 46 || x.Credits != 8.75 || x.BytesIn != 1010 || x.BytesOut != 2020 || x.UsageDropped != 1 {
		t.Fatalf("restored=%#v", x)
	}
	if x.Requests["ollama|2xx"] != 1 || x.Requests["openai|5xx"] != 1 || x.Errors["openai|5xx"] != 1 {
		t.Fatalf("counters=%#v errors=%#v", x.Requests, x.Errors)
	}
}
