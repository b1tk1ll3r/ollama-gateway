package quota

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestQuotaPersistenceRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "quota.json")
	a := NewMemory()
	lim := Limits{ActorCreditsPerMinute: 60, ActorBurstCredits: 120, TenantCreditsPerMinute: 300, TenantBurstCredits: 600}
	d, err := a.Reserve(context.Background(), "team", "user", 50, lim)
	if err != nil || !d.Allowed {
		t.Fatalf("reserve allowed=%v err=%v", d.Allowed, err)
	}
	if err := a.SavePersistent(path); err != nil {
		t.Fatal(err)
	}
	b := NewMemory()
	if err := b.LoadPersistent(path); err != nil {
		t.Fatal(err)
	}
	d2, err := b.Reserve(context.Background(), "team", "user", 80, lim)
	if err != nil {
		t.Fatal(err)
	}
	// Actor started with 120, spent 50, so an immediate 80-credit request
	// remains over budget after restart (apart from negligible refill time).
	if d2.Allowed {
		t.Fatalf("quota reset across restart: %#v", d2)
	}
	if d2.RetryAfter <= 0 || d2.RetryAfter > 20*time.Second {
		t.Fatalf("unexpected retry: %v", d2.RetryAfter)
	}
}
