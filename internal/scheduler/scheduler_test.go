package scheduler

import (
	"context"
	"testing"
	"time"
)

type acquired struct {
	name  string
	lease *Lease
	err   error
}

func waitQueued(t *testing.T, s *Local, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.Stats(context.Background()).Queued == int64(n) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue never reached %d", n)
}

func TestLocalFairnessWithinTenant(t *testing.T) {
	s := NewLocal(1, 20, 20)
	ctx := context.Background()
	first, err := s.Acquire(ctx, Request{Tenant: "T", Actor: "T/A", Cost: 1, TenantWeight: 1, ActorWeight: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan acquired, 2)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "T", Actor: "T/A", Cost: 10, TenantWeight: 1, ActorWeight: 1, Timeout: time.Second})
		ch <- acquired{"A-heavy", l, e}
	}()
	waitQueued(t, s, 1)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "T", Actor: "T/B", Cost: 1, TenantWeight: 1, ActorWeight: 1, Timeout: time.Second})
		ch <- acquired{"B-small", l, e}
	}()
	waitQueued(t, s, 2)
	first.Release()
	got := <-ch
	if got.err != nil {
		t.Fatal(got.err)
	}
	defer got.lease.Release()
	if got.name != "B-small" {
		t.Fatalf("expected small second actor first, got %s", got.name)
	}
}

func TestLocalTenantFairness(t *testing.T) {
	s := NewLocal(1, 20, 20)
	ctx := context.Background()
	first, err := s.Acquire(ctx, Request{Tenant: "A", Actor: "A/u", Cost: 1, TenantWeight: 1, ActorWeight: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan acquired, 2)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "A", Actor: "A/u", Cost: 1, TenantWeight: 1, ActorWeight: 1, Timeout: time.Second})
		ch <- acquired{"A2", l, e}
	}()
	waitQueued(t, s, 1)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "B", Actor: "B/u", Cost: 1, TenantWeight: 1, ActorWeight: 1, Timeout: time.Second})
		ch <- acquired{"B1", l, e}
	}()
	waitQueued(t, s, 2)
	first.Release()
	got := <-ch
	if got.err != nil {
		t.Fatal(got.err)
	}
	defer got.lease.Release()
	if got.name != "B1" {
		t.Fatalf("expected other tenant next, got %s", got.name)
	}
}

func TestActorQueueIsolatedByTenant(t *testing.T) {
	s := NewLocal(1, 20, 1)
	ctx := context.Background()
	first, err := s.Acquire(ctx, Request{Tenant: "root", Actor: "holder", Cost: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	ch := make(chan error, 2)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "A", Actor: "same", Cost: 1, Timeout: time.Second})
		if l != nil {
			l.Release()
		}
		ch <- e
	}()
	waitQueued(t, s, 1)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "B", Actor: "same", Cost: 1, Timeout: time.Second})
		if l != nil {
			l.Release()
		}
		ch <- e
	}()
	waitQueued(t, s, 2)
	first.Release()
	for i := 0; i < 2; i++ {
		if e := <-ch; e != nil {
			t.Fatalf("tenant-isolated actor queue failed: %v", e)
		}
	}
}

func TestCancelRemovesQueuedTicketImmediately(t *testing.T) {
	s := NewLocal(1, 20, 20)
	holder, err := s.Acquire(context.Background(), Request{Tenant: "A", Actor: "holder", Cost: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		l, err := s.Acquire(ctx, Request{Tenant: "B", Actor: "user", Cost: 1, Timeout: time.Second})
		if l != nil {
			l.Release()
		}
		done <- err
	}()
	waitQueued(t, s, 1)
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("err=%v want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled Acquire did not return")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.Stats(context.Background()).Queued == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("queue still contains cancelled ticket: %+v", s.Stats(context.Background()))
}

func TestServiceClassCapDoesNotHeadOfLineBlock(t *testing.T) {
	s := NewLocal(2, 20, 20)
	ctx := context.Background()
	bg, err := s.Acquire(ctx, Request{Tenant: "T", Actor: "bg-holder", Cost: 1, ServiceClass: "background", ClassWeight: 1, ClassMaxConcurrent: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer bg.Release()
	sys, err := s.Acquire(ctx, Request{Tenant: "T", Actor: "sys-holder", Cost: 1, ServiceClass: "system", ClassWeight: 1, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	ch := make(chan acquired, 2)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "T", Actor: "bg2", Cost: 1, ServiceClass: "background", ClassWeight: 10, ClassMaxConcurrent: 1, Timeout: time.Second})
		ch <- acquired{"background", l, e}
	}()
	waitQueued(t, s, 1)
	go func() {
		l, e := s.Acquire(ctx, Request{Tenant: "T", Actor: "chat", Cost: 1, ServiceClass: "interactive", ClassWeight: 1, Timeout: time.Second})
		ch <- acquired{"interactive", l, e}
	}()
	waitQueued(t, s, 2)

	sys.Release()
	select {
	case got := <-ch:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if got.name != "interactive" {
			t.Fatalf("expected interactive to bypass saturated background class, got %s", got.name)
		}
		got.lease.Release()
	case <-time.After(time.Second):
		t.Fatal("interactive request was head-of-line blocked")
	}
}
