package quota

import (
	"context"
	"sync"
	"time"
)

type Limits struct {
	ActorCreditsPerMinute  float64
	ActorBurstCredits      float64
	TenantCreditsPerMinute float64
	TenantBurstCredits     float64
}

type Reservation struct {
	Tenant, Actor string
	Amount        float64
	Limits        Limits
}
type Decision struct {
	Allowed         bool
	RetryAfter      time.Duration
	RemainingActor  float64
	RemainingTenant float64
	Reservation     Reservation
}
type Ledger interface {
	Reserve(context.Context, string, string, float64, Limits) (Decision, error)
	Reconcile(context.Context, Reservation, float64) error
	Health(context.Context) error
}

// Disabled permits all requests.
type Disabled struct{}

func (Disabled) Reserve(_ context.Context, t, a string, amt float64, l Limits) (Decision, error) {
	return Decision{Allowed: true, Reservation: Reservation{Tenant: t, Actor: a, Amount: amt, Limits: l}}, nil
}
func (Disabled) Reconcile(context.Context, Reservation, float64) error { return nil }
func (Disabled) Health(context.Context) error                          { return nil }

type bucket struct {
	balance float64
	updated time.Time
}
type Memory struct {
	mu      sync.Mutex
	buckets map[string]bucket
}

func NewMemory() *Memory                       { return &Memory{buckets: map[string]bucket{}} }
func (m *Memory) Health(context.Context) error { return nil }
func (m *Memory) Reserve(_ context.Context, tenant, actor string, amount float64, l Limits) (Decision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	ab, _ := m.refill("a:"+tenant+":"+actor, now, l.ActorCreditsPerMinute, l.ActorBurstCredits)
	tb, _ := m.refill("t:"+tenant, now, l.TenantCreditsPerMinute, l.TenantBurstCredits)
	allowed := (l.ActorCreditsPerMinute <= 0 || ab >= amount) && (l.TenantCreditsPerMinute <= 0 || tb >= amount)
	retry := time.Duration(0)
	if l.ActorCreditsPerMinute > 0 && ab < amount {
		retry = maxDur(retry, time.Duration((amount-ab)/(l.ActorCreditsPerMinute/60)*float64(time.Second)))
	}
	if l.TenantCreditsPerMinute > 0 && tb < amount {
		retry = maxDur(retry, time.Duration((amount-tb)/(l.TenantCreditsPerMinute/60)*float64(time.Second)))
	}
	if allowed {
		if l.ActorCreditsPerMinute > 0 {
			ab -= amount
			m.buckets["a:"+tenant+":"+actor] = bucket{ab, now}
		}
		if l.TenantCreditsPerMinute > 0 {
			tb -= amount
			m.buckets["t:"+tenant] = bucket{tb, now}
		}
		retry = 0
	}
	return Decision{Allowed: allowed, RetryAfter: retry, RemainingActor: ab, RemainingTenant: tb, Reservation: Reservation{tenant, actor, amount, l}}, nil
}
func (m *Memory) refill(key string, now time.Time, rateMin, cap float64) (float64, time.Duration) {
	if rateMin <= 0 {
		return 1e18, 0
	}
	if cap <= 0 {
		cap = rateMin
	}
	b, ok := m.buckets[key]
	if !ok {
		return cap, 0
	}
	bal := b.balance + now.Sub(b.updated).Minutes()*rateMin
	if bal > cap {
		bal = cap
	}
	wait := time.Duration(0)
	if bal < 0 {
		wait = time.Duration((-bal) / (rateMin / 60) * float64(time.Second))
	}
	return bal, wait
}
func (m *Memory) Reconcile(_ context.Context, r Reservation, actual float64) error {
	delta := r.Amount - actual
	if delta == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, x := range []struct {
		key       string
		rate, cap float64
	}{{"a:" + r.Tenant + ":" + r.Actor, r.Limits.ActorCreditsPerMinute, r.Limits.ActorBurstCredits}, {"t:" + r.Tenant, r.Limits.TenantCreditsPerMinute, r.Limits.TenantBurstCredits}} {
		if x.rate <= 0 {
			continue
		}
		bal, _ := m.refill(x.key, now, x.rate, x.cap)
		bal += delta
		if x.cap <= 0 {
			x.cap = x.rate
		}
		if bal > x.cap {
			bal = x.cap
		}
		m.buckets[x.key] = bucket{bal, now}
	}
	return nil
}

func maxDur(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}
