package notifications

import (
	"sync"
	"testing"
	"time"

	"watchtower/internal/alerts"
)

func TestIntentUniquenessPolicyAndDeliveryAttempts(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	tr := alerts.Transition{ID: "transition", SeriesID: "series", Generation: 4, Kind: alerts.Open, At: now}
	p := New()
	policy := Policy{OnOpen: true, OnRecovery: true, Audience: "support-operations"}
	i, created := p.Plan(tr, policy)
	if !created || i.ID != IntentID(tr) {
		t.Fatalf("intent=%+v created=%v", i, created)
	}
	i2, created := p.Plan(tr, policy)
	if created || i2.ID != i.ID || len(p.Intents()) != 1 {
		t.Fatalf("duplicate=%+v %v", i2, created)
	}
	if _, ok := p.Plan(alerts.Transition{ID: "disabled", Kind: alerts.Recovery}, Policy{OnOpen: true}); ok {
		t.Fatal("disabled recovery planned")
	}
	if _, ok := p.Plan(alerts.Transition{ID: "unknown", Kind: alerts.TransitionKind(99)}, policy); ok {
		t.Fatal("unknown transition planned")
	}
	d := NewDeliveryReducer()
	d.Ensure(i)
	d.Ensure(i)
	first, ok := d.Begin(i.ID, now)
	if !ok || first.Status != Claimed || len(first.Attempts) != 1 {
		t.Fatal(first, ok)
	}
	if _, ok := d.Begin(i.ID, now.Add(100*time.Millisecond)); ok {
		t.Fatal("concurrent active claim accepted")
	}
	if got, ok := d.Fail(i.ID, first.ClaimToken, now.Add(time.Second), nil); !ok || got.Status != Failed {
		t.Fatal(got, ok)
	}
	second, ok := d.Begin(i.ID, now.Add(2*time.Second))
	if !ok || second.Status != Claimed || len(second.Attempts) != 2 {
		t.Fatal(second, ok)
	}
	if _, ok := d.Complete(i.ID, first.ClaimToken, now.Add(2500*time.Millisecond)); ok {
		t.Fatal("stale completion accepted")
	}
	if got, ok := d.Complete(i.ID, second.ClaimToken, now.Add(3*time.Second)); !ok || got.Status != Delivered || len(got.Attempts) != 2 {
		t.Fatal(got, ok)
	}
	if _, ok := d.Begin(i.ID, now.Add(4*time.Second)); ok {
		t.Fatal("delivered intent claimed")
	}
}

func TestDeliveryConcurrentClaimsHaveOneWinner(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	i, _ := New().Plan(alerts.Transition{ID: "concurrent", Kind: alerts.Open}, Policy{OnOpen: true})
	d := NewDeliveryReducer()
	d.Ensure(i)
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for n := 0; n < 16; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := d.Begin(i.ID, now); ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("claim winners=%d", wins)
	}
}

func TestIntentIDsDistinguishDelimiterContainingTransitionIDs(t *testing.T) {
	a := alerts.Transition{ID: "a|1", Generation: 2, Kind: alerts.Open}
	b := alerts.Transition{ID: "a", Generation: 1, Kind: alerts.Recovery}
	if IntentID(a) == IntentID(b) {
		t.Fatal("intent IDs collided")
	}
}
