// Package notifications plans deterministic notification intents from alert
// lifecycle transitions. It does not perform delivery.
package notifications

import (
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"watchtower/internal/alerts"
)

type Policy struct {
	OnOpen     bool
	OnRecovery bool
	Audience   string // logical audience, not a transport/provider identity
}

type Intent struct {
	ID                string
	AlertTransitionID string
	SeriesID          string
	Generation        uint64
	Kind              alerts.TransitionKind
	Audience          string
	At                time.Time
	EffectiveAt       time.Time
	EvidenceAt        time.Time
}

type Planner struct{ intents map[string]Intent }

func New() *Planner { return &Planner{intents: make(map[string]Intent)} }

// Plan is idempotent: replaying a transition returns the original intent and
// does not create a second logical notification.
func (p *Planner) Plan(t alerts.Transition, policy Policy) (Intent, bool) {
	if t.Kind != alerts.Open && t.Kind != alerts.Recovery {
		return Intent{}, false
	}
	if t.Kind == alerts.Open && !policy.OnOpen || t.Kind == alerts.Recovery && !policy.OnRecovery {
		return Intent{}, false
	}
	id := IntentID(t)
	if existing, ok := p.intents[id]; ok {
		return existing, false
	}
	i := Intent{ID: id, AlertTransitionID: t.ID, SeriesID: t.SeriesID, Generation: t.Generation, Kind: t.Kind, Audience: policy.Audience, At: t.At, EffectiveAt: t.EffectiveAt, EvidenceAt: t.EvidenceAt}
	p.intents[id] = i
	return i, true
}

// PlanTransition is a stateless convenience for applications that persist the
// returned intent transactionally with their alert transition.
func PlanTransition(t alerts.Transition, policy Policy) (Intent, bool) {
	return New().Plan(t, policy)
}
func IntentID(t alerts.Transition) string {
	return stableID("notification-intent", canonicalParts(t.ID, strconv.FormatUint(t.Generation, 10), strconv.Itoa(int(t.Kind))))
}
func (p *Planner) Intents() []Intent {
	out := make([]Intent, 0, len(p.intents))
	for _, i := range p.intents {
		out = append(out, i)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (p *Planner) Intent(id string) (Intent, bool) { i, ok := p.intents[id]; return i, ok }

// Delivery models durable outbox state without choosing a storage adapter.
type DeliveryStatus uint8

const (
	Pending DeliveryStatus = iota
	Claimed
	Delivered
	Failed
)

// ClaimToken fences a worker's completion/failure callback from later retries.
type ClaimToken string

type DeliveryAttempt struct {
	Number     uint64
	Token      ClaimToken
	Status     DeliveryStatus
	StartedAt  time.Time
	FinishedAt time.Time
	Error      string
}
type Delivery struct {
	IntentID   string
	Status     DeliveryStatus
	ClaimToken ClaimToken
	Attempts   []DeliveryAttempt
}
type DeliveryReducer struct {
	mu         sync.Mutex
	deliveries map[string]*Delivery
}

func NewDeliveryReducer() *DeliveryReducer {
	return &DeliveryReducer{deliveries: make(map[string]*Delivery)}
}
func (r *DeliveryReducer) Ensure(i Intent) Delivery {
	r.mu.Lock()
	defer r.mu.Unlock()
	if d := r.deliveries[i.ID]; d != nil {
		return copyDelivery(d)
	}
	d := &Delivery{IntentID: i.ID, Status: Pending}
	r.deliveries[i.ID] = d
	return copyDelivery(d)
}

// Begin rejects a second active claim. Storage adapters should persist this
// compare-and-swap with the intent/outbox transaction.
func (r *DeliveryReducer) Begin(intentID string, at time.Time) (Delivery, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.deliveries[intentID]
	if d == nil || d.Status == Delivered || d.Status == Claimed {
		return Delivery{}, false
	}
	n := uint64(len(d.Attempts) + 1)
	token := ClaimToken(stableID("delivery-claim", canonicalParts(intentID, strconv.FormatUint(n, 10))))
	d.Status = Claimed
	d.ClaimToken = token
	d.Attempts = append(d.Attempts, DeliveryAttempt{Number: n, Token: token, Status: Claimed, StartedAt: at})
	return copyDelivery(d), true
}

// Complete succeeds only for the current claim. A stale or duplicate callback
// is rejected rather than being applied to a later attempt.
func (r *DeliveryReducer) Complete(intentID string, token ClaimToken, at time.Time) (Delivery, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.deliveries[intentID]
	if d == nil || d.Status != Claimed || d.ClaimToken != token || len(d.Attempts) == 0 {
		return Delivery{}, false
	}
	a := &d.Attempts[len(d.Attempts)-1]
	if a.Status != Claimed || a.Token != token {
		return Delivery{}, false
	}
	a.Status = Delivered
	a.FinishedAt = at
	d.Status = Delivered
	d.ClaimToken = ""
	return copyDelivery(d), true
}

// Fail follows the same claim fence as Complete and makes a new retry
// possible without creating a new notification intent.
func (r *DeliveryReducer) Fail(intentID string, token ClaimToken, at time.Time, err error) (Delivery, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d := r.deliveries[intentID]
	if d == nil || d.Status != Claimed || d.ClaimToken != token || len(d.Attempts) == 0 {
		return Delivery{}, false
	}
	a := &d.Attempts[len(d.Attempts)-1]
	if a.Status != Claimed || a.Token != token {
		return Delivery{}, false
	}
	a.Status = Failed
	a.FinishedAt = at
	if err != nil {
		a.Error = err.Error()
	}
	d.Status = Failed
	d.ClaimToken = ""
	return copyDelivery(d), true
}
func (r *DeliveryReducer) Delivery(id string) (Delivery, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	d, ok := r.deliveries[id]
	if !ok {
		return Delivery{}, false
	}
	return copyDelivery(d), true
}
func copyDelivery(d *Delivery) Delivery {
	x := *d
	x.Attempts = append([]DeliveryAttempt(nil), d.Attempts...)
	return x
}
func canonicalParts(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}
func stableID(prefix, v string) string {
	// Hex encoding is injective; canonicalParts makes each component
	// boundary unambiguous before it is encoded.
	return prefix + "_" + hex.EncodeToString([]byte(v))
}
