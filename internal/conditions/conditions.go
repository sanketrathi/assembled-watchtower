// Package conditions contains deterministic, storage-independent condition reducers.
package conditions

import (
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"time"
)

type SubjectKind string

const (
	SubjectQueue SubjectKind = "queue"
	SubjectAgent SubjectKind = "agent"
)

type SubjectKey struct {
	Kind SubjectKind
	ID   string
}

type ConditionKey struct {
	RuleID   string
	Revision int64
	Subject  SubjectKey
}

type Result uint8

const (
	Unknown Result = iota
	False
	True
)

func (r Result) String() string {
	switch r {
	case True:
		return "true"
	case False:
		return "false"
	default:
		return "unknown"
	}
}

type Direction uint8

const (
	Trigger Direction = iota
	Clear
)

func (d Direction) String() string {
	if d == Clear {
		return "clear"
	}
	return "trigger"
}

type Times struct {
	// EffectiveAt is the source event's effective timestamp.
	EffectiveAt time.Time
	// DueAt is the timer qualification timestamp, when applicable.
	DueAt time.Time
	// ProcessingAt is the logical processing timestamp.
	ProcessingAt time.Time
	// EvidenceAt identifies when the evidence says the interval began.
	EvidenceAt time.Time
}

// ObservationID is an application-assigned occurrence identity. It must not be
// a source event_id when source IDs can repeat.
type Observation struct {
	Key           ConditionKey
	Direction     Direction
	Result        Result
	EffectiveAt   time.Time
	ProcessingAt  time.Time
	EvidenceAt    time.Time
	TrueSince     time.Time // optional upstream interval onset
	ObservationID string    // optional stable occurrence identity
	Reason        string
}

type Transition struct {
	ID string
	// EpisodeID identifies the durable condition episode opened or closed by
	// this transition. It lets downstream alert lifecycle composition preserve
	// contributor identity without scanning mutable reducer state.
	EpisodeID string
	Key       ConditionKey
	Direction Direction
	At        time.Time
	Times     Times
	Reason    string
	Attempt   uint64
}

type Episode struct {
	ID       string
	Key      ConditionKey
	Trigger  Transition
	Clear    *Transition
	OpenedAt time.Time
	ClosedAt time.Time
}

func (e Episode) Open() bool { return e.Clear == nil }

type RuleConfig struct {
	RuleID          string
	Revision        int64
	TriggerDuration time.Duration
	ClearDuration   time.Duration
}

// Reducer tracks one configured duration policy across condition keys. The key
// includes the rule revision, so replacing a revision cannot share timers.
type Reducer struct {
	config RuleConfig
	states map[ConditionKey]*trackerState
	// seenOccurrences fences replay of an application occurrence across a
	// completed lifecycle. Durable ingestion should persist the same fence.
	seenOccurrences map[string]struct{}
	episodes        map[string]*Episode
	transitions     []Transition
	logical         time.Time
}

type trackerState struct {
	phase       phase
	attempt     uint64
	startedAt   time.Time
	effectiveAt time.Time
	evidenceAt  time.Time
	reason      string
	episodeID   string
	sequence    uint64
}

type phase uint8

const (
	inactive phase = iota
	triggering
	active
	clearing
)

// Phase is the externally visible lifecycle of a condition key.
type Phase uint8

const (
	Inactive Phase = iota
	PendingTrigger
	Active
	PendingClear
)

type TrackerSnapshot struct {
	Key         ConditionKey
	Phase       Phase
	Attempt     uint64
	StartedAt   time.Time
	EffectiveAt time.Time
	EvidenceAt  time.Time
	EpisodeID   string
}

func (r *Reducer) State(key ConditionKey) (TrackerSnapshot, bool) {
	s, ok := r.states[key]
	if !ok {
		return TrackerSnapshot{}, false
	}
	p := Inactive
	switch s.phase {
	case triggering:
		p = PendingTrigger
	case active:
		p = Active
	case clearing:
		p = PendingClear
	}
	return TrackerSnapshot{Key: key, Phase: p, Attempt: s.attempt, StartedAt: s.startedAt, EffectiveAt: s.effectiveAt, EvidenceAt: s.evidenceAt, EpisodeID: s.episodeID}, true
}

type Timer struct {
	Key       ConditionKey
	Direction Direction
	DueAt     time.Time
	Attempt   uint64
}

func New(config RuleConfig) *Reducer {
	if config.TriggerDuration < 0 || config.ClearDuration < 0 {
		panic("conditions: negative duration")
	}
	return &Reducer{config: config, states: make(map[ConditionKey]*trackerState), seenOccurrences: make(map[string]struct{}), episodes: make(map[string]*Episode)}
}

// NewReducer is a concise constructor for a single rule revision.
func NewReducer(triggerDuration, clearDuration time.Duration) *Reducer {
	return New(RuleConfig{TriggerDuration: triggerDuration, ClearDuration: clearDuration})
}

func (r *Reducer) Config() RuleConfig     { return r.config }
func (r *Reducer) LogicalTime() time.Time { return r.logical }

func (r *Reducer) Apply(o Observation) []Transition {
	if r.config.RuleID != "" && (o.Key.RuleID != r.config.RuleID || o.Key.Revision != r.config.Revision) {
		return nil
	}
	if o.Direction != Trigger && o.Direction != Clear || o.Result != Unknown && o.Result != False && o.Result != True {
		return nil
	}
	processing := o.ProcessingAt
	if processing.IsZero() {
		processing = o.EffectiveAt
	}
	if processing.IsZero() {
		return nil
	}
	if o.ObservationID != "" {
		if _, seen := r.seenOccurrences[o.ObservationID]; seen {
			return nil
		}
		r.seenOccurrences[o.ObservationID] = struct{}{}
	}
	out := r.Advance(processing)
	s := r.states[o.Key]
	if s == nil {
		s = &trackerState{}
		r.states[o.Key] = s
	}
	// Freshness and source-specific late-evidence handling belong to the
	// operational projections. This reducer consumes the resulting evaluation
	// and therefore never rejects an evaluation solely by effective timestamp.
	if o.EffectiveAt.IsZero() {
		o.EffectiveAt = processing
	}
	s.effectiveAt = o.EffectiveAt
	processing = r.logical
	s.evidenceAt = o.EvidenceAt
	if s.evidenceAt.IsZero() {
		s.evidenceAt = o.EffectiveAt
	}
	s.reason = o.Reason

	if o.Direction == Trigger {
		if o.Result == True && s.phase == inactive {
			return append(out, r.start(o.Key, Trigger, s, r.config.TriggerDuration, o, processing)...)
		}
		if o.Result != True && s.phase == triggering {
			s.phase = inactive
			s.attempt++
		}
		return out
	}
	if o.Result == True && s.phase == active {
		return append(out, r.start(o.Key, Clear, s, r.config.ClearDuration, o, processing)...)
	}
	if o.Result != True && s.phase == clearing {
		s.phase = active
		s.attempt++
	}
	return out
}

func (r *Reducer) start(key ConditionKey, dir Direction, s *trackerState, duration time.Duration, o Observation, processing time.Time) []Transition {
	start := o.EffectiveAt
	if !o.TrueSince.IsZero() && !o.TrueSince.After(o.EffectiveAt) {
		start = o.TrueSince
	}
	if start.IsZero() {
		start = processing
	}
	s.startedAt, s.phase = start, phaseFor(dir)
	s.attempt++
	due := start.Add(duration)
	if !due.After(r.logical) {
		commitAt := due
		if commitAt.Before(r.logical) {
			commitAt = r.logical
		}
		return r.commit(key, dir, s, commitAt, due, processing, o)
	}
	return nil
}

func phaseFor(d Direction) phase {
	if d == Clear {
		return clearing
	}
	return triggering
}

// Advance fires every due timer through now. Timers are selected by due time,
// canonical condition key, and attempt, as required by replay ordering.
func (r *Reducer) Advance(now time.Time) []Transition {
	if now.IsZero() {
		return nil
	}
	if now.Before(r.logical) {
		now = r.logical
	}
	r.logical = now
	var out []Transition
	for {
		timers := r.timers()
		var chosen *Timer
		for i := range timers {
			if timers[i].DueAt.After(now) {
				break
			}
			s := r.states[timers[i].Key]
			if s != nil && s.attempt == timers[i].Attempt && s.phase == phaseFor(timers[i].Direction) {
				chosen = &timers[i]
				break
			}
		}
		if chosen == nil {
			break
		}
		s := r.states[chosen.Key]
		out = append(out, r.commit(chosen.Key, chosen.Direction, s, chosen.DueAt, chosen.DueAt, now, Observation{Key: chosen.Key, Direction: chosen.Direction, Result: True, EffectiveAt: s.effectiveAt, EvidenceAt: s.evidenceAt, Reason: s.reason})...)
	}
	return out
}

// Timers returns a deterministic snapshot of currently pending timers.
func (r *Reducer) Timers() []Timer { return r.timers() }

// Fire applies one claimed timer callback. A callback is accepted only when
// its attempt still matches the pending tracker and it is due at logical time.
func (r *Reducer) Fire(timer Timer, now time.Time) []Transition {
	if now.IsZero() {
		return nil
	}
	if now.Before(r.logical) {
		now = r.logical
	} else {
		r.logical = now
	}
	if timer.Direction != Trigger && timer.Direction != Clear {
		return nil
	}
	s := r.states[timer.Key]
	if s == nil || timer.DueAt.After(r.logical) || s.attempt != timer.Attempt || s.phase != phaseFor(timer.Direction) {
		return nil
	}
	duration := r.config.TriggerDuration
	if timer.Direction == Clear {
		duration = r.config.ClearDuration
	}
	canonicalDue := s.startedAt.Add(duration)
	if !timer.DueAt.Equal(canonicalDue) {
		return nil
	}
	// Both replay advancement and a claimed callback journal the canonical
	// qualification timestamp. ProcessingAt remains the callback's logical time.
	return r.commit(timer.Key, timer.Direction, s, timer.DueAt, timer.DueAt, r.logical, Observation{Key: timer.Key, Direction: timer.Direction, Result: True, EffectiveAt: s.effectiveAt, EvidenceAt: s.evidenceAt, Reason: s.reason})
}

func (r *Reducer) timers() []Timer {
	out := make([]Timer, 0)
	for k, s := range r.states {
		if s.phase != triggering && s.phase != clearing {
			continue
		}
		d := r.config.TriggerDuration
		if s.phase == clearing {
			d = r.config.ClearDuration
		}
		out = append(out, Timer{Key: k, Direction: directionFor(s.phase), DueAt: s.startedAt.Add(d), Attempt: s.attempt})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].DueAt.Equal(out[j].DueAt) {
			return out[i].DueAt.Before(out[j].DueAt)
		}
		if out[i].Key.RuleID != out[j].Key.RuleID {
			return out[i].Key.RuleID < out[j].Key.RuleID
		}
		if out[i].Key.Revision != out[j].Key.Revision {
			return out[i].Key.Revision < out[j].Key.Revision
		}
		if out[i].Key.Subject.Kind != out[j].Key.Subject.Kind {
			return out[i].Key.Subject.Kind < out[j].Key.Subject.Kind
		}
		if out[i].Key.Subject.ID != out[j].Key.Subject.ID {
			return out[i].Key.Subject.ID < out[j].Key.Subject.ID
		}
		return out[i].Attempt < out[j].Attempt
	})
	return out
}
func directionFor(p phase) Direction {
	if p == clearing {
		return Clear
	}
	return Trigger
}

func (r *Reducer) commit(key ConditionKey, dir Direction, s *trackerState, at, due, processing time.Time, o Observation) []Transition {
	s.phase = active
	if dir == Clear {
		s.phase = inactive
	}
	s.sequence++
	id := stableID("condition-transition", canonicalParts(key.RuleID, strconv.FormatInt(key.Revision, 10), string(key.Subject.Kind), key.Subject.ID, dir.String(), strconv.FormatUint(s.sequence, 10)))
	episodeID := s.episodeID
	if dir == Trigger {
		episodeID = stableID("condition-episode", canonicalParts(key.RuleID, strconv.FormatInt(key.Revision, 10), string(key.Subject.Kind), key.Subject.ID, strconv.FormatUint(s.sequence, 10)))
	}
	t := Transition{ID: id, EpisodeID: episodeID, Key: key, Direction: dir, At: at, Attempt: s.attempt, Reason: o.Reason, Times: Times{EffectiveAt: o.EffectiveAt, DueAt: due, ProcessingAt: processing, EvidenceAt: o.EvidenceAt}}
	if t.Times.EffectiveAt.IsZero() {
		t.Times.EffectiveAt = at
	}
	if t.Times.EvidenceAt.IsZero() {
		t.Times.EvidenceAt = s.evidenceAt
	}
	if dir == Trigger {
		s.episodeID = t.EpisodeID
		r.episodes[t.EpisodeID] = &Episode{ID: t.EpisodeID, Key: key, Trigger: t, OpenedAt: at}
	} else if t.EpisodeID != "" {
		if ep := r.episodes[t.EpisodeID]; ep != nil {
			c := t
			ep.Clear = &c
			ep.ClosedAt = at
		}
		s.episodeID = ""
	}
	r.transitions = append(r.transitions, t)
	return []Transition{t}
}

func (r *Reducer) Episodes() []Episode {
	out := make([]Episode, 0, len(r.episodes))
	for _, e := range r.episodes {
		x := *e
		if e.Clear != nil {
			c := *e.Clear
			x.Clear = &c
		}
		out = append(out, x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (r *Reducer) Transitions() []Transition { return append([]Transition(nil), r.transitions...) }
func (r *Reducer) Episode(id string) (Episode, bool) {
	e, ok := r.episodes[id]
	if !ok {
		return Episode{}, false
	}
	x := *e
	if e.Clear != nil {
		c := *e.Clear
		x.Clear = &c
	}
	return x, true
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

func stableID(prefix, value string) string {
	// Hex encoding is injective; canonicalParts makes each component
	// boundary unambiguous before it is encoded.
	return prefix + "_" + hex.EncodeToString([]byte(value))
}
