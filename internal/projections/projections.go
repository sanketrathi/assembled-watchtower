// Package projections builds source-specific operational projection updates.
//
// It accepts already admitted occurrences from internal/app. It keeps immutable
// source observations separate from the three live projections, and deliberately
// does not evaluate rules, advance a logical clock, fire timers, or write to
// PostgreSQL. A coordinator owns those operations and persists these updates
// with the corresponding accepted occurrence.
package projections

import (
	"bytes"
	"fmt"
	"reflect"
	"time"

	"watchtower/internal/app"
	"watchtower/internal/events"
)

// Provenance identifies the accepted occurrence from which a projection
// observation or state was built. OccurrenceID is application identity; the
// source event ID is provenance only and is never used as a deduplication key.
type Provenance struct {
	OccurrenceID   string
	Source         string
	IdempotencyKey string
	IngestPosition uint64
	SourceEventID  string
	EffectiveAt    time.Time
}

// QueueObservation is immutable queue source evidence. Raw JSON remains on the
// accepted occurrence owned by app/storage; it is not folded into live state.
type QueueObservation struct {
	Provenance Provenance
	Snapshot   events.QueueSnapshot
}

// AgentStateObservation is immutable agent-state source evidence. QueueIDs are
// retained only as source provenance and never establish group membership.
type AgentStateObservation struct {
	Provenance Provenance
	Change     events.AgentStateChange
}

// AdherenceObservation is immutable upstream adherence evidence. QueueIDs are
// source provenance only; ActualState does not modify the agent-state source
// projection.
type AdherenceObservation struct {
	Provenance Provenance
	Check      events.AdherenceCheck
}

// QueueState is the held live queue snapshot. It is distinct from the immutable
// QueueObservation that supplied it.
type QueueState struct {
	QueueID               string
	TicketsWaiting        uint64
	LongestWaitSec        uint64
	SLATargetSec          uint64
	AgentsAvailable       uint64
	AgentsOnCall          uint64
	VolumeLast15m         uint64
	VolumeForecastNext15m *uint64
	Provenance            Provenance
}

// AgentState is the held live state-transition result. Previous-state duration
// and queue IDs remain observation evidence rather than inferred live state.
type AgentState struct {
	AgentID    string
	Current    events.State
	Provenance Provenance
}

// AdherenceState is the held upstream adherence result. KnownViolationSince is
// derived only from continuous accepted adherence evidence: an explicit source
// onset wins; otherwise a continuing true state retains its first known-true
// time. It is zero while InViolation is false.
type AdherenceState struct {
	AgentID             string
	ScheduledState      events.State
	ActualState         events.State
	InViolation         bool
	ViolationStartedAt  *time.Time
	KnownViolationSince time.Time
	Provenance          Provenance
}

// QueueUpdate retains one source observation and reports the live queue state
// after applying it. CurrentChanged is false for retained late evidence.
type QueueUpdate struct {
	Observation    QueueObservation
	Current        QueueState
	CurrentChanged bool
}

// AgentStateUpdate retains one source observation and reports the live
// agent-state projection after applying it.
type AgentStateUpdate struct {
	Observation    AgentStateObservation
	Current        AgentState
	CurrentChanged bool
}

// AdherenceUpdate retains one source observation and reports the live adherence
// projection after applying it.
type AdherenceUpdate struct {
	Observation    AdherenceObservation
	Current        AdherenceState
	CurrentChanged bool
}

// Update contains exactly one source-specific update for an accepted occurrence.
// Duplicate is true when the same immutable application occurrence is built
// again; callers must not store a second source observation for that result.
type Update struct {
	Duplicate  bool
	Queue      *QueueUpdate
	AgentState *AgentStateUpdate
	Adherence  *AdherenceUpdate
}

type admissionKey struct {
	source string
	key    string
}

type acceptedIdentity struct {
	source, idempotencyKey, sourceEventID string
	ingestPosition                        uint64
	effectiveAt                           time.Time
	event                                 events.Event
	raw                                   []byte
}

// Builder owns deterministic in-memory projection state. It accepts occurrences
// in increasing ingestion position; effective time selects each source-local
// live pointer, with ingestion position as the deterministic equal-time tie
// breaker. This allows a late occurrence to initialize its own projection while
// preventing it from regressing a newer one.
type Builder struct {
	queues    map[string]QueueState
	agents    map[string]AgentState
	adherence map[string]AdherenceState

	queueHistory     map[string][]QueueObservation
	agentHistory     map[string][]AgentStateObservation
	adherenceHistory map[string][]AdherenceObservation

	occurrences  map[string]acceptedIdentity
	admissions   map[admissionKey]string
	lastPosition uint64
}

func (b *Builder) ensureMaps() {
	if b.queues == nil {
		b.queues = make(map[string]QueueState)
	}
	if b.agents == nil {
		b.agents = make(map[string]AgentState)
	}
	if b.adherence == nil {
		b.adherence = make(map[string]AdherenceState)
	}
	if b.queueHistory == nil {
		b.queueHistory = make(map[string][]QueueObservation)
	}
	if b.agentHistory == nil {
		b.agentHistory = make(map[string][]AgentStateObservation)
	}
	if b.adherenceHistory == nil {
		b.adherenceHistory = make(map[string][]AdherenceObservation)
	}
	if b.occurrences == nil {
		b.occurrences = make(map[string]acceptedIdentity)
	}
	if b.admissions == nil {
		b.admissions = make(map[admissionKey]string)
	}
}

// NewBuilder returns an empty source projection builder.
func NewBuilder() *Builder {
	return &Builder{
		queues:           make(map[string]QueueState),
		agents:           make(map[string]AgentState),
		adherence:        make(map[string]AdherenceState),
		queueHistory:     make(map[string][]QueueObservation),
		agentHistory:     make(map[string][]AgentStateObservation),
		adherenceHistory: make(map[string][]AdherenceObservation),
		occurrences:      make(map[string]acceptedIdentity),
		admissions:       make(map[admissionKey]string),
	}
}

// Build turns one accepted occurrence into its source-specific projection
// update. It never sorts inputs or changes logical time; the application
// coordinator owns logical time and timer ordering.
func (b *Builder) Build(occurrence app.AcceptedOccurrence) (Update, error) {
	if b == nil {
		return Update{}, fmt.Errorf("projection builder is nil")
	}
	if err := occurrence.Validate(); err != nil {
		return Update{}, fmt.Errorf("validate accepted occurrence: %w", err)
	}
	b.ensureMaps()
	identity := identityOf(occurrence)
	if prior, ok := b.occurrences[occurrence.ID]; ok {
		if !sameIdentity(prior, identity) {
			return Update{}, fmt.Errorf("accepted occurrence %q conflicts with immutable identity", occurrence.ID)
		}
		return b.duplicate(occurrence)
	}
	key := admissionKey{source: occurrence.Source, key: occurrence.IdempotencyKey}
	if priorID, ok := b.admissions[key]; ok {
		return Update{}, fmt.Errorf("accepted occurrence %q reuses admitted source/idempotency key from %q", occurrence.ID, priorID)
	}
	if occurrence.IngestPosition <= b.lastPosition {
		return Update{}, fmt.Errorf("accepted occurrence %q ingestion position %d does not follow %d", occurrence.ID, occurrence.IngestPosition, b.lastPosition)
	}

	var update Update
	switch event := occurrence.Event.(type) {
	case events.QueueSnapshot:
		update.Queue = b.buildQueue(occurrence, event)
	case events.AgentStateChange:
		update.AgentState = b.buildAgentState(occurrence, event)
	case events.AdherenceCheck:
		update.Adherence = b.buildAdherence(occurrence, event)
	default:
		return Update{}, fmt.Errorf("build projection for unsupported event %T", occurrence.Event)
	}
	b.occurrences[occurrence.ID] = identity
	b.admissions[key] = occurrence.ID
	b.lastPosition = occurrence.IngestPosition
	return cloneUpdate(update), nil
}

func (b *Builder) duplicate(occurrence app.AcceptedOccurrence) (Update, error) {
	var update Update
	switch event := occurrence.Event.(type) {
	case events.QueueSnapshot:
		current, ok := b.queues[event.QueueID]
		if !ok {
			return Update{}, fmt.Errorf("duplicate queue occurrence %q has no live state", occurrence.ID)
		}
		update.Queue = &QueueUpdate{Observation: queueObservation(occurrence, event), Current: cloneQueueState(current)}
	case events.AgentStateChange:
		current, ok := b.agents[event.AgentID]
		if !ok {
			return Update{}, fmt.Errorf("duplicate agent-state occurrence %q has no live state", occurrence.ID)
		}
		update.AgentState = &AgentStateUpdate{Observation: agentObservation(occurrence, event), Current: current}
	case events.AdherenceCheck:
		current, ok := b.adherence[event.AgentID]
		if !ok {
			return Update{}, fmt.Errorf("duplicate adherence occurrence %q has no live state", occurrence.ID)
		}
		update.Adherence = &AdherenceUpdate{Observation: adherenceObservation(occurrence, event), Current: cloneAdherenceState(current)}
	default:
		return Update{}, fmt.Errorf("build duplicate projection for unsupported event %T", occurrence.Event)
	}
	update.Duplicate = true
	return cloneUpdate(update), nil
}

func (b *Builder) buildQueue(occurrence app.AcceptedOccurrence, snapshot events.QueueSnapshot) *QueueUpdate {
	observation := queueObservation(occurrence, snapshot)
	b.queueHistory[snapshot.QueueID] = append(b.queueHistory[snapshot.QueueID], observation)
	candidate := QueueState{
		QueueID: snapshot.QueueID, TicketsWaiting: snapshot.TicketsWaiting, LongestWaitSec: snapshot.LongestWaitSec,
		SLATargetSec: snapshot.SLATargetSec, AgentsAvailable: snapshot.AgentsAvailable, AgentsOnCall: snapshot.AgentsOnCall,
		VolumeLast15m: snapshot.VolumeLast15m, VolumeForecastNext15m: cloneUint64(snapshot.VolumeForecastNext15m), Provenance: provenance(occurrence),
	}
	current, changed := b.queues[snapshot.QueueID]
	if !changed || newer(candidate.Provenance, current.Provenance) {
		b.queues[snapshot.QueueID] = candidate
		current, changed = candidate, true
	} else {
		changed = false
	}
	return &QueueUpdate{Observation: observation, Current: cloneQueueState(current), CurrentChanged: changed}
}

func (b *Builder) buildAgentState(occurrence app.AcceptedOccurrence, change events.AgentStateChange) *AgentStateUpdate {
	observation := agentObservation(occurrence, change)
	b.agentHistory[change.AgentID] = append(b.agentHistory[change.AgentID], observation)
	candidate := AgentState{AgentID: change.AgentID, Current: change.NewState, Provenance: provenance(occurrence)}
	current, changed := b.agents[change.AgentID]
	if !changed || newer(candidate.Provenance, current.Provenance) {
		b.agents[change.AgentID] = candidate
		current, changed = candidate, true
	} else {
		changed = false
	}
	return &AgentStateUpdate{Observation: observation, Current: current, CurrentChanged: changed}
}

func (b *Builder) buildAdherence(occurrence app.AcceptedOccurrence, check events.AdherenceCheck) *AdherenceUpdate {
	observation := adherenceObservation(occurrence, check)
	b.adherenceHistory[check.AgentID] = append(b.adherenceHistory[check.AgentID], observation)
	prior, hadPrior := b.adherence[check.AgentID]
	candidate := AdherenceState{
		AgentID: check.AgentID, ScheduledState: check.ScheduledState, ActualState: check.ActualState,
		InViolation: check.InViolation, ViolationStartedAt: cloneTime(check.ViolationStartedAt), Provenance: provenance(occurrence),
	}
	if check.InViolation {
		switch {
		case check.ViolationStartedAt != nil:
			candidate.KnownViolationSince = *check.ViolationStartedAt
		case hadPrior && prior.InViolation && !prior.KnownViolationSince.IsZero():
			candidate.KnownViolationSince = prior.KnownViolationSince
		default:
			candidate.KnownViolationSince = occurrence.EffectiveAt
		}
	}
	current := prior
	currentChanged := false
	if !hadPrior || newer(candidate.Provenance, prior.Provenance) {
		b.adherence[check.AgentID] = candidate
		current, currentChanged = candidate, true
	}
	return &AdherenceUpdate{Observation: observation, Current: cloneAdherenceState(current), CurrentChanged: currentChanged}
}

// Queue returns a copy of current queue state.
func (b *Builder) Queue(id string) (QueueState, bool) {
	if b == nil {
		return QueueState{}, false
	}
	state, ok := b.queues[id]
	return cloneQueueState(state), ok
}

// Agent returns current agent-state source state.
func (b *Builder) Agent(id string) (AgentState, bool) {
	if b == nil {
		return AgentState{}, false
	}
	state, ok := b.agents[id]
	return state, ok
}

// Adherence returns a copy of current adherence source state.
func (b *Builder) Adherence(id string) (AdherenceState, bool) {
	if b == nil {
		return AdherenceState{}, false
	}
	state, ok := b.adherence[id]
	return cloneAdherenceState(state), ok
}

// QueueHistory returns immutable queue observations in physical ingestion order.
func (b *Builder) QueueHistory(id string) []QueueObservation {
	if b == nil {
		return nil
	}
	return cloneQueueObservations(b.queueHistory[id])
}

// AgentStateHistory returns immutable agent-state observations in physical
// ingestion order.
func (b *Builder) AgentStateHistory(id string) []AgentStateObservation {
	if b == nil {
		return nil
	}
	return cloneAgentObservations(b.agentHistory[id])
}

// AdherenceHistory returns immutable adherence observations in physical
// ingestion order.
func (b *Builder) AdherenceHistory(id string) []AdherenceObservation {
	if b == nil {
		return nil
	}
	return cloneAdherenceObservations(b.adherenceHistory[id])
}

func provenance(o app.AcceptedOccurrence) Provenance {
	return Provenance{OccurrenceID: o.ID, Source: o.Source, IdempotencyKey: o.IdempotencyKey, IngestPosition: o.IngestPosition, SourceEventID: o.SourceEventID, EffectiveAt: o.EffectiveAt}
}
func queueObservation(o app.AcceptedOccurrence, event events.QueueSnapshot) QueueObservation {
	return QueueObservation{Provenance: provenance(o), Snapshot: cloneQueueSnapshot(event)}
}
func agentObservation(o app.AcceptedOccurrence, event events.AgentStateChange) AgentStateObservation {
	return AgentStateObservation{Provenance: provenance(o), Change: cloneAgentStateChange(event)}
}
func adherenceObservation(o app.AcceptedOccurrence, event events.AdherenceCheck) AdherenceObservation {
	return AdherenceObservation{Provenance: provenance(o), Check: cloneAdherenceCheck(event)}
}
func newer(candidate, current Provenance) bool {
	return candidate.EffectiveAt.After(current.EffectiveAt) || candidate.EffectiveAt.Equal(current.EffectiveAt) && candidate.IngestPosition > current.IngestPosition
}
func identityOf(o app.AcceptedOccurrence) acceptedIdentity {
	return acceptedIdentity{source: o.Source, idempotencyKey: o.IdempotencyKey, sourceEventID: o.SourceEventID, ingestPosition: o.IngestPosition, effectiveAt: o.EffectiveAt, event: cloneEvent(o.Event), raw: append([]byte(nil), o.Raw...)}
}
func sameIdentity(a, b acceptedIdentity) bool {
	return a.source == b.source && a.idempotencyKey == b.idempotencyKey && a.sourceEventID == b.sourceEventID && a.ingestPosition == b.ingestPosition && a.effectiveAt.Equal(b.effectiveAt) && reflect.DeepEqual(a.event, b.event) && bytes.Equal(a.raw, b.raw)
}
func cloneEvent(value events.Event) events.Event {
	switch event := value.(type) {
	case events.QueueSnapshot:
		return cloneQueueSnapshot(event)
	case events.AgentStateChange:
		return cloneAgentStateChange(event)
	case events.AdherenceCheck:
		return cloneAdherenceCheck(event)
	default:
		return nil
	}
}
func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}
func cloneQueueSnapshot(value events.QueueSnapshot) events.QueueSnapshot {
	value.VolumeForecastNext15m = cloneUint64(value.VolumeForecastNext15m)
	return value
}
func cloneAgentStateChange(value events.AgentStateChange) events.AgentStateChange {
	if value.PreviousState != nil {
		previous := *value.PreviousState
		value.PreviousState = &previous
	}
	value.PreviousStateDurationSec = cloneUint64(value.PreviousStateDurationSec)
	value.QueueIDs = append([]string(nil), value.QueueIDs...)
	return value
}
func cloneAdherenceCheck(value events.AdherenceCheck) events.AdherenceCheck {
	value.ViolationStartedAt = cloneTime(value.ViolationStartedAt)
	value.QueueIDs = append([]string(nil), value.QueueIDs...)
	return value
}
func cloneQueueState(value QueueState) QueueState {
	value.VolumeForecastNext15m = cloneUint64(value.VolumeForecastNext15m)
	return value
}
func cloneAdherenceState(value AdherenceState) AdherenceState {
	value.ViolationStartedAt = cloneTime(value.ViolationStartedAt)
	return value
}
func cloneQueueObservations(values []QueueObservation) []QueueObservation {
	out := make([]QueueObservation, len(values))
	for i := range values {
		out[i] = values[i]
		out[i].Snapshot = cloneQueueSnapshot(values[i].Snapshot)
	}
	return out
}
func cloneAgentObservations(values []AgentStateObservation) []AgentStateObservation {
	out := make([]AgentStateObservation, len(values))
	for i := range values {
		out[i] = values[i]
		out[i].Change = cloneAgentStateChange(values[i].Change)
	}
	return out
}
func cloneAdherenceObservations(values []AdherenceObservation) []AdherenceObservation {
	out := make([]AdherenceObservation, len(values))
	for i := range values {
		out[i] = values[i]
		out[i].Check = cloneAdherenceCheck(values[i].Check)
	}
	return out
}
func cloneUpdate(value Update) Update {
	if value.Queue != nil {
		item := *value.Queue
		item.Observation.Snapshot = cloneQueueSnapshot(item.Observation.Snapshot)
		item.Current = cloneQueueState(item.Current)
		value.Queue = &item
	}
	if value.AgentState != nil {
		item := *value.AgentState
		item.Observation.Change = cloneAgentStateChange(item.Observation.Change)
		value.AgentState = &item
	}
	if value.Adherence != nil {
		item := *value.Adherence
		item.Observation.Check = cloneAdherenceCheck(item.Observation.Check)
		item.Current = cloneAdherenceState(item.Current)
		value.Adherence = &item
	}
	return value
}
