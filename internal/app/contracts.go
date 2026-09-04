// Package app defines contracts between Watchtower's transport/storage adapters
// and future application orchestration. It intentionally contains no replay
// coordinator, timer runtime, HTTP handler, or PostgreSQL adapter.
package app

import (
	"fmt"
	"sort"
	"time"

	"watchtower/internal/alerts"
	"watchtower/internal/conditions"
	"watchtower/internal/events"
	"watchtower/internal/notifications"
	"watchtower/internal/rules"
)

// AcceptedOccurrence is immutable application input after strict event decoding
// and durable admission. ID is an opaque application occurrence identity;
// SourceEventID remains source metadata and is never a deduplication key.
//
// File replay uses Source plus the one-based IngestPosition to identify an
// input line. API admission uses its stable idempotency key before allocating
// IngestPosition. Both forms retain the decoded event and exact raw payload.
type AcceptedOccurrence struct {
	ID             string
	Source         string
	IdempotencyKey string
	IngestPosition uint64
	SourceEventID  string
	EffectiveAt    time.Time
	Event          events.Event
	Raw            []byte
}

// Validate checks the invariants that must already hold at the application
// boundary. It does not decode Raw again; internal/events is the sole strict
// decoder for external events.
func (o AcceptedOccurrence) Validate() error {
	if o.ID == "" {
		return fmt.Errorf("accepted occurrence ID is required")
	}
	if o.Source == "" {
		return fmt.Errorf("accepted occurrence source is required")
	}
	if o.IdempotencyKey == "" {
		return fmt.Errorf("accepted occurrence idempotency key is required")
	}
	if o.IngestPosition == 0 {
		return fmt.Errorf("accepted occurrence ingest position is required")
	}
	if o.SourceEventID == "" {
		return fmt.Errorf("accepted occurrence source event ID is required")
	}
	if o.ID == o.SourceEventID {
		return fmt.Errorf("accepted occurrence ID must not be the source event ID")
	}
	if o.IdempotencyKey == o.SourceEventID {
		return fmt.Errorf("accepted occurrence idempotency key must not be the source event ID")
	}
	if o.EffectiveAt.IsZero() {
		return fmt.Errorf("accepted occurrence effective time is required")
	}
	if o.Event == nil {
		return fmt.Errorf("accepted occurrence event is required")
	}
	if o.Event.GetEventID() != o.SourceEventID {
		return fmt.Errorf("accepted occurrence source event ID does not match decoded event")
	}
	if !eventEffectiveAt(o.Event).Equal(o.EffectiveAt) {
		return fmt.Errorf("accepted occurrence effective time does not match decoded event")
	}
	if len(o.Raw) == 0 {
		return fmt.Errorf("accepted occurrence raw payload is required")
	}
	return nil
}

// Clone returns an occurrence whose raw payload does not alias the input.
// Events are immutable values produced by the strict decoder; callers must not
// mutate a decoded event after it has crossed this boundary.
func (o AcceptedOccurrence) Clone() AcceptedOccurrence {
	o.Raw = append([]byte(nil), o.Raw...)
	return o
}

func eventEffectiveAt(event events.Event) time.Time {
	switch value := event.(type) {
	case events.QueueSnapshot:
		return value.Timestamp
	case events.AgentStateChange:
		return value.Timestamp
	case events.AdherenceCheck:
		return value.Timestamp
	default:
		return time.Time{}
	}
}

// ActiveRule is the application view of one immutable rule revision selected
// for evaluation. Rule definition content remains public/canonical; compiled
// plans remain private to internal/rules.
type ActiveRule struct {
	ID         string
	Revision   int64
	Definition rules.RuleDefinition
}

func (r ActiveRule) validate() error {
	if r.ID == "" {
		return fmt.Errorf("active rule ID is required")
	}
	if r.Revision <= 0 {
		return fmt.Errorf("active rule %q revision must be positive", r.ID)
	}
	if err := r.Definition.Validate(); err != nil {
		return fmt.Errorf("active rule %q definition: %w", r.ID, err)
	}
	return nil
}

// EvaluationRequest supplies a future evaluator with an already accepted
// occurrence, the nondecreasing logical time at which it is processed, and
// active immutable revisions. Projection state stays owned by the projection
// implementation; this contract deliberately does not duplicate its schema.
//
// Rules are canonicalized by ID and revision so equal-time evaluation has a
// stable order. A late occurrence is valid when LogicalTime is later than its
// EffectiveAt; it must never make logical time go backward.
type EvaluationRequest struct {
	Occurrence  AcceptedOccurrence
	LogicalTime time.Time
	ActiveRules []ActiveRule
}

// NewEvaluationRequest validates and copies its inputs. Multiple revisions may
// be represented explicitly while rule lifecycle reconciliation remains owned by
// the lifecycle goal; this contract never silently selects one of them.
func NewEvaluationRequest(occurrence AcceptedOccurrence, logicalTime time.Time, activeRules []ActiveRule) (EvaluationRequest, error) {
	if err := occurrence.Validate(); err != nil {
		return EvaluationRequest{}, err
	}
	if logicalTime.IsZero() {
		return EvaluationRequest{}, fmt.Errorf("evaluation logical time is required")
	}
	if logicalTime.Before(occurrence.EffectiveAt) {
		return EvaluationRequest{}, fmt.Errorf("evaluation logical time precedes occurrence effective time")
	}
	rulesCopy := append([]ActiveRule(nil), activeRules...)
	sort.Slice(rulesCopy, func(i, j int) bool {
		if rulesCopy[i].ID != rulesCopy[j].ID {
			return rulesCopy[i].ID < rulesCopy[j].ID
		}
		return rulesCopy[i].Revision < rulesCopy[j].Revision
	})
	for i := range rulesCopy {
		if err := rulesCopy[i].validate(); err != nil {
			return EvaluationRequest{}, err
		}
		if i > 0 && rulesCopy[i-1].ID == rulesCopy[i].ID && rulesCopy[i-1].Revision == rulesCopy[i].Revision {
			return EvaluationRequest{}, fmt.Errorf("duplicate active rule revision %q/%d", rulesCopy[i].ID, rulesCopy[i].Revision)
		}
	}
	return EvaluationRequest{Occurrence: occurrence.Clone(), LogicalTime: logicalTime, ActiveRules: rulesCopy}, nil
}

// EvaluationResult is the ordered set of trigger and clear observations
// produced for an EvaluationRequest. Each ObservationID is an
// application-assigned evaluation occurrence identity, not a raw event_id.
// Unknown remains an explicit conditions.Unknown result.
type EvaluationResult struct {
	Observations []conditions.Observation
}

// NewEvaluationResult validates and canonically orders observations. It does
// not invoke condition tracking; the future orchestrator advances due timers
// before obtaining this result, then passes observations to the reducer.
func NewEvaluationResult(request EvaluationRequest, observations []conditions.Observation) (EvaluationResult, error) {
	if _, err := NewEvaluationRequest(request.Occurrence, request.LogicalTime, request.ActiveRules); err != nil {
		return EvaluationResult{}, fmt.Errorf("evaluation request: %w", err)
	}
	active := make(map[ruleRevision]struct{}, len(request.ActiveRules))
	for _, rule := range request.ActiveRules {
		active[ruleRevision{ID: rule.ID, Revision: rule.Revision}] = struct{}{}
	}
	out := append([]conditions.Observation(nil), observations...)
	sort.Slice(out, func(i, j int) bool { return lessObservation(out[i], out[j]) })
	seen := make(map[string]struct{}, len(out))
	for _, observation := range out {
		if err := validateObservation(request, active, observation); err != nil {
			return EvaluationResult{}, err
		}
		if _, ok := seen[observation.ObservationID]; ok {
			return EvaluationResult{}, fmt.Errorf("duplicate evaluation observation ID %q", observation.ObservationID)
		}
		seen[observation.ObservationID] = struct{}{}
	}
	return EvaluationResult{Observations: out}, nil
}

type ruleRevision struct {
	ID       string
	Revision int64
}

func validateObservation(request EvaluationRequest, active map[ruleRevision]struct{}, observation conditions.Observation) error {
	if observation.ObservationID == "" {
		return fmt.Errorf("evaluation observation ID is required")
	}
	if observation.ObservationID == request.Occurrence.SourceEventID {
		return fmt.Errorf("evaluation observation ID must not be the source event ID")
	}
	if _, ok := active[ruleRevision{ID: observation.Key.RuleID, Revision: observation.Key.Revision}]; !ok {
		return fmt.Errorf("evaluation observation has inactive rule revision %q/%d", observation.Key.RuleID, observation.Key.Revision)
	}
	if observation.Key.Subject.Kind != conditions.SubjectQueue && observation.Key.Subject.Kind != conditions.SubjectAgent || observation.Key.Subject.ID == "" {
		return fmt.Errorf("evaluation observation has invalid subject")
	}
	if observation.Direction != conditions.Trigger && observation.Direction != conditions.Clear {
		return fmt.Errorf("evaluation observation has invalid direction")
	}
	if observation.Result != conditions.Unknown && observation.Result != conditions.False && observation.Result != conditions.True {
		return fmt.Errorf("evaluation observation has invalid result")
	}
	if observation.EffectiveAt.IsZero() || observation.ProcessingAt.IsZero() {
		return fmt.Errorf("evaluation observation times are required")
	}
	if !observation.ProcessingAt.Equal(request.LogicalTime) {
		return fmt.Errorf("evaluation observation processing time must equal logical time")
	}
	if observation.EffectiveAt.After(observation.ProcessingAt) {
		return fmt.Errorf("evaluation observation effective time follows processing time")
	}
	return nil
}

func lessObservation(a, b conditions.Observation) bool {
	if a.Key.RuleID != b.Key.RuleID {
		return a.Key.RuleID < b.Key.RuleID
	}
	if a.Key.Revision != b.Key.Revision {
		return a.Key.Revision < b.Key.Revision
	}
	if a.Key.Subject.Kind != b.Key.Subject.Kind {
		return a.Key.Subject.Kind < b.Key.Subject.Kind
	}
	if a.Key.Subject.ID != b.Key.Subject.ID {
		return a.Key.Subject.ID < b.Key.Subject.ID
	}
	if a.Direction != b.Direction {
		return a.Direction < b.Direction
	}
	return a.ObservationID < b.ObservationID
}

// SemanticCommit is the application-owned record of semantic outputs from one
// occurrence or timer transaction. Its slice order is commit-sequence order;
// a persistence adapter must keep these records atomic with the corresponding
// projection/tracker changes. Projection schemas are intentionally not copied
// here because they are owned by the projection package.
//
// The application path derives ConditionTransitions first, then
// AlertTransitions, then NotificationIntents. It must use the same path for
// preview and replay; delivery attempts are outside this commit.
type SemanticCommit struct {
	Occurrence           AcceptedOccurrence
	LogicalTime          time.Time
	ConditionTransitions []conditions.Transition
	AlertTransitions     []alerts.Transition
	NotificationIntents  []notifications.Intent
}

// NewSemanticCommit copies a valid commit record. Empty output slices are
// valid: an accepted occurrence often changes no condition or alert state.
func NewSemanticCommit(occurrence AcceptedOccurrence, logicalTime time.Time, conditionTransitions []conditions.Transition, alertTransitions []alerts.Transition, notificationIntents []notifications.Intent) (SemanticCommit, error) {
	if err := occurrence.Validate(); err != nil {
		return SemanticCommit{}, err
	}
	if logicalTime.IsZero() {
		return SemanticCommit{}, fmt.Errorf("semantic commit logical time is required")
	}
	if logicalTime.Before(occurrence.EffectiveAt) {
		return SemanticCommit{}, fmt.Errorf("semantic commit logical time precedes occurrence effective time")
	}
	if err := uniqueIDs("condition transition", conditionTransitionIDs(conditionTransitions)); err != nil {
		return SemanticCommit{}, err
	}
	if err := uniqueIDs("alert transition", alertTransitionIDs(alertTransitions)); err != nil {
		return SemanticCommit{}, err
	}
	if err := uniqueIDs("notification intent", notificationIntentIDs(notificationIntents)); err != nil {
		return SemanticCommit{}, err
	}
	return SemanticCommit{
		Occurrence:           occurrence.Clone(),
		LogicalTime:          logicalTime,
		ConditionTransitions: append([]conditions.Transition(nil), conditionTransitions...),
		AlertTransitions:     append([]alerts.Transition(nil), alertTransitions...),
		NotificationIntents:  append([]notifications.Intent(nil), notificationIntents...),
	}, nil
}

func uniqueIDs(kind string, ids []string) error {
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			return fmt.Errorf("%s ID is required", kind)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate %s ID %q", kind, id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func conditionTransitionIDs(values []conditions.Transition) []string {
	ids := make([]string, len(values))
	for i := range values {
		ids[i] = values[i].ID
	}
	return ids
}
func alertTransitionIDs(values []alerts.Transition) []string {
	ids := make([]string, len(values))
	for i := range values {
		ids[i] = values[i].ID
	}
	return ids
}
func notificationIntentIDs(values []notifications.Intent) []string {
	ids := make([]string, len(values))
	for i := range values {
		ids[i] = values[i].ID
	}
	return ids
}
