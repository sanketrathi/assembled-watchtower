// Package evaluation composes active compiled rules with condition reducers.
//
// It deliberately does not decode or ingest events, maintain projections, run
// replay, persist work, or derive alerts and notifications. A coordinator owns
// those boundaries and calls this package with an admitted occurrence and
// projection-owned evidence.
package evaluation

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"watchtower/internal/app"
	"watchtower/internal/conditions"
	"watchtower/internal/events"
	"watchtower/internal/rules"
)

// Evidence is the projection-owned input for one selected subject. Lookup
// returns current source-specific field values; absent or incompatible values
// remain unknown. TrueSince is set only when the projection can prove the
// complete trigger predicate has been continuously true since that time (for
// example, a supplied adherence onset). ClearTrueSince has the equivalent
// meaning for a clear predicate.
type Evidence struct {
	Lookup         rules.FieldLookup
	TrueSince      time.Time
	ClearTrueSince time.Time
	Reason         string
}

// EvidenceProvider keeps projection representation outside this package. It
// must not derive configured membership from an event's queue_ids; the runtime
// supplies only targets pinned at activation.
type EvidenceProvider interface {
	EvidenceFor(rules.ResolvedTarget) (Evidence, bool)
}

// EvidenceProviderFunc adapts a function to EvidenceProvider.
type EvidenceProviderFunc func(rules.ResolvedTarget) (Evidence, bool)

func (f EvidenceProviderFunc) EvidenceFor(target rules.ResolvedTarget) (Evidence, bool) {
	if f == nil {
		return Evidence{}, false
	}
	return f(target)
}

// CoordinatorBoundary is the narrow integration surface for a future replay or
// live coordinator. The coordinator advances time before evidence evaluation;
// this package never reads JSONL or owns ingestion order.
type CoordinatorBoundary interface {
	Advance(time.Time) ([]conditions.Transition, error)
	Evaluate(app.EvaluationRequest, EvidenceProvider) (app.EvaluationResult, error)
	Apply(app.EvaluationResult) ([]conditions.Transition, error)
}

// Runtime holds one activation set and its storage-independent condition
// reducers. It is intentionally single-threaded; a storage/application adapter
// serializes logical-clock work before calling it.
type Runtime struct {
	active  []activation
	byKey   map[ruleKey]int
	logical time.Time
}

type activation struct {
	key       ruleKey
	signature string
	plan      rules.EvaluationPlan
	reducer   *conditions.Reducer
}

type ruleKey struct {
	id       string
	revision int64
}

// Activate validates, compiles, and pins each immutable active rule revision.
// A configured group is resolved exactly once through resolver. Lifecycle and
// old/new revision reconciliation are intentionally outside this boundary.
func Activate(activeRules []app.ActiveRule, resolver rules.GroupResolver) (*Runtime, error) {
	values := append([]app.ActiveRule(nil), activeRules...)
	sort.Slice(values, func(i, j int) bool {
		if values[i].ID != values[j].ID {
			return values[i].ID < values[j].ID
		}
		return values[i].Revision < values[j].Revision
	})
	runtime := &Runtime{byKey: make(map[ruleKey]int, len(values))}
	for i, active := range values {
		if active.ID == "" {
			return nil, fmt.Errorf("active rule ID is required")
		}
		if active.Revision <= 0 {
			return nil, fmt.Errorf("active rule %q revision must be positive", active.ID)
		}
		key := ruleKey{id: active.ID, revision: active.Revision}
		if i > 0 && values[i-1].ID == active.ID && values[i-1].Revision == active.Revision {
			return nil, fmt.Errorf("duplicate active rule revision %q/%d", active.ID, active.Revision)
		}
		var plan rules.EvaluationPlan
		var err error
		if resolver == nil {
			plan, err = rules.CompileForEvaluation(active.Definition)
		} else {
			plan, err = rules.CompileForEvaluation(active.Definition, resolver)
		}
		if err != nil {
			return nil, fmt.Errorf("compile active rule %q/%d: %w", active.ID, active.Revision, err)
		}
		signature, err := definitionSignature(active.Definition)
		if err != nil {
			return nil, fmt.Errorf("canonicalize active rule %q/%d: %w", active.ID, active.Revision, err)
		}
		entry := activation{
			key: key, signature: signature, plan: plan,
			reducer: conditions.New(conditions.RuleConfig{
				RuleID: active.ID, Revision: active.Revision,
				TriggerDuration: plan.TriggerDuration(), ClearDuration: plan.ClearDuration(),
			}),
		}
		runtime.active = append(runtime.active, entry)
		runtime.byKey[key] = len(runtime.active) - 1
	}
	return runtime, nil
}

// LogicalTime returns the most recently advanced logical time.
func (r *Runtime) LogicalTime() time.Time {
	if r == nil {
		return time.Time{}
	}
	return r.logical
}

// Advance fires every pending timer due through now in global canonical order:
// due time, rule ID, revision, subject kind, subject ID, then attempt. The
// caller must invoke it before evaluating evidence at the same logical time.
func (r *Runtime) Advance(now time.Time) ([]conditions.Transition, error) {
	if r == nil {
		return nil, fmt.Errorf("evaluation runtime is required")
	}
	if now.IsZero() {
		return nil, fmt.Errorf("logical time is required")
	}
	if !r.logical.IsZero() && now.Before(r.logical) {
		return nil, fmt.Errorf("logical time %s precedes runtime clock %s", now, r.logical)
	}
	var out []conditions.Transition
	for {
		entry, timer, found := r.nextDue(now)
		if !found {
			break
		}
		out = append(out, entry.reducer.Fire(timer, now)...)
	}
	// Fire above consumes every due timer. Advance the individual reducers only
	// to synchronize their clocks; it cannot emit another due transition now.
	for i := range r.active {
		out = append(out, r.active[i].reducer.Advance(now)...)
	}
	r.logical = now
	return out, nil
}

func (r *Runtime) nextDue(now time.Time) (*activation, conditions.Timer, bool) {
	var selected *activation
	var selectedTimer conditions.Timer
	for i := range r.active {
		for _, timer := range r.active[i].reducer.Timers() {
			if timer.DueAt.After(now) {
				break
			}
			if selected == nil || lessTimer(timer, selectedTimer) {
				selected, selectedTimer = &r.active[i], timer
			}
		}
	}
	return selected, selectedTimer, selected != nil
}

func lessTimer(left, right conditions.Timer) bool {
	if !left.DueAt.Equal(right.DueAt) {
		return left.DueAt.Before(right.DueAt)
	}
	if left.Key.RuleID != right.Key.RuleID {
		return left.Key.RuleID < right.Key.RuleID
	}
	if left.Key.Revision != right.Key.Revision {
		return left.Key.Revision < right.Key.Revision
	}
	if left.Key.Subject.Kind != right.Key.Subject.Kind {
		return left.Key.Subject.Kind < right.Key.Subject.Kind
	}
	if left.Key.Subject.ID != right.Key.Subject.ID {
		return left.Key.Subject.ID < right.Key.Subject.ID
	}
	return left.Attempt < right.Attempt
}

// Evaluate produces canonical trigger and clear observations for the admitted
// occurrence. It neither advances timers nor changes reducers. Projection
// adapters supply state through evidence; queue_ids are never consulted.
func (r *Runtime) Evaluate(request app.EvaluationRequest, provider EvidenceProvider) (app.EvaluationResult, error) {
	if r == nil {
		return app.EvaluationResult{}, fmt.Errorf("evaluation runtime is required")
	}
	if err := r.validateRequest(request); err != nil {
		return app.EvaluationResult{}, err
	}
	target, ok := occurrenceTarget(request.Occurrence.Event)
	if !ok {
		return app.EvaluationResult{}, fmt.Errorf("unsupported accepted event %T", request.Occurrence.Event)
	}
	evidence, available := Evidence{}, false
	if provider != nil {
		evidence, available = provider.EvidenceFor(target)
	}
	observations := make([]conditions.Observation, 0, len(r.active)*2)
	for i := range r.active {
		entry := &r.active[i]
		if !entry.plan.HasTarget(target.Kind, target.ID) {
			continue
		}
		trigger, clear := conditions.Unknown, conditions.Unknown
		if available {
			trigger = conditionResult(entry.plan.EvaluateTrigger(evidence.Lookup))
			clear = conditionResult(entry.plan.EvaluateClear(evidence.Lookup))
		}
		key := conditions.ConditionKey{RuleID: entry.key.id, Revision: entry.key.revision, Subject: conditionSubject(target)}
		observations = append(observations,
			observation(request, key, conditions.Trigger, trigger, evidence.TrueSince, evidence.Reason),
			observation(request, key, conditions.Clear, clear, evidence.ClearTrueSince, evidence.Reason),
		)
	}
	result, err := app.NewEvaluationResult(request, observations)
	if err != nil {
		return app.EvaluationResult{}, fmt.Errorf("build evaluation result: %w", err)
	}
	return result, nil
}

// Apply records observations in canonical order after the coordinator has
// advanced logical time. It returns only condition transitions; alert and
// notification workflow belongs to its own domain/application path.
func (r *Runtime) Apply(result app.EvaluationResult) ([]conditions.Transition, error) {
	if r == nil {
		return nil, fmt.Errorf("evaluation runtime is required")
	}
	if err := r.validateResult(result); err != nil {
		return nil, err
	}
	var out []conditions.Transition
	for _, observation := range result.Observations {
		entry := r.active[r.byKey[ruleKey{id: observation.Key.RuleID, revision: observation.Key.Revision}]]
		out = append(out, entry.reducer.Apply(observation)...)
	}
	return out, nil
}

func (r *Runtime) validateRequest(request app.EvaluationRequest) error {
	validated, err := app.NewEvaluationRequest(request.Occurrence, request.LogicalTime, request.ActiveRules)
	if err != nil {
		return fmt.Errorf("evaluation request: %w", err)
	}
	if !r.logical.Equal(validated.LogicalTime) {
		return fmt.Errorf("runtime clock %s does not match evaluation logical time %s; advance first", r.logical, validated.LogicalTime)
	}
	if len(validated.ActiveRules) != len(r.active) {
		return fmt.Errorf("evaluation request active revisions do not match activation")
	}
	for _, active := range validated.ActiveRules {
		index, ok := r.byKey[ruleKey{id: active.ID, revision: active.Revision}]
		if !ok {
			return fmt.Errorf("evaluation request has unactivated rule revision %q/%d", active.ID, active.Revision)
		}
		signature, err := definitionSignature(active.Definition)
		if err != nil {
			return fmt.Errorf("canonicalize request rule %q/%d: %w", active.ID, active.Revision, err)
		}
		if signature != r.active[index].signature {
			return fmt.Errorf("evaluation request definition differs from activated rule %q/%d", active.ID, active.Revision)
		}
	}
	return nil
}

func (r *Runtime) validateResult(result app.EvaluationResult) error {
	seen := make(map[string]struct{}, len(result.Observations))
	for i, observation := range result.Observations {
		index, ok := r.byKey[ruleKey{id: observation.Key.RuleID, revision: observation.Key.Revision}]
		if !ok {
			return fmt.Errorf("evaluation observation has unactivated rule revision %q/%d", observation.Key.RuleID, observation.Key.Revision)
		}
		entry := r.active[index]
		if !entry.plan.HasTarget(rules.SubjectKind(observation.Key.Subject.Kind), observation.Key.Subject.ID) {
			return fmt.Errorf("evaluation observation has unselected subject %q", observation.Key.Subject.ID)
		}
		if observation.ObservationID == "" {
			return fmt.Errorf("evaluation observation ID is required")
		}
		if _, duplicate := seen[observation.ObservationID]; duplicate {
			return fmt.Errorf("duplicate evaluation observation ID %q", observation.ObservationID)
		}
		seen[observation.ObservationID] = struct{}{}
		if !observation.ProcessingAt.Equal(r.logical) {
			return fmt.Errorf("evaluation observation processing time %s does not match runtime clock %s", observation.ProcessingAt, r.logical)
		}
		if observation.EffectiveAt.IsZero() || observation.EffectiveAt.After(observation.ProcessingAt) {
			return fmt.Errorf("evaluation observation has invalid effective time")
		}
		if observation.Direction != conditions.Trigger && observation.Direction != conditions.Clear {
			return fmt.Errorf("evaluation observation has invalid direction")
		}
		if observation.Result != conditions.Unknown && observation.Result != conditions.False && observation.Result != conditions.True {
			return fmt.Errorf("evaluation observation has invalid result")
		}
		if i > 0 && lessObservation(observation, result.Observations[i-1]) {
			return fmt.Errorf("evaluation observations are not in canonical order")
		}
	}
	return nil
}

func occurrenceTarget(event events.Event) (rules.ResolvedTarget, bool) {
	switch value := event.(type) {
	case events.QueueSnapshot:
		return rules.ResolvedTarget{Kind: rules.SubjectQueue, ID: value.QueueID}, true
	case events.AgentStateChange:
		return rules.ResolvedTarget{Kind: rules.SubjectAgent, ID: value.AgentID}, true
	case events.AdherenceCheck:
		return rules.ResolvedTarget{Kind: rules.SubjectAgent, ID: value.AgentID}, true
	default:
		return rules.ResolvedTarget{}, false
	}
}

func conditionSubject(target rules.ResolvedTarget) conditions.SubjectKey {
	return conditions.SubjectKey{Kind: conditions.SubjectKind(target.Kind), ID: target.ID}
}

func conditionResult(result rules.PredicateResult) conditions.Result {
	switch result {
	case rules.PredicateTrue:
		return conditions.True
	case rules.PredicateFalse:
		return conditions.False
	default:
		return conditions.Unknown
	}
}

func observation(request app.EvaluationRequest, key conditions.ConditionKey, direction conditions.Direction, result conditions.Result, trueSince time.Time, reason string) conditions.Observation {
	return conditions.Observation{
		Key: key, Direction: direction, Result: result,
		EffectiveAt: request.Occurrence.EffectiveAt, ProcessingAt: request.LogicalTime,
		EvidenceAt: request.Occurrence.EffectiveAt, TrueSince: trueSince,
		ObservationID: observationID(request.Occurrence.ID, key, direction), Reason: reason,
	}
}

func observationID(occurrenceID string, key conditions.ConditionKey, direction conditions.Direction) string {
	parts := []string{occurrenceID, key.RuleID, strconv.FormatInt(key.Revision, 10), string(key.Subject.Kind), key.Subject.ID, direction.String()}
	var value strings.Builder
	for _, part := range parts {
		value.WriteString(strconv.Itoa(len(part)))
		value.WriteByte(':')
		value.WriteString(part)
	}
	return "evaluation_" + hex.EncodeToString([]byte(value.String()))
}

func definitionSignature(definition rules.RuleDefinition) (string, error) {
	encoded, err := json.Marshal(definition)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func lessObservation(left, right conditions.Observation) bool {
	if left.Key.RuleID != right.Key.RuleID {
		return left.Key.RuleID < right.Key.RuleID
	}
	if left.Key.Revision != right.Key.Revision {
		return left.Key.Revision < right.Key.Revision
	}
	if left.Key.Subject.Kind != right.Key.Subject.Kind {
		return left.Key.Subject.Kind < right.Key.Subject.Kind
	}
	if left.Key.Subject.ID != right.Key.Subject.ID {
		return left.Key.Subject.ID < right.Key.Subject.ID
	}
	if left.Direction != right.Direction {
		return left.Direction < right.Direction
	}
	return left.ObservationID < right.ObservationID
}

var _ CoordinatorBoundary = (*Runtime)(nil)
