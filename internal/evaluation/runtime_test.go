package evaluation

import (
	"testing"
	"time"

	"watchtower/internal/app"
	"watchtower/internal/conditions"
	"watchtower/internal/events"
	"watchtower/internal/rules"
)

func TestRuntimeUsesPinnedTargetsAndNeverEventQueueMembership(t *testing.T) {
	definition := queueRule(0, 0, []string{"billing"}, nil)
	runtime, err := Activate([]app.ActiveRule{{ID: "billing", Revision: 1, Definition: definition}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	at := testTime()
	if _, err := runtime.Advance(at); err != nil {
		t.Fatal(err)
	}

	// An agent observation names billing only as observational queue_ids. It is
	// not a queue target and must not evaluate the queue rule.
	event := events.AgentStateChange{Common: events.Common{EventID: "repeated-source", Timestamp: at, Type: "agent_state_change"}, AgentID: "a_05", NewState: events.OnCall, QueueIDs: []string{"billing"}}
	request := requestFor(t, occurrence("occurrence-1", event), at, definition)
	result, err := runtime.Evaluate(request, EvidenceProviderFunc(func(rules.ResolvedTarget) (Evidence, bool) { return Evidence{}, true }))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Observations) != 0 {
		t.Fatalf("queue_ids created membership observations: %+v", result.Observations)
	}

	queueEvent := events.QueueSnapshot{Common: events.Common{EventID: "repeated-source", Timestamp: at, Type: "queue_snapshot"}, QueueID: "billing", LongestWaitSec: 600, SLATargetSec: 300}
	request = requestFor(t, occurrence("occurrence-2", queueEvent), at, definition)
	result, err = runtime.Evaluate(request, queueEvidence(true, false, time.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Observations) != 2 || result.Observations[0].Result != conditions.True || result.Observations[1].Result != conditions.False {
		t.Fatalf("observations=%+v", result.Observations)
	}
	if result.Observations[0].ObservationID == "repeated-source" || result.Observations[0].ObservationID == result.Observations[1].ObservationID {
		t.Fatalf("observation identity uses source ID: %+v", result.Observations)
	}
}

func TestRuntimeFiresDueTriggerBeforeSameTimeClear(t *testing.T) {
	definition := queueRule(5*time.Minute, 0, []string{"billing"}, nil)
	runtime, err := Activate([]app.ActiveRule{{ID: "billing", Revision: 1, Definition: definition}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := testTime()
	first := events.QueueSnapshot{Common: events.Common{EventID: "source-1", Timestamp: base, Type: "queue_snapshot"}, QueueID: "billing"}
	if transitions := applyEvent(t, runtime, occurrence("occ-1", first), base, definition, queueEvidence(true, false, time.Time{})); len(transitions) != 0 {
		t.Fatalf("initial transitions=%+v", transitions)
	}

	due := base.Add(5 * time.Minute)
	dueTransitions, err := runtime.Advance(due)
	if err != nil {
		t.Fatal(err)
	}
	if len(dueTransitions) != 1 || dueTransitions[0].Direction != conditions.Trigger || !dueTransitions[0].At.Equal(due) {
		t.Fatalf("due transitions=%+v", dueTransitions)
	}
	clear := events.QueueSnapshot{Common: events.Common{EventID: "source-2", Timestamp: due, Type: "queue_snapshot"}, QueueID: "billing"}
	clearTransitions := applyAfterAdvance(t, runtime, occurrence("occ-2", clear), due, definition, queueEvidence(false, true, time.Time{}))
	if len(clearTransitions) != 1 || clearTransitions[0].Direction != conditions.Clear || !clearTransitions[0].At.Equal(due) {
		t.Fatalf("clear transitions=%+v", clearTransitions)
	}
}

func TestRuntimeAdherenceOnsetAndUnknownReset(t *testing.T) {
	definition := adherenceRule(10*time.Minute, 0)
	runtime, err := Activate([]app.ActiveRule{{ID: "adherence", Revision: 1, Definition: definition}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := testTime()
	observed := base.Add(time.Minute)
	check := events.AdherenceCheck{Common: events.Common{EventID: "source-1", Timestamp: observed, Type: "adherence_check"}, AgentID: "a_19", InViolation: true}
	if transitions := applyEvent(t, runtime, occurrence("occ-1", check), observed, definition, adherenceEvidence(true, base)); len(transitions) != 0 {
		t.Fatalf("initial transitions=%+v", transitions)
	}
	if transitions, err := runtime.Advance(base.Add(10 * time.Minute)); err != nil || len(transitions) != 1 || !transitions[0].At.Equal(base.Add(10*time.Minute)) {
		t.Fatalf("onset transition=%+v err=%v", transitions, err)
	}

	// A separate pending condition demonstrates that unknown cancels duration
	// tracking rather than silently qualifying it.
	unknownDefinition := adherenceRule(time.Minute, 0)
	unknownRuntime, err := Activate([]app.ActiveRule{{ID: "adherence", Revision: 1, Definition: unknownDefinition}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := events.AdherenceCheck{Common: events.Common{EventID: "source-2", Timestamp: base, Type: "adherence_check"}, AgentID: "a_19", InViolation: true}
	applyEvent(t, unknownRuntime, occurrence("occ-2", start), base, unknownDefinition, adherenceEvidence(true, time.Time{}))
	unknownAt := base.Add(30 * time.Second)
	unknown := events.AdherenceCheck{Common: events.Common{EventID: "source-3", Timestamp: unknownAt, Type: "adherence_check"}, AgentID: "a_19", InViolation: true}
	applyEvent(t, unknownRuntime, occurrence("occ-3", unknown), unknownAt, unknownDefinition, EvidenceProviderFunc(func(rules.ResolvedTarget) (Evidence, bool) { return Evidence{}, false }))
	if transitions, err := unknownRuntime.Advance(base.Add(2 * time.Minute)); err != nil || len(transitions) != 0 {
		t.Fatalf("unknown qualified pending trigger=%+v err=%v", transitions, err)
	}
}

func queueRule(triggerFor, clearFor time.Duration, ids, groups []string) rules.RuleDefinition {
	trigger := rules.NewCompare(rules.FieldOperand("queue.longest_wait"), rules.OpGreater, rules.FieldOperand("queue.sla_target"))
	clear := rules.NewCompare(rules.FieldOperand("queue.longest_wait"), rules.OpLessOrEqual, rules.FieldOperand("queue.sla_target"))
	return rules.NewRuleDefinition("queue", "", rules.NewTargets(rules.SubjectQueue, ids, groups), rules.NewCondition(trigger, rules.Duration(triggerFor)), rules.NewCondition(clear, rules.Duration(clearFor)), rules.NotificationPolicy{OnOpen: true, OnRecovery: true, Audience: "ops"})
}
func adherenceRule(triggerFor, clearFor time.Duration) rules.RuleDefinition {
	trigger := rules.NewCompare(rules.FieldOperand("adherence.violation"), rules.OpEqual, rules.BooleanOperand(true))
	clear := rules.NewCompare(rules.FieldOperand("adherence.violation"), rules.OpEqual, rules.BooleanOperand(false))
	return rules.NewRuleDefinition("adherence", "", rules.NewTargets(rules.SubjectAgent, []string{"a_19"}, nil), rules.NewCondition(trigger, rules.Duration(triggerFor)), rules.NewCondition(clear, rules.Duration(clearFor)), rules.NotificationPolicy{OnOpen: true, OnRecovery: true, Audience: "ops"})
}
func queueEvidence(trigger, clear bool, onset time.Time) EvidenceProvider {
	return EvidenceProviderFunc(func(rules.ResolvedTarget) (Evidence, bool) {
		return Evidence{Lookup: rules.FieldLookupFunc(func(field rules.FieldSpec) (rules.Operand, bool) {
			switch field.Name {
			case "queue.longest_wait":
				if trigger {
					return rules.DurationOperand(rules.Duration(600 * time.Second)), true
				}
				return rules.DurationOperand(rules.Duration(0)), true
			case "queue.sla_target":
				return rules.DurationOperand(rules.Duration(300 * time.Second)), true
			}
			return rules.Operand{}, false
		}), TrueSince: onset}, true
	})
}
func adherenceEvidence(value bool, onset time.Time) EvidenceProvider {
	return EvidenceProviderFunc(func(rules.ResolvedTarget) (Evidence, bool) {
		return Evidence{Lookup: rules.FieldLookupFunc(func(field rules.FieldSpec) (rules.Operand, bool) {
			if field.Name == "adherence.violation" {
				return rules.BooleanOperand(value), true
			}
			return rules.Operand{}, false
		}), TrueSince: onset}, true
	})
}
func testTime() time.Time { return time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC) }
func occurrence(id string, event events.Event) app.AcceptedOccurrence {
	return app.AcceptedOccurrence{ID: id, Source: "test", IdempotencyKey: "key-" + id, IngestPosition: 1, SourceEventID: event.GetEventID(), EffectiveAt: eventTime(event), Event: event, Raw: []byte(`{"event_id":"source"}`)}
}
func eventTime(event events.Event) time.Time {
	switch value := event.(type) {
	case events.QueueSnapshot:
		return value.Timestamp
	case events.AgentStateChange:
		return value.Timestamp
	case events.AdherenceCheck:
		return value.Timestamp
	}
	panic("unsupported event")
}
func requestFor(t *testing.T, value app.AcceptedOccurrence, logical time.Time, definition rules.RuleDefinition) app.EvaluationRequest {
	t.Helper()
	request, err := app.NewEvaluationRequest(value, logical, []app.ActiveRule{{ID: activeID(definition), Revision: 1, Definition: definition}})
	if err != nil {
		t.Fatal(err)
	}
	return request
}
func activeID(definition rules.RuleDefinition) string {
	if definition.Targets.Kind == rules.SubjectAgent {
		return "adherence"
	}
	return "billing"
}
func applyEvent(t *testing.T, runtime *Runtime, value app.AcceptedOccurrence, logical time.Time, definition rules.RuleDefinition, provider EvidenceProvider) []conditions.Transition {
	t.Helper()
	if _, err := runtime.Advance(logical); err != nil {
		t.Fatal(err)
	}
	return applyAfterAdvance(t, runtime, value, logical, definition, provider)
}
func applyAfterAdvance(t *testing.T, runtime *Runtime, value app.AcceptedOccurrence, logical time.Time, definition rules.RuleDefinition, provider EvidenceProvider) []conditions.Transition {
	t.Helper()
	result, err := runtime.Evaluate(requestFor(t, value, logical, definition), provider)
	if err != nil {
		t.Fatal(err)
	}
	transitions, err := runtime.Apply(result)
	if err != nil {
		t.Fatal(err)
	}
	return transitions
}

func TestRuntimeOrdersEqualDueTimersAcrossRules(t *testing.T) {
	definition := queueRule(time.Minute, 0, []string{"billing"}, nil)
	runtime, err := Activate([]app.ActiveRule{
		{ID: "z-rule", Revision: 1, Definition: definition},
		{ID: "a-rule", Revision: 2, Definition: definition},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	base := testTime()
	if _, err := runtime.Advance(base); err != nil {
		t.Fatal(err)
	}
	event := events.QueueSnapshot{Common: events.Common{EventID: "source", Timestamp: base, Type: "queue_snapshot"}, QueueID: "billing"}
	request, err := app.NewEvaluationRequest(occurrence("two-rules", event), base, []app.ActiveRule{
		{ID: "z-rule", Revision: 1, Definition: definition},
		{ID: "a-rule", Revision: 2, Definition: definition},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Evaluate(request, queueEvidence(true, false, time.Time{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Apply(result); err != nil {
		t.Fatal(err)
	}
	transitions, err := runtime.Advance(base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 2 || transitions[0].Key.RuleID != "a-rule" || transitions[1].Key.RuleID != "z-rule" {
		t.Fatalf("equal due order=%+v", transitions)
	}
}
