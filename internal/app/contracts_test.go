package app

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"watchtower/internal/alerts"
	"watchtower/internal/conditions"
	"watchtower/internal/events"
	"watchtower/internal/notifications"
	"watchtower/internal/rules"
)

func TestAcceptedOccurrenceValidationAndClone(t *testing.T) {
	occurrence := testOccurrence()
	if err := occurrence.Validate(); err != nil {
		t.Fatal(err)
	}
	clone := occurrence.Clone()
	clone.Raw[0] = '['
	if bytes.Equal(clone.Raw, occurrence.Raw) {
		t.Fatal("clone raw payload aliases source")
	}
	for _, tc := range []struct {
		name string
		edit func(*AcceptedOccurrence)
	}{
		{"missing ID", func(o *AcceptedOccurrence) { o.ID = "" }},
		{"missing source", func(o *AcceptedOccurrence) { o.Source = "" }},
		{"missing idempotency key", func(o *AcceptedOccurrence) { o.IdempotencyKey = "" }},
		{"zero ingest position", func(o *AcceptedOccurrence) { o.IngestPosition = 0 }},
		{"source event ID as occurrence ID", func(o *AcceptedOccurrence) { o.ID = o.SourceEventID }},
		{"source event ID as idempotency key", func(o *AcceptedOccurrence) { o.IdempotencyKey = o.SourceEventID }},
		{"event ID mismatch", func(o *AcceptedOccurrence) { o.SourceEventID = "other" }},
		{"effective time mismatch", func(o *AcceptedOccurrence) { o.EffectiveAt = o.EffectiveAt.Add(time.Second) }},
		{"missing raw", func(o *AcceptedOccurrence) { o.Raw = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := testOccurrence()
			tc.edit(&got)
			if err := got.Validate(); err == nil {
				t.Fatal("accepted invalid occurrence")
			}
		})
	}
}

func TestEvaluationRequestCanonicalizesRulesAndAllowsLateEvidence(t *testing.T) {
	occurrence := testOccurrence()
	logical := occurrence.EffectiveAt.Add(time.Minute)
	request, err := NewEvaluationRequest(occurrence, logical, []ActiveRule{testRule("z", 1), testRule("a", 2)})
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{request.ActiveRules[0].ID, request.ActiveRules[1].ID}; strings.Join(got, ",") != "a,z" {
		t.Fatalf("rules=%v", got)
	}
	if !request.LogicalTime.Equal(logical) {
		t.Fatalf("logical time=%s", request.LogicalTime)
	}
	if _, err := NewEvaluationRequest(occurrence, logical, []ActiveRule{testRule("same", 1), testRule("same", 2)}); err != nil {
		t.Fatalf("multiple explicit revisions: %v", err)
	}
	if _, err := NewEvaluationRequest(occurrence, logical, []ActiveRule{testRule("same", 1), testRule("same", 1)}); err == nil {
		t.Fatal("accepted duplicate active rule revision")
	}
	if _, err := NewEvaluationRequest(occurrence, occurrence.EffectiveAt.Add(-time.Nanosecond), nil); err == nil {
		t.Fatal("accepted a logical rewind")
	}
}

func TestEvaluationResultOrdersAndFencesSourceEventID(t *testing.T) {
	occurrence := testOccurrence()
	request, err := NewEvaluationRequest(occurrence, occurrence.EffectiveAt, []ActiveRule{testRule("rule", 1)})
	if err != nil {
		t.Fatal(err)
	}
	key := conditions.ConditionKey{RuleID: "rule", Revision: 1, Subject: conditions.SubjectKey{Kind: conditions.SubjectQueue, ID: "billing"}}
	clear := conditions.Observation{Key: key, Direction: conditions.Clear, Result: conditions.False, EffectiveAt: occurrence.EffectiveAt, ProcessingAt: request.LogicalTime, ObservationID: "occurrence-1:clear"}
	trigger := conditions.Observation{Key: key, Direction: conditions.Trigger, Result: conditions.True, EffectiveAt: occurrence.EffectiveAt, ProcessingAt: request.LogicalTime, ObservationID: "occurrence-1:trigger"}
	result, err := NewEvaluationResult(request, []conditions.Observation{clear, trigger})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Observations) != 2 || result.Observations[0].Direction != conditions.Trigger || result.Observations[1].Direction != conditions.Clear {
		t.Fatalf("observations=%+v", result.Observations)
	}
	invalid := trigger
	invalid.ObservationID = occurrence.SourceEventID
	if _, err := NewEvaluationResult(request, []conditions.Observation{invalid}); err == nil {
		t.Fatal("accepted raw source event ID as observation identity")
	}
	invalid = trigger
	invalid.ProcessingAt = request.LogicalTime.Add(time.Second)
	if _, err := NewEvaluationResult(request, []conditions.Observation{invalid}); err == nil {
		t.Fatal("accepted processing time that differs from logical time")
	}
}

func TestSemanticCommitCopiesOutputsAndFencesDuplicateIDs(t *testing.T) {
	occurrence := testOccurrence()
	at := occurrence.EffectiveAt
	conditionTransitions := []conditions.Transition{{ID: "condition-1"}}
	alertTransitions := []alerts.Transition{{ID: "alert-1"}}
	intents := []notifications.Intent{{ID: "intent-1"}}
	commit, err := NewSemanticCommit(occurrence, at, conditionTransitions, alertTransitions, intents)
	if err != nil {
		t.Fatal(err)
	}
	conditionTransitions[0].ID = "changed"
	alertTransitions[0].ID = "changed"
	intents[0].ID = "changed"
	if commit.ConditionTransitions[0].ID != "condition-1" || commit.AlertTransitions[0].ID != "alert-1" || commit.NotificationIntents[0].ID != "intent-1" {
		t.Fatalf("commit aliases output slices: %+v", commit)
	}
	if _, err := NewSemanticCommit(occurrence, at, []conditions.Transition{{ID: "same"}, {ID: "same"}}, nil, nil); err == nil {
		t.Fatal("accepted duplicate condition transition IDs")
	}
}

func testOccurrence() AcceptedOccurrence {
	at := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	event := events.QueueSnapshot{Common: events.Common{EventID: "source-event", Timestamp: at, Type: "queue_snapshot"}, QueueID: "billing"}
	return AcceptedOccurrence{ID: "occurrence-1", Source: "fixture", IdempotencyKey: "fixture#1", IngestPosition: 1, SourceEventID: "source-event", EffectiveAt: at, Event: event, Raw: []byte(`{"event_id":"source-event"}`)}
}

func testRule(id string, revision int64) ActiveRule {
	predicate := rules.NewCompare(rules.FieldOperand("queue.longest_wait"), rules.OpGreater, rules.FieldOperand("queue.sla_target"))
	definition := rules.NewRuleDefinition(id, "", rules.NewTargets(rules.SubjectQueue, []string{"billing"}, nil), rules.NewCondition(predicate, 0), rules.NewCondition(predicate, 0), rules.NotificationPolicy{OnOpen: true, OnRecovery: true, Audience: "ops"})
	return ActiveRule{ID: id, Revision: revision, Definition: definition}
}
