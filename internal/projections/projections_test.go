package projections

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"watchtower/internal/app"
	"watchtower/internal/events"
)

func TestBuilderBuildsDistinctSourceUpdates(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	builder := NewBuilder()
	queue := queueEvent("source-queue", base, "billing", 4)
	agent := agentEvent("source-agent", base.Add(time.Minute), "a_31", events.OnCall, []string{"billing"})
	adherence := adherenceEvent("source-adherence", base.Add(2*time.Minute), "a_31", events.Available, events.OnBreak, true, nil, []string{"billing"})

	cases := []struct {
		name       string
		occurrence app.AcceptedOccurrence
		check      func(t *testing.T, update Update)
	}{
		{
			name:       "queue snapshot",
			occurrence: occurrence("queue-occurrence", "fixture#1", 1, queue),
			check: func(t *testing.T, update Update) {
				t.Helper()
				if update.Queue == nil || update.AgentState != nil || update.Adherence != nil {
					t.Fatalf("update=%+v", update)
				}
				if !update.Queue.CurrentChanged || update.Queue.Current.QueueID != "billing" || update.Queue.Current.TicketsWaiting != 4 {
					t.Fatalf("queue update=%+v", update.Queue)
				}
				if got := update.Queue.Observation.Provenance; got.OccurrenceID != "queue-occurrence" || got.SourceEventID != "source-queue" || got.IngestPosition != 1 {
					t.Fatalf("provenance=%+v", got)
				}
			},
		},
		{
			name:       "agent state change",
			occurrence: occurrence("agent-occurrence", "fixture#2", 2, agent),
			check: func(t *testing.T, update Update) {
				t.Helper()
				if update.Queue != nil || update.AgentState == nil || update.Adherence != nil {
					t.Fatalf("update=%+v", update)
				}
				if !update.AgentState.CurrentChanged || update.AgentState.Current.Current != events.OnCall {
					t.Fatalf("agent update=%+v", update.AgentState)
				}
			},
		},
		{
			name:       "adherence check stays separate from agent state",
			occurrence: occurrence("adherence-occurrence", "fixture#3", 3, adherence),
			check: func(t *testing.T, update Update) {
				t.Helper()
				if update.Queue != nil || update.AgentState != nil || update.Adherence == nil {
					t.Fatalf("update=%+v", update)
				}
				if !update.Adherence.CurrentChanged || update.Adherence.Current.ActualState != events.OnBreak || !update.Adherence.Current.InViolation {
					t.Fatalf("adherence update=%+v", update.Adherence)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			update, err := builder.Build(tc.occurrence)
			if err != nil {
				t.Fatal(err)
			}
			tc.check(t, update)
		})
	}
	if state, ok := builder.Agent("a_31"); !ok || state.Current != events.OnCall {
		t.Fatalf("adherence actual state overwrote agent projection: %+v ok=%v", state, ok)
	}
	if state, ok := builder.Adherence("a_31"); !ok || state.ActualState != events.OnBreak {
		t.Fatalf("adherence state=%+v ok=%v", state, ok)
	}
}

func TestBuilderRetainsRepeatedSourceEventIDs(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	builder := NewBuilder()
	for _, tc := range []struct {
		occurrence app.AcceptedOccurrence
		waiting    uint64
	}{
		{occurrence("occurrence-1", "fixture#1", 1, queueEvent("duplicated-source-id", base, "billing", 2)), 2},
		{occurrence("occurrence-2", "fixture#2", 2, queueEvent("duplicated-source-id", base.Add(time.Minute), "billing", 7)), 7},
	} {
		update, err := builder.Build(tc.occurrence)
		if err != nil {
			t.Fatal(err)
		}
		if update.Queue == nil || update.Queue.Current.TicketsWaiting != tc.waiting {
			t.Fatalf("update=%+v", update)
		}
	}
	history := builder.QueueHistory("billing")
	if len(history) != 2 || history[0].Provenance.SourceEventID != "duplicated-source-id" || history[1].Provenance.SourceEventID != "duplicated-source-id" {
		t.Fatalf("repeated source event ID was not retained: %+v", history)
	}
}

func TestBuilderLateEvidenceIsSourceLocalAndDoesNotRegressCurrentState(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	builder := NewBuilder()
	cases := []struct {
		name        string
		occurrence  app.AcceptedOccurrence
		current     bool
		checkUpdate func(t *testing.T, update Update)
	}{
		{
			name: "new queue evidence becomes current",
			occurrence: occurrence("queue-current", "fixture#1", 1,
				queueEvent("queue-new", base.Add(2*time.Hour), "vip", 9)),
			current: true,
			checkUpdate: func(t *testing.T, update Update) {
				if update.Queue.Current.TicketsWaiting != 9 {
					t.Fatalf("%+v", update.Queue)
				}
			},
		},
		{
			name: "late agent evidence initializes a different projection",
			occurrence: occurrence("agent-late-global", "fixture#2", 2,
				agentEvent("agent-old", base, "a_31", events.OnCall, nil)),
			current: true,
			checkUpdate: func(t *testing.T, update Update) {
				if update.AgentState.Current.Current != events.OnCall {
					t.Fatalf("%+v", update.AgentState)
				}
			},
		},
		{
			name: "late queue evidence remains historical",
			occurrence: occurrence("queue-late", "fixture#3", 3,
				queueEvent("queue-old", base, "vip", 1)),
			current: false,
			checkUpdate: func(t *testing.T, update Update) {
				if update.Queue.Current.TicketsWaiting != 9 || update.Queue.Observation.Snapshot.TicketsWaiting != 1 {
					t.Fatalf("%+v", update.Queue)
				}
			},
		},
		{
			name: "late adherence evidence initializes a third projection",
			occurrence: occurrence("adherence-late-global", "fixture#4", 4,
				adherenceEvent("adherence-old", base.Add(time.Minute), "a_31", events.Available, events.OnCall, true, nil, nil)),
			current: true,
			checkUpdate: func(t *testing.T, update Update) {
				if !update.Adherence.Current.InViolation {
					t.Fatalf("%+v", update.Adherence)
				}
			},
		},
		{
			name: "equal effective time uses later ingestion position",
			occurrence: occurrence("agent-equal-occurrence", "fixture#5", 5,
				agentEvent("agent-equal", base, "a_31", events.OnBreak, nil)),
			current: true,
			checkUpdate: func(t *testing.T, update Update) {
				if update.AgentState.Current.Current != events.OnBreak {
					t.Fatalf("%+v", update.AgentState)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			update, err := builder.Build(tc.occurrence)
			if err != nil {
				t.Fatal(err)
			}
			if got := updateCurrentChanged(update); got != tc.current {
				t.Fatalf("current changed=%v, want %v; update=%+v", got, tc.current, update)
			}
			tc.checkUpdate(t, update)
		})
	}
	if history := builder.QueueHistory("vip"); len(history) != 2 || history[1].Provenance.OccurrenceID != "queue-late" {
		t.Fatalf("late queue evidence was not retained: %+v", history)
	}
}

func TestBuilderAdherenceOnsetUsesAcceptedEvidence(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	explicit := base.Add(-5 * time.Minute)
	builder := NewBuilder()
	cases := []struct {
		name         string
		check        events.AdherenceCheck
		wantKnown    time.Time
		wantReported bool
	}{
		{"explicit onset can precede observation", adherenceEvent("one", base, "a_23", events.Available, events.OnCall, true, &explicit, nil), explicit, true},
		{"missing onset keeps continuous known true time", adherenceEvent("two", base.Add(time.Minute), "a_23", events.Available, events.OnCall, true, nil, nil), explicit, false},
		{"false clears known true interval", adherenceEvent("three", base.Add(2*time.Minute), "a_23", events.Available, events.Available, false, nil, nil), time.Time{}, false},
		{"new missing onset starts at first known true check", adherenceEvent("four", base.Add(3*time.Minute), "a_23", events.Available, events.OnCall, true, nil, nil), base.Add(3 * time.Minute), false},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			update, err := builder.Build(occurrence(fmt.Sprintf("adherence-%d", index), fmt.Sprintf("fixture#%d", index+1), uint64(index+1), tc.check))
			if err != nil {
				t.Fatal(err)
			}
			if update.Adherence == nil || !update.Adherence.Current.KnownViolationSince.Equal(tc.wantKnown) {
				t.Fatalf("update=%+v want known=%s", update, tc.wantKnown)
			}
			if got := update.Adherence.Current.ViolationStartedAt != nil; got != tc.wantReported {
				t.Fatalf("reported onset present=%v, want %v", got, tc.wantReported)
			}
		})
	}
}

func TestBuilderDoesNotInferMembershipFromQueueIDs(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	builder := NewBuilder()
	agent := agentEvent("agent", base, "a_05", events.Available, []string{"billing", "vip"})
	adherence := adherenceEvent("adherence", base.Add(time.Minute), "a_05", events.Available, events.OnCall, true, nil, []string{"billing", "vip"})
	if _, err := builder.Build(occurrence("agent-occurrence", "fixture#1", 1, agent)); err != nil {
		t.Fatal(err)
	}
	if _, err := builder.Build(occurrence("adherence-occurrence", "fixture#2", 2, adherence)); err != nil {
		t.Fatal(err)
	}
	if got := builder.AgentStateHistory("a_05")[0].Change.QueueIDs; !reflect.DeepEqual(got, []string{"billing", "vip"}) {
		t.Fatalf("agent queue provenance=%v", got)
	}
	if got := builder.AdherenceHistory("a_05")[0].Check.QueueIDs; !reflect.DeepEqual(got, []string{"billing", "vip"}) {
		t.Fatalf("adherence queue provenance=%v", got)
	}
	if _, found := reflect.TypeOf(AgentState{}).FieldByName("QueueIDs"); found {
		t.Fatal("agent state inferred queue membership")
	}
	if _, found := reflect.TypeOf(AdherenceState{}).FieldByName("QueueIDs"); found {
		t.Fatal("adherence state inferred queue membership")
	}
}

func TestBuilderCopiesMutableSourceEvidence(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	onset := base.Add(-time.Minute)
	queueIDs := []string{"billing"}
	check := adherenceEvent("source", base, "a_19", events.Available, events.OnCall, true, &onset, queueIDs)
	builder := NewBuilder()
	update, err := builder.Build(occurrence("occurrence", "fixture#1", 1, check))
	if err != nil {
		t.Fatal(err)
	}
	queueIDs[0] = "mutated-input"
	onset = onset.Add(time.Hour)
	update.Adherence.Observation.Check.QueueIDs[0] = "mutated-output"
	*update.Adherence.Current.ViolationStartedAt = base
	history := builder.AdherenceHistory("a_19")
	state, ok := builder.Adherence("a_19")
	if !ok || len(history) != 1 || history[0].Check.QueueIDs[0] != "billing" || !history[0].Check.ViolationStartedAt.Equal(base.Add(-time.Minute)) || !state.ViolationStartedAt.Equal(base.Add(-time.Minute)) {
		t.Fatalf("history=%+v state=%+v", history, state)
	}
}

func TestBuilderFencesIdentityAndPhysicalOrder(t *testing.T) {
	base := time.Date(2026, 5, 26, 9, 0, 0, 0, time.UTC)
	builder := NewBuilder()
	first := occurrence("occurrence-1", "fixture#1", 2, queueEvent("same-source-id", base, "billing", 1))
	if _, err := builder.Build(first); err != nil {
		t.Fatal(err)
	}
	if duplicate, err := builder.Build(first); err != nil || !duplicate.Duplicate || duplicate.Queue == nil || duplicate.Queue.CurrentChanged {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	conflict := first
	conflict.Raw = []byte(`{"changed":true}`)
	if _, err := builder.Build(conflict); err == nil {
		t.Fatal("accepted conflicting immutable occurrence")
	}
	if _, err := builder.Build(occurrence("occurrence-2", "fixture#1", 3, queueEvent("same-source-id", base.Add(time.Minute), "billing", 2))); err == nil {
		t.Fatal("accepted reused source/idempotency key")
	}
	if _, err := builder.Build(occurrence("occurrence-3", "fixture#3", 1, queueEvent("another-source-id", base.Add(time.Minute), "billing", 2))); err == nil {
		t.Fatal("accepted out-of-order ingestion position")
	}
}

func updateCurrentChanged(update Update) bool {
	if update.Queue != nil {
		return update.Queue.CurrentChanged
	}
	if update.AgentState != nil {
		return update.AgentState.CurrentChanged
	}
	return update.Adherence != nil && update.Adherence.CurrentChanged
}

func occurrence(id, key string, position uint64, event events.Event) app.AcceptedOccurrence {
	at := eventTime(event)
	return app.AcceptedOccurrence{ID: id, Source: "fixture", IdempotencyKey: key, IngestPosition: position, SourceEventID: event.GetEventID(), EffectiveAt: at, Event: event, Raw: []byte(fmt.Sprintf(`{"event_id":%q,"position":%d}`, event.GetEventID(), position))}
}
func eventTime(event events.Event) time.Time {
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
func queueEvent(id string, at time.Time, queue string, waiting uint64) events.QueueSnapshot {
	return events.QueueSnapshot{Common: events.Common{EventID: id, Timestamp: at, Type: "queue_snapshot"}, QueueID: queue, TicketsWaiting: waiting}
}
func agentEvent(id string, at time.Time, agent string, state events.State, queueIDs []string) events.AgentStateChange {
	return events.AgentStateChange{Common: events.Common{EventID: id, Timestamp: at, Type: "agent_state_change"}, AgentID: agent, NewState: state, QueueIDs: queueIDs}
}
func adherenceEvent(id string, at time.Time, agent string, scheduled, actual events.State, violation bool, onset *time.Time, queueIDs []string) events.AdherenceCheck {
	return events.AdherenceCheck{Common: events.Common{EventID: id, Timestamp: at, Type: "adherence_check"}, AgentID: agent, ScheduledState: scheduled, ActualState: actual, InViolation: violation, ViolationStartedAt: onset, QueueIDs: queueIDs}
}
