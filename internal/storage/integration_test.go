//go:build integration

package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func integrationDB(t *testing.T) (*DB, context.Context) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	db, err := Open(ctx, url, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	if err = Reset(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

func TestIntegrationMigrationsAndOccurrenceIdentity(t *testing.T) {
	db, ctx := integrationDB(t)
	if err := ApplyMigrations(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	const payload = ` { "event_id": "same", "type": "queue_snapshot", "ts": "2025-01-01T00:00:00Z" }`
	for i := int64(1); i <= 2; i++ {
		pos := i
		err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
			id, inserted, e := InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "same", StreamPosition: &pos, IdempotencyKey: "fixture#" + string(rune('0'+i)), EffectiveAt: time.Unix(i, 0).UTC(), Payload: []byte(payload)})
			if e != nil {
				return e
			}
			if !inserted {
				return fmt.Errorf("occurrence %d unexpectedly deduplicated", id)
			}
			return EnsureProcessing(ctx, tx, id)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// The same line ordinal is valid in a different physical stream.
	pos := int64(1)
	if err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		id, inserted, e := InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "same", StreamPosition: &pos, IdempotencyKey: "other/events.jsonl#1", EffectiveAt: time.Unix(3, 0).UTC(), Payload: []byte(payload)})
		if e != nil {
			return e
		}
		if !inserted {
			return fmt.Errorf("cross-stream occurrence %d unexpectedly deduplicated", id)
		}
		return EnsureProcessing(ctx, tx, id)
	}); err != nil {
		t.Fatal(err)
	}

	var n int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM occurrences WHERE source_event_id='same'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("occurrence count=%d, want 3", n)
	}
	retryPos := int64(1)
	var sameID int64
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		id, inserted, e := InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "same", StreamPosition: &retryPos, IdempotencyKey: "fixture#1", EffectiveAt: time.Unix(1, 0).UTC(), Payload: []byte(payload)})
		sameID = id
		if e != nil {
			return e
		}
		if inserted {
			return fmt.Errorf("retry inserted")
		}
		return nil
	})
	if err != nil || sameID == 0 {
		t.Fatalf("retry: id=%d err=%v", sameID, err)
	}
	var storedRaw []byte
	if err := db.Pool.QueryRow(ctx, `SELECT payload_raw FROM occurrences WHERE source='file' AND idempotency_key='fixture#1'`).Scan(&storedRaw); err != nil {
		t.Fatal(err)
	}
	if string(storedRaw) != payload {
		t.Fatalf("payload_raw=%q want exact source bytes %q", storedRaw, payload)
	}
}

func TestIntegrationTimerGenerationFence(t *testing.T) {
	db, ctx := integrationDB(t)
	now := time.Unix(100, 0).UTC()
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, `INSERT INTO condition_trackers(condition_key,rule_id,revision,subject_kind,subject_id,phase) VALUES('c','r',1,'agent','a','idle')`)
		if e != nil {
			return e
		}
		_, e = ReplaceTimer(ctx, tx, "c", "trigger", now)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	var old TimerClaim
	err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		claims, e := ClaimDueTimers(ctx, tx, now, 1, time.Minute, "old")
		if e != nil {
			return e
		}
		if len(claims) != 1 {
			return fmt.Errorf("claims=%d", len(claims))
		}
		old = claims[0]
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var completed bool
	err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		var e error
		completed, e = CompleteTimer(ctx, tx, old.ID, old.Generation, old.ClaimToken, now.Add(2*time.Minute))
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed {
		t.Fatal("expired timer claim completed")
	}
	if err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error { _, e := ReplaceTimer(ctx, tx, "c", "clear", now); return e }); err != nil {
		t.Fatal(err)
	}
	err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		var e error
		completed, e = CompleteTimer(ctx, tx, old.ID, old.Generation, old.ClaimToken, now.Add(2*time.Minute))
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed {
		t.Fatal("stale timer claim completed after replacement")
	}
}

func TestIntegrationQueuePointerUsesOccurrenceIdentity(t *testing.T) {
	db, ctx := integrationDB(t)
	var first, second int64
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		p := int64(1)
		id, _, e := InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "q1", StreamPosition: &p, IdempotencyKey: "q#1", EffectiveAt: time.Unix(1, 0).UTC(), Payload: []byte(`{"type":"queue_snapshot"}`)})
		if e != nil {
			return e
		}
		first = id
		p2 := int64(2)
		_, _, e = InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "a1", StreamPosition: &p2, IdempotencyKey: "q#2", EffectiveAt: time.Unix(2, 0).UTC(), Payload: []byte(`{"type":"agent_state_change"}`)})
		if e != nil {
			return e
		}
		p3 := int64(3)
		second, _, e = InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "q2", StreamPosition: &p3, IdempotencyKey: "q#3", EffectiveAt: time.Unix(3, 0).UTC(), Payload: []byte(`{"type":"queue_snapshot"}`)})
		if e != nil {
			return e
		}
		if _, e = ApplyQueueSnapshot(ctx, tx, QueueSnapshot{OccurrenceID: first, QueueID: "billing", EffectiveAt: time.Unix(1, 0).UTC()}); e != nil {
			return e
		}
		_, e = ApplyQueueSnapshot(ctx, tx, QueueSnapshot{OccurrenceID: second, QueueID: "billing", EffectiveAt: time.Unix(3, 0).UTC()})
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	var currentPos, expectedPos int64
	if err = db.Pool.QueryRow(ctx, `SELECT stream_position FROM queue_state_current WHERE queue_id='billing'`).Scan(&currentPos); err != nil {
		t.Fatal(err)
	}
	if err = db.Pool.QueryRow(ctx, `SELECT ingest_position FROM occurrences WHERE occurrence_id=$1`, second).Scan(&expectedPos); err != nil {
		t.Fatal(err)
	}
	if currentPos != expectedPos {
		t.Fatalf("current stream position=%d want %d", currentPos, expectedPos)
	}
}

func TestIntegrationNotificationRetryHasOneVisibleDelivery(t *testing.T) {
	db, ctx := integrationDB(t)
	now := time.Unix(100, 0).UTC()
	var intent int64
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		var series, generation, transition int64
		if e := tx.QueryRow(ctx, `INSERT INTO alert_series(rule_id,subject_kind,subject_id) VALUES('r','agent','a') RETURNING alert_series_id`).Scan(&series); e != nil {
			return e
		}
		if e := tx.QueryRow(ctx, `INSERT INTO alert_generations(alert_series_id,generation,status,opened_at) VALUES($1,1,'open',$2) RETURNING alert_generation_id`, series, now).Scan(&generation); e != nil {
			return e
		}
		if e := tx.QueryRow(ctx, `INSERT INTO alert_transitions(alert_generation_id,transition_type,occurred_at) VALUES($1,'open',$2) RETURNING alert_transition_id`, generation, now).Scan(&transition); e != nil {
			return e
		}
		var e error
		intent, _, e = EnsureNotificationIntentForTransition(ctx, tx, transition, generation, "open", "ops", []byte(`{"x":1}`))
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	var firstAttempt int64
	err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		claims, e := ClaimNotificationIntents(ctx, tx, now, 1, time.Minute, "one")
		if e != nil {
			return e
		}
		if len(claims) != 1 {
			return fmt.Errorf("claims=%d", len(claims))
		}
		_, firstAttempt, e = BeginDeliveryAttempt(ctx, tx, intent, "one", now)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		ok, e := FinishDeliveryAttempt(ctx, tx, intent, firstAttempt, "one", false, "failed", now.Add(30*time.Second), now.Add(2*time.Minute))
		if e != nil {
			return e
		}
		if ok {
			return fmt.Errorf("expired delivery claim finalized")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var secondAttempt int64
	err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		claims, e := ClaimNotificationIntents(ctx, tx, now.Add(2*time.Minute), 1, time.Minute, "two")
		if e != nil {
			return e
		}
		if len(claims) != 1 {
			return fmt.Errorf("reclaim claims=%d", len(claims))
		}
		_, secondAttempt, e = BeginDeliveryAttempt(ctx, tx, intent, "two", now.Add(2*time.Minute))
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		ok, e := FinishDeliveryAttempt(ctx, tx, intent, secondAttempt, "two", true, "delivered", now.Add(2*time.Minute+30*time.Second), now.Add(2*time.Minute+30*time.Second))
		if e != nil {
			return e
		}
		if !ok {
			return fmt.Errorf("successful attempt not finalized")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var attempts, deliveries int
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM delivery_attempts WHERE intent_id=$1`, intent).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM notification_deliveries WHERE intent_id=$1`, intent).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || deliveries != 1 {
		t.Fatalf("attempts=%d deliveries=%d", attempts, deliveries)
	}
}

func TestIntegrationOccurrenceClaimIsExclusive(t *testing.T) {
	db, ctx := integrationDB(t)
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		p := int64(1)
		id, _, e := InsertOccurrence(ctx, tx, Occurrence{Source: "file", SourceEventID: "e", StreamPosition: &p, IdempotencyKey: "claim#1", EffectiveAt: time.Unix(1, 0).UTC(), Payload: []byte(`{"event_id":"e","type":"queue_snapshot","ts":"2025-01-01T00:00:00Z"}`)})
		if e != nil {
			return e
		}
		return EnsureProcessing(ctx, tx, id)
	})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		ok  bool
		id  int64
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			e := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
				p, ok, e := NextPendingOccurrence(ctx, tx)
				results <- result{ok, p.ID, e}
				if e != nil {
					return e
				}
				if !ok {
					return nil
				}
				time.Sleep(100 * time.Millisecond)
				return nil
			})
			if e != nil {
				results <- result{err: e}
			}
		}()
	}
	var claimed int
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.ok {
			claimed++
		}
	}
	if claimed != 1 {
		t.Fatalf("claimed=%d want 1", claimed)
	}
}

func TestIntegrationMigrationUpgradeFromVersionOne(t *testing.T) {
	db, ctx := integrationDB(t)
	if _, e := db.Pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); e != nil {
		t.Fatal(e)
	}
	ms, e := migrations()
	if e != nil {
		t.Fatal(e)
	}
	if e = EnsureMigrationTable(ctx, db.Pool); e != nil {
		t.Fatal(e)
	}
	tx, e := db.Pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, ms[0].SQL); e != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, `INSERT INTO occurrences(source,source_event_id,stream_position,idempotency_key,effective_at,payload,payload_hash) VALUES('legacy','legacy-event',1,'legacy#1',$1,$2::jsonb,'legacy-hash')`, time.Unix(1, 0).UTC(), []byte(`{"event_id":"legacy-event","type":"legacy","ts":"2025-01-01T00:00:00Z"}`)); e != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, `INSERT INTO schema_migrations(version,name,checksum) VALUES($1,$2,$3)`, ms[0].Version, ms[0].Name, ms[0].Checksum); e != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(e)
	}
	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	if e = ApplyMigrations(ctx, db.Pool); e != nil {
		t.Fatal(e)
	}
	var nullable string
	if e = db.Pool.QueryRow(ctx, `SELECT is_nullable FROM information_schema.columns WHERE table_name='occurrences' AND column_name='payload_raw'`).Scan(&nullable); e != nil {
		t.Fatal(e)
	}
	if nullable != "NO" {
		t.Fatalf("payload_raw is_nullable=%s", nullable)
	}
	var reconstructedDefault string
	if e = db.Pool.QueryRow(ctx, `SELECT column_default FROM information_schema.columns WHERE table_name='occurrences' AND column_name='payload_raw_reconstructed'`).Scan(&reconstructedDefault); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(reconstructedDefault, "false") {
		t.Fatalf("raw reconstruction default=%s", reconstructedDefault)
	}
	var reconstructed bool
	if e = db.Pool.QueryRow(ctx, `SELECT payload_raw_reconstructed FROM occurrences WHERE idempotency_key='legacy#1'`).Scan(&reconstructed); e != nil {
		t.Fatal(e)
	}
	if !reconstructed {
		t.Fatal("legacy payload was not marked reconstructed")
	}
}

func TestIntegrationTimerClaimsAreDeterministicallyOrdered(t *testing.T) {
	db, ctx := integrationDB(t)
	now := time.Unix(100, 0).UTC()
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		for _, key := range []string{"c", "a", "b"} {
			if _, e := tx.Exec(ctx, `INSERT INTO condition_trackers(condition_key,rule_id,revision,subject_kind,subject_id,phase) VALUES($1,'r',1,'agent',$1,'idle')`, key); e != nil {
				return e
			}
			if _, e := ReplaceTimer(ctx, tx, key, "trigger", now); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		claims, e := ClaimDueTimers(ctx, tx, now, 3, time.Minute, "ordered")
		if e != nil {
			return e
		}
		if len(claims) != 3 {
			return fmt.Errorf("claims=%d", len(claims))
		}
		for i, want := range []string{"a", "b", "c"} {
			if claims[i].ConditionKey != want {
				return fmt.Errorf("claim %d=%s want %s", i, claims[i].ConditionKey, want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationAgentAndAdherenceProjectionsRemainSeparate(t *testing.T) {
	db, ctx := integrationDB(t)
	base := time.Unix(100, 0).UTC()
	newer := base.Add(time.Minute)
	var latestAgent, latestAdherence int64
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		insert := func(position int64, key string, effectiveAt time.Time) (int64, error) {
			id, inserted, err := InsertOccurrence(ctx, tx, Occurrence{
				Source: "file", SourceEventID: key, StreamPosition: &position,
				IdempotencyKey: key, EffectiveAt: effectiveAt,
				Payload: []byte(`{"event_id":"` + key + `","type":"projection"}`),
			})
			if err != nil {
				return 0, err
			}
			if !inserted {
				return 0, fmt.Errorf("projection occurrence %q unexpectedly deduplicated", key)
			}
			return id, nil
		}

		initialAgent, err := insert(1, "agent-new", newer)
		if err != nil {
			return err
		}
		current, err := ApplyAgentStateChange(ctx, tx, AgentStateChange{
			OccurrenceID: initialAgent, AgentID: "a_31", EffectiveAt: newer,
			NewState: "on_call", QueueIDs: nil,
		})
		if err != nil {
			return err
		}
		if !current {
			return fmt.Errorf("initial agent state was not current")
		}
		initialAdherence, err := insert(2, "adherence-true", newer)
		if err != nil {
			return err
		}
		onset := base
		current, err = ApplyAdherenceCheck(ctx, tx, AdherenceCheck{
			OccurrenceID: initialAdherence, AgentID: "a_31", EffectiveAt: newer,
			ScheduledState: "available", ActualState: "on_break", InViolation: true,
			ViolationStartedAt: &onset, QueueIDs: []string{"billing"},
		})
		if err != nil {
			return err
		}
		if !current {
			return fmt.Errorf("initial adherence state was not current")
		}
		lateAgent, err := insert(3, "agent-late", base)
		if err != nil {
			return err
		}
		current, err = ApplyAgentStateChange(ctx, tx, AgentStateChange{
			OccurrenceID: lateAgent, AgentID: "a_31", EffectiveAt: base, NewState: "available",
		})
		if err != nil {
			return err
		}
		if current {
			return fmt.Errorf("late agent state unexpectedly became current")
		}
		latestAdherence, err = insert(4, "adherence-false", newer)
		if err != nil {
			return err
		}
		current, err = ApplyAdherenceCheck(ctx, tx, AdherenceCheck{
			OccurrenceID: latestAdherence, AgentID: "a_31", EffectiveAt: newer,
			ScheduledState: "available", ActualState: "available", InViolation: false,
			QueueIDs: []string{"billing", "priority"},
		})
		if err != nil {
			return err
		}
		if !current {
			return fmt.Errorf("same-time later adherence state was not current")
		}
		latestAgent, err = insert(5, "agent-tie", newer)
		if err != nil {
			return err
		}
		previous := "on_call"
		previousDuration := int64(60)
		current, err = ApplyAgentStateChange(ctx, tx, AgentStateChange{
			OccurrenceID: latestAgent, AgentID: "a_31", EffectiveAt: newer,
			PreviousState: &previous, NewState: "on_break", PreviousStateDurationSec: &previousDuration,
			QueueIDs: []string{},
		})
		if err != nil {
			return err
		}
		if !current {
			return fmt.Errorf("same-time later agent state was not current")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	var agentOccurrence, agentPosition int64
	var previousState *string
	var newState, agentQueueIDs string
	var previousDuration *int64
	err = db.Pool.QueryRow(ctx, `
		SELECT observation.occurrence_id, current.stream_position, observation.previous_state,
			observation.new_state, observation.previous_state_duration_sec, observation.queue_ids::text
		FROM agent_state_current current
		JOIN agent_state_observations observation ON observation.observation_id=current.observation_id
		WHERE current.agent_id='a_31'
	`).Scan(&agentOccurrence, &agentPosition, &previousState, &newState, &previousDuration, &agentQueueIDs)
	if err != nil {
		t.Fatal(err)
	}
	if agentOccurrence != latestAgent || agentPosition != 5 || previousState == nil || *previousState != "on_call" ||
		newState != "on_break" || previousDuration == nil || *previousDuration != 60 || agentQueueIDs != "[]" {
		t.Fatalf("agent current=(occurrence=%d position=%d previous=%v new=%q duration=%v queues=%s)",
			agentOccurrence, agentPosition, previousState, newState, previousDuration, agentQueueIDs)
	}

	var adherenceOccurrence, adherencePosition int64
	var actualState, adherenceQueueIDs string
	var inViolation bool
	var violationStartedAt *time.Time
	err = db.Pool.QueryRow(ctx, `
		SELECT observation.occurrence_id, current.stream_position, observation.actual_state,
			observation.in_violation, observation.violation_started_at, observation.queue_ids::text
		FROM adherence_current current
		JOIN adherence_observations observation ON observation.observation_id=current.observation_id
		WHERE current.agent_id='a_31'
	`).Scan(&adherenceOccurrence, &adherencePosition, &actualState, &inViolation, &violationStartedAt, &adherenceQueueIDs)
	if err != nil {
		t.Fatal(err)
	}
	if adherenceOccurrence != latestAdherence || adherencePosition != 4 || actualState != "available" ||
		inViolation || violationStartedAt != nil || adherenceQueueIDs != `["billing", "priority"]` {
		t.Fatalf("adherence current=(occurrence=%d position=%d actual=%q violation=%v onset=%v queues=%s)",
			adherenceOccurrence, adherencePosition, actualState, inViolation, violationStartedAt, adherenceQueueIDs)
	}

	var agentObservations, adherenceObservations int
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM agent_state_observations WHERE agent_id='a_31'`).Scan(&agentObservations); err != nil {
		t.Fatal(err)
	}
	if err = db.Pool.QueryRow(ctx, `SELECT count(*) FROM adherence_observations WHERE agent_id='a_31'`).Scan(&adherenceObservations); err != nil {
		t.Fatal(err)
	}
	if agentObservations != 3 || adherenceObservations != 2 {
		t.Fatalf("observation history agent=%d adherence=%d", agentObservations, adherenceObservations)
	}
	var initialAgentQueueIDs string
	if err = db.Pool.QueryRow(ctx, `
		SELECT queue_ids::text FROM agent_state_observations
		WHERE agent_id='a_31' ORDER BY stream_position LIMIT 1
	`).Scan(&initialAgentQueueIDs); err != nil {
		t.Fatal(err)
	}
	if initialAgentQueueIDs != "null" {
		t.Fatalf("nullable agent queue IDs=%s, want null", initialAgentQueueIDs)
	}
}

func TestIntegrationConflictingProjectionReapplicationUsesStoredSubjectAndTime(t *testing.T) {
	db, ctx := integrationDB(t)
	base := time.Unix(100, 0).UTC()
	conflictingTime := base.Add(time.Minute)
	var agentOccurrence, adherenceOccurrence int64
	err := WithTx(ctx, db.Pool, func(tx pgx.Tx) error {
		insert := func(position int64, key string, effectiveAt time.Time) (int64, error) {
			id, inserted, err := InsertOccurrence(ctx, tx, Occurrence{
				Source: "file", SourceEventID: key, StreamPosition: &position,
				IdempotencyKey: key, EffectiveAt: effectiveAt,
				Payload: []byte(`{"event_id":"` + key + `","type":"projection"}`),
			})
			if err != nil {
				return 0, err
			}
			if !inserted {
				return 0, fmt.Errorf("projection occurrence %q unexpectedly deduplicated", key)
			}
			return id, nil
		}

		var err error
		agentOccurrence, err = insert(1, "agent-conflict", base)
		if err != nil {
			return err
		}
		if _, err = ApplyAgentStateChange(ctx, tx, AgentStateChange{
			OccurrenceID: agentOccurrence, AgentID: "a_original", EffectiveAt: base, NewState: "available",
		}); err != nil {
			return err
		}
		if _, err = ApplyAgentStateChange(ctx, tx, AgentStateChange{
			OccurrenceID: agentOccurrence, AgentID: "a_conflicting", EffectiveAt: conflictingTime, NewState: "on_call",
		}); err != nil {
			return err
		}

		adherenceOccurrence, err = insert(2, "adherence-conflict", base)
		if err != nil {
			return err
		}
		if _, err = ApplyAdherenceCheck(ctx, tx, AdherenceCheck{
			OccurrenceID: adherenceOccurrence, AgentID: "a_original", EffectiveAt: base,
			ScheduledState: "available", ActualState: "available", InViolation: false,
		}); err != nil {
			return err
		}
		if _, err = ApplyAdherenceCheck(ctx, tx, AdherenceCheck{
			OccurrenceID: adherenceOccurrence, AgentID: "a_conflicting", EffectiveAt: conflictingTime,
			ScheduledState: "available", ActualState: "on_break", InViolation: true,
		}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, projection := range []struct {
		name             string
		currentTable     string
		observationTable string
		occurrenceID     int64
	}{
		{name: "agent state", currentTable: "agent_state_current", observationTable: "agent_state_observations", occurrenceID: agentOccurrence},
		{name: "adherence", currentTable: "adherence_current", observationTable: "adherence_observations", occurrenceID: adherenceOccurrence},
	} {
		var pointers int
		query := fmt.Sprintf(`
			SELECT count(*) FROM %s current
			JOIN %s observation ON observation.observation_id=current.observation_id
			WHERE current.agent_id IN ('a_original', 'a_conflicting')
		`, projection.currentTable, projection.observationTable)
		if err := db.Pool.QueryRow(ctx, query).Scan(&pointers); err != nil {
			t.Fatalf("count %s current pointers: %v", projection.name, err)
		}
		if pointers != 1 {
			t.Fatalf("%s current pointers=%d, want 1", projection.name, pointers)
		}

		var subject string
		var effectiveAt time.Time
		var occurrenceID int64
		query = fmt.Sprintf(`
			SELECT current.agent_id, current.effective_at, observation.occurrence_id
			FROM %s current
			JOIN %s observation ON observation.observation_id=current.observation_id
			WHERE current.agent_id='a_original'
		`, projection.currentTable, projection.observationTable)
		if err := db.Pool.QueryRow(ctx, query).Scan(&subject, &effectiveAt, &occurrenceID); err != nil {
			t.Fatalf("read %s current pointer: %v", projection.name, err)
		}
		if subject != "a_original" || !effectiveAt.Equal(base) || occurrenceID != projection.occurrenceID {
			t.Fatalf("%s current=(subject=%q effective=%s occurrence=%d), want stored observation",
				projection.name, subject, effectiveAt, occurrenceID)
		}
	}
}
