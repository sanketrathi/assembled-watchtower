// Package storage provides the PostgreSQL adapter and durable work queues.
// Domain reducers should depend on records/interfaces, not this package.
package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string, maxConns int32) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &DB{Pool: pool}, nil
}
func (db *DB) Close() {
	if db != nil && db.Pool != nil {
		db.Pool.Close()
	}
}

// WithTx commits only when fn returns nil. The rollback is deliberately attempted
// after every failure so a caller can safely reuse the pool connection.
func WithTx(ctx context.Context, db *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

type Occurrence struct {
	Source         string
	SourceEventID  string
	StreamPosition *int64
	IdempotencyKey string
	EffectiveAt    time.Time
	Payload        []byte
}

func InsertOccurrence(ctx context.Context, tx pgx.Tx, o Occurrence) (id int64, inserted bool, err error) {
	h := sha256.Sum256(o.Payload)
	hash := hex.EncodeToString(h[:])
	err = tx.QueryRow(ctx, `
        INSERT INTO occurrences(source,source_event_id,stream_position,idempotency_key,effective_at,payload,payload_raw,payload_raw_reconstructed,payload_hash)
        VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::bytea,FALSE,$8)
        ON CONFLICT (source,idempotency_key) DO NOTHING
        RETURNING occurrence_id`, o.Source, o.SourceEventID, o.StreamPosition, o.IdempotencyKey,
		o.EffectiveAt, o.Payload, o.Payload, hash).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if err != pgx.ErrNoRows {
		return 0, false, fmt.Errorf("insert occurrence %q: %w", o.IdempotencyKey, err)
	}
	var existingHash, existingSourceEventID string
	var existingEffective time.Time
	var existingPos *int64
	err = tx.QueryRow(ctx, `SELECT occurrence_id,source_event_id,effective_at,stream_position,payload_hash FROM occurrences WHERE source=$1 AND idempotency_key=$2`, o.Source, o.IdempotencyKey).Scan(&id, &existingSourceEventID, &existingEffective, &existingPos, &existingHash)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, false, fmt.Errorf("occurrence %q conflicts with an existing stream position", o.IdempotencyKey)
		}
		return 0, false, fmt.Errorf("find existing occurrence %q: %w", o.IdempotencyKey, err)
	}
	if existingHash != hash || existingSourceEventID != o.SourceEventID || !existingEffective.Equal(o.EffectiveAt) || !samePosition(existingPos, o.StreamPosition) {
		return 0, false, fmt.Errorf("occurrence %q replay metadata or payload differs from the accepted occurrence", o.IdempotencyKey)
	}
	return id, false, nil
}

func samePosition(a, b *int64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func EnsureProcessing(ctx context.Context, tx pgx.Tx, occurrenceID int64) error {
	_, err := tx.Exec(ctx, `INSERT INTO occurrence_processing(occurrence_id,status) VALUES($1,'pending') ON CONFLICT DO NOTHING`, occurrenceID)
	if err != nil {
		return fmt.Errorf("create occurrence processing %d: %w", occurrenceID, err)
	}
	return nil
}

// QueueSnapshot is the adapter representation of a queue projection observation.
type QueueSnapshot struct {
	OccurrenceID                                                                               int64
	QueueID                                                                                    string
	EffectiveAt                                                                                time.Time
	TicketsWaiting, LongestWaitSec, SLATargetSec, AgentsAvailable, AgentsOnCall, VolumeLast15m int64
	VolumeForecastNext15m                                                                      *int64
}

// ApplyQueueSnapshot returns whether this observation is current after the write.
// A false result means a newer queue snapshot remains live, while this immutable
// observation is still retained as historical evidence.
func ApplyQueueSnapshot(ctx context.Context, tx pgx.Tx, s QueueSnapshot) (bool, error) {
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO queue_observations(occurrence_id,queue_id,effective_at,stream_position,tickets_waiting,longest_wait_sec,sla_target_sec,agents_available,agents_on_call,volume_last_15m,volume_forecast_next_15m) VALUES($1,$2,$3,(SELECT ingest_position FROM occurrences WHERE occurrence_id=$11),$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(occurrence_id) DO UPDATE SET occurrence_id=EXCLUDED.occurrence_id RETURNING observation_id`, s.OccurrenceID, s.QueueID, s.EffectiveAt, s.TicketsWaiting, s.LongestWaitSec, s.SLATargetSec, s.AgentsAvailable, s.AgentsOnCall, s.VolumeLast15m, s.VolumeForecastNext15m, s.OccurrenceID).Scan(&id)
	if err != nil {
		return false, fmt.Errorf("store queue observation: %w", err)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO queue_state_current(queue_id,observation_id,effective_at,stream_position) VALUES($1,$2,$3,(SELECT ingest_position FROM occurrences WHERE occurrence_id=$4)) ON CONFLICT(queue_id) DO UPDATE SET observation_id=EXCLUDED.observation_id,effective_at=EXCLUDED.effective_at,stream_position=EXCLUDED.stream_position WHERE (queue_state_current.effective_at, queue_state_current.stream_position) <= (EXCLUDED.effective_at, EXCLUDED.stream_position)`, s.QueueID, id, s.EffectiveAt, s.OccurrenceID)
	if err != nil {
		return false, fmt.Errorf("update queue state %q: %w", s.QueueID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// AgentStateChange is the adapter representation of one agent-state source
// observation. Queue IDs are retained as observation provenance and never
// define configured group membership.
type AgentStateChange struct {
	OccurrenceID             int64
	AgentID                  string
	EffectiveAt              time.Time
	PreviousState            *string
	NewState                 string
	PreviousStateDurationSec *int64
	QueueIDs                 []string
}

// ApplyAgentStateChange records immutable source evidence and advances only the
// agent-state current pointer. An older effective observation remains available
// in history but cannot regress live agent state; an equal effective time uses
// occurrence ingestion order as the deterministic tie breaker. It returns whether
// the observation is current after the write.
func ApplyAgentStateChange(ctx context.Context, tx pgx.Tx, change AgentStateChange) (bool, error) {
	queueIDs, err := json.Marshal(change.QueueIDs)
	if err != nil {
		return false, fmt.Errorf("encode agent state queue IDs: %w", err)
	}
	var observationID int64
	var agentID string
	var effectiveAt time.Time
	var streamPosition int64
	err = tx.QueryRow(ctx, `
		INSERT INTO agent_state_observations(
			occurrence_id,agent_id,effective_at,stream_position,previous_state,new_state,
			previous_state_duration_sec,queue_ids
		) VALUES(
			$1,$2,$3,(SELECT ingest_position FROM occurrences WHERE occurrence_id=$1),$4,$5,$6,$7::jsonb
		)
		ON CONFLICT(occurrence_id) DO UPDATE SET occurrence_id=EXCLUDED.occurrence_id
		RETURNING observation_id,agent_id,effective_at,stream_position
	`, change.OccurrenceID, change.AgentID, change.EffectiveAt, change.PreviousState,
		change.NewState, change.PreviousStateDurationSec, queueIDs).Scan(&observationID, &agentID, &effectiveAt, &streamPosition)
	if err != nil {
		return false, fmt.Errorf("store agent state observation: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO agent_state_current(agent_id,observation_id,effective_at,stream_position)
		VALUES($1,$2,$3,$4)
		ON CONFLICT(agent_id) DO UPDATE
		SET observation_id=EXCLUDED.observation_id,effective_at=EXCLUDED.effective_at,
			stream_position=EXCLUDED.stream_position
		WHERE (agent_state_current.effective_at, agent_state_current.stream_position) <=
			(EXCLUDED.effective_at, EXCLUDED.stream_position)
	`, agentID, observationID, effectiveAt, streamPosition)
	if err != nil {
		return false, fmt.Errorf("update agent state %q: %w", change.AgentID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// AdherenceCheck is the adapter representation of one upstream adherence
// observation. Its state is intentionally separate from agent-state evidence.
type AdherenceCheck struct {
	OccurrenceID       int64
	AgentID            string
	EffectiveAt        time.Time
	ScheduledState     string
	ActualState        string
	InViolation        bool
	ViolationStartedAt *time.Time
	QueueIDs           []string
}

// ApplyAdherenceCheck records immutable upstream adherence evidence and advances
// only the adherence current pointer. It uses the same source-local effective
// time and ingestion-order comparison as the other operational projections. It
// returns whether the observation is current after the write.
func ApplyAdherenceCheck(ctx context.Context, tx pgx.Tx, check AdherenceCheck) (bool, error) {
	queueIDs, err := json.Marshal(check.QueueIDs)
	if err != nil {
		return false, fmt.Errorf("encode adherence queue IDs: %w", err)
	}
	var observationID int64
	var agentID string
	var effectiveAt time.Time
	var streamPosition int64
	err = tx.QueryRow(ctx, `
		INSERT INTO adherence_observations(
			occurrence_id,agent_id,effective_at,stream_position,scheduled_state,actual_state,
			in_violation,violation_started_at,queue_ids
		) VALUES(
			$1,$2,$3,(SELECT ingest_position FROM occurrences WHERE occurrence_id=$1),$4,$5,$6,$7,$8::jsonb
		)
		ON CONFLICT(occurrence_id) DO UPDATE SET occurrence_id=EXCLUDED.occurrence_id
		RETURNING observation_id,agent_id,effective_at,stream_position
	`, check.OccurrenceID, check.AgentID, check.EffectiveAt, check.ScheduledState,
		check.ActualState, check.InViolation, check.ViolationStartedAt, queueIDs).Scan(&observationID, &agentID, &effectiveAt, &streamPosition)
	if err != nil {
		return false, fmt.Errorf("store adherence observation: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO adherence_current(agent_id,observation_id,effective_at,stream_position)
		VALUES($1,$2,$3,$4)
		ON CONFLICT(agent_id) DO UPDATE
		SET observation_id=EXCLUDED.observation_id,effective_at=EXCLUDED.effective_at,
			stream_position=EXCLUDED.stream_position
		WHERE (adherence_current.effective_at, adherence_current.stream_position) <=
			(EXCLUDED.effective_at, EXCLUDED.stream_position)
	`, agentID, observationID, effectiveAt, streamPosition)
	if err != nil {
		return false, fmt.Errorf("update adherence state %q: %w", check.AgentID, err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkProcessed is for the semantic processing transaction only.
// Replay leaves accepted evidence pending.
func MarkProcessed(ctx context.Context, tx pgx.Tx, occurrenceID int64) error {
	tag, err := tx.Exec(ctx, `UPDATE occurrence_processing SET status='processed',updated_at=clock_timestamp() WHERE occurrence_id=$1 AND status <> 'processed'`, occurrenceID)
	if err != nil {
		return fmt.Errorf("mark occurrence %d processed: %w", occurrenceID, err)
	}
	_ = tag
	return nil
}
