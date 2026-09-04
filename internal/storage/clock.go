package storage

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"
)

type Clock struct{ LogicalNow, GreatestObservedAt *time.Time }

// LockClock serializes replay/event and timer semantic transactions. Callers
// must keep the returned transaction open until all reducer writes commit.
func LockClock(ctx context.Context, tx pgx.Tx) (Clock, error) {
	var c Clock
	err := tx.QueryRow(ctx, `SELECT logical_now,greatest_observed_at FROM runtime_clock WHERE clock_id=TRUE FOR UPDATE`).Scan(&c.LogicalNow, &c.GreatestObservedAt)
	if err != nil {
		return c, fmt.Errorf("lock runtime clock: %w", err)
	}
	return c, nil
}
func AdvanceClock(ctx context.Context, tx pgx.Tx, now time.Time) (time.Time, error) {
	var logical time.Time
	err := tx.QueryRow(ctx, `UPDATE runtime_clock SET logical_now=GREATEST(COALESCE(logical_now,'-infinity'::timestamptz),$1), greatest_observed_at=GREATEST(COALESCE(greatest_observed_at,'-infinity'::timestamptz),$1) WHERE clock_id=TRUE RETURNING logical_now`, now).Scan(&logical)
	if err != nil {
		return time.Time{}, fmt.Errorf("advance runtime clock: %w", err)
	}
	return logical, nil
}

type PendingOccurrence struct {
	ID, IngestPosition int64
	EffectiveAt        time.Time
	Payload            []byte
}

// NextPendingOccurrence intentionally uses ordered locking, not SKIP LOCKED:
// physical ingestion order is part of the replay contract.
func NextPendingOccurrence(ctx context.Context, tx pgx.Tx) (PendingOccurrence, bool, error) {
	var p PendingOccurrence
	err := tx.QueryRow(ctx, `
        WITH candidate AS (
          SELECT o.occurrence_id
          FROM occurrences o
          JOIN occurrence_processing op ON op.occurrence_id=o.occurrence_id
          WHERE op.status='pending'
          ORDER BY o.ingest_position
          FOR UPDATE OF o, op
          LIMIT 1
        )
        UPDATE occurrence_processing op
        SET status='processing', attempts=op.attempts+1, updated_at=clock_timestamp()
        FROM candidate c JOIN occurrences o ON o.occurrence_id=c.occurrence_id
        WHERE op.occurrence_id=c.occurrence_id
        RETURNING o.occurrence_id,o.ingest_position,o.effective_at,o.payload`).Scan(&p.ID, &p.IngestPosition, &p.EffectiveAt, &p.Payload)
	if err == pgx.ErrNoRows {
		return p, false, nil
	}
	if err != nil {
		return p, false, fmt.Errorf("claim next occurrence: %w", err)
	}
	return p, true, nil
}
