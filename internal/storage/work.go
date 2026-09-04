package storage

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

type Timer struct {
	ID           int64
	ConditionKey string
	Phase        string
	DueAt        time.Time
	Generation   int64
	ClaimToken   string
}
type TimerClaim struct {
	Timer
	ClaimedUntil time.Time
}

// ReplaceTimer advances the generation before inserting a timer. Any callback
// carrying an older generation, including one already leased, is therefore stale.
func ReplaceTimer(ctx context.Context, tx pgx.Tx, conditionKey, phase string, dueAt time.Time) (int64, error) {
	if phase != "trigger" && phase != "clear" {
		return 0, fmt.Errorf("invalid timer phase %q", phase)
	}
	var generation int64
	err := tx.QueryRow(ctx, `UPDATE condition_trackers SET timer_generation=timer_generation+1,updated_at=clock_timestamp() WHERE condition_key=$1 RETURNING timer_generation`, conditionKey).Scan(&generation)
	if err != nil {
		return 0, fmt.Errorf("advance timer generation %q: %w", conditionKey, err)
	}
	if _, err = tx.Exec(ctx, `UPDATE timers SET status='cancelled',claimed_until=NULL WHERE condition_key=$1 AND status IN ('pending','claimed')`, conditionKey); err != nil {
		return 0, fmt.Errorf("cancel timers %q: %w", conditionKey, err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO timers(condition_key,due_at,phase,generation,status) VALUES($1,$2,$3,$4,'pending')`, conditionKey, dueAt, phase, generation)
	if err != nil {
		return 0, fmt.Errorf("insert timer %q generation %d: %w", conditionKey, generation, err)
	}
	return generation, nil
}

// ClaimDueTimers claims a bounded batch with row locks and an expiring lease.
// claimToken must be fresh per worker attempt and is the fencing credential.
func ClaimDueTimers(ctx context.Context, tx pgx.Tx, now time.Time, limit int32, lease time.Duration, claimToken string) ([]TimerClaim, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("timer claim limit must be positive")
	}
	if claimToken == "" {
		return nil, fmt.Errorf("timer claim token is required")
	}
	if lease <= 0 {
		return nil, fmt.Errorf("timer claim lease must be positive")
	}
	until := now.Add(lease)
	rows, err := tx.Query(ctx, `
        WITH candidates AS (
          SELECT timer_id FROM timers
          WHERE status IN ('pending','claimed') AND due_at <= $1
            AND (claimed_until IS NULL OR claimed_until <= $1)
          ORDER BY due_at, condition_key COLLATE "C", generation
          FOR UPDATE SKIP LOCKED LIMIT $2
        )
        UPDATE timers t SET status='claimed',claim_token=$3,claimed_until=$4
        FROM candidates c WHERE t.timer_id=c.timer_id
        RETURNING t.timer_id,t.condition_key,t.due_at,t.phase,t.generation,t.claimed_until`, now, limit, claimToken, until)
	if err != nil {
		return nil, fmt.Errorf("claim timers: %w", err)
	}
	defer rows.Close()
	var out []TimerClaim
	for rows.Next() {
		var x TimerClaim
		if err := rows.Scan(&x.ID, &x.ConditionKey, &x.DueAt, &x.Phase, &x.Generation, &x.ClaimedUntil); err != nil {
			return nil, fmt.Errorf("scan timer claim: %w", err)
		}
		x.ClaimToken = claimToken
		out = append(out, x)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate timer claims: %w", err)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].DueAt.Equal(out[j].DueAt) {
			if out[i].ConditionKey == out[j].ConditionKey {
				return out[i].Generation < out[j].Generation
			}
			return out[i].ConditionKey < out[j].ConditionKey
		}
		return out[i].DueAt.Before(out[j].DueAt)
	})
	return out, nil
}

// CompleteTimer is fenced by both generation and lease token.
func CompleteTimer(ctx context.Context, tx pgx.Tx, timerID, generation int64, claimToken string, now time.Time) (bool, error) {
	tag, err := tx.Exec(ctx, `UPDATE timers SET status='done',claimed_until=NULL WHERE timer_id=$1 AND generation=$2 AND status='claimed' AND claim_token=$3 AND claimed_until > $4`, timerID, generation, claimToken, now)
	if err != nil {
		return false, fmt.Errorf("complete timer %d: %w", timerID, err)
	}
	return tag.RowsAffected() == 1, nil
}

type Intent struct {
	ID                int64
	DedupeKey         string
	AlertGenerationID int64
	TransitionType    string
	Audience          string
	State             string
	Payload           []byte
	ClaimToken        string
}

func EnsureNotificationIntent(ctx context.Context, tx pgx.Tx, dedupe string, alertGenerationID int64, transitionType, audience string, payload []byte) (int64, bool, error) {
	var id int64
	err := tx.QueryRow(ctx, `INSERT INTO notification_intents(dedupe_key,alert_generation_id,transition_type,audience,state,payload) VALUES($1,$2,$3,$4,'pending',$5::jsonb) ON CONFLICT(dedupe_key) DO NOTHING RETURNING intent_id`, dedupe, alertGenerationID, transitionType, audience, payload).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if err != pgx.ErrNoRows {
		return 0, false, fmt.Errorf("create notification intent %q: %w", dedupe, err)
	}
	var existingGeneration int64
	var existingType, existingAudience string
	var samePayload bool
	err = tx.QueryRow(ctx, `SELECT intent_id,alert_generation_id,transition_type,audience,payload=$2::jsonb FROM notification_intents WHERE dedupe_key=$1`, dedupe, payload).Scan(&id, &existingGeneration, &existingType, &existingAudience, &samePayload)
	if err != nil {
		return 0, false, fmt.Errorf("find notification intent %q: %w", dedupe, err)
	}
	if existingGeneration != alertGenerationID || existingType != transitionType || existingAudience != audience || !samePayload {
		return 0, false, fmt.Errorf("notification intent %q conflicts with immutable intent data", dedupe)
	}
	return id, false, nil
}

// EnsureNotificationIntentForTransition ties intent identity to the durable alert transition.
func EnsureNotificationIntentForTransition(ctx context.Context, tx pgx.Tx, transitionID, generationID int64, transitionType, audience string, payload []byte) (int64, bool, error) {
	var id int64
	dedupe := fmt.Sprintf("alert-transition:%d:%s:%s", transitionID, base64.RawURLEncoding.EncodeToString([]byte(audience)), transitionType)
	err := tx.QueryRow(ctx, `INSERT INTO notification_intents(alert_generation_id,alert_transition_id,transition_type,audience,state,payload,dedupe_key) VALUES($1,$2,$3,$4,'pending',$5::jsonb,$6) ON CONFLICT DO NOTHING RETURNING intent_id`, generationID, transitionID, transitionType, audience, payload, dedupe).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if err != pgx.ErrNoRows {
		return 0, false, fmt.Errorf("create transition notification intent: %w", err)
	}
	var existingGeneration int64
	var samePayload bool
	err = tx.QueryRow(ctx, `SELECT intent_id,alert_generation_id,payload=$4::jsonb FROM notification_intents WHERE alert_transition_id=$1 AND audience=$2 AND transition_type=$3`, transitionID, audience, transitionType, payload).Scan(&id, &existingGeneration, &samePayload)
	if err != nil {
		return 0, false, fmt.Errorf("find transition notification intent: %w", err)
	}
	if existingGeneration != generationID || !samePayload {
		return 0, false, fmt.Errorf("transition notification intent conflicts with immutable intent data")
	}
	return id, false, nil
}

func ClaimNotificationIntents(ctx context.Context, tx pgx.Tx, now time.Time, limit int32, lease time.Duration, token string) ([]Intent, error) {
	if limit <= 0 || token == "" {
		return nil, fmt.Errorf("invalid notification claim arguments")
	}
	if lease <= 0 {
		return nil, fmt.Errorf("notification claim lease must be positive")
	}
	until := now.Add(lease)
	rows, err := tx.Query(ctx, `WITH candidates AS (SELECT intent_id FROM notification_intents WHERE state IN ('pending','claimed','failed') AND (claimed_until IS NULL OR claimed_until <= $1) ORDER BY created_at,intent_id FOR UPDATE SKIP LOCKED LIMIT $2) UPDATE notification_intents n SET state='claimed',claim_token=$3,claimed_until=$4 FROM candidates c WHERE n.intent_id=c.intent_id RETURNING n.intent_id,n.dedupe_key,n.alert_generation_id,n.transition_type,n.audience,n.state,n.payload`, now, limit, token, until)
	if err != nil {
		return nil, fmt.Errorf("claim notification intents: %w", err)
	}
	defer rows.Close()
	var out []Intent
	for rows.Next() {
		var i Intent
		if err := rows.Scan(&i.ID, &i.DedupeKey, &i.AlertGenerationID, &i.TransitionType, &i.Audience, &i.State, &i.Payload); err != nil {
			return nil, err
		}
		i.ClaimToken = token
		out = append(out, i)
	}
	return out, rows.Err()
}

// BeginDeliveryAttempt appends an attempt under the intent row lock. It never
// creates another intent, even after a failed or expired delivery claim.
func BeginDeliveryAttempt(ctx context.Context, tx pgx.Tx, intentID int64, claimToken string, now time.Time) (int32, int64, error) {
	var state string
	if err := tx.QueryRow(ctx, `SELECT state FROM notification_intents WHERE intent_id=$1 AND state='claimed' AND claim_token=$2 AND claimed_until > $3 FOR UPDATE`, intentID, claimToken, now).Scan(&state); err != nil {
		return 0, 0, fmt.Errorf("lock claimed intent %d: %w", intentID, err)
	}
	var n int32
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(attempt_number),0)+1 FROM delivery_attempts WHERE intent_id=$1`, intentID).Scan(&n); err != nil {
		return 0, 0, fmt.Errorf("next delivery attempt %d: %w", intentID, err)
	}
	var id int64
	if err := tx.QueryRow(ctx, `INSERT INTO delivery_attempts(intent_id,attempt_number,state) VALUES($1,$2,'started') RETURNING attempt_id`, intentID, n).Scan(&id); err != nil {
		return 0, 0, fmt.Errorf("create delivery attempt %d: %w", intentID, err)
	}
	return n, id, nil
}

func FinishDeliveryAttempt(ctx context.Context, tx pgx.Tx, intentID, attemptID int64, claimToken string, success bool, message string, deliveryAt, now time.Time) (bool, error) {
	status := "failed"
	if success {
		status = "succeeded"
	}
	tag, err := tx.Exec(ctx, `UPDATE delivery_attempts a SET state=$1,visible_message=$2,finished_at=$3 FROM notification_intents n WHERE a.attempt_id=$4 AND a.intent_id=$5 AND n.intent_id=a.intent_id AND n.state='claimed' AND n.claim_token=$6 AND n.claimed_until > $7`, status, message, deliveryAt, attemptID, intentID, claimToken, now)
	if err != nil {
		return false, fmt.Errorf("finish delivery attempt %d: %w", attemptID, err)
	}
	if tag.RowsAffected() != 1 {
		return false, nil
	}
	if success {
		if _, err = tx.Exec(ctx, `INSERT INTO notification_deliveries(intent_id,attempt_id,delivered_at,outcome,visible_message) VALUES($1,$2,$3,'stub',$4)`, intentID, attemptID, deliveryAt, message); err != nil {
			return false, fmt.Errorf("record visible delivery %d: %w", intentID, err)
		}
	}
	if success {
		_, err = tx.Exec(ctx, `UPDATE notification_intents SET state='delivered',delivered_at=$1,claimed_until=NULL WHERE intent_id=$2 AND state='claimed' AND claim_token=$3`, deliveryAt, intentID, claimToken)
	} else {
		_, err = tx.Exec(ctx, `UPDATE notification_intents SET state='failed',claimed_until=NULL WHERE intent_id=$1 AND state='claimed' AND claim_token=$2`, intentID, claimToken)
	}
	if err != nil {
		return false, fmt.Errorf("finish notification intent %d: %w", intentID, err)
	}
	return true, nil
}
