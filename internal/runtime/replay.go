// Package demo provides deterministic PostgreSQL replay and dashboard query support.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"watchtower/internal/alerts"
	"watchtower/internal/app"
	"watchtower/internal/conditions"
	"watchtower/internal/evaluation"
	"watchtower/internal/events"
	"watchtower/internal/notifications"
	"watchtower/internal/projections"
	"watchtower/internal/rules"
	"watchtower/internal/storage"
)

type Config struct {
	EventPath string
	RulesPath string
	Source    string
	StreamID  string
}
type Summary struct{ Occurrences, Alerts, Notifications int }
type ruleFile struct {
	ID         string               `json:"id"`
	Revision   int64                `json:"revision"`
	Definition rules.RuleDefinition `json:"definition"`
}

// Run admits physical JSONL lines and stores source projections, alert
// transitions, notification intents, and visible stub deliveries in PostgreSQL.
func Run(ctx context.Context, pool *pgxpool.Pool, cfg Config) (Summary, error) {
	if pool == nil {
		return Summary{}, fmt.Errorf("database pool is required")
	}
	if cfg.EventPath == "" || cfg.RulesPath == "" {
		return Summary{}, fmt.Errorf("event and rules paths are required")
	}
	if cfg.Source == "" {
		cfg.Source = "demo-replay"
	}
	if cfg.StreamID == "" {
		cfg.StreamID = "demo-events"
	}
	rawRules, err := os.ReadFile(cfg.RulesPath)
	if err != nil {
		return Summary{}, fmt.Errorf("read rules: %w", err)
	}
	var loaded []ruleFile
	if err := json.Unmarshal(rawRules, &loaded); err != nil {
		return Summary{}, fmt.Errorf("decode rules: %w", err)
	}
	active := make([]app.ActiveRule, len(loaded))
	policies := make(map[string]notifications.Policy, len(loaded))
	for i, r := range loaded {
		active[i] = app.ActiveRule{ID: r.ID, Revision: r.Revision, Definition: r.Definition}
		policies[key(r.ID, r.Revision)] = notifications.Policy{OnOpen: r.Definition.Notifications.OnOpen, OnRecovery: r.Definition.Notifications.OnRecovery, Audience: r.Definition.Notifications.Audience}
	}
	if err := seedRules(ctx, pool, loaded); err != nil {
		return Summary{}, fmt.Errorf("seed active rules: %w", err)
	}
	active, err = activeFromDB(ctx, pool)
	if err != nil {
		return Summary{}, err
	}
	policies = make(map[string]notifications.Policy, len(active))
	for _, r := range active {
		policies[key(r.ID, r.Revision)] = notifications.Policy{OnOpen: r.Definition.Notifications.OnOpen, OnRecovery: r.Definition.Notifications.OnRecovery, Audience: r.Definition.Notifications.Audience}
	}
	runtime, err := evaluation.Activate(active, nil)
	if err != nil {
		return Summary{}, err
	}
	builder := projections.NewBuilder()
	alertReducer := alerts.New()
	planner := notifications.New()
	file, err := os.Open(cfg.EventPath)
	if err != nil {
		return Summary{}, fmt.Errorf("open events: %w", err)
	}
	defer file.Close()
	var summary Summary
	var logical time.Time
	emit := func(tx pgx.Tx, transitions []conditions.Transition) error {
		for _, transition := range transitions {
			series := alerts.SeriesKey{RuleID: transition.Key.RuleID, SubjectKind: alerts.SubjectKind(transition.Key.Subject.Kind), SubjectID: transition.Key.Subject.ID}
			var alertTransitions []alerts.Transition
			if transition.Direction == conditions.Trigger {
				alertTransitions = alertReducer.ApplyStart(alerts.Episode{ID: transition.EpisodeID, Series: series, Revision: transition.Key.Revision, Opened: true, At: transition.At, EffectiveAt: transition.Times.EffectiveAt, EvidenceAt: transition.Times.EvidenceAt})
			} else {
				alertTransitions = alertReducer.ApplyClear(transition.EpisodeID, series, transition.At, transition.Times.EffectiveAt, transition.Times.EvidenceAt)
			}
			for _, at := range alertTransitions {
				generationID, transitionID, err := persistAlert(ctx, tx, at)
				if err != nil {
					return err
				}
				summary.Alerts++
				intent, created := planner.Plan(at, policies[key(transition.Key.RuleID, transition.Key.Revision)])
				if !created {
					continue
				}
				payload, _ := json.Marshal(map[string]any{"rule_id": at.Series.RuleID, "subject_kind": at.Series.SubjectKind, "subject_id": at.Series.SubjectID, "kind": at.Kind.String(), "at": at.At})
				intentID, inserted, err := storage.EnsureNotificationIntentForTransition(ctx, tx, transitionID, generationID, at.Kind.String(), intent.Audience, payload)
				if err != nil {
					return err
				}
				if inserted {
					if err := deliver(ctx, tx, intentID, at.At); err != nil {
						return err
					}
					summary.Notifications++
				}
			}
		}
		return nil
	}
	err = events.Stream(file, cfg.StreamID, func(env events.Envelope) error {
		effective := timestamp(env.Event)
		if effective.IsZero() {
			return fmt.Errorf("unsupported event %T", env.Event)
		}
		if effective.After(logical) {
			logical = effective
		}
		return storage.WithTx(ctx, pool, func(tx pgx.Tx) error {
			dbID, inserted, err := storage.InsertOccurrence(ctx, tx, storage.Occurrence{Source: cfg.Source, SourceEventID: env.Event.GetEventID(), StreamPosition: int64p(int64(env.Line)), IdempotencyKey: fmt.Sprintf("%s:%d", cfg.StreamID, env.Line), EffectiveAt: effective, Payload: env.Raw})
			if err != nil {
				return err
			}
			if err := storage.EnsureProcessing(ctx, tx, dbID); err != nil {
				return err
			}
			if !inserted {
				return nil
			} // Demo replay is explicitly fresh-db only.
			occurrence := app.AcceptedOccurrence{ID: fmt.Sprintf("occurrence:%d", dbID), Source: cfg.Source, IdempotencyKey: fmt.Sprintf("%s:%d", cfg.StreamID, env.Line), IngestPosition: env.Line, SourceEventID: env.Event.GetEventID(), EffectiveAt: effective, Event: env.Event, Raw: env.Raw}
			before, err := runtime.Advance(logical)
			if err != nil {
				return err
			}
			if err := emit(tx, before); err != nil {
				return err
			}
			update, err := builder.Build(occurrence)
			if err != nil {
				return err
			}
			if err := persistProjection(ctx, tx, dbID, update); err != nil {
				return err
			}
			request, err := app.NewEvaluationRequest(occurrence, logical, active)
			if err != nil {
				return err
			}
			result, err := runtime.Evaluate(request, evidenceProvider(builder))
			if err != nil {
				return err
			}
			transitions, err := runtime.Apply(result)
			if err != nil {
				return err
			}
			if err := emit(tx, transitions); err != nil {
				return err
			}
			if err := storage.MarkProcessed(ctx, tx, dbID); err != nil {
				return err
			}
			summary.Occurrences++
			return nil
		})
	})
	if err != nil {
		return summary, fmt.Errorf("replay: %w", err)
	}
	// The final event already advanced through its effective time. This call is
	// retained for explicit EOF semantics and is a deterministic no-op or drain.
	if !logical.IsZero() {
		_, err = runtime.Advance(logical)
	}
	return summary, err
}
func key(id string, revision int64) string { return fmt.Sprintf("%s/%d", id, revision) }
func int64p(v int64) *int64                { return &v }
func timestamp(e events.Event) time.Time {
	switch x := e.(type) {
	case events.QueueSnapshot:
		return x.Timestamp
	case events.AgentStateChange:
		return x.Timestamp
	case events.AdherenceCheck:
		return x.Timestamp
	}
	return time.Time{}
}
func persistProjection(ctx context.Context, tx pgx.Tx, id int64, u projections.Update) error {
	if u.Queue != nil {
		x := u.Queue.Observation.Snapshot
		_, err := storage.ApplyQueueSnapshot(ctx, tx, storage.QueueSnapshot{OccurrenceID: id, QueueID: x.QueueID, EffectiveAt: x.Timestamp, TicketsWaiting: int64(x.TicketsWaiting), LongestWaitSec: int64(x.LongestWaitSec), SLATargetSec: int64(x.SLATargetSec), AgentsAvailable: int64(x.AgentsAvailable), AgentsOnCall: int64(x.AgentsOnCall), VolumeLast15m: int64(x.VolumeLast15m), VolumeForecastNext15m: uint64p(x.VolumeForecastNext15m)})
		return err
	}
	if u.AgentState != nil {
		x := u.AgentState.Observation.Change
		var prev *string
		if x.PreviousState != nil {
			v := string(*x.PreviousState)
			prev = &v
		}
		_, err := storage.ApplyAgentStateChange(ctx, tx, storage.AgentStateChange{OccurrenceID: id, AgentID: x.AgentID, EffectiveAt: x.Timestamp, PreviousState: prev, NewState: string(x.NewState), PreviousStateDurationSec: uint64p(x.PreviousStateDurationSec), QueueIDs: x.QueueIDs})
		return err
	}
	x := u.Adherence.Observation.Check
	_, err := storage.ApplyAdherenceCheck(ctx, tx, storage.AdherenceCheck{OccurrenceID: id, AgentID: x.AgentID, EffectiveAt: x.Timestamp, ScheduledState: string(x.ScheduledState), ActualState: string(x.ActualState), InViolation: x.InViolation, ViolationStartedAt: x.ViolationStartedAt, QueueIDs: x.QueueIDs})
	return err
}
func uint64p(v *uint64) *int64 {
	if v == nil {
		return nil
	}
	n := int64(*v)
	return &n
}
func evidenceProvider(b *projections.Builder) evaluation.EvidenceProvider {
	return evaluation.EvidenceProviderFunc(func(t rules.ResolvedTarget) (evaluation.Evidence, bool) {
		lookup := rules.FieldLookupFunc(func(f rules.FieldSpec) (rules.Operand, bool) {
			if t.Kind == rules.SubjectQueue {
				q, ok := b.Queue(t.ID)
				if !ok {
					return rules.Operand{}, false
				}
				switch f.Name {
				case "queue.longest_wait":
					return rules.DurationOperand(rules.Duration(time.Duration(q.LongestWaitSec) * time.Second)), true
				case "queue.sla_target":
					return rules.DurationOperand(rules.Duration(time.Duration(q.SLATargetSec) * time.Second)), true
				case "queue.tickets_waiting":
					return rules.IntegerOperand(int64(q.TicketsWaiting)), true
				case "queue.agents_available":
					return rules.IntegerOperand(int64(q.AgentsAvailable)), true
				case "queue.agents_on_call":
					return rules.IntegerOperand(int64(q.AgentsOnCall)), true
				case "queue.volume_last_15m":
					return rules.IntegerOperand(int64(q.VolumeLast15m)), true
				case "queue.volume_forecast_next_15m":
					if q.VolumeForecastNext15m != nil {
						return rules.IntegerOperand(int64(*q.VolumeForecastNext15m)), true
					}
				}
			}
			a, ok := b.Agent(t.ID)
			if f.Name == "agent.current_state" && ok {
				return rules.AgentStateOperand(string(a.Current)), true
			}
			h, ok := b.Adherence(t.ID)
			if f.Name == "adherence.violation" && ok {
				return rules.BooleanOperand(h.InViolation), true
			}
			return rules.Operand{}, false
		})
		e := evaluation.Evidence{Lookup: lookup}
		if h, ok := b.Adherence(t.ID); ok && h.InViolation {
			e.TrueSince = h.KnownViolationSince
		}
		return e, true
	})
}
func persistAlert(ctx context.Context, tx pgx.Tx, a alerts.Transition) (int64, int64, error) {
	var seriesID, genID, transitionID int64
	err := tx.QueryRow(ctx, `INSERT INTO alert_series(rule_id,subject_kind,subject_id) VALUES($1,$2,$3) ON CONFLICT(rule_id,subject_kind,subject_id) DO UPDATE SET rule_id=EXCLUDED.rule_id RETURNING alert_series_id`, a.Series.RuleID, a.Series.SubjectKind, a.Series.SubjectID).Scan(&seriesID)
	if err != nil {
		return 0, 0, err
	}
	if a.Kind == alerts.Open {
		err = tx.QueryRow(ctx, `INSERT INTO alert_generations(alert_series_id,generation,status,opened_at) VALUES($1,$2,'open',$3) RETURNING alert_generation_id`, seriesID, int64(a.Generation), a.At).Scan(&genID)
	} else {
		err = tx.QueryRow(ctx, `UPDATE alert_generations SET status='recovered',recovered_at=$2 WHERE alert_series_id=$1 AND generation=$3 RETURNING alert_generation_id`, seriesID, a.At, int64(a.Generation)).Scan(&genID)
	}
	if err != nil {
		return 0, 0, err
	}
	err = tx.QueryRow(ctx, `INSERT INTO alert_transitions(alert_generation_id,transition_type,occurred_at) VALUES($1,$2,$3) RETURNING alert_transition_id`, genID, a.Kind.String(), a.At).Scan(&transitionID)
	return genID, transitionID, err
}
func deliver(ctx context.Context, tx pgx.Tx, intentID int64, at time.Time) error {
	token := fmt.Sprintf("demo-%d", intentID)
	rows, err := storage.ClaimNotificationIntents(ctx, tx, at, 1, time.Minute, token)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return fmt.Errorf("claim notification %d", intentID)
	}
	_, attempt, err := storage.BeginDeliveryAttempt(ctx, tx, intentID, token, at)
	if err != nil {
		return err
	}
	_, err = storage.FinishDeliveryAttempt(ctx, tx, intentID, attempt, token, true, "delivered by deterministic demo replay", at, at)
	return err
}

func seedRules(ctx context.Context, pool *pgxpool.Pool, loaded []ruleFile) error {
	return storage.WithTx(ctx, pool, func(tx pgx.Tx) error {
		for _, r := range loaded {
			definition, err := json.Marshal(r.Definition)
			if err != nil {
				return fmt.Errorf("encode rule %s: %w", r.ID, err)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO rule_resources(rule_id,status) VALUES($1,'active') ON CONFLICT(rule_id) DO NOTHING`, r.ID); err != nil {
				return fmt.Errorf("create rule %s: %w", r.ID, err)
			}
			if _, err = tx.Exec(ctx, `INSERT INTO rule_revisions(rule_id,revision,definition) VALUES($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`, r.ID, r.Revision, definition); err != nil {
				return fmt.Errorf("create revision %s: %w", r.ID, err)
			}
			if _, err = tx.Exec(ctx, `UPDATE rule_resources SET status='active',active_revision=$2,updated_at=clock_timestamp() WHERE rule_id=$1 AND active_revision IS NULL`, r.ID, r.Revision); err != nil {
				return fmt.Errorf("activate rule %s: %w", r.ID, err)
			}
		}
		return nil
	})
}
