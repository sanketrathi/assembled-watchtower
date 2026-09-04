// Package demoruntime replays JSONL events through active rules for the demo command.
package runtime

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"watchtower/internal/alerts"
	"watchtower/internal/app"
	"watchtower/internal/conditions"
	"watchtower/internal/evaluation"
	"watchtower/internal/events"
	"watchtower/internal/notifications"
	"watchtower/internal/rules"
)

// Rule is one active immutable rule revision used by demo replay.
type Rule struct {
	ID         string               `json:"id"`
	Revision   int64                `json:"revision"`
	Definition rules.RuleDefinition `json:"definition"`
}

// Notification is a planned notification, with its alert transition retained
// so callers that render alerts do not need to recreate lifecycle state.
type Notification struct {
	notifications.Intent
	Alert       alerts.Transition
	RuleID      string
	SubjectKind string
	SubjectID   string
}

// LoadRules strictly decodes a JSON array of active demo rules.
func LoadRules(input io.Reader) ([]Rule, error) {
	if input == nil {
		return nil, fmt.Errorf("rules reader is required")
	}
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	var values []Rule
	if err := decoder.Decode(&values); err != nil {
		return nil, fmt.Errorf("decode rules: %w", err)
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, fmt.Errorf("decode rules: trailing data")
	}
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.ID == "" || value.Revision <= 0 {
			return nil, fmt.Errorf("invalid active rule %q/%d", value.ID, value.Revision)
		}
		key := fmt.Sprintf("%s/%d", value.ID, value.Revision)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate active rule %q", key)
		}
		seen[key] = struct{}{}
		if err := value.Definition.Validate(); err != nil {
			return nil, fmt.Errorf("rule %q: %w", value.ID, err)
		}
	}
	return values, nil
}

// Replay consumes physical JSONL order. It advances a nondecreasing logical
// clock before applying each current projection update. Late events remain
// historical evidence and never replace newer source-specific projections.
func Replay(input io.Reader, configured []Rule) ([]Notification, error) {
	if input == nil {
		return nil, fmt.Errorf("events reader is required")
	}
	active := make([]app.ActiveRule, len(configured))
	policies := make(map[string]notifications.Policy, len(configured))
	for i, rule := range configured {
		active[i] = app.ActiveRule{ID: rule.ID, Revision: rule.Revision, Definition: rule.Definition}
		policies[rule.ID] = notifications.Policy{OnOpen: rule.Definition.Notifications.OnOpen, OnRecovery: rule.Definition.Notifications.OnRecovery, Audience: rule.Definition.Notifications.Audience}
	}
	runtime, err := evaluation.Activate(active, nil)
	if err != nil {
		return nil, fmt.Errorf("activate rules: %w", err)
	}
	alertReducer, planner := alerts.New(), notifications.New()
	var output []Notification
	queues := map[string]events.QueueSnapshot{}
	agents := map[string]events.AgentStateChange{}
	adherence := map[string]events.AdherenceCheck{}
	var logical time.Time

	emit := func(transitions []conditions.Transition) error {
		for _, transition := range transitions {
			key := alerts.SeriesKey{RuleID: transition.Key.RuleID, SubjectKind: alerts.SubjectKind(transition.Key.Subject.Kind), SubjectID: transition.Key.Subject.ID}
			var alertTransitions []alerts.Transition
			if transition.Direction == conditions.Trigger {
				alertTransitions = alertReducer.ApplyStart(alerts.Episode{ID: transition.EpisodeID, Series: key, Revision: transition.Key.Revision, Opened: true, At: transition.At, EffectiveAt: transition.Times.EffectiveAt, EvidenceAt: transition.Times.EvidenceAt})
			} else {
				alertTransitions = alertReducer.ApplyClear(transition.EpisodeID, key, transition.At, transition.Times.EffectiveAt, transition.Times.EvidenceAt)
			}
			for _, alertTransition := range alertTransitions {
				intent, created := planner.Plan(alertTransition, policies[transition.Key.RuleID])
				if created {
					output = append(output, Notification{Intent: intent, Alert: alertTransition, RuleID: transition.Key.RuleID, SubjectKind: string(alertTransition.Series.SubjectKind), SubjectID: alertTransition.Series.SubjectID})
				}
			}
		}
		return nil
	}
	provider := evaluation.EvidenceProviderFunc(func(target rules.ResolvedTarget) (evaluation.Evidence, bool) {
		lookup := rules.FieldLookupFunc(func(field rules.FieldSpec) (rules.Operand, bool) {
			switch target.Kind {
			case rules.SubjectQueue:
				value, ok := queues[target.ID]
				if !ok {
					return rules.Operand{}, false
				}
				switch field.Name {
				case "queue.longest_wait":
					return rules.DurationOperand(rules.Duration(time.Duration(value.LongestWaitSec) * time.Second)), true
				case "queue.sla_target":
					return rules.DurationOperand(rules.Duration(time.Duration(value.SLATargetSec) * time.Second)), true
				case "queue.tickets_waiting":
					return rules.IntegerOperand(int64(value.TicketsWaiting)), true
				case "queue.agents_available":
					return rules.IntegerOperand(int64(value.AgentsAvailable)), true
				case "queue.agents_on_call":
					return rules.IntegerOperand(int64(value.AgentsOnCall)), true
				case "queue.volume_last_15m":
					return rules.IntegerOperand(int64(value.VolumeLast15m)), true
				}
			case rules.SubjectAgent:
				if value, ok := agents[target.ID]; ok && field.Name == "agent.current_state" {
					return rules.AgentStateOperand(string(value.NewState)), true
				}
				if value, ok := adherence[target.ID]; ok && field.Name == "adherence.violation" {
					return rules.BooleanOperand(value.InViolation), true
				}
			}
			return rules.Operand{}, false
		})
		evidence := evaluation.Evidence{Lookup: lookup}
		if value, ok := adherence[target.ID]; ok && value.ViolationStartedAt != nil {
			evidence.TrueSince = *value.ViolationStartedAt
		}
		return evidence, true
	})
	process := func(envelope events.Envelope) error {
		at := previewTimestamp(envelope.Event)
		if at.After(logical) {
			logical = at
		}
		transitions, err := runtime.Advance(logical)
		if err != nil {
			return fmt.Errorf("advance line %d: %w", envelope.Line, err)
		}
		if err := emit(transitions); err != nil {
			return err
		}
		switch value := envelope.Event.(type) {
		case events.QueueSnapshot:
			if old, ok := queues[value.QueueID]; !ok || !value.Timestamp.Before(old.Timestamp) {
				queues[value.QueueID] = value
			}
		case events.AgentStateChange:
			if old, ok := agents[value.AgentID]; !ok || !value.Timestamp.Before(old.Timestamp) {
				agents[value.AgentID] = value
			}
		case events.AdherenceCheck:
			if old, ok := adherence[value.AgentID]; !ok || !value.Timestamp.Before(old.Timestamp) {
				adherence[value.AgentID] = value
			}
		}
		occurrence := app.AcceptedOccurrence{ID: envelope.ID.String(), Source: envelope.ID.StreamID, IdempotencyKey: envelope.ID.String(), IngestPosition: envelope.Line, SourceEventID: envelope.Event.GetEventID(), EffectiveAt: at, Event: envelope.Event, Raw: envelope.Raw}
		request, err := app.NewEvaluationRequest(occurrence, logical, active)
		if err != nil {
			return fmt.Errorf("evaluation request line %d: %w", envelope.Line, err)
		}
		result, err := runtime.Evaluate(request, provider)
		if err != nil {
			return fmt.Errorf("evaluate line %d: %w", envelope.Line, err)
		}
		transitions, err = runtime.Apply(result)
		if err != nil {
			return fmt.Errorf("apply line %d: %w", envelope.Line, err)
		}
		return emit(transitions)
	}
	if err := events.Stream(input, "demo-events", process); err != nil {
		return nil, fmt.Errorf("replay events: %w", err)
	}
	if !logical.IsZero() {
		transitions, err := runtime.Advance(logical)
		if err != nil {
			return nil, err
		}
		if err := emit(transitions); err != nil {
			return nil, err
		}
	}
	return output, nil
}

func previewTimestamp(event events.Event) time.Time {
	switch value := event.(type) {
	case events.QueueSnapshot:
		return value.Timestamp
	case events.AgentStateChange:
		return value.Timestamp
	case events.AdherenceCheck:
		return value.Timestamp
	}
	return time.Time{}
}
