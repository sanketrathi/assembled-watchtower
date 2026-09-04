package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"watchtower/internal/rules"
	"watchtower/internal/web"
)

// Site is the small read model used by the submitted dashboard.
type Site struct{ Pool *pgxpool.Pool }

func (s Site) Dashboard(ctx context.Context) (web.DashboardView, error) {
	if s.Pool == nil {
		return web.DashboardView{}, fmt.Errorf("database is required")
	}
	v := web.DashboardView{Title: "Team lead dashboard"}
	rows, err := s.Pool.Query(ctx, `SELECT r.rule_id, rr.definition->>'name', r.status, r.active_revision, r.updated_at FROM rule_resources r LEFT JOIN rule_revisions rr ON rr.rule_id=r.rule_id AND rr.revision=r.active_revision ORDER BY r.updated_at DESC`)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var x web.RuleCard
		if err := rows.Scan(&x.ID, &x.Name, &x.Status, &x.Revision, &x.UpdatedAt); err != nil {
			return v, err
		}
		x.Summary = "Active operational notification rule"
		x.EditURL = "/rules/" + x.ID + "/edit"
		v.Rules = append(v.Rules, x)
	}
	if err := rows.Err(); err != nil {
		return v, err
	}
	rows, err = s.Pool.Query(ctx, `SELECT a.alert_series_id, COALESCE(rr.definition->>'name', a.rule_id), a.subject_kind||':'||a.subject_id, g.status, g.opened_at FROM alert_series a JOIN alert_generations g ON g.alert_series_id=a.alert_series_id LEFT JOIN rule_resources r ON r.rule_id=a.rule_id LEFT JOIN rule_revisions rr ON rr.rule_id=r.rule_id AND rr.revision=r.active_revision WHERE g.status='open' ORDER BY g.opened_at DESC`)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var x web.AlertCard
		var opened time.Time
		if err := rows.Scan(&x.ID, &x.RuleName, &x.Subject, &x.Status, &opened); err != nil {
			return v, err
		}
		x.OpenedAt = &opened
		v.Alerts = append(v.Alerts, x)
	}
	if err := rows.Err(); err != nil {
		return v, err
	}
	rows, err = s.Pool.Query(ctx, `SELECT n.intent_id,COALESCE(rr.definition->>'name',a.rule_id),a.subject_kind||':'||a.subject_id,n.transition_type,n.audience,n.state,n.created_at FROM notification_intents n JOIN alert_generations g ON g.alert_generation_id=n.alert_generation_id JOIN alert_series a ON a.alert_series_id=g.alert_series_id LEFT JOIN rule_resources r ON r.rule_id=a.rule_id LEFT JOIN rule_revisions rr ON rr.rule_id=r.rule_id AND rr.revision=r.active_revision ORDER BY n.intent_id DESC LIMIT 8`)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var x web.NotificationCard
		if err := rows.Scan(&x.ID, &x.RuleName, &x.Subject, &x.Kind, &x.Audience, &x.Delivery, &x.At); err != nil {
			return v, err
		}
		v.Notifications = append(v.Notifications, x)
	}
	return v, rows.Err()
}

func (s Site) RuleForm(ctx context.Context, request web.RuleFormRequest) (web.RuleFormView, error) {
	template := request.Template
	if template == "" {
		template = web.TemplateQueueSLA
	}
	v := web.RuleFormView{Mode: "create", Title: "Create a rule", Action: "/rules", CancelURL: "/", SelectedTemplate: template, Templates: web.StandardTemplates(), Values: web.RuleFormValues{TriggerFor: "5m", ClearFor: "0s", Audience: "support-operations", NotifyOnOpen: true, NotifyOnRecovery: true}}
	if request.RuleID != "" {
		var raw []byte
		if err := s.Pool.QueryRow(ctx, `SELECT r.active_revision,rr.definition FROM rule_resources r JOIN rule_revisions rr ON rr.rule_id=r.rule_id AND rr.revision=r.active_revision WHERE r.rule_id=$1`, request.RuleID).Scan(&v.ExpectedRevision, &raw); err != nil {
			return v, err
		}
		var definition rules.RuleDefinition
		if err := json.Unmarshal(raw, &definition); err != nil {
			return v, err
		}
		v.Mode = "edit"
		v.Title = "Edit rule"
		v.RuleID = request.RuleID
		v.Action = "/rules/" + request.RuleID
		v.Values = web.RuleFormValues{Name: definition.Name, Description: definition.Description, TargetIDs: strings.Join(definition.Targets.IDs, ", "), TriggerFor: definition.Trigger.For.String(), ClearFor: definition.Clear.For.String(), Audience: definition.Notifications.Audience, NotifyOnOpen: definition.Notifications.OnOpen, NotifyOnRecovery: definition.Notifications.OnRecovery, CanonicalJSON: string(raw)}
		v.SelectedTemplate = templateFor(definition)
	}
	return v, nil
}

func templateFor(d rules.RuleDefinition) web.TemplateKind {
	if c, ok := d.Trigger.Predicate.(rules.CompareExpression); ok {
		switch c.Left.Field {
		case "queue.longest_wait":
			return web.TemplateQueueSLA
		case "adherence.violation":
			return web.TemplateAdherence
		case "agent.current_state":
			return web.TemplateLongCall
		}
	}
	return web.TemplateQueueSLA
}
