package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"watchtower/internal/app"
	"watchtower/internal/rules"
	"watchtower/internal/web"
)

func IDs(raw string) []string {
	var out []string
	for _, x := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == '\n' || r == ' ' }) {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
}
func Definition(template web.TemplateKind, name, description, targets, duration, audience string) (rules.RuleDefinition, error) {
	d, e := rules.ParseDuration(duration)
	if e != nil {
		return rules.RuleDefinition{}, e
	}
	if name == "" {
		return rules.RuleDefinition{}, fmt.Errorf("rule name is required")
	}
	if audience == "" {
		return rules.RuleDefinition{}, fmt.Errorf("audience is required")
	}
	var kind rules.SubjectKind
	var trigger, clear rules.CompareExpression
	switch template {
	case web.TemplateQueueSLA:
		kind = rules.SubjectQueue
		trigger = rules.NewCompare(rules.FieldOperand("queue.longest_wait"), rules.OpGreater, rules.FieldOperand("queue.sla_target"))
		clear = rules.NewCompare(rules.FieldOperand("queue.longest_wait"), rules.OpLessOrEqual, rules.FieldOperand("queue.sla_target"))
	case web.TemplateAdherence:
		kind = rules.SubjectAgent
		trigger = rules.NewCompare(rules.FieldOperand("adherence.violation"), rules.OpEqual, rules.BooleanOperand(true))
		clear = rules.NewCompare(rules.FieldOperand("adherence.violation"), rules.OpEqual, rules.BooleanOperand(false))
	case web.TemplateLongCall:
		kind = rules.SubjectAgent
		trigger = rules.NewCompare(rules.FieldOperand("agent.current_state"), rules.OpEqual, rules.AgentStateOperand("on_call"))
		clear = rules.NewCompare(rules.FieldOperand("agent.current_state"), rules.OpNotEqual, rules.AgentStateOperand("on_call"))
	default:
		return rules.RuleDefinition{}, fmt.Errorf("choose a rule template")
	}
	out := rules.NewRuleDefinition(name, description, rules.NewTargets(kind, IDs(targets), nil), rules.NewCondition(trigger, d), rules.NewCondition(clear, 0), rules.NotificationPolicy{OnOpen: true, OnRecovery: true, Audience: audience})
	return out, out.Validate()
}
func SaveRule(ctx context.Context, p *pgxpool.Pool, id string, d rules.RuleDefinition) (string, error) {
	if id == "" {
		id = "rule-" + strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(d.Name, " ", "-"), "/", "-"))
	}
	raw, e := json.Marshal(d)
	if e != nil {
		return "", e
	}
	e = withTx(ctx, p, func(tx pgx.Tx) error {
		var rev int64
		e := tx.QueryRow(ctx, `SELECT COALESCE(MAX(revision),0)+1 FROM rule_revisions WHERE rule_id=$1`, id).Scan(&rev)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `INSERT INTO rule_resources(rule_id,status) VALUES($1,'active') ON CONFLICT(rule_id) DO NOTHING`, id)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `INSERT INTO rule_revisions(rule_id,revision,definition) VALUES($1,$2,$3::jsonb)`, id, rev, raw)
		if e != nil {
			return e
		}
		_, e = tx.Exec(ctx, `UPDATE rule_resources SET status='active',active_revision=$2,updated_at=clock_timestamp() WHERE rule_id=$1`, id, rev)
		return e
	})
	return id, e
}
func withTx(ctx context.Context, p *pgxpool.Pool, fn func(pgx.Tx) error) error {
	tx, e := p.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if e = fn(tx); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func activeFromDB(ctx context.Context, p *pgxpool.Pool) ([]app.ActiveRule, error) {
	rows, e := p.Query(ctx, `SELECT r.rule_id,r.active_revision,rr.definition FROM rule_resources r JOIN rule_revisions rr ON rr.rule_id=r.rule_id AND rr.revision=r.active_revision WHERE r.status='active' ORDER BY r.rule_id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []app.ActiveRule
	for rows.Next() {
		var x app.ActiveRule
		var raw []byte
		if e = rows.Scan(&x.ID, &x.Revision, &raw); e != nil {
			return nil, e
		}
		if e = json.Unmarshal(raw, &x.Definition); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
