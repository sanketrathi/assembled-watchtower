package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
)

type AlertRecord struct {
	ID          int64   `json:"id"`
	RuleID      string  `json:"rule_id"`
	SubjectKind string  `json:"subject_kind"`
	SubjectID   string  `json:"subject_id"`
	Status      string  `json:"status"`
	OpenedAt    string  `json:"opened_at"`
	RecoveredAt *string `json:"recovered_at,omitempty"`
}
type NotificationRecord struct {
	ID          int64  `json:"id"`
	RuleID      string `json:"rule_id"`
	SubjectKind string `json:"subject_kind"`
	SubjectID   string `json:"subject_id"`
	Kind        string `json:"kind"`
	Audience    string `json:"audience"`
	State       string `json:"state"`
	CreatedAt   string `json:"created_at"`
	Message     string `json:"message"`
}

func Alerts(ctx context.Context, p *pgxpool.Pool) ([]AlertRecord, error) {
	rows, e := p.Query(ctx, `SELECT s.alert_series_id,s.rule_id,s.subject_kind,s.subject_id,g.status,g.opened_at::text,CASE WHEN g.recovered_at IS NULL THEN NULL ELSE g.recovered_at::text END FROM alert_series s JOIN alert_generations g ON g.alert_series_id=s.alert_series_id ORDER BY g.opened_at DESC`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []AlertRecord
	for rows.Next() {
		var x AlertRecord
		if e = rows.Scan(&x.ID, &x.RuleID, &x.SubjectKind, &x.SubjectID, &x.Status, &x.OpenedAt, &x.RecoveredAt); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func Notifications(ctx context.Context, p *pgxpool.Pool) ([]NotificationRecord, error) {
	rows, e := p.Query(ctx, `SELECT n.intent_id,s.rule_id,s.subject_kind,s.subject_id,n.transition_type,n.audience,n.state,n.created_at::text,COALESCE(d.visible_message,'') FROM notification_intents n JOIN alert_generations g ON g.alert_generation_id=n.alert_generation_id JOIN alert_series s ON s.alert_series_id=g.alert_series_id LEFT JOIN notification_deliveries d ON d.intent_id=n.intent_id ORDER BY n.intent_id`)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []NotificationRecord
	for rows.Next() {
		var x NotificationRecord
		if e = rows.Scan(&x.ID, &x.RuleID, &x.SubjectKind, &x.SubjectID, &x.Kind, &x.Audience, &x.State, &x.CreatedAt, &x.Message); e != nil {
			return nil, e
		}
		out = append(out, x)
	}
	return out, rows.Err()
}
func API(pool *pgxpool.Pool) http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("/alerts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		x, e := Alerts(r.Context(), pool)
		if e != nil {
			http.Error(w, fmt.Sprintf("load alerts: %v", e), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(x)
	})
	m.HandleFunc("/notifications", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		x, e := Notifications(r.Context(), pool)
		if e != nil {
			http.Error(w, fmt.Sprintf("load notifications: %v", e), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(x)
	})
	return m
}
