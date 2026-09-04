// Package web renders the dependency-free team-lead web interface.
//
// It deliberately owns presentation only. Callers provide view models from
// their application/query layer and retain ownership of rule mutation,
// validation, preview execution, and API routing.
package web

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
	"time"
)

//go:embed assets/watchtower.css
var assets embed.FS

// TemplateKind identifies one guided, canonical-rule template.
type TemplateKind string

const (
	TemplateQueueSLA  TemplateKind = "queue_sla"
	TemplateAdherence TemplateKind = "adherence"
	TemplateLongCall  TemplateKind = "long_call"
)

// RuleTemplate describes a selectable form starting point. The three standard
// templates are returned by StandardTemplates; callers can add local wording
// but should not use this as a second rule-definition language.
type RuleTemplate struct {
	Kind        TemplateKind
	Name        string
	Summary     string
	SubjectKind string
}

func StandardTemplates() []RuleTemplate {
	return []RuleTemplate{
		{TemplateQueueSLA, "Queue SLA breach", "Alert when a queue's longest wait exceeds its SLA.", "queue"},
		{TemplateAdherence, "Adherence violation", "Alert when selected agents remain out of adherence.", "agent"},
		{TemplateLongCall, "Long call", "Alert when selected agents remain on a call too long.", "agent"},
	}
}

// RuleCard is a compact lifecycle view for the dashboard.
type RuleCard struct {
	ID        string
	Name      string
	Status    string
	Revision  int64
	UpdatedAt time.Time
	Summary   string
	EditURL   string
}

// AlertCard is one current or recent alert series. Subject is already a
// display value such as "queue:billing"; a group is never an alert subject.
type AlertCard struct {
	ID        string
	RuleName  string
	Subject   string
	Status    string
	OpenedAt  *time.Time
	DetailURL string
}

// NotificationCard describes one durable logical notification intent, not an
// individual delivery attempt.
type NotificationCard struct {
	ID       string
	RuleName string
	Subject  string
	Kind     string
	Audience string
	Delivery string
	At       time.Time
}

// DashboardView contains the display-ready dashboard data. Slices may be nil.
type DashboardView struct {
	Title            string
	Rules            []RuleCard
	Alerts           []AlertCard
	Notifications    []NotificationCard
	NewRuleURL       string
	RulesURL         string
	AlertsURL        string
	NotificationsURL string
	Notice           string
}

// FieldError is a human-readable validation error returned by the application
// layer. Field uses the form control name, for example "targets".
type FieldError struct{ Field, Message string }

// RuleFormValues are user-facing values for a guided form. Targets and groups
// are comma or newline separated display/input text; the application adapter
// converts them to canonical selectors. CanonicalJSON is read-only technical
// detail prepared by that adapter.
type RuleFormValues struct {
	Name             string
	Description      string
	TargetIDs        string
	GroupIDs         string
	TriggerFor       string
	ClearFor         string
	Audience         string
	NotifyOnOpen     bool
	NotifyOnRecovery bool
	Threshold        string
	AgentState       string
	CanonicalJSON    string
}

// PreviewView displays deterministic preview output returned by the preview
// application use case. It does not claim that a preview was applied.
type PreviewView struct {
	Status      string
	Summary     string
	EvaluatedAt *time.Time
	Alerts      []PreviewAlert
	Message     string
}
type PreviewAlert struct{ Subject, Outcome, At, Explanation string }

// RuleFormView is used for both creation and editing. Action is the receiving
// URL for a caller-owned form endpoint. ExpectedRevision is only used on edit.
type RuleFormView struct {
	Title            string
	Mode             string // "create" or "edit"
	Action           string
	CancelURL        string
	RulesURL         string
	AlertsURL        string
	NotificationsURL string
	RuleID           string
	ExpectedRevision int64
	Templates        []RuleTemplate
	SelectedTemplate TemplateKind
	Values           RuleFormValues
	Errors           []FieldError
	Preview          *PreviewView
}

// RuleFormRequest tells a ViewSource which initial form the user requested.
type RuleFormRequest struct {
	RuleID   string
	Template TemplateKind
}

// ViewSource connects routing to application-owned query/form preparation.
// It intentionally has no mutation method: form submission belongs to the API
// or application adapter selected by the integrator.
type ViewSource interface {
	Dashboard(context.Context) (DashboardView, error)
	RuleForm(context.Context, RuleFormRequest) (RuleFormView, error)
}

// Handler serves the dashboard and form shell on GET. It also serves scoped
// CSS at /_watchtower/web.css. The caller may mount it below any prefix.
type Handler struct{ Source ViewSource }

func NewHandler(source ViewSource) http.Handler { return Handler{Source: source} }
func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Path == "/_watchtower/web.css" {
		h.serveCSS(w)
		return
	}
	if h.Source == nil {
		http.Error(w, "web view source is not configured", http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.URL.Path == "/" || r.URL.Path == "/dashboard":
		view, err := h.Source.Dashboard(r.Context())
		if err != nil {
			http.Error(w, "load dashboard", http.StatusInternalServerError)
			return
		}
		renderHTTP(w, RenderDashboard, view)
	case r.URL.Path == "/rules/new":
		view, err := h.Source.RuleForm(r.Context(), RuleFormRequest{Template: TemplateKind(r.URL.Query().Get("template"))})
		if err != nil {
			http.Error(w, "load rule form", http.StatusInternalServerError)
			return
		}
		renderHTTP(w, RenderRuleForm, view)
	case strings.HasPrefix(r.URL.Path, "/rules/") && strings.HasSuffix(r.URL.Path, "/edit"):
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/rules/"), "/edit")
		if id == "" || strings.Contains(id, "/") {
			http.NotFound(w, r)
			return
		}
		view, err := h.Source.RuleForm(r.Context(), RuleFormRequest{RuleID: id})
		if err != nil {
			http.Error(w, "load rule form", http.StatusInternalServerError)
			return
		}
		renderHTTP(w, RenderRuleForm, view)
	default:
		http.NotFound(w, r)
	}
}
func (h Handler) serveCSS(w http.ResponseWriter) {
	b, err := assets.ReadFile("assets/watchtower.css")
	if err != nil {
		http.Error(w, "web stylesheet unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	_, _ = w.Write(b)
}
func renderHTTP[T any](w http.ResponseWriter, render func(io.Writer, T) error, view T) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := render(w, view); err != nil {
		http.Error(w, "render page", 500)
	}
}

var functions = template.FuncMap{"formatTime": formatTime, "selected": func(a, b TemplateKind) bool { return a == b }, "hasErrors": func(es []FieldError, field string) bool {
	for _, e := range es {
		if e.Field == field {
			return true
		}
	}
	return false
}}
var pageTemplate = template.Must(template.New("page").Funcs(functions).Parse(`{{define "head"}}<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>{{.Title}}</title><link rel="stylesheet" href="/_watchtower/web.css"></head><body><header class="topbar"><a class="brand" href="/">Watchtower</a><nav aria-label="Primary"><a href="{{.RulesURL}}">Rules</a><a href="{{.AlertsURL}}">Alerts</a><a href="{{.NotificationsURL}}">Notifications</a></nav></header>{{end}}
{{define "dashboard"}}{{template "head" .}}<main class="shell"><div class="page-heading"><div><p class="eyebrow">Operations overview</p><h1>{{.Title}}</h1><p class="muted">Track live conditions, rule coverage, and visible delivery activity.</p></div><a class="button" href="{{.NewRuleURL}}">Create rule</a></div>{{if .Notice}}<p class="notice">{{.Notice}}</p>{{end}}<section class="metrics" aria-label="Summary"><div><strong>{{len .Alerts}}</strong><span>open alerts</span></div><div><strong>{{len .Rules}}</strong><span>rules tracked</span></div><div><strong>{{len .Notifications}}</strong><span>notification intents</span></div></section><section class="panel"><div class="section-heading"><h2>Rules</h2><a href="{{.RulesURL}}">All rules</a></div>{{if .Rules}}<div class="rule-grid">{{range .Rules}}<article class="rule-card"><div class="card-top"><span class="status {{.Status}}">{{.Status}}</span><span class="muted">rev {{.Revision}}</span></div><h3>{{.Name}}</h3><p>{{.Summary}}</p><footer><time>{{formatTime .UpdatedAt}}</time>{{if .EditURL}}<a href="{{.EditURL}}">Edit</a>{{end}}</footer></article>{{end}}</div>{{else}}<div class="empty"><h3>No rules yet</h3><p>Start with a guided template to define the first condition.</p><a class="button" href="{{.NewRuleURL}}">Create rule</a></div>{{end}}</section><div class="two-column"><section class="panel"><div class="section-heading"><h2>Alert activity</h2><a href="{{.AlertsURL}}">View alerts</a></div>{{if .Alerts}}<ul class="activity">{{range .Alerts}}<li><span class="dot {{.Status}}"></span><div><strong>{{.RuleName}}</strong><p>{{.Subject}} · {{.Status}}{{if .OpenedAt}} · opened {{formatTime .OpenedAt}}{{end}}</p></div>{{if .DetailURL}}<a href="{{.DetailURL}}">Details</a>{{end}}</li>{{end}}</ul>{{else}}<p class="empty-copy">No open alerts. Current conditions are quiet.</p>{{end}}</section><section class="panel"><div class="section-heading"><h2>Notifications</h2><a href="{{.NotificationsURL}}">View activity</a></div>{{if .Notifications}}<ul class="activity">{{range .Notifications}}<li><span class="dot neutral"></span><div><strong>{{.Kind}} · {{.RuleName}}</strong><p>{{.Subject}} → {{.Audience}} · {{.Delivery}}</p></div><time>{{formatTime .At}}</time></li>{{end}}</ul>{{else}}<p class="empty-copy">No notification activity to show.</p>{{end}}</section></div></main></body></html>{{end}}
{{define "rule-form"}}{{template "head" .}}<main class="shell"><div class="breadcrumb"><a href="{{.CancelURL}}">Rules</a><span>/</span><span>{{.Mode}} rule</span></div><div class="page-heading"><div><p class="eyebrow">Guided composer</p><h1>{{.Title}}</h1><p class="muted">Use a template to create one canonical rule definition.</p></div></div>{{if .Errors}}<aside class="error-summary" aria-label="Fix these fields"><strong>Review the highlighted fields</strong><ul>{{range .Errors}}<li>{{.Message}}</li>{{end}}</ul></aside>{{end}}<div class="composer"><form method="post" action="{{.Action}}" class="panel form-panel"><input type="hidden" name="template" value="{{.SelectedTemplate}}">{{if .RuleID}}<input type="hidden" name="rule_id" value="{{.RuleID}}"><input type="hidden" name="expected_revision" value="{{.ExpectedRevision}}">{{end}}<fieldset><legend>1. Choose a starting point</legend><div class="template-grid">{{range .Templates}}<label class="template {{if selected .Kind $.SelectedTemplate}}selected{{end}}"><input type="radio" name="template_choice" value="{{.Kind}}" {{if selected .Kind $.SelectedTemplate}}checked{{end}}><strong>{{.Name}}</strong><span>{{.Summary}}</span><small>{{.SubjectKind}} rule</small></label>{{end}}</div></fieldset><fieldset><legend>2. Describe the rule</legend><label>Rule name<input name="name" value="{{.Values.Name}}" required aria-invalid="{{hasErrors .Errors "name"}}"></label><label>Description <span class="optional">optional</span><textarea name="description" rows="3">{{.Values.Description}}</textarea></label></fieldset><fieldset><legend>3. Select subjects</legend><p class="hint">Choose the queues or agents this team lead watches.</p><label>Subject IDs<input name="target_ids" value="{{.Values.TargetIDs}}" placeholder="billing, a_19"></label></fieldset><fieldset><legend>4. Set the condition</legend><div class="form-grid"><label>Trigger duration<input name="trigger_for" value="{{.Values.TriggerFor}}" placeholder="5m" required></label></div><p class="hint">The selected template supplies the condition and an immediate recovery rule. Positive durations qualify at or after the stated duration.</p></fieldset><fieldset><legend>5. Notify the team</legend><label>Logical audience<input name="audience" value="{{.Values.Audience}}" placeholder="support-operations" required></label><p class="hint">Watchtower records one notification when an alert opens and one when it recovers.</p></fieldset><div class="form-actions"><a href="{{.CancelURL}}">Cancel</a><button class="secondary" type="submit" name="intent" value="preview">Validate and preview</button><button type="submit" name="intent" value="save">{{if eq .Mode "edit"}}Save new revision{{else}}Create rule{{end}}</button></div></form><aside class="preview-column"><section class="panel preview"><div class="section-heading"><h2>Preview</h2>{{if .Preview}}<span class="status neutral">{{.Preview.Status}}</span>{{end}}</div>{{if .Preview}}<p>{{.Preview.Summary}}</p>{{if .Preview.EvaluatedAt}}<p class="muted">Evaluated {{formatTime .Preview.EvaluatedAt}}</p>{{end}}{{if .Preview.Alerts}}<ol class="preview-list">{{range .Preview.Alerts}}<li><strong>{{.Outcome}}</strong><span>{{.Subject}}{{if .At}} · {{.At}}{{end}}</span><small>{{.Explanation}}</small></li>{{end}}</ol>{{end}}{{if .Preview.Message}}<p class="hint">{{.Preview.Message}}</p>{{end}}{{else}}<p class="empty-copy">Validate this draft to see a deterministic preview against the selected history.</p>{{end}}</section><details class="panel technical"><summary>Canonical rule JSON</summary><p class="hint">Read-only technical detail. This is the definition submitted by the adapter.</p><pre>{{.Values.CanonicalJSON}}</pre></details></aside></div></main></body></html>{{end}}`))

// RenderDashboard writes a complete dashboard document.
func RenderDashboard(w io.Writer, view DashboardView) error {
	if view.Title == "" {
		view.Title = "Team lead dashboard"
	}
	defaultsDashboard(&view)
	if err := pageTemplate.ExecuteTemplate(w, "dashboard", view); err != nil {
		return fmt.Errorf("render dashboard: %w", err)
	}
	return nil
}
func defaultsDashboard(v *DashboardView) {
	if v.NewRuleURL == "" {
		v.NewRuleURL = "/rules/new"
	}
	if v.RulesURL == "" {
		v.RulesURL = "/rules/new"
	}
	if v.AlertsURL == "" {
		v.AlertsURL = "/api/alerts"
	}
	if v.NotificationsURL == "" {
		v.NotificationsURL = "/api/notifications"
	}
}

// RenderRuleForm writes the guided rule composer and optional preview.
func RenderRuleForm(w io.Writer, view RuleFormView) error {
	if view.Mode == "" {
		view.Mode = "create"
	}
	if view.Title == "" {
		view.Title = "Create rule"
		if view.Mode == "edit" {
			view.Title = "Edit rule"
		}
	}
	if view.Action == "" {
		view.Action = "/rules"
	}
	if view.CancelURL == "" {
		view.CancelURL = "/rules"
	}
	if view.RulesURL == "" {
		view.RulesURL = "/rules"
	}
	if view.AlertsURL == "" {
		view.AlertsURL = "/alerts"
	}
	if view.NotificationsURL == "" {
		view.NotificationsURL = "/notifications"
	}
	if len(view.Templates) == 0 {
		view.Templates = StandardTemplates()
	}
	if view.SelectedTemplate == "" {
		view.SelectedTemplate = TemplateQueueSLA
	}
	if err := pageTemplate.ExecuteTemplate(w, "rule-form", view); err != nil {
		return fmt.Errorf("render rule form: %w", err)
	}
	return nil
}
func formatTime(v any) string {
	var t time.Time
	switch x := v.(type) {
	case time.Time:
		t = x
	case *time.Time:
		if x == nil {
			return ""
		}
		t = *x
	}
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("02 Jan 15:04 UTC")
}
