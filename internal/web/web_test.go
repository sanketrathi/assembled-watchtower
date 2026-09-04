package web

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestRenderDashboardEscapesAndIncludesActivity(t *testing.T) {
	var out bytes.Buffer
	now := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	err := RenderDashboard(&out, DashboardView{Rules: []RuleCard{{Name: "<unsafe>", Status: "active", Revision: 2}}, Alerts: []AlertCard{{RuleName: "SLA", Subject: "queue:billing", Status: "open", OpenedAt: &now}}})
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"Watchtower", "&lt;unsafe&gt;", "Alert activity", "/_watchtower/web.css"} {
		if !strings.Contains(got, want) {
			t.Errorf("page missing %q", want)
		}
	}
}
func TestRenderRuleFormProvidesThreeTemplatesAndPreview(t *testing.T) {
	var out bytes.Buffer
	err := RenderRuleForm(&out, RuleFormView{Values: RuleFormValues{Name: "Long call", CanonicalJSON: `{"schema_version":1}`}, Preview: &PreviewView{Status: "valid", Summary: "One alert would open", Alerts: []PreviewAlert{{Subject: "agent:a_31", Outcome: "Open"}}}})
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"Queue SLA breach", "Adherence violation", "Long call", "One alert would open", "Canonical rule JSON"} {
		if !strings.Contains(got, want) {
			t.Errorf("page missing %q", want)
		}
	}
}
