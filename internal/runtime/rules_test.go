package runtime

import (
	"testing"
	"watchtower/internal/web"
)

func TestDefinitionTemplates(t *testing.T) {
	cases := []web.TemplateKind{web.TemplateQueueSLA, web.TemplateAdherence, web.TemplateLongCall}
	for _, kind := range cases {
		t.Run(string(kind), func(t *testing.T) {
			d, err := Definition(kind, "Morning watch", "", "billing", "5m", "support-operations")
			if err != nil {
				t.Fatal(err)
			}
			if err = d.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestDefinitionRejectsMissingTargets(t *testing.T) {
	if _, err := Definition(web.TemplateQueueSLA, "watch", "", "", "5m", "ops"); err == nil {
		t.Fatal("expected error")
	}
}
