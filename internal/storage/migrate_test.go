package storage

import (
	"strings"
	"testing"
)

func TestEmbeddedMigrationContainsDurabilityBoundaries(t *testing.T) {
	ms, err := migrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("migrations=%d", len(ms))
	}
	m := ms[0]
	all := ms[0].SQL + ms[1].SQL
	for _, want := range []string{"payload_hash TEXT NOT NULL", "payload_raw BYTEA", "runtime_clock", "semantic_commit_seq", "notification_deliveries", "alert_transition_id", "group_revision_members"} {
		if !strings.Contains(all, want) {
			t.Errorf("migration missing %q", want)
		}
	}
	if len(m.Checksum) != 64 {
		t.Fatalf("checksum=%q", m.Checksum)
	}
}

func TestEmbeddedMigrationSeparatesOperationalProjectionSources(t *testing.T) {
	ms, err := migrations()
	if err != nil {
		t.Fatal(err)
	}
	var schema strings.Builder
	for _, migration := range ms {
		schema.WriteString(migration.SQL)
	}
	for _, want := range []string{
		"CREATE TABLE queue_observations",
		"CREATE TABLE agent_state_observations",
		"CREATE TABLE adherence_observations",
		"CREATE TABLE queue_state_current",
		"CREATE TABLE agent_state_current",
		"CREATE TABLE adherence_current",
		"previous_state_duration_sec BIGINT",
		"violation_started_at TIMESTAMPTZ",
		"queue_ids JSONB",
	} {
		if !strings.Contains(schema.String(), want) {
			t.Errorf("projection schema missing %q", want)
		}
	}
}
