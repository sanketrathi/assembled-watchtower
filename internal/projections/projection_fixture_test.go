// Package projections_test verifies the accepted projection builder at its
// public occurrence-to-projection boundary.
package projections_test

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"watchtower/internal/app"
	"watchtower/internal/events"
	"watchtower/internal/projections"
)

const projectionFixtureStream = "projection-verification-fixture"

// TestCanonicalProjectionFixture verifies the fixture through the accepted
// occurrence-to-projection boundary. The projections.Builder implementation is
// owned by a separate integration dependency; this test deliberately uses its
// public API instead of introducing another source projection model.
func TestCanonicalProjectionFixture(t *testing.T) {
	root := repositoryRoot(t)
	builder := projections.NewBuilder()

	var occurrences []app.AcceptedOccurrence
	var previousLine uint64
	var logicalTime time.Time
	var repeated []app.AcceptedOccurrence
	var lateVIPCurrentChanged *bool
	var lateAdherenceCurrentChanged *bool

	fixture, err := os.Open(filepath.Join(root, "data", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	if err := events.Stream(fixture, projectionFixtureStream, func(envelope events.Envelope) error {
		if envelope.Line != previousLine+1 {
			return fmt.Errorf("physical line %d follows %d", envelope.Line, previousLine)
		}
		previousLine = envelope.Line
		occurrence := acceptedOccurrence(envelope)
		if err := occurrence.Validate(); err != nil {
			return fmt.Errorf("accepted occurrence at line %d: %w", envelope.Line, err)
		}
		if occurrence.EffectiveAt.After(logicalTime) {
			logicalTime = occurrence.EffectiveAt
		}
		if logicalTime.Before(occurrence.EffectiveAt) {
			return fmt.Errorf("logical time regressed at line %d", envelope.Line)
		}
		if !slices.Equal(occurrence.Raw, envelope.Raw) {
			return fmt.Errorf("line %d did not retain exact raw payload", envelope.Line)
		}

		update, err := builder.Build(occurrence)
		if err != nil {
			return fmt.Errorf("build line %d: %w", envelope.Line, err)
		}
		if update.Duplicate || sourceUpdates(update) != 1 {
			return fmt.Errorf("line %d update duplicate=%t source updates=%d, want one new source update",
				envelope.Line, update.Duplicate, sourceUpdates(update))
		}
		if err := assertUpdateProvenance(update, occurrence); err != nil {
			return fmt.Errorf("line %d: %w", envelope.Line, err)
		}
		if envelope.Line == 96 {
			lateVIPCurrentChanged = &update.Queue.CurrentChanged
		}
		if envelope.Line == 95 {
			lateAdherenceCurrentChanged = &update.Adherence.CurrentChanged
		}
		if occurrence.SourceEventID == "evt_01HXYZ050" {
			repeated = append(repeated, occurrence)
		}
		occurrences = append(occurrences, occurrence)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(occurrences) != 96 || previousLine != 96 {
		t.Fatalf("occurrences=%d through line=%d, want 96", len(occurrences), previousLine)
	}
	if !logicalTime.Equal(time.Date(2026, 5, 26, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("logical time=%s, want fixture maximum", logicalTime)
	}
	if len(repeated) != 2 || repeated[0].IngestPosition != 50 || repeated[1].IngestPosition != 95 ||
		repeated[0].ID == repeated[1].ID || repeated[0].IdempotencyKey == repeated[1].IdempotencyKey ||
		slices.Equal(repeated[0].Raw, repeated[1].Raw) {
		t.Fatalf("repeated source event provenance=%+v, want distinct retained lines 50 and 95", repeated)
	}
	var retainedRepeatedPositions []uint64
	for _, observation := range builder.AdherenceHistory("a_19") {
		if observation.Provenance.SourceEventID == "evt_01HXYZ050" {
			retainedRepeatedPositions = append(retainedRepeatedPositions, observation.Provenance.IngestPosition)
		}
	}
	if !slices.Equal(retainedRepeatedPositions, []uint64{50, 95}) {
		t.Fatalf("retained repeated adherence positions=%v, want [50 95]", retainedRepeatedPositions)
	}
	if lateVIPCurrentChanged == nil || *lateVIPCurrentChanged {
		t.Fatalf("late VIP snapshot current changed=%v, want false", lateVIPCurrentChanged)
	}
	if lateAdherenceCurrentChanged == nil || *lateAdherenceCurrentChanged {
		t.Fatalf("late adherence evidence current changed=%v, want false", lateAdherenceCurrentChanged)
	}

	vip, ok := builder.Queue("vip")
	if !ok || vip.Provenance.IngestPosition != 93 || !vip.Provenance.EffectiveAt.Equal(time.Date(2026, 5, 26, 10, 30, 0, 0, time.UTC)) {
		t.Fatalf("VIP current=%+v, want line 93 at 10:30", vip)
	}
	vipHistory := builder.QueueHistory("vip")
	if positions := queuePositions(vipHistory); !slices.Equal(positions, []uint64{3, 22, 93, 96}) {
		t.Fatalf("VIP history positions=%v, want physical order [3 22 93 96]", positions)
	}

	a31, ok := builder.Agent("a_31")
	if !ok || a31.Current != events.Available || a31.Provenance.IngestPosition != 90 {
		t.Fatalf("a_31 agent state=%+v, want available from line 90", a31)
	}
	a31Adherence, ok := builder.Adherence("a_31")
	if !ok || a31Adherence.InViolation || a31Adherence.ActualState != events.OnCall ||
		a31Adherence.Provenance.IngestPosition != 80 {
		t.Fatalf("a_31 adherence=%+v, want independent false/on_call evidence from line 80", a31Adherence)
	}

	a23, ok := builder.Adherence("a_23")
	wantA23Onset := time.Date(2026, 5, 26, 10, 15, 30, 0, time.UTC)
	if !ok || !a23.InViolation || a23.ViolationStartedAt != nil || !a23.KnownViolationSince.Equal(wantA23Onset) {
		t.Fatalf("a_23 adherence=%+v, want null-onset violation beginning at first known-true check", a23)
	}

	a05 := builder.AgentStateHistory("a_05")
	if len(a05) == 0 || !slices.Equal(a05[0].Change.QueueIDs, []string{"vip"}) {
		t.Fatalf("a_05 queue ID provenance=%+v, want retained observation queue ID", a05)
	}
}

func acceptedOccurrence(envelope events.Envelope) app.AcceptedOccurrence {
	return app.AcceptedOccurrence{
		ID:             envelope.ID.String(),
		Source:         projectionFixtureStream,
		IdempotencyKey: fmt.Sprintf("%s#%d", projectionFixtureStream, envelope.Line),
		IngestPosition: envelope.Line,
		SourceEventID:  envelope.Event.GetEventID(),
		EffectiveAt:    eventTime(envelope.Event),
		Event:          envelope.Event,
		Raw:            append([]byte(nil), envelope.Raw...),
	}
}

func eventTime(event events.Event) time.Time {
	switch value := event.(type) {
	case events.QueueSnapshot:
		return value.Timestamp
	case events.AgentStateChange:
		return value.Timestamp
	case events.AdherenceCheck:
		return value.Timestamp
	default:
		return time.Time{}
	}
}

func sourceUpdates(update projections.Update) int {
	count := 0
	if update.Queue != nil {
		count++
	}
	if update.AgentState != nil {
		count++
	}
	if update.Adherence != nil {
		count++
	}
	return count
}

func assertUpdateProvenance(update projections.Update, occurrence app.AcceptedOccurrence) error {
	var provenance projections.Provenance
	switch {
	case update.Queue != nil:
		provenance = update.Queue.Observation.Provenance
	case update.AgentState != nil:
		provenance = update.AgentState.Observation.Provenance
	case update.Adherence != nil:
		provenance = update.Adherence.Observation.Provenance
	default:
		return fmt.Errorf("missing source update")
	}
	if provenance.OccurrenceID != occurrence.ID || provenance.Source != occurrence.Source ||
		provenance.IdempotencyKey != occurrence.IdempotencyKey || provenance.IngestPosition != occurrence.IngestPosition ||
		provenance.SourceEventID != occurrence.SourceEventID || !provenance.EffectiveAt.Equal(occurrence.EffectiveAt) {
		return fmt.Errorf("provenance=%+v does not match accepted occurrence", provenance)
	}
	return nil
}

func queuePositions(history []projections.QueueObservation) []uint64 {
	positions := make([]uint64, len(history))
	for i := range history {
		positions[i] = history[i].Provenance.IngestPosition
	}
	return positions
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.Abs(filepath.Join(cwd, "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
