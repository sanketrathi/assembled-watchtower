package alerts

import (
	"testing"
	"time"

	"watchtower/internal/conditions"
)

func TestStableSeriesContributorsAndGenerations(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	k := conditions.ConditionKey{RuleID: "rule", Revision: 1, Subject: conditions.SubjectKey{Kind: conditions.SubjectQueue, ID: "billing"}}
	r := New()
	first := conditions.Episode{ID: "ep-1", Key: k, Trigger: conditions.Transition{ID: "ct-1", Key: k, Direction: conditions.Trigger, At: base}}
	if got := r.ApplyEpisode(first); len(got) != 1 || got[0].Kind != Open || got[0].Generation != 1 {
		t.Fatalf("open=%+v", got)
	}
	seriesKey := SeriesKey{RuleID: "rule", SubjectKind: SubjectQueue, SubjectID: "billing"}
	id := gotSeriesID(t, r, seriesKey)
	if got := r.ApplyEpisode(first); len(got) != 0 {
		t.Fatal(got)
	}
	second := conditions.Episode{ID: "ep-2", Key: k, Trigger: conditions.Transition{ID: "ct-2", Key: k, Direction: conditions.Trigger, At: base.Add(time.Minute)}}
	if got := r.ApplyEpisode(second); len(got) != 0 {
		t.Fatal(got)
	}
	if got := r.ApplyClear("ep-1", seriesKey, base.Add(2*time.Minute), base.Add(2*time.Minute), time.Time{}); len(got) != 0 {
		t.Fatal(got)
	}
	if got := r.ApplyClear("ep-2", seriesKey, base.Add(3*time.Minute), base.Add(3*time.Minute), time.Time{}); len(got) != 1 || got[0].Kind != Recovery {
		t.Fatalf("recovery=%+v", got)
	}
	third := conditions.Episode{ID: "ep-3", Key: k, Trigger: conditions.Transition{ID: "ct-3", Key: k, Direction: conditions.Trigger, At: base.Add(4 * time.Minute)}}
	got := r.ApplyEpisode(third)
	if len(got) != 1 || got[0].Kind != Open || got[0].Generation != 2 {
		t.Fatalf("second open=%+v", got)
	}
	if got[0].SeriesID != id {
		t.Fatalf("series changed: %q %q", id, got[0].SeriesID)
	}
}

func gotSeriesID(t *testing.T, r *Reducer, k SeriesKey) string {
	t.Helper()
	s, ok := r.Series(k)
	if !ok || !s.Open {
		t.Fatalf("series=%+v ok=%v", s, ok)
	}
	if len(s.Contributors) != 1 {
		t.Fatal(s)
	}
	return s.ID
}

func TestClosedEpisodeIsOpenThenRecoveredAndUnknownClearIsNoop(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	k := conditions.ConditionKey{RuleID: "r", Revision: 2, Subject: conditions.SubjectKey{Kind: conditions.SubjectAgent, ID: "a"}}
	e := conditions.Episode{ID: "ep", Key: k, Trigger: conditions.Transition{ID: "t", Key: k, At: base}, Clear: &conditions.Transition{ID: "c", Key: k, At: base.Add(time.Minute)}}
	r := New()
	got := r.ApplyEpisode(e)
	if len(got) != 2 || got[0].Kind != Open || got[1].Kind != Recovery {
		t.Fatalf("%+v", got)
	}
	if got := r.ApplyClear("missing", SeriesKey{RuleID: "r", SubjectKind: SubjectAgent, SubjectID: "a"}, base, time.Time{}, time.Time{}); len(got) != 0 {
		t.Fatal(got)
	}
}

func TestTimerEpisodePreservesTriggerEffectiveTime(t *testing.T) {
	at := time.Date(2025, 1, 1, 10, 10, 0, 0, time.UTC)
	effective := at.Add(-9 * time.Minute)
	k := conditions.ConditionKey{RuleID: "r", Revision: 7, Subject: conditions.SubjectKey{Kind: conditions.SubjectAgent, ID: "a"}}
	e := conditions.Episode{ID: "timer-episode", Key: k, Trigger: conditions.Transition{ID: "timer-transition", Key: k, At: at, Times: conditions.Times{EffectiveAt: effective, DueAt: at, ProcessingAt: at, EvidenceAt: effective}}}
	got := New().ApplyEpisode(e)
	if len(got) != 1 || !got[0].EffectiveAt.Equal(effective) || !got[0].At.Equal(at) {
		t.Fatalf("alert transition=%+v", got)
	}
}

func TestSeriesIDsDistinguishDelimiterContainingKeys(t *testing.T) {
	one := SeriesKey{RuleID: "r|agent", SubjectKind: SubjectQueue, SubjectID: "s"}
	two := SeriesKey{RuleID: "r", SubjectKind: SubjectAgent, SubjectID: "queue|s"}
	if SeriesID(one) == SeriesID(two) {
		t.Fatal("series IDs collided")
	}
}
