package conditions

import (
	"testing"
	"time"
)

func obs(k ConditionKey, d Direction, result Result, at time.Time) Observation {
	return Observation{Key: k, Direction: d, Result: result, EffectiveAt: at, ProcessingAt: at}
}
func key() ConditionKey {
	return ConditionKey{RuleID: "r", Revision: 1, Subject: SubjectKey{Kind: SubjectAgent, ID: "a"}}
}

func TestDurationBoundaryAndEpisodeLifecycle(t *testing.T) {
	base := time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC)
	r := NewReducer(5*time.Minute, 0)
	if got := r.Apply(obs(key(), Trigger, True, base)); len(got) != 0 {
		t.Fatal(got)
	}
	if got := r.Advance(base.Add(5 * time.Minute)); len(got) != 1 || got[0].At != base.Add(5*time.Minute) {
		t.Fatalf("trigger=%+v", got)
	}
	got := r.Apply(obs(key(), Clear, True, base.Add(6*time.Minute)))
	if len(got) != 1 || got[0].Direction != Clear {
		t.Fatalf("clear=%+v", got)
	}
	eps := r.Episodes()
	if len(eps) != 1 || eps[0].Open() {
		t.Fatalf("episodes=%+v", eps)
	}
}

func TestUnknownCancelsAndRestartRequiresFullDuration(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(time.Minute, 0)
	r.Apply(obs(key(), Trigger, True, base))
	r.Apply(obs(key(), Trigger, Unknown, base.Add(30*time.Second)))
	if got := r.Advance(base.Add(2 * time.Minute)); len(got) != 0 {
		t.Fatal(got)
	}
	r.Apply(obs(key(), Trigger, True, base.Add(2*time.Minute)))
	if got := r.Advance(base.Add(3 * time.Minute)); len(got) != 1 {
		t.Fatal(got)
	}
}

func TestStaleTimerAndDuplicateInput(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(time.Minute, 0)
	o := obs(key(), Trigger, True, base)
	r.Apply(o)
	timer := r.Timers()[0]
	r.Apply(obs(key(), Trigger, False, base.Add(10*time.Second)))
	if got := r.Fire(timer, base.Add(time.Minute)); len(got) != 0 {
		t.Fatal(got)
	}
	o.ObservationID = "occ-1"
	r = NewReducer(time.Minute, 0)
	r.Apply(o)
	r.Apply(o)
	if got := r.Advance(base.Add(time.Minute)); len(got) != 1 {
		t.Fatalf("duplicate transition=%+v", got)
	}
}

func TestSameTimestampDueTimerPrecedesClearAndZeroDuration(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(time.Minute, 0)
	r.Apply(obs(key(), Trigger, True, base))
	got := r.Apply(obs(key(), Clear, True, base.Add(time.Minute)))
	if len(got) != 2 || got[0].Direction != Trigger || got[0].At != base.Add(time.Minute) || got[1].Direction != Clear {
		t.Fatalf("transitions=%+v", got)
	}
	r = NewReducer(0, 0)
	got = r.Apply(obs(key(), Trigger, True, base))
	if len(got) != 1 || got[0].At != base {
		t.Fatalf("zero=%+v", got)
	}
}

func TestOverdueOnsetCommitsAtLogicalTimeAndPreservesDue(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(10*time.Minute, 0)
	o := obs(key(), Trigger, True, base.Add(time.Hour))
	o.TrueSince = base
	got := r.Apply(o)
	if len(got) != 1 {
		t.Fatal(got)
	}
	if got[0].At != base.Add(time.Hour) || got[0].Times.DueAt != base.Add(10*time.Minute) {
		t.Fatalf("%+v", got[0])
	}
}

func TestDurationBoundaries(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		name     string
		duration time.Duration
		advance  time.Duration
		want     int
	}{
		{"just before", time.Minute, time.Minute - time.Nanosecond, 0},
		{"exactly due", time.Minute, time.Minute, 1},
		{"zero duration", 0, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReducer(tc.duration, 0)
			got := r.Apply(obs(key(), Trigger, True, base))
			got = append(got, r.Advance(base.Add(tc.advance))...)
			if len(got) != tc.want {
				t.Fatalf("got %d transitions: %+v", len(got), got)
			}
		})
	}
}

func TestTimerOrderingUsesCanonicalKey(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(time.Minute, 0)
	first := key()
	second := first
	second.Subject.ID = "b"
	r.Apply(obs(second, Trigger, True, base))
	r.Apply(obs(first, Trigger, True, base))
	got := r.Advance(base.Add(time.Minute))
	if len(got) != 2 || got[0].Key.Subject.ID != "a" || got[1].Key.Subject.ID != "b" {
		t.Fatalf("ordered transitions=%+v", got)
	}
}

func TestOccurrenceReplayAfterClearIsIgnored(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(0, 0)
	trigger := obs(key(), Trigger, True, base)
	trigger.ObservationID = "accepted-occurrence"
	if got := r.Apply(trigger); len(got) != 1 {
		t.Fatal(got)
	}
	if got := r.Apply(obs(key(), Clear, True, base.Add(time.Minute))); len(got) != 1 {
		t.Fatal(got)
	}
	if got := r.Apply(trigger); len(got) != 0 {
		t.Fatalf("replayed transition=%+v", got)
	}
	if len(r.Episodes()) != 1 {
		t.Fatal(r.Episodes())
	}
}

func TestFireValidatesCanonicalDue(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := NewReducer(time.Minute, 0)
	r.Apply(obs(key(), Trigger, True, base))
	timer := r.Timers()[0]
	forged := timer
	forged.DueAt = base.Add(30 * time.Second)
	if got := r.Fire(forged, base.Add(time.Minute)); len(got) != 0 {
		t.Fatal(got)
	}
	got := r.Fire(timer, base.Add(2*time.Minute))
	if len(got) != 1 || got[0].At != timer.DueAt {
		t.Fatalf("callback=%+v", got)
	}
	if got[0].Times.ProcessingAt != base.Add(2*time.Minute) {
		t.Fatalf("processing time=%v", got[0].Times.ProcessingAt)
	}
	if got := r.Fire(timer, base.Add(2*time.Minute)); len(got) != 0 {
		t.Fatalf("replayed callback=%+v", got)
	}
}

func TestStableIDsDistinguishDelimiterContainingKeys(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	one := ConditionKey{RuleID: "r|agent", Revision: 1, Subject: SubjectKey{Kind: SubjectQueue, ID: "s"}}
	two := ConditionKey{RuleID: "r", Revision: 1, Subject: SubjectKey{Kind: SubjectAgent, ID: "queue|s"}}
	r := NewReducer(0, 0)
	a := r.Apply(obs(one, Trigger, True, base))
	b := r.Apply(obs(two, Trigger, True, base))
	if len(a) != 1 || len(b) != 1 || a[0].ID == b[0].ID {
		t.Fatalf("IDs collided: %q %q", a[0].ID, b[0].ID)
	}
}

func TestAdvanceAndFireUseSameQualificationTimestamp(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	o := obs(key(), Trigger, True, base)
	replay := NewReducer(time.Minute, 0)
	replay.Apply(o)
	advance := replay.Advance(base.Add(2 * time.Minute))
	claimed := NewReducer(time.Minute, 0)
	claimed.Apply(o)
	timer := claimed.Timers()[0]
	fire := claimed.Fire(timer, base.Add(2*time.Minute))
	if len(advance) != 1 || len(fire) != 1 || !advance[0].At.Equal(fire[0].At) || !advance[0].Times.DueAt.Equal(fire[0].Times.DueAt) {
		t.Fatalf("advance=%+v fire=%+v", advance, fire)
	}
}

func TestTransitionsCarryTheOwningEpisodeID(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		duration time.Duration
		trigger  func(*Reducer) []Transition
	}{
		{
			name: "immediate observation",
			trigger: func(r *Reducer) []Transition {
				return r.Apply(obs(key(), Trigger, True, base))
			},
		},
		{
			name:     "due timer",
			duration: time.Minute,
			trigger: func(r *Reducer) []Transition {
				r.Apply(obs(key(), Trigger, True, base))
				return r.Advance(base.Add(time.Minute))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReducer(tc.duration, 0)
			trigger := tc.trigger(r)
			if len(trigger) != 1 || trigger[0].EpisodeID == "" {
				t.Fatalf("trigger=%+v", trigger)
			}
			episode, ok := r.Episode(trigger[0].EpisodeID)
			if !ok || episode.Trigger != trigger[0] {
				t.Fatalf("episode=%+v ok=%v", episode, ok)
			}
			clearAt := base.Add(2 * time.Minute)
			clear := r.Apply(obs(key(), Clear, True, clearAt))
			if len(clear) != 1 || clear[0].EpisodeID != trigger[0].EpisodeID {
				t.Fatalf("clear=%+v trigger=%+v", clear, trigger)
			}
			episode, ok = r.Episode(clear[0].EpisodeID)
			if !ok || episode.Clear == nil || *episode.Clear != clear[0] {
				t.Fatalf("closed episode=%+v ok=%v", episode, ok)
			}
		})
	}
}
