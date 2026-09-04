package conditions

import (
	"testing"
	"time"
)

// This model-oriented fuzz test checks the central safety invariant: committed
// transitions alternate and stale/repeated inputs cannot create extra edges.
func FuzzTransitionsAlternate(f *testing.F) {
	f.Add([]byte{1, 1, 0, 1, 0, 1})
	f.Add([]byte{0, 2, 2, 1, 1, 0, 2})
	f.Fuzz(func(t *testing.T, data []byte) {
		base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
		r := NewReducer(time.Second, 0)
		k := ConditionKey{RuleID: "r", Revision: 1, Subject: SubjectKey{Kind: SubjectAgent, ID: "a"}}
		for i, b := range data {
			at := base.Add(time.Duration(i) * 2 * time.Second)
			direction := Trigger
			if b&2 != 0 {
				direction = Clear
			}
			result := Unknown
			if b&1 != 0 {
				result = True
			}
			if b&4 != 0 {
				result = False
			}
			r.Apply(Observation{Key: k, Direction: direction, Result: result, EffectiveAt: at, ProcessingAt: at, ObservationID: string(rune(i))})
		}
		r.Advance(base.Add(time.Duration(len(data)+1) * 2 * time.Second))
		last := Trigger
		for i, transition := range r.Transitions() {
			if i > 0 && transition.Direction == last {
				t.Fatalf("same-direction transitions: %+v", r.Transitions())
			}
			last = transition.Direction
		}
	})
}
