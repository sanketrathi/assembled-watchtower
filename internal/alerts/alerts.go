// Package alerts reduces condition episodes into stable per-rule, per-subject
// alert series and generations.
package alerts

import (
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"watchtower/internal/conditions"
)

type SubjectKind string

const (
	SubjectQueue SubjectKind = "queue"
	SubjectAgent SubjectKind = "agent"
)

type SeriesKey struct {
	RuleID      string
	SubjectKind SubjectKind
	SubjectID   string
}

type Contributor struct {
	EpisodeID  string
	Revision   int64
	OpenedAt   time.Time
	EvidenceAt time.Time
}

type Episode struct {
	ID          string
	Series      SeriesKey
	Revision    int64
	Opened      bool
	At          time.Time
	EffectiveAt time.Time
	EvidenceAt  time.Time
}

type TransitionKind uint8

const (
	Open TransitionKind = iota
	Recovery
)

func (k TransitionKind) String() string {
	if k == Recovery {
		return "recovery"
	}
	return "open"
}

type Transition struct {
	ID          string
	Series      SeriesKey
	SeriesID    string
	Generation  uint64
	Kind        TransitionKind
	At          time.Time
	EffectiveAt time.Time
	EvidenceAt  time.Time
	EpisodeID   string
	Revision    int64
}

type Series struct {
	Key          SeriesKey
	ID           string
	Generation   uint64
	Open         bool
	Contributors []Contributor
}

type Reducer struct {
	series      map[SeriesKey]*seriesState
	closed      map[string]struct{}
	transitions []Transition
}
type seriesState struct {
	series       Series
	contributors map[string]Contributor
}

func New() *Reducer {
	return &Reducer{series: make(map[SeriesKey]*seriesState), closed: make(map[string]struct{})}
}

// ApplyEpisode accepts a complete condition episode. A closed episode is
// applied as its opening contribution followed by its clearing contribution.
func (r *Reducer) ApplyEpisode(e conditions.Episode) []Transition {
	key := SeriesKey{RuleID: e.Key.RuleID, SubjectKind: SubjectKind(e.Key.Subject.Kind), SubjectID: e.Key.Subject.ID}
	out := r.ApplyStart(Episode{ID: e.ID, Series: key, Revision: e.Key.Revision, Opened: true, At: e.Trigger.At, EffectiveAt: e.Trigger.Times.EffectiveAt, EvidenceAt: e.Trigger.Times.EvidenceAt})
	if e.Clear != nil {
		out = append(out, r.ApplyClear(e.ID, key, e.Clear.At, e.Clear.Times.EffectiveAt, e.Clear.Times.EvidenceAt)...)
	}
	return out
}

func (r *Reducer) ApplyStart(e Episode) []Transition {
	if e.ID == "" {
		return nil
	}
	if _, ok := r.closed[e.ID]; ok {
		return nil
	}
	s := r.get(e.Series)
	if _, ok := s.contributors[e.ID]; ok {
		return nil
	}
	s.contributors[e.ID] = Contributor{EpisodeID: e.ID, Revision: e.Revision, OpenedAt: e.At, EvidenceAt: e.EvidenceAt}
	if s.series.Open {
		return nil
	}
	s.series.Open = true
	s.series.Generation++
	effectiveAt := e.EffectiveAt
	if effectiveAt.IsZero() {
		effectiveAt = e.At
	}
	if effectiveAt.IsZero() {
		effectiveAt = e.EvidenceAt
	}
	t := Transition{ID: stableID("alert-transition", fmt.Sprintf("%s|%d|%d", s.series.ID, s.series.Generation, Open)), Series: s.series.Key, SeriesID: s.series.ID, Generation: s.series.Generation, Kind: Open, At: e.At, EffectiveAt: effectiveAt, EvidenceAt: e.EvidenceAt, EpisodeID: e.ID, Revision: e.Revision}
	r.transitions = append(r.transitions, t)
	return []Transition{t}
}

func (r *Reducer) ApplyClear(episodeID string, key SeriesKey, at, effectiveAt, evidenceAt time.Time) []Transition {
	if episodeID == "" {
		return nil
	}
	if _, ok := r.closed[episodeID]; ok {
		return nil
	}
	s := r.get(key)
	c, ok := s.contributors[episodeID]
	if !ok {
		return nil
	}
	delete(s.contributors, episodeID)
	r.closed[episodeID] = struct{}{}
	if len(s.contributors) > 0 || !s.series.Open {
		return nil
	}
	s.series.Open = false
	if effectiveAt.IsZero() {
		effectiveAt = at
	}
	if evidenceAt.IsZero() {
		evidenceAt = c.EvidenceAt
	}
	t := Transition{ID: stableID("alert-transition", fmt.Sprintf("%s|%d|%d", s.series.ID, s.series.Generation, Recovery)), Series: s.series.Key, SeriesID: s.series.ID, Generation: s.series.Generation, Kind: Recovery, At: at, EffectiveAt: effectiveAt, EvidenceAt: evidenceAt, EpisodeID: episodeID, Revision: c.Revision}
	if t.EffectiveAt.IsZero() {
		t.EffectiveAt = at
	}
	r.transitions = append(r.transitions, t)
	return []Transition{t}
}

func (r *Reducer) get(k SeriesKey) *seriesState {
	if s := r.series[k]; s != nil {
		return s
	}
	s := &seriesState{series: Series{Key: k, ID: SeriesID(k)}, contributors: make(map[string]Contributor)}
	r.series[k] = s
	return s
}
func (r *Reducer) Series(k SeriesKey) (Series, bool) {
	s, ok := r.series[k]
	if !ok {
		return Series{}, false
	}
	return copySeries(s), true
}
func (r *Reducer) SeriesList() []Series {
	out := make([]Series, 0, len(r.series))
	for _, s := range r.series {
		out = append(out, copySeries(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func copySeries(s *seriesState) Series {
	x := s.series
	x.Contributors = make([]Contributor, 0, len(s.contributors))
	for _, c := range s.contributors {
		x.Contributors = append(x.Contributors, c)
	}
	sort.Slice(x.Contributors, func(i, j int) bool { return x.Contributors[i].EpisodeID < x.Contributors[j].EpisodeID })
	return x
}
func (r *Reducer) Transitions() []Transition { return append([]Transition(nil), r.transitions...) }
func SeriesID(k SeriesKey) string {
	return stableID("alert-series", canonicalParts(k.RuleID, string(k.SubjectKind), k.SubjectID))
}
func canonicalParts(parts ...string) string {
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(strconv.Itoa(len(part)))
		b.WriteByte(':')
		b.WriteString(part)
	}
	return b.String()
}

func stableID(prefix, v string) string {
	// Hex encoding is injective; canonicalParts makes each component
	// boundary unambiguous before it is encoded.
	return prefix + "_" + hex.EncodeToString([]byte(v))
}
