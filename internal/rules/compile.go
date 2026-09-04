package rules

import (
	"fmt"
	"sort"
)

// compiledPlan is deliberately private: indexes, dependency handles, and
// normalization choices are runtime implementation details, not rule JSON.
// Its exported read-only methods expose only the pinned target selection that
// condition evaluation and durable activation need.
type compiledPlan struct {
	subject              SubjectKind
	explicitIDs          []string
	groupIDs             []string
	pinnedGroups         []pinnedGroup
	pinnedTargetIDs      []string
	targetIndex          map[string]struct{}
	trigger, clear       []compiledComparison
	triggerFor, clearFor Duration
	notifications        NotificationPolicy
}
type pinnedGroup struct {
	id       string
	revision int64
	members  []string
}
type compiledComparison struct {
	left, right Operand
	operator    string
}

// ResolvedTarget identifies one individual subject selected at activation.
// It never represents a group-level target or an event observation.
type ResolvedTarget struct {
	Kind SubjectKind
	ID   string
}

// PinnedGroup records the immutable configured-group revision used to compile
// a plan. Members are a canonical, de-duplicated copy of that revision.
type PinnedGroup struct {
	ID       string
	Revision int64
	Members  []string
}

// Compile validates a definition and creates an execution plan. Definitions
// containing groups require a resolver so the plan cannot accidentally follow
// later group edits. Group snapshots and target IDs are normalized into a
// deterministic order because selectors and configured membership are sets.
func (r RuleDefinition) Compile(resolvers ...GroupResolver) (*compiledPlan, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	var resolver GroupResolver
	if len(resolvers) > 1 {
		return nil, fmt.Errorf("at most one group resolver is allowed")
	}
	if len(resolvers) == 1 {
		resolver = resolvers[0]
	}
	if len(r.Targets.GroupIDs) > 0 && resolver == nil {
		return nil, fmt.Errorf("group resolver is required to compile configured groups")
	}
	p := &compiledPlan{
		subject: r.Targets.Kind, explicitIDs: canonicalIDs(r.Targets.IDs),
		groupIDs: canonicalIDs(r.Targets.GroupIDs), triggerFor: r.Trigger.For,
		clearFor: r.Clear.For, notifications: r.Notifications,
	}
	p.pinnedTargetIDs = append([]string(nil), p.explicitIDs...)
	for _, id := range p.groupIDs {
		snapshot, err := resolver.ResolveGroup(r.Targets.Kind, id)
		if err != nil {
			return nil, fmt.Errorf("resolve group %q: %w", id, err)
		}
		if snapshot.Kind != r.Targets.Kind {
			return nil, fmt.Errorf("resolve group %q: wrong subject kind", id)
		}
		if snapshot.Revision <= 0 {
			return nil, fmt.Errorf("resolve group %q: invalid revision", id)
		}
		members := canonicalIDs(snapshot.Members)
		p.pinnedGroups = append(p.pinnedGroups, pinnedGroup{id: id, revision: snapshot.Revision, members: members})
		p.pinnedTargetIDs = append(p.pinnedTargetIDs, members...)
	}
	p.pinnedTargetIDs = canonicalIDs(p.pinnedTargetIDs)
	p.targetIndex = make(map[string]struct{}, len(p.pinnedTargetIDs))
	for _, id := range p.pinnedTargetIDs {
		p.targetIndex[id] = struct{}{}
	}
	p.trigger = flatten(r.Trigger.Predicate)
	p.clear = flatten(r.Clear.Predicate)
	return p, nil
}
func Compile(r RuleDefinition, resolvers ...GroupResolver) (*compiledPlan, error) {
	return r.Compile(resolvers...)
}

// Targets returns a sorted copy of the individual subjects selected by this
// plan. A configured group is expanded here and never becomes a group alert.
func (p *compiledPlan) Targets() []ResolvedTarget {
	if p == nil {
		return nil
	}
	out := make([]ResolvedTarget, len(p.pinnedTargetIDs))
	for i, id := range p.pinnedTargetIDs {
		out[i] = ResolvedTarget{Kind: p.subject, ID: id}
	}
	return out
}

// HasTarget reports whether the individual subject belongs to this pinned
// plan. It does not consult event queue_ids or any mutable group source.
func (p *compiledPlan) HasTarget(kind SubjectKind, id string) bool {
	if p == nil || p.subject != kind {
		return false
	}
	_, ok := p.targetIndex[id]
	return ok
}

// PinnedGroups returns copies of the immutable group revisions selected by
// this plan, ordered by group ID. Later group edits cannot change this result.
func (p *compiledPlan) PinnedGroups() []PinnedGroup {
	if p == nil || len(p.pinnedGroups) == 0 {
		return nil
	}
	out := make([]PinnedGroup, len(p.pinnedGroups))
	for i, group := range p.pinnedGroups {
		out[i] = PinnedGroup{ID: group.id, Revision: group.revision, Members: append([]string(nil), group.members...)}
	}
	return out
}

func canonicalIDs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	end := 0
	for _, value := range out {
		if end > 0 && out[end-1] == value {
			continue
		}
		out[end] = value
		end++
	}
	return out[:end]
}

func flatten(x Expression) []compiledComparison {
	switch e := x.(type) {
	case *CompareExpression:
		if e == nil {
			return nil
		}
		return []compiledComparison{{e.Left, e.Right, string(e.Operator)}}
	case CompareExpression:
		return []compiledComparison{{e.Left, e.Right, string(e.Operator)}}
	case *AllExpression:
		if e == nil {
			return nil
		}
		out := make([]compiledComparison, 0, len(e.Conditions))
		for _, c := range e.Conditions {
			out = append(out, flatten(c)...)
		}
		return out
	case AllExpression:
		return flatten(&e)
	default:
		return nil
	}
}
