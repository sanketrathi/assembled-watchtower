package rules

import "fmt"

// GroupSnapshot is an immutable configured-group revision returned at
// activation time. Members are subject IDs, not event observations; a compiled
// plan copies and pins them without later lookup.
type GroupSnapshot struct {
	Kind     SubjectKind
	Revision int64
	Members  []string
}

// GroupResolver supplies configured, same-kind group membership at activation
// time. It must resolve configured group revisions rather than deriving
// membership from event queue_ids. Persistence and HTTP concerns remain outside
// this package.
type GroupResolver interface {
	ResolveGroup(kind SubjectKind, groupID string) (GroupSnapshot, error)
}

// ValidateWithGroups performs ordinary validation and checks every configured
// group without changing the immutable definition. Activation should use
// Compile with the same resolver to retain the returned snapshots.
func (r RuleDefinition) ValidateWithGroups(resolver GroupResolver) error {
	base := r.validationErrors()
	if resolver != nil {
		for i, id := range r.Targets.GroupIDs {
			snapshot, err := resolver.ResolveGroup(r.Targets.Kind, id)
			if err != nil {
				add(&base, "group_resolution_failed", ptr("/targets/group_ids", fmt.Sprint(i)), err.Error())
				continue
			}
			if snapshot.Kind != r.Targets.Kind || snapshot.Revision <= 0 {
				add(&base, "group_resolution_failed", ptr("/targets/group_ids", fmt.Sprint(i)), "group snapshot has wrong kind or revision")
			}
		}
	}
	if len(base) == 0 {
		return nil
	}
	return sortedValidationErrors(base)
}
