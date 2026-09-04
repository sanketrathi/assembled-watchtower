package rules

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func loadGolden(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestGoldenValidDefinitions(t *testing.T) {
	for _, name := range []string{"billing-sla", "adherence-10m", "long-call-45m", "explicit-and-group", "flat-all"} {
		t.Run(name, func(t *testing.T) {
			r, err := DecodeRuleDefinition(loadGolden(t, name))
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Validate(); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			r2, err := DecodeRuleDefinition(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if err := r2.Validate(); err != nil {
				t.Fatal(err)
			}
			encoded2, _ := json.Marshal(r2)
			if !bytes.Equal(encoded, encoded2) {
				t.Fatalf("not stable:\n%s\n%s", encoded, encoded2)
			}
			if name == "explicit-and-group" {
				if _, err := r.Compile(testGroupResolver{}); err != nil {
					t.Fatal(err)
				}
			} else if _, err := r.Compile(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestInvalidGoldens(t *testing.T) {
	for _, name := range []string{"field-subject-mismatch", "operand-type-mismatch", "invalid-operator", "null-required", "invalid-duration", "empty-targets", "unsupported-shape"} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRuleDefinition(loadGolden(t, name)); err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}
func TestBooleanFalseRoundTrip(t *testing.T) {
	r, err := DecodeRuleDefinition(loadGolden(t, "adherence-10m"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(r)
	if !strings.Contains(string(b), `"value":false`) {
		t.Fatal("false was not retained")
	}
	var r2 RuleDefinition
	if err = json.Unmarshal(b, &r2); err != nil {
		t.Fatal(err)
	}
	if c := r2.Clear.Predicate.(*CompareExpression); c.Right.Kind != OperandBoolean || c.Right.Boolean {
		t.Fatal("false changed")
	}
}
func TestStrictTrailingAndUnknown(t *testing.T) {
	b := loadGolden(t, "billing-sla")
	for _, in := range [][]byte{append(append([]byte{}, b...), []byte(" {}")...), []byte(strings.Replace(string(b), `"name": "billing-sla"`, `"unknown": 1, "name": "billing-sla"`, 1))} {
		if _, err := DecodeRuleDefinition(in); err == nil {
			t.Fatal("expected strict decode error")
		}
	}
}
func TestDurationBoundaries(t *testing.T) {
	for _, tc := range []struct {
		s  string
		ok bool
	}{{"0s", true}, {"5m", true}, {"45m", true}, {"1h", true}, {"01s", false}, {"1.5s", false}, {"-1s", false}, {"5d", false}, {"", false}} {
		d, err := ParseDuration(tc.s)
		if (err == nil) != tc.ok {
			t.Errorf("%q err=%v", tc.s, err)
		}
		if tc.ok {
			b, _ := json.Marshal(d)
			var canonical string
			if err := json.Unmarshal(b, &canonical); err != nil {
				t.Fatal(err)
			}
			if _, err := ParseDuration(canonical); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func FuzzDecodeExpression(f *testing.F) {
	f.Add([]byte(`{"kind":"compare","left":{"kind":"field","name":"queue.longest_wait"},"op":"gt","right":{"kind":"duration","value":"5m"}}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = decodeExpression(b) })
}

func TestValidationErrorOrder(t *testing.T) {
	r, err := DecodeRuleDefinition(loadGolden(t, "billing-sla"))
	if err != nil {
		t.Fatal(err)
	}
	r.Targets.Kind = SubjectAgent
	r.Targets.IDs = []string{"", "x", "x"}
	err = r.Validate()
	ve, ok := err.(ValidationErrors)
	if !ok {
		t.Fatalf("type %T", err)
	}
	for i := 1; i < len(ve); i++ {
		if ve[i-1].Path > ve[i].Path {
			t.Fatalf("not sorted: %#v", ve)
		}
	}
	if ve[0].Path != "/clear/predicate/left/name" {
		t.Fatalf("first path %s", ve[0].Path)
	}
}
func TestRequiredMembersAreStrict(t *testing.T) {
	b := string(loadGolden(t, "billing-sla"))
	for _, fragment := range []string{`"name": "billing-sla"`, `"notifications": {`} {
		_ = fragment
	}
	if _, err := DecodeRuleDefinition([]byte(strings.Replace(b, `"schema_version": 1,`, ``, 1))); err == nil {
		t.Fatal("missing schema version accepted")
	}
	if _, err := DecodeRuleDefinition([]byte(strings.Replace(b, `"targets": {`, `"targets": null, "unused": {`, 1))); err == nil {
		t.Fatal("null targets accepted")
	}
}

type testGroupResolver struct{ err error }

func (r testGroupResolver) ResolveGroup(kind SubjectKind, id string) (GroupSnapshot, error) {
	if r.err != nil {
		return GroupSnapshot{}, r.err
	}
	return GroupSnapshot{Kind: kind, Revision: 1, Members: []string{id + "-member"}}, nil
}
func TestGroupResolutionIsSeparateFromDefinition(t *testing.T) {
	r, err := DecodeRuleDefinition(loadGolden(t, "explicit-and-group"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := r.ValidateWithGroups(testGroupResolver{err: os.ErrNotExist}); err == nil {
		t.Fatal("expected unresolved group")
	}
}

func TestDecoderRejectsQuotedIntegersAndForbiddenNullMembers(t *testing.T) {
	base := string(loadGolden(t, "billing-sla"))
	quoted := strings.Replace(base, `"value": 10`, `"value": "10"`, 1)
	// Use the group example, which has an integer literal.
	quoted = strings.Replace(string(loadGolden(t, "explicit-and-group")), `"value": 10`, `"value": "10"`, 1)
	if _, err := DecodeRuleDefinition([]byte(quoted)); err == nil {
		t.Fatal("quoted integer accepted")
	}
	compareNull := strings.Replace(base, `"right": {`, `"conditions": null, "right": {`, 1)
	if _, err := DecodeRuleDefinition([]byte(compareNull)); err == nil {
		t.Fatal("null forbidden compare member accepted")
	}
	allNull := strings.Replace(string(loadGolden(t, "flat-all")), `"kind": "all",`, `"kind": "all", "op": null,`, 1)
	if _, err := DecodeRuleDefinition([]byte(allNull)); err == nil {
		t.Fatal("null forbidden all member accepted")
	}
}
func TestDecoderRejectsUnsupportedAllShapesAndBounds(t *testing.T) {
	base := string(loadGolden(t, "flat-all"))
	nested := strings.Replace(base, `"kind": "compare",`, `"kind": "all", "conditions": [],`, 1)
	if _, err := DecodeRuleDefinition([]byte(nested)); err == nil {
		t.Fatal("nested all accepted")
	}
	conditions := make([]string, MaxExpressionNodes)
	for i := range conditions {
		conditions[i] = `{"kind":"compare","left":{"kind":"field","name":"queue.tickets_waiting"},"op":"gt","right":{"kind":"integer","value":1}}`
	}
	oversized := strings.Replace(base, `"conditions": [`, `"conditions": [`+strings.Join(conditions, ","), 1)
	if _, err := DecodeRuleDefinition([]byte(oversized)); err == nil {
		t.Fatal("oversized all accepted")
	}
}

func TestTypedNilExpressionIsSafe(t *testing.T) {
	var compare *CompareExpression
	r := NewRuleDefinition("nil", "", NewTargets(SubjectQueue, []string{"billing"}, nil), NewCondition(compare, 0), NewCondition(compare, 0), NotificationPolicy{OnOpen: true, OnRecovery: true, Audience: "ops"})
	if err := r.Validate(); err == nil {
		t.Fatal("typed nil validated")
	}
	if _, err := r.Compile(); err == nil {
		t.Fatal("typed nil compiled")
	}
}
func TestGroupCompilationPinsMembership(t *testing.T) {
	r, err := DecodeRuleDefinition(loadGolden(t, "explicit-and-group"))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := r.Compile(testGroupResolver{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.pinnedGroups) != 1 || plan.pinnedGroups[0].revision != 1 {
		t.Fatalf("groups not pinned: %#v", plan.pinnedGroups)
	}
	if len(plan.pinnedTargetIDs) != 2 || plan.pinnedTargetIDs[0] != "billing" || plan.pinnedTargetIDs[1] != "priority-queues-member" {
		t.Fatalf("targets not pinned: %#v", plan.pinnedTargetIDs)
	}
	if _, err := r.Compile(); err == nil {
		t.Fatal("group compiled without resolver")
	}
}
func TestFieldCatalogIncludesProjection(t *testing.T) {
	f, ok := FieldSpecFor("queue.longest_wait")
	if !ok || f.SourceProjection != "longest_wait_sec" || f.Dependency != "queue_snapshot" {
		t.Fatalf("catalog metadata: %#v", f)
	}
}

type recordingGroupResolver struct {
	snapshots map[string]GroupSnapshot
	calls     []string
}

func (r *recordingGroupResolver) ResolveGroup(kind SubjectKind, id string) (GroupSnapshot, error) {
	r.calls = append(r.calls, id)
	snapshot, ok := r.snapshots[id]
	if !ok {
		return GroupSnapshot{}, os.ErrNotExist
	}
	return snapshot, nil
}

func TestCompileResolvesCanonicalPinnedTargets(t *testing.T) {
	rule, err := DecodeRuleDefinition(loadGolden(t, "billing-sla"))
	if err != nil {
		t.Fatal(err)
	}
	rule.Targets = NewTargets(SubjectQueue, []string{"vip", "billing"}, []string{"slow", "fast"})
	resolver := &recordingGroupResolver{snapshots: map[string]GroupSnapshot{
		"fast": {Kind: SubjectQueue, Revision: 2, Members: []string{"priority", "billing", "priority"}},
		"slow": {Kind: SubjectQueue, Revision: 1, Members: []string{"vip", "tier_2"}},
	}}

	plan, err := rule.Compile(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := resolver.calls, []string{"fast", "slow"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resolver calls=%v, want %v", got, want)
	}
	wantTargets := []ResolvedTarget{
		{Kind: SubjectQueue, ID: "billing"},
		{Kind: SubjectQueue, ID: "priority"},
		{Kind: SubjectQueue, ID: "tier_2"},
		{Kind: SubjectQueue, ID: "vip"},
	}
	if got := plan.Targets(); !reflect.DeepEqual(got, wantTargets) {
		t.Fatalf("targets=%#v, want %#v", got, wantTargets)
	}
	if !plan.HasTarget(SubjectQueue, "priority") || plan.HasTarget(SubjectAgent, "priority") || plan.HasTarget(SubjectQueue, "missing") {
		t.Fatalf("unexpected target membership: %#v", plan.Targets())
	}
	wantGroups := []PinnedGroup{
		{ID: "fast", Revision: 2, Members: []string{"billing", "priority"}},
		{ID: "slow", Revision: 1, Members: []string{"tier_2", "vip"}},
	}
	if got := plan.PinnedGroups(); !reflect.DeepEqual(got, wantGroups) {
		t.Fatalf("pinned groups=%#v, want %#v", got, wantGroups)
	}

	// Mutating a resolver-owned current snapshot or a returned copy cannot
	// rewrite the compiled target set. A fresh compile intentionally adopts it.
	resolver.snapshots["fast"] = GroupSnapshot{Kind: SubjectQueue, Revision: 3, Members: []string{"escalations"}}
	groups := plan.PinnedGroups()
	groups[0].Members[0] = "mutated-copy"
	if got := plan.PinnedGroups(); !reflect.DeepEqual(got, wantGroups) {
		t.Fatalf("compiled groups changed after mutation: %#v", got)
	}
	if got := plan.Targets(); !reflect.DeepEqual(got, wantTargets) {
		t.Fatalf("compiled targets changed after mutation: %#v", got)
	}

	recompiled, err := rule.Compile(resolver)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := recompiled.PinnedGroups()[0], (PinnedGroup{ID: "fast", Revision: 3, Members: []string{"escalations"}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("recompiled group=%#v, want %#v", got, want)
	}
	if recompiled.HasTarget(SubjectQueue, "priority") || !recompiled.HasTarget(SubjectQueue, "escalations") {
		t.Fatalf("recompiled targets=%#v", recompiled.Targets())
	}
}

func TestCompiledPlanWithoutGroupsCanonicalizesExplicitTargets(t *testing.T) {
	rule, err := DecodeRuleDefinition(loadGolden(t, "billing-sla"))
	if err != nil {
		t.Fatal(err)
	}
	rule.Targets = NewTargets(SubjectQueue, []string{"vip", "billing"}, nil)
	plan, err := rule.Compile()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := plan.Targets(), []ResolvedTarget{{Kind: SubjectQueue, ID: "billing"}, {Kind: SubjectQueue, ID: "vip"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("targets=%#v, want %#v", got, want)
	}
	if got := plan.PinnedGroups(); got != nil {
		t.Fatalf("pinned groups=%#v, want nil", got)
	}
}
