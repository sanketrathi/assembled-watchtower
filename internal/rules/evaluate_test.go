package rules

import (
	"testing"
	"time"
)

func TestCompiledEvaluationPreservesUnknownAndFlatAll(t *testing.T) {
	wait := FieldOperand("queue.longest_wait")
	sla := FieldOperand("queue.sla_target")
	volume := FieldOperand("queue.volume_last_15m")
	trigger := NewAll(
		NewCompare(wait, OpGreater, sla),
		NewCompare(volume, OpGreater, IntegerOperand(5)),
	)
	clear := NewCompare(wait, OpLessOrEqual, sla)
	definition := NewRuleDefinition("queue", "", NewTargets(SubjectQueue, []string{"billing"}, nil), NewCondition(trigger, 0), NewCondition(clear, 0), NotificationPolicy{OnOpen: true, Audience: "ops"})
	plan, err := CompileForEvaluation(definition)
	if err != nil {
		t.Fatal(err)
	}

	lookup := func(values map[string]Operand) FieldLookup {
		return FieldLookupFunc(func(field FieldSpec) (Operand, bool) { value, ok := values[field.Name]; return value, ok })
	}
	if got := plan.EvaluateTrigger(lookup(map[string]Operand{
		"queue.longest_wait": DurationOperand(Duration(10 * time.Second)),
		"queue.sla_target":   DurationOperand(Duration(5 * time.Second)),
	})); got != PredicateUnknown {
		t.Fatalf("missing conjunction field=%v, want unknown", got)
	}
	if got := plan.EvaluateTrigger(lookup(map[string]Operand{
		"queue.longest_wait": DurationOperand(Duration(5 * time.Second)),
		"queue.sla_target":   DurationOperand(Duration(5 * time.Second)),
	})); got != PredicateFalse {
		t.Fatalf("known false conjunction=%v, want false", got)
	}
	if got := plan.EvaluateTrigger(lookup(map[string]Operand{
		"queue.longest_wait":    DurationOperand(Duration(10 * time.Second)),
		"queue.sla_target":      DurationOperand(Duration(5 * time.Second)),
		"queue.volume_last_15m": IntegerOperand(6),
	})); got != PredicateTrue {
		t.Fatalf("known predicate=%v, want true", got)
	}
	if got := plan.EvaluateTrigger(lookup(map[string]Operand{
		"queue.longest_wait":    IntegerOperand(10), // wrong projection value type
		"queue.sla_target":      DurationOperand(Duration(5 * time.Second)),
		"queue.volume_last_15m": IntegerOperand(6),
	})); got != PredicateUnknown {
		t.Fatalf("wrong value type=%v, want unknown", got)
	}
	if got := plan.EvaluateTrigger(lookup(map[string]Operand{
		"queue.longest_wait":    DurationOperand(Duration(-time.Second)),
		"queue.sla_target":      DurationOperand(Duration(5 * time.Second)),
		"queue.volume_last_15m": IntegerOperand(6),
	})); got != PredicateUnknown {
		t.Fatalf("invalid projection value=%v, want unknown", got)
	}
}

func TestCompiledEvaluationUsesPinnedTargetsAndDurations(t *testing.T) {
	compare := NewCompare(FieldOperand("adherence.violation"), OpEqual, BooleanOperand(true))
	definition := NewRuleDefinition("adherence", "", NewTargets(SubjectAgent, []string{"a_1"}, nil), NewCondition(compare, Duration(10*time.Minute)), NewCondition(compare, Duration(time.Minute)), NotificationPolicy{Audience: "ops"})
	plan, err := CompileForEvaluation(definition)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.HasTarget(SubjectAgent, "a_1") || plan.HasTarget(SubjectQueue, "a_1") {
		t.Fatal("unexpected pinned target membership")
	}
	if plan.TriggerDuration() != 10*time.Minute || plan.ClearDuration() != time.Minute {
		t.Fatalf("durations=(%s,%s)", plan.TriggerDuration(), plan.ClearDuration())
	}
}
