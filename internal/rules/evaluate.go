// Package rules contains the canonical, typed rule-definition contract.
package rules

import "time"

// PredicateResult preserves the three-valued result of evaluating a compiled
// predicate. Missing projection evidence is Unknown; it is never coerced to
// false or true.
type PredicateResult uint8

const (
	PredicateUnknown PredicateResult = iota
	PredicateFalse
	PredicateTrue
)

// FieldLookup supplies a value from projection-owned state. The boolean is
// false when the field is not known. Callers must return the value kind named by
// the FieldSpec; a mismatched value is treated as unknown rather than trusted.
type FieldLookup interface {
	LookupField(FieldSpec) (Operand, bool)
}

// FieldLookupFunc adapts a function to FieldLookup.
type FieldLookupFunc func(FieldSpec) (Operand, bool)

func (f FieldLookupFunc) LookupField(field FieldSpec) (Operand, bool) {
	if f == nil {
		return Operand{}, false
	}
	return f(field)
}

// EvaluationPlan is the read-only runtime view of a private compiled plan.
// It exposes only pinned targets, durations, and predicate outcomes; private
// normalization and compilation data remain inside this package.
type EvaluationPlan interface {
	Targets() []ResolvedTarget
	HasTarget(SubjectKind, string) bool
	PinnedGroups() []PinnedGroup
	TriggerDuration() time.Duration
	ClearDuration() time.Duration
	EvaluateTrigger(FieldLookup) PredicateResult
	EvaluateClear(FieldLookup) PredicateResult
}

// CompileForEvaluation validates and compiles an immutable definition for the
// evaluation runtime. Group membership is resolved once here and pinned in the
// returned private plan.
func CompileForEvaluation(definition RuleDefinition, resolvers ...GroupResolver) (EvaluationPlan, error) {
	return definition.Compile(resolvers...)
}

// TriggerDuration returns the configured trigger qualifier.
func (p *compiledPlan) TriggerDuration() time.Duration {
	if p == nil {
		return 0
	}
	return time.Duration(p.triggerFor)
}

// ClearDuration returns the configured clear qualifier.
func (p *compiledPlan) ClearDuration() time.Duration {
	if p == nil {
		return 0
	}
	return time.Duration(p.clearFor)
}

// EvaluateTrigger evaluates the compiled trigger predicate with projection
// evidence supplied by the caller.
func (p *compiledPlan) EvaluateTrigger(lookup FieldLookup) PredicateResult {
	if p == nil {
		return PredicateUnknown
	}
	return evaluateAll(p.trigger, lookup)
}

// EvaluateClear evaluates the compiled clear predicate with projection evidence
// supplied by the caller.
func (p *compiledPlan) EvaluateClear(lookup FieldLookup) PredicateResult {
	if p == nil {
		return PredicateUnknown
	}
	return evaluateAll(p.clear, lookup)
}

func evaluateAll(comparisons []compiledComparison, lookup FieldLookup) PredicateResult {
	unknown := false
	for _, comparison := range comparisons {
		result := evaluateComparison(comparison, lookup)
		if result == PredicateFalse {
			return PredicateFalse
		}
		if result == PredicateUnknown {
			unknown = true
		}
	}
	if unknown {
		return PredicateUnknown
	}
	return PredicateTrue
}

func evaluateComparison(comparison compiledComparison, lookup FieldLookup) PredicateResult {
	left, ok := resolveOperand(comparison.left, lookup)
	if !ok {
		return PredicateUnknown
	}
	right, ok := resolveOperand(comparison.right, lookup)
	if !ok || left.Kind != right.Kind {
		return PredicateUnknown
	}
	var relation int
	switch left.Kind {
	case OperandInteger:
		relation = compareInt(left.Integer, right.Integer)
	case OperandDuration:
		relation = compareDuration(left.Duration, right.Duration)
	case OperandBoolean:
		relation = compareBool(left.Boolean, right.Boolean)
	case OperandAgentState:
		relation = compareString(string(left.AgentState), string(right.AgentState))
	default:
		return PredicateUnknown
	}
	switch comparison.operator {
	case string(OpEqual):
		return truth(relation == 0)
	case string(OpNotEqual):
		return truth(relation != 0)
	case string(OpGreater):
		return truth(relation > 0)
	case string(OpGreaterOrEqual):
		return truth(relation >= 0)
	case string(OpLess):
		return truth(relation < 0)
	case string(OpLessOrEqual):
		return truth(relation <= 0)
	default:
		return PredicateUnknown
	}
}

func resolveOperand(operand Operand, lookup FieldLookup) (Operand, bool) {
	if operand.Kind != OperandField {
		return operand, operand.Kind == OperandInteger || operand.Kind == OperandDuration || operand.Kind == OperandBoolean || operand.Kind == OperandAgentState
	}
	if lookup == nil {
		return Operand{}, false
	}
	field, ok := FieldSpecFor(operand.Field)
	if !ok {
		return Operand{}, false
	}
	value, known := lookup.LookupField(field)
	if !known || !validFieldValue(value, field.Type) {
		return Operand{}, false
	}
	return value, true
}

func validFieldValue(value Operand, typ FieldType) bool {
	switch typ {
	case TypeCount:
		return value.Kind == OperandInteger && value.Integer >= 0
	case TypeDuration:
		return value.Kind == OperandDuration && !timeDurationInvalid(value.Duration)
	case TypeBoolean:
		return value.Kind == OperandBoolean
	case TypeAgentState:
		return value.Kind == OperandAgentState && validAgentStates[value.AgentState]
	default:
		return false
	}
}

func truth(value bool) PredicateResult {
	if value {
		return PredicateTrue
	}
	return PredicateFalse
}
func compareInt(left, right int64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
func compareDuration(left, right Duration) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
func compareBool(left, right bool) int {
	if left == right {
		return 0
	}
	if !left {
		return -1
	}
	return 1
}
func compareString(left, right string) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}
