package rules

import (
	"fmt"
	"sort"
	"strings"
)

type ValidationError struct {
	Code    string `json:"code"`
	Path    string `json:"path"`
	Message string `json:"message"`
}
type ValidationErrors []ValidationError

func (e ValidationErrors) Error() string {
	if len(e) == 0 {
		return ""
	}
	x := append(ValidationErrors(nil), e...)
	sort.SliceStable(x, func(i, j int) bool {
		if x[i].Path != x[j].Path {
			return x[i].Path < x[j].Path
		}
		return x[i].Code < x[j].Code
	})
	parts := make([]string, len(x))
	for i, v := range x {
		parts[i] = fmt.Sprintf("%s (%s): %s", v.Path, v.Code, v.Message)
	}
	return strings.Join(parts, "; ")
}
func add(es *ValidationErrors, code, path, msg string) {
	*es = append(*es, ValidationError{code, path, msg})
}
func ptr(base, part string) string {
	part = strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1")
	return base + "/" + part
}
func (r RuleDefinition) Validate() error {
	es := r.validationErrors()
	if len(es) == 0 {
		return nil
	}
	return sortedValidationErrors(es)
}
func sortValidationErrors(es ValidationErrors) {
	sort.SliceStable(es, func(i, j int) bool {
		if es[i].Path != es[j].Path {
			return es[i].Path < es[j].Path
		}
		return es[i].Code < es[j].Code
	})
}
func sortedValidationErrors(es ValidationErrors) ValidationErrors {
	sortValidationErrors(es)
	return es
}
func (r RuleDefinition) validationErrors() ValidationErrors {
	var es ValidationErrors
	if r.SchemaVersion != CurrentSchemaVersion {
		add(&es, "invalid_schema_version", "/schema_version", "schema_version must be 1")
	}
	if r.Name == "" {
		add(&es, "required", "/name", "name is required")
	} else if len(r.Name) > 200 {
		add(&es, "size_limit", "/name", "name must be at most 200 characters")
	}
	if len(r.Description) > 2000 {
		add(&es, "size_limit", "/description", "description must be at most 2000 characters")
	}
	if r.Targets.Kind != SubjectQueue && r.Targets.Kind != SubjectAgent {
		add(&es, "invalid_subject_kind", "/targets/kind", "kind must be queue or agent")
	}
	if len(r.Targets.IDs) == 0 && len(r.Targets.GroupIDs) == 0 {
		add(&es, "empty_targets", "/targets", "at least one explicit ID or group ID is required")
	}
	seen := map[string]string{}
	for i, id := range r.Targets.IDs {
		p := ptr("/targets/ids", fmt.Sprint(i))
		if id == "" {
			add(&es, "invalid_selector", p, "selector ID cannot be empty")
		}
		if j, ok := seen[id]; ok {
			add(&es, "duplicate_selector", p, "selector duplicates "+j)
		} else {
			seen[id] = p
		}
	}
	seen = map[string]string{}
	for i, id := range r.Targets.GroupIDs {
		p := ptr("/targets/group_ids", fmt.Sprint(i))
		if id == "" {
			add(&es, "invalid_selector", p, "group ID cannot be empty")
		}
		if j, ok := seen[id]; ok {
			add(&es, "duplicate_selector", p, "group ID duplicates "+j)
		} else {
			seen[id] = p
		}
	}
	if r.Trigger.Predicate == nil {
		add(&es, "required", "/trigger/predicate", "trigger predicate is required")
	} else {
		es = append(es, validateExpression(r.Trigger.Predicate, "/trigger/predicate", r.Targets.Kind)...)
	}
	if timeDurationInvalid(r.Trigger.For) {
		add(&es, "invalid_duration", "/trigger/for", "duration must be non-negative whole seconds")
	}
	if r.Clear.Predicate == nil {
		add(&es, "required", "/clear/predicate", "clear predicate is required")
	} else {
		es = append(es, validateExpression(r.Clear.Predicate, "/clear/predicate", r.Targets.Kind)...)
	}
	if timeDurationInvalid(r.Clear.For) {
		add(&es, "invalid_duration", "/clear/for", "duration must be non-negative whole seconds")
	}
	if r.Notifications.Audience == "" {
		add(&es, "required", "/notifications/audience", "audience is required")
	} else if len(r.Notifications.Audience) > 200 {
		add(&es, "size_limit", "/notifications/audience", "audience must be at most 200 characters")
	}
	return es
}
func timeDurationInvalid(d Duration) bool {
	return d < 0 || int64(d)%int64(1e9) != 0 || d > Duration(365*24*60*60*1e9)
}
func validateExpression(x Expression, path string, subject SubjectKind) ValidationErrors {
	var es ValidationErrors
	count := 0
	var walk func(Expression, string, bool)
	walk = func(x Expression, p string, nested bool) {
		count++
		if count > MaxExpressionNodes {
			add(&es, "size_limit", p, "expression exceeds maximum node count")
			return
		}
		switch e := x.(type) {
		case *CompareExpression:
			if e == nil {
				add(&es, "unsupported_expression", p, "expression cannot be nil")
				return
			}
			validateCompare(&es, *e, p, subject)
		case CompareExpression:
			validateCompare(&es, e, p, subject)
		case *AllExpression:
			if e == nil {
				add(&es, "unsupported_expression", p, "expression cannot be nil")
				return
			}
			if nested {
				add(&es, "unsupported_expression", p, "nested all expressions are not supported")
			}
			if len(e.Conditions) == 0 {
				add(&es, "unsupported_expression", ptr(p, "conditions"), "all requires at least one condition")
			}
			for i, c := range e.Conditions {
				switch c.(type) {
				case *CompareExpression, CompareExpression:
					walk(c, ptr(ptr(p, "conditions"), fmt.Sprint(i)), true)
				default:
					add(&es, "unsupported_expression", ptr(ptr(p, "conditions"), fmt.Sprint(i)), "all may contain compare expressions only")
				}
			}
		case AllExpression:
			walk(&e, p, nested)
		default:
			add(&es, "unsupported_expression", p, "unsupported expression")
		}
	}
	walk(x, path, false)
	return es
}
func validateCompare(es *ValidationErrors, e CompareExpression, p string, subject SubjectKind) {
	left := validateOperand(es, e.Left, ptr(p, "left"), subject)
	right := validateOperand(es, e.Right, ptr(p, "right"), subject)
	if left == "" || right == "" {
		return
	}
	if left != right {
		add(es, "operand_type_mismatch", p, "comparison operands must have compatible types")
		return
	}
	if !allowedOperatorType(left, string(e.Operator)) {
		add(es, "invalid_operator", ptr(p, "op"), "operator is not allowed for this type")
	}
}
func validateOperand(es *ValidationErrors, o Operand, p string, subject SubjectKind) FieldType {
	switch o.Kind {
	case OperandField:
		f, ok := field(o.Field)
		if !ok {
			add(es, "unknown_field_name", ptr(p, "name"), "unknown field")
			return ""
		}
		if f.Subject != subject {
			add(es, "subject_mismatch", ptr(p, "name"), "field does not apply to target subject")
			return ""
		}
		return f.Type
	case OperandInteger:
		if o.Integer < 0 {
			add(es, "invalid_value", ptr(p, "value"), "count/integer cannot be negative")
		}
		return TypeCount
	case OperandDuration:
		if timeDurationInvalid(o.Duration) {
			add(es, "invalid_duration", ptr(p, "value"), "invalid duration")
		}
		return TypeDuration
	case OperandBoolean:
		return TypeBoolean
	case OperandAgentState:
		if !validAgentStates[o.AgentState] {
			add(es, "invalid_value", ptr(p, "value"), "unknown agent state")
		}
		return TypeAgentState
	default:
		add(es, "unsupported_operand", p, "unsupported operand kind")
		return ""
	}
}

func allowedOperatorType(t FieldType, op string) bool {
	switch t {
	case TypeDuration, TypeCount:
		return contains([]string{"eq", "ne", "gt", "gte", "lt", "lte"}, op)
	case TypeBoolean, TypeAgentState:
		return op == "eq" || op == "ne"
	default:
		return false
	}
}
