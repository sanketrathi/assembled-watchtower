package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
)

func isNull(data []byte) bool { return bytes.Equal(bytes.TrimSpace(data), []byte("null")) }

func (o Operand) MarshalJSON() ([]byte, error) {
	switch o.Kind {
	case OperandField:
		return json.Marshal(struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		}{string(o.Kind), o.Field})
	case OperandInteger:
		return json.Marshal(struct {
			Kind  string `json:"kind"`
			Value int64  `json:"value"`
		}{string(o.Kind), o.Integer})
	case OperandDuration:
		return json.Marshal(struct {
			Kind  string   `json:"kind"`
			Value Duration `json:"value"`
		}{string(o.Kind), o.Duration})
	case OperandBoolean:
		return json.Marshal(struct {
			Kind  string `json:"kind"`
			Value bool   `json:"value"`
		}{string(o.Kind), o.Boolean})
	case OperandAgentState:
		return json.Marshal(struct {
			Kind  string `json:"kind"`
			Value string `json:"value"`
		}{string(o.Kind), string(o.AgentState)})
	default:
		return nil, fmt.Errorf("unknown operand kind %q", o.Kind)
	}
}
func (o *Operand) UnmarshalJSON(data []byte) error {
	if isNull(data) {
		return fmt.Errorf("operand is required")
	}
	var w struct {
		Kind  *string         `json:"kind"`
		Name  json.RawMessage `json:"name"`
		Value json.RawMessage `json:"value"`
	}
	if err := strictObject(data, &w); err != nil {
		return err
	}
	if w.Kind == nil {
		return fmt.Errorf("operand kind is required")
	}
	kind := OperandKind(*w.Kind)
	switch kind {
	case OperandField:
		if w.Name == nil || isNull(w.Name) {
			return fmt.Errorf("field operand requires name")
		}
		if w.Value != nil {
			return fmt.Errorf("field operand does not accept value")
		}
		var name string
		if err := json.Unmarshal(w.Name, &name); err != nil {
			return fmt.Errorf("field name must be a string")
		}
		*o = FieldOperand(name)
	case OperandInteger:
		if w.Name != nil {
			return fmt.Errorf("integer operand does not accept name")
		}
		if w.Value == nil || isNull(w.Value) {
			return fmt.Errorf("integer operand requires value")
		}
		var n json.Number
		value := bytes.TrimSpace(w.Value)
		if len(value) == 0 || (value[0] != '-' && (value[0] < '0' || value[0] > '9')) {
			return fmt.Errorf("integer operand value must be an integer")
		}
		d := json.NewDecoder(bytes.NewReader(value))
		d.UseNumber()
		if err := d.Decode(&n); err != nil {
			return fmt.Errorf("integer operand value must be an integer")
		}
		v, err := strconv.ParseInt(string(n), 10, 64)
		if err != nil {
			return fmt.Errorf("integer operand value must be an integer")
		}
		*o = IntegerOperand(v)
	case OperandDuration:
		if w.Name != nil {
			return fmt.Errorf("duration operand does not accept name")
		}
		if w.Value == nil || isNull(w.Value) {
			return fmt.Errorf("duration operand requires value")
		}
		var d Duration
		if err := json.Unmarshal(w.Value, &d); err != nil {
			return fmt.Errorf("duration operand: %w", err)
		}
		*o = DurationOperand(d)
	case OperandBoolean:
		if w.Name != nil {
			return fmt.Errorf("Boolean operand does not accept name")
		}
		if w.Value == nil || isNull(w.Value) {
			return fmt.Errorf("Boolean operand requires value")
		}
		var b bool
		if err := json.Unmarshal(w.Value, &b); err != nil {
			return fmt.Errorf("Boolean operand value must be Boolean")
		}
		*o = BooleanOperand(b)
	case OperandAgentState:
		if w.Name != nil {
			return fmt.Errorf("agent-state operand does not accept name")
		}
		if w.Value == nil || isNull(w.Value) {
			return fmt.Errorf("agent-state operand requires value")
		}
		var s string
		if err := json.Unmarshal(w.Value, &s); err != nil {
			return fmt.Errorf("agent-state operand value must be a string")
		}
		*o = AgentStateOperand(s)
	default:
		return fmt.Errorf("unsupported operand kind %q", kind)
	}
	return nil
}

type expressionEnvelope struct {
	Kind       *string         `json:"kind"`
	Left       json.RawMessage `json:"left"`
	Op         json.RawMessage `json:"op"`
	Right      json.RawMessage `json:"right"`
	Conditions json.RawMessage `json:"conditions"`
}

func (e *CompareExpression) UnmarshalJSON(data []byte) error {
	var w expressionEnvelope
	if err := strictObject(data, &w); err != nil {
		return err
	}
	if w.Kind == nil {
		return fmt.Errorf("expression kind is required")
	}
	if *w.Kind != "compare" {
		return fmt.Errorf("unsupported expression kind %q", *w.Kind)
	}
	if w.Conditions != nil {
		return fmt.Errorf("compare does not accept conditions")
	}
	if w.Left == nil || isNull(w.Left) || w.Op == nil || isNull(w.Op) || w.Right == nil || isNull(w.Right) {
		return fmt.Errorf("compare requires left, op, and right")
	}
	var op string
	if err := json.Unmarshal(w.Op, &op); err != nil {
		return fmt.Errorf("compare op must be a string")
	}
	var l, r Operand
	if err := json.Unmarshal(w.Left, &l); err != nil {
		return fmt.Errorf("left: %w", err)
	}
	if err := json.Unmarshal(w.Right, &r); err != nil {
		return fmt.Errorf("right: %w", err)
	}
	*e = CompareExpression{Left: l, Operator: Operator(op), Right: r}
	return nil
}
func (e *AllExpression) UnmarshalJSON(data []byte) error {
	var w expressionEnvelope
	if err := strictObject(data, &w); err != nil {
		return err
	}
	if w.Kind == nil {
		return fmt.Errorf("expression kind is required")
	}
	if *w.Kind != "all" {
		return fmt.Errorf("unsupported expression kind %q", *w.Kind)
	}
	if w.Left != nil || w.Op != nil || w.Right != nil {
		return fmt.Errorf("all does not accept compare members")
	}
	if w.Conditions == nil || isNull(w.Conditions) {
		return fmt.Errorf("all requires conditions")
	}
	var rawConditions []json.RawMessage
	if err := json.Unmarshal(w.Conditions, &rawConditions); err != nil {
		return fmt.Errorf("all conditions must be an array")
	}
	if len(rawConditions) == 0 {
		return fmt.Errorf("all requires at least one condition")
	}
	if len(rawConditions)+1 > MaxExpressionNodes {
		return fmt.Errorf("all exceeds maximum expression size")
	}
	out := make([]Expression, len(rawConditions))
	for i, raw := range rawConditions {
		x, err := decodeExpression(raw)
		if err != nil {
			return fmt.Errorf("conditions[%d]: %w", i, err)
		}
		switch x.(type) {
		case *CompareExpression, CompareExpression:
		default:
			return fmt.Errorf("conditions[%d]: nested or unsupported expression", i)
		}
		out[i] = x
	}
	*e = AllExpression{Conditions: out}
	return nil
}
func decodeExpression(data []byte) (Expression, error) {
	var t expressionEnvelope
	if err := strictObject(data, &t); err != nil {
		return nil, err
	}
	if t.Kind == nil {
		return nil, fmt.Errorf("expression kind is required")
	}
	switch *t.Kind {
	case "compare":
		var x CompareExpression
		return &x, json.Unmarshal(data, &x)
	case "all":
		var x AllExpression
		return &x, json.Unmarshal(data, &x)
	default:
		return nil, fmt.Errorf("unsupported expression kind %q", *t.Kind)
	}
}
func (e CompareExpression) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Kind  string  `json:"kind"`
		Left  Operand `json:"left"`
		Op    string  `json:"op"`
		Right Operand `json:"right"`
	}{"compare", e.Left, string(e.Operator), e.Right})
}
func (e AllExpression) MarshalJSON() ([]byte, error) {
	conditions := e.Conditions
	if conditions == nil {
		conditions = []Expression{}
	}
	return json.Marshal(struct {
		Kind       string       `json:"kind"`
		Conditions []Expression `json:"conditions"`
	}{"all", conditions})
}

func (t *Targets) UnmarshalJSON(data []byte) error {
	var w struct {
		Kind     *string   `json:"kind"`
		IDs      *[]string `json:"ids"`
		GroupIDs *[]string `json:"group_ids"`
	}
	if err := strictObject(data, &w); err != nil {
		return err
	}
	if w.Kind == nil || isNull(data) {
		return fmt.Errorf("targets kind is required")
	}
	if w.IDs == nil || w.GroupIDs == nil {
		return fmt.Errorf("targets requires ids and group_ids")
	}
	*t = Targets{Kind: SubjectKind(*w.Kind), IDs: append([]string(nil), (*w.IDs)...), GroupIDs: append([]string(nil), (*w.GroupIDs)...)}
	return nil
}
func (t Targets) MarshalJSON() ([]byte, error) {
	ids := t.IDs
	if ids == nil {
		ids = []string{}
	}
	groups := t.GroupIDs
	if groups == nil {
		groups = []string{}
	}
	return json.Marshal(struct {
		Kind     SubjectKind `json:"kind"`
		IDs      []string    `json:"ids"`
		GroupIDs []string    `json:"group_ids"`
	}{t.Kind, ids, groups})
}
func (c *Condition) UnmarshalJSON(data []byte) error {
	var w struct {
		Predicate json.RawMessage `json:"predicate"`
		For       *Duration       `json:"for"`
	}
	if err := strictObject(data, &w); err != nil {
		return err
	}
	if w.Predicate == nil || isNull(w.Predicate) || w.For == nil {
		return fmt.Errorf("condition requires predicate and for")
	}
	p, err := decodeExpression(w.Predicate)
	if err != nil {
		return fmt.Errorf("predicate: %w", err)
	}
	*c = Condition{Predicate: p, For: *w.For}
	return nil
}
func (n *NotificationPolicy) UnmarshalJSON(data []byte) error {
	var w struct {
		OnOpen     *bool   `json:"on_open"`
		OnRecovery *bool   `json:"on_recovery"`
		Audience   *string `json:"audience"`
	}
	if err := strictObject(data, &w); err != nil {
		return err
	}
	if w.OnOpen == nil || w.OnRecovery == nil || w.Audience == nil {
		return fmt.Errorf("notifications requires on_open, on_recovery, and audience")
	}
	*n = NotificationPolicy{OnOpen: *w.OnOpen, OnRecovery: *w.OnRecovery, Audience: *w.Audience}
	return nil
}

func (r *RuleDefinition) UnmarshalJSON(data []byte) error {
	var w struct {
		SchemaVersion json.RawMessage `json:"schema_version"`
		Name          json.RawMessage `json:"name"`
		Description   json.RawMessage `json:"description"`
		Targets       json.RawMessage `json:"targets"`
		Trigger       json.RawMessage `json:"trigger"`
		Clear         json.RawMessage `json:"clear"`
		Notifications json.RawMessage `json:"notifications"`
	}
	if err := strictObject(data, &w); err != nil {
		return err
	}
	for field, raw := range map[string]json.RawMessage{"schema_version": w.SchemaVersion, "name": w.Name, "targets": w.Targets, "trigger": w.Trigger, "clear": w.Clear, "notifications": w.Notifications} {
		if raw == nil {
			return fmt.Errorf("%s is required", field)
		}
		if isNull(raw) {
			return fmt.Errorf("%s cannot be null", field)
		}
	}
	var version int
	if err := json.Unmarshal(w.SchemaVersion, &version); err != nil {
		return fmt.Errorf("schema_version must be an integer")
	}
	var name string
	if err := json.Unmarshal(w.Name, &name); err != nil {
		return fmt.Errorf("name must be a string")
	}
	var targets Targets
	if err := json.Unmarshal(w.Targets, &targets); err != nil {
		return fmt.Errorf("targets: %w", err)
	}
	var trigger, clear Condition
	if err := json.Unmarshal(w.Trigger, &trigger); err != nil {
		return fmt.Errorf("trigger: %w", err)
	}
	if err := json.Unmarshal(w.Clear, &clear); err != nil {
		return fmt.Errorf("clear: %w", err)
	}
	var notifications NotificationPolicy
	if err := json.Unmarshal(w.Notifications, &notifications); err != nil {
		return fmt.Errorf("notifications: %w", err)
	}
	*r = RuleDefinition{SchemaVersion: version, Name: name, Targets: targets, Trigger: trigger, Clear: clear, Notifications: notifications, targetsPresent: true, triggerPresent: true, clearPresent: true, notificationsPresent: true}
	if w.Description != nil {
		if isNull(w.Description) {
			return fmt.Errorf("description cannot be null")
		}
		if err := json.Unmarshal(w.Description, &r.Description); err != nil {
			return fmt.Errorf("description must be a string")
		}
	}
	return nil
}

func (r RuleDefinition) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		SchemaVersion int                `json:"schema_version"`
		Name          string             `json:"name"`
		Description   string             `json:"description,omitempty"`
		Targets       Targets            `json:"targets"`
		Trigger       Condition          `json:"trigger"`
		Clear         Condition          `json:"clear"`
		Notifications NotificationPolicy `json:"notifications"`
	}{r.SchemaVersion, r.Name, r.Description, r.Targets, r.Trigger, r.Clear, r.Notifications})
}

// strictObject decodes exactly one JSON value and rejects trailing values.
func strictObject(data []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}
		return fmt.Errorf("trailing data: %w", err)
	}
	return nil
}
