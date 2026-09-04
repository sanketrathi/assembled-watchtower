// Package rules contains the canonical, typed rule-definition contract.
package rules

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"
)

const CurrentSchemaVersion = 1
const MaxExpressionNodes = 32

// Duration is a non-negative duration represented on the wire by a canonical
// whole-unit string. It is intentionally not a raw Go nanosecond value.
type Duration time.Duration

var durationPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(s|m|h)$`)

func ParseDuration(s string) (Duration, error) {
	m := durationPattern.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("duration must be a non-negative integer followed by s, m, or h")
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("duration value is too large")
	}
	mult := int64(time.Second)
	switch m[2] {
	case "m":
		mult *= 60
	case "h":
		mult *= 3600
	}
	if n > int64((365*24*time.Hour))/mult {
		return 0, fmt.Errorf("duration exceeds one year")
	}
	return Duration(n * mult), nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	value, err := canonicalDurationString(d)
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}
func canonicalDurationString(d Duration) (string, error) {
	if d < 0 || time.Duration(d) > 365*24*time.Hour || time.Duration(d)%time.Second != 0 {
		return "", fmt.Errorf("duration must be a non-negative whole number of seconds")
	}
	n := int64(time.Duration(d) / time.Hour)
	if n > 0 && time.Duration(d)%time.Hour == 0 {
		return strconv.FormatInt(n, 10) + "h", nil
	}
	n = int64(time.Duration(d) / time.Minute)
	if n > 0 && time.Duration(d)%time.Minute == 0 {
		return strconv.FormatInt(n, 10) + "m", nil
	}
	return strconv.FormatInt(int64(time.Duration(d)/time.Second), 10) + "s", nil
}
func (d *Duration) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	v, err := ParseDuration(s)
	if err != nil {
		return err
	}
	*d = v
	return nil
}
func (d Duration) String() string { value, _ := canonicalDurationString(d); return value }

// SubjectKind identifies the subjects to which a rule applies.
type SubjectKind string

const (
	SubjectQueue SubjectKind = "queue"
	SubjectAgent SubjectKind = "agent"
)

type Operator string

const (
	OpEqual          Operator = "eq"
	OpNotEqual       Operator = "ne"
	OpGreater        Operator = "gt"
	OpGreaterOrEqual Operator = "gte"
	OpLess           Operator = "lt"
	OpLessOrEqual    Operator = "lte"
)

type AgentState string

const (
	StateAvailable AgentState = "available"
	StateOnCall    AgentState = "on_call"
	StateOnBreak   AgentState = "on_break"
	StateInMeeting AgentState = "in_meeting"
)

// Targets are the inclusive union of explicit IDs and configured group IDs.
type Targets struct {
	Kind     SubjectKind `json:"kind"`
	IDs      []string    `json:"ids"`
	GroupIDs []string    `json:"group_ids"`
}

// Condition is a predicate and the continuous time for which it must be true.
type Condition struct {
	Predicate Expression `json:"predicate"`
	For       Duration   `json:"for"`
}

type NotificationPolicy struct {
	OnOpen     bool   `json:"on_open"`
	OnRecovery bool   `json:"on_recovery"`
	Audience   string `json:"audience"`
}

// RuleDefinition is immutable revision content. Lifecycle identity, status, and
// revision numbers belong to RuleResource/RuleRevision, not this type.
type RuleDefinition struct {
	SchemaVersion int                `json:"schema_version"`
	Name          string             `json:"name"`
	Description   string             `json:"description,omitempty"`
	Targets       Targets            `json:"targets"`
	Trigger       Condition          `json:"trigger"`
	Clear         Condition          `json:"clear"`
	Notifications NotificationPolicy `json:"notifications"`

	targetsPresent, triggerPresent, clearPresent, notificationsPresent bool
}

type RuleResource struct {
	ID             string `json:"id"`
	Status         string `json:"status"`
	ActiveRevision int64  `json:"active_revision"`
}
type RuleRevision struct {
	Revision   int64          `json:"revision"`
	Definition RuleDefinition `json:"definition"`
}

// NewTargets and NewRuleDefinition construct definitions with canonical defaults.
func NewTargets(kind SubjectKind, ids, groupIDs []string) Targets {
	return Targets{Kind: kind, IDs: append([]string(nil), ids...), GroupIDs: append([]string(nil), groupIDs...)}
}
func NewCondition(predicate Expression, duration Duration) Condition {
	return Condition{Predicate: predicate, For: duration}
}
func NewRuleDefinition(name, description string, targets Targets, trigger, clear Condition, notifications NotificationPolicy) RuleDefinition {
	return RuleDefinition{SchemaVersion: CurrentSchemaVersion, Name: name, Description: description, Targets: targets, Trigger: trigger, Clear: clear, Notifications: notifications}
}

type CreateRuleRequest struct {
	Definition RuleDefinition `json:"definition"`
}
type UpdateRuleRequest struct {
	ExpectedRevision int64          `json:"expected_revision"`
	Definition       RuleDefinition `json:"definition"`
}

// Operand is one typed field reference or literal.
type OperandKind string

const (
	OperandField      OperandKind = "field"
	OperandInteger    OperandKind = "integer"
	OperandDuration   OperandKind = "duration"
	OperandBoolean    OperandKind = "boolean"
	OperandAgentState OperandKind = "agent_state"
)

type Operand struct {
	Kind       OperandKind
	Field      string
	Integer    int64
	Duration   Duration
	Boolean    bool
	AgentState AgentState
}

func FieldOperand(v string) Operand      { return Operand{Kind: OperandField, Field: v} }
func IntegerOperand(v int64) Operand     { return Operand{Kind: OperandInteger, Integer: v} }
func DurationOperand(v Duration) Operand { return Operand{Kind: OperandDuration, Duration: v} }
func BooleanOperand(v bool) Operand      { return Operand{Kind: OperandBoolean, Boolean: v} }
func AgentStateOperand(v string) Operand {
	return Operand{Kind: OperandAgentState, AgentState: AgentState(v)}
}
func NewCompare(left Operand, op Operator, right Operand) CompareExpression {
	return CompareExpression{Left: left, Operator: op, Right: right}
}
func NewAll(conditions ...Expression) AllExpression {
	return AllExpression{Conditions: append([]Expression(nil), conditions...)}
}

// Expression is either a typed comparison or a flat conjunction.
type Expression interface{ isExpression() }
type CompareExpression struct {
	Left     Operand  `json:"left"`
	Operator Operator `json:"op"`
	Right    Operand  `json:"right"`
}

func (CompareExpression) isExpression() {}

type AllExpression struct {
	Conditions []Expression `json:"conditions"`
}

func (AllExpression) isExpression() {}

// FieldType and field metadata are part of the stable public field catalog.
type FieldType string

const (
	TypeDuration   FieldType = "duration"
	TypeCount      FieldType = "count"
	TypeBoolean    FieldType = "boolean"
	TypeAgentState FieldType = "agent_state"
)

type FieldSpec struct {
	Name             string
	Subject          SubjectKind
	Type             FieldType
	Operators        []string
	SourceProjection string
	Dependency       string
}

var fieldCatalog = map[string]FieldSpec{
	"queue.longest_wait":             {Name: "queue.longest_wait", Subject: SubjectQueue, Type: TypeDuration, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "longest_wait_sec", Dependency: "queue_snapshot"},
	"queue.sla_target":               {Name: "queue.sla_target", Subject: SubjectQueue, Type: TypeDuration, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "sla_target_sec", Dependency: "queue_snapshot"},
	"queue.tickets_waiting":          {Name: "queue.tickets_waiting", Subject: SubjectQueue, Type: TypeCount, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "tickets_waiting", Dependency: "queue_snapshot"},
	"queue.agents_available":         {Name: "queue.agents_available", Subject: SubjectQueue, Type: TypeCount, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "agents_available", Dependency: "queue_snapshot"},
	"queue.agents_on_call":           {Name: "queue.agents_on_call", Subject: SubjectQueue, Type: TypeCount, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "agents_on_call", Dependency: "queue_snapshot"},
	"queue.volume_last_15m":          {Name: "queue.volume_last_15m", Subject: SubjectQueue, Type: TypeCount, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "volume_last_15m", Dependency: "queue_snapshot"},
	"queue.volume_forecast_next_15m": {Name: "queue.volume_forecast_next_15m", Subject: SubjectQueue, Type: TypeCount, Operators: []string{"eq", "ne", "gt", "gte", "lt", "lte"}, SourceProjection: "volume_forecast_next_15m", Dependency: "queue_snapshot"},
	"adherence.violation":            {Name: "adherence.violation", Subject: SubjectAgent, Type: TypeBoolean, Operators: []string{"eq", "ne"}, SourceProjection: "in_violation", Dependency: "adherence"},
	"agent.current_state":            {Name: "agent.current_state", Subject: SubjectAgent, Type: TypeAgentState, Operators: []string{"eq", "ne"}, SourceProjection: "new_state", Dependency: "agent_state"},
}
var validAgentStates = map[AgentState]bool{StateAvailable: true, StateOnCall: true, StateOnBreak: true, StateInMeeting: true}

func field(name string) (FieldSpec, bool) { f, ok := fieldCatalog[name]; return f, ok }
func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// FieldSpecFor returns a copy of the stable catalog entry for a public field.
func FieldSpecFor(name string) (FieldSpec, bool) {
	f, ok := field(name)
	if !ok {
		return FieldSpec{}, false
	}
	f.Operators = append([]string(nil), f.Operators...)
	return f, true
}

// FieldCatalog returns the stable field catalog in name order.
func FieldCatalog() []FieldSpec {
	names := make([]string, 0, len(fieldCatalog))
	for name := range fieldCatalog {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]FieldSpec, 0, len(names))
	for _, name := range names {
		f, _ := FieldSpecFor(name)
		out = append(out, f)
	}
	return out
}
