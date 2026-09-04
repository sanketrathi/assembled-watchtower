// Package events defines the strict external event boundary.
package events

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type State string

const (
	Available State = "available"
	OnCall    State = "on_call"
	OnBreak   State = "on_break"
	InMeeting State = "in_meeting"
)

func (s State) valid() bool { return s == Available || s == OnCall || s == OnBreak || s == InMeeting }

type Event interface {
	event()
	GetEventID() string
}
type Common struct {
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"ts"`
	Type      string    `json:"type"`
}
type QueueSnapshot struct {
	Common
	QueueID               string  `json:"queue_id"`
	TicketsWaiting        uint64  `json:"tickets_waiting"`
	LongestWaitSec        uint64  `json:"longest_wait_sec"`
	SLATargetSec          uint64  `json:"sla_target_sec"`
	AgentsAvailable       uint64  `json:"agents_available"`
	AgentsOnCall          uint64  `json:"agents_on_call"`
	VolumeLast15m         uint64  `json:"volume_last_15m"`
	VolumeForecastNext15m *uint64 `json:"volume_forecast_next_15m"`
}

func (QueueSnapshot) event()               {}
func (q QueueSnapshot) GetEventID() string { return q.EventID }

type AgentStateChange struct {
	Common
	AgentID                  string   `json:"agent_id"`
	PreviousState            *State   `json:"previous_state"`
	NewState                 State    `json:"new_state"`
	PreviousStateDurationSec *uint64  `json:"previous_state_duration_sec"`
	QueueIDs                 []string `json:"queue_ids"`
}

func (AgentStateChange) event()               {}
func (a AgentStateChange) GetEventID() string { return a.EventID }

type AdherenceCheck struct {
	Common
	AgentID            string     `json:"agent_id"`
	ScheduledState     State      `json:"scheduled_state"`
	ActualState        State      `json:"actual_state"`
	InViolation        bool       `json:"in_violation"`
	ViolationStartedAt *time.Time `json:"violation_started_at"`
	QueueIDs           []string   `json:"queue_ids"`
}

func (AdherenceCheck) event()               {}
func (a AdherenceCheck) GetEventID() string { return a.EventID }

type OccurrenceID struct {
	StreamID string
	Line     uint64
}

func (id OccurrenceID) String() string { return fmt.Sprintf("%s:%d", id.StreamID, id.Line) }

type Envelope struct {
	ID    OccurrenceID
	Line  uint64
	Event Event
	Raw   json.RawMessage
}

const MaxLineBytes = 16 * 1024 * 1024

var strictTimestamp = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$`)

var schemas = map[string]map[string]bool{
	"queue_snapshot":     {"event_id": true, "ts": true, "type": true, "queue_id": true, "tickets_waiting": true, "longest_wait_sec": true, "sla_target_sec": true, "agents_available": true, "agents_on_call": true, "volume_last_15m": true, "volume_forecast_next_15m": true},
	"agent_state_change": {"event_id": true, "ts": true, "type": true, "agent_id": true, "previous_state": true, "previous_state_duration_sec": true, "new_state": true, "queue_ids": true},
	"adherence_check":    {"event_id": true, "ts": true, "type": true, "agent_id": true, "scheduled_state": true, "actual_state": true, "in_violation": true, "violation_started_at": true, "queue_ids": true},
}

func Decode(data []byte) (Event, error) {
	if err := rejectDuplicateKeys(data); err != nil {
		return nil, err
	}
	var obj map[string]json.RawMessage
	d := json.NewDecoder(bytes.NewReader(data))
	if err := d.Decode(&obj); err != nil {
		return nil, fmt.Errorf("event JSON: %w", err)
	}
	if err := ensureEOF(d); err != nil {
		return nil, err
	}
	typ, err := str(obj, "type", false)
	if err != nil {
		return nil, err
	}
	allowed, ok := schemas[typ]
	if !ok {
		return nil, fieldErr("type", "unknown event type %q", typ)
	}
	for k := range obj {
		if !allowed[k] {
			return nil, fieldErr(k, "unknown field")
		}
	}
	if err := requireFields(obj, allowed); err != nil {
		return nil, err
	}
	c, err := common(obj, typ)
	if err != nil {
		return nil, err
	}
	switch typ {
	case "queue_snapshot":
		q := QueueSnapshot{Common: c}
		var e error
		q.QueueID, e = str(obj, "queue_id", false)
		if e != nil {
			return nil, e
		}
		if q.TicketsWaiting, e = uint(obj, "tickets_waiting", false); e != nil {
			return nil, e
		}
		if q.LongestWaitSec, e = uint(obj, "longest_wait_sec", false); e != nil {
			return nil, e
		}
		if q.SLATargetSec, e = uint(obj, "sla_target_sec", false); e != nil {
			return nil, e
		}
		if q.AgentsAvailable, e = uint(obj, "agents_available", false); e != nil {
			return nil, e
		}
		if q.AgentsOnCall, e = uint(obj, "agents_on_call", false); e != nil {
			return nil, e
		}
		if q.VolumeLast15m, e = uint(obj, "volume_last_15m", false); e != nil {
			return nil, e
		}
		q.VolumeForecastNext15m, e = uintPtr(obj, "volume_forecast_next_15m")
		if e != nil {
			return nil, e
		}
		return q, nil
	case "agent_state_change":
		a := AgentStateChange{Common: c}
		var e error
		a.AgentID, e = str(obj, "agent_id", false)
		if e != nil {
			return nil, e
		}
		a.PreviousState, e = statePtr(obj, "previous_state")
		if e != nil {
			return nil, e
		}
		a.NewState, e = state(obj, "new_state")
		if e != nil {
			return nil, e
		}
		a.PreviousStateDurationSec, e = uintPtr(obj, "previous_state_duration_sec")
		if e != nil {
			return nil, e
		}
		a.QueueIDs, e = stringsArr(obj, "queue_ids", true)
		if e != nil {
			return nil, e
		}
		return a, nil
	default:
		a := AdherenceCheck{Common: c}
		var e error
		a.AgentID, e = str(obj, "agent_id", false)
		if e != nil {
			return nil, e
		}
		a.ScheduledState, e = state(obj, "scheduled_state")
		if e != nil {
			return nil, e
		}
		a.ActualState, e = state(obj, "actual_state")
		if e != nil {
			return nil, e
		}
		a.InViolation, e = boolean(obj, "in_violation")
		if e != nil {
			return nil, e
		}
		a.ViolationStartedAt, e = timePtr(obj, "violation_started_at")
		if e != nil {
			return nil, e
		}
		a.QueueIDs, e = stringsArr(obj, "queue_ids", false)
		if e != nil {
			return nil, e
		}
		return a, nil
	}
}
func rejectDuplicateKeys(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	if err := walkJSON(d); err != nil {
		return fmt.Errorf("event JSON: %w", err)
	}
	return nil
}
func walkJSON(d *json.Decoder) error {
	tok, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	if delim == '{' {
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name := key.(string)
			if seen[name] {
				return fmt.Errorf("duplicate field %q", name)
			}
			seen[name] = true
			if err := walkJSON(d); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	}
	if delim == '[' {
		for d.More() {
			if err := walkJSON(d); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	}
	return nil
}

func ensureEOF(d *json.Decoder) error {
	var x any
	if err := d.Decode(&x); err != io.EOF {
		return fmt.Errorf("event JSON: trailing data")
	}
	return nil
}
func fieldErr(f, format string, a ...any) error {
	return fmt.Errorf("field %q: %s", f, fmt.Sprintf(format, a...))
}
func requireFields(o map[string]json.RawMessage, s map[string]bool) error {
	for k := range s {
		if _, ok := o[k]; !ok {
			return fieldErr(k, "missing required field")
		}
	}
	return nil
}
func str(o map[string]json.RawMessage, k string, null bool) (string, error) {
	r := o[k]
	if bytes.Equal(bytes.TrimSpace(r), []byte("null")) {
		if null {
			return "", nil
		}
		return "", fieldErr(k, "must be a string")
	}
	var v string
	if err := json.Unmarshal(r, &v); err != nil || v == "" {
		return "", fieldErr(k, "must be a non-empty string")
	}
	if len(v) > 256 {
		return "", fieldErr(k, "string is too long")
	}
	return v, nil
}
func common(o map[string]json.RawMessage, typ string) (Common, error) {
	id, e := str(o, "event_id", false)
	if e != nil {
		return Common{}, e
	}
	ts, e := timeVal(o, "ts")
	if e != nil {
		return Common{}, e
	}
	return Common{EventID: id, Timestamp: ts, Type: typ}, nil
}
func uint(o map[string]json.RawMessage, k string, null bool) (uint64, error) {
	r := bytes.TrimSpace(o[k])
	if bytes.Equal(r, []byte("null")) {
		if null {
			return 0, nil
		}
		return 0, fieldErr(k, "must be a nonnegative integer")
	}
	if len(r) == 0 || r[0] == '-' || r[0] == '+' {
		return 0, fieldErr(k, "must be a nonnegative integer")
	}
	v, e := strconv.ParseUint(string(r), 10, 64)
	if e != nil {
		return 0, fieldErr(k, "must be a nonnegative integer")
	}
	return v, nil
}
func uintPtr(o map[string]json.RawMessage, k string) (*uint64, error) {
	if bytes.Equal(bytes.TrimSpace(o[k]), []byte("null")) {
		return nil, nil
	}
	v, e := uint(o, k, false)
	if e != nil {
		return nil, e
	}
	return &v, nil
}
func timeVal(o map[string]json.RawMessage, k string) (time.Time, error) {
	var value string
	if err := json.Unmarshal(o[k], &value); err != nil || !strictTimestamp.MatchString(value) {
		return time.Time{}, fieldErr(k, "must be an RFC 3339 timestamp")
	}
	// The lexical check prevents Go's comma-fraction and out-of-range-offset
	// extensions from entering the boundary. Parse validates calendar/time ranges.
	offset := value[len(value)-6:]
	if value[len(value)-1] != 'Z' {
		hour, _ := strconv.Atoi(offset[1:3])
		minute, _ := strconv.Atoi(offset[4:6])
		if hour > 23 || minute > 59 {
			return time.Time{}, fieldErr(k, "must be an RFC 3339 timestamp")
		}
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fieldErr(k, "must be an RFC 3339 timestamp: %v", err)
	}
	return t, nil
}
func timePtr(o map[string]json.RawMessage, k string) (*time.Time, error) {
	if bytes.Equal(bytes.TrimSpace(o[k]), []byte("null")) {
		return nil, nil
	}
	t, e := timeVal(o, k)
	if e != nil {
		return nil, e
	}
	return &t, nil
}
func state(o map[string]json.RawMessage, k string) (State, error) {
	s, e := str(o, k, false)
	if e != nil {
		return "", e
	}
	v := State(s)
	if !v.valid() {
		return "", fieldErr(k, "unknown agent state %q", s)
	}
	return v, nil
}
func statePtr(o map[string]json.RawMessage, k string) (*State, error) {
	if bytes.Equal(bytes.TrimSpace(o[k]), []byte("null")) {
		return nil, nil
	}
	v, e := state(o, k)
	if e != nil {
		return nil, e
	}
	return &v, nil
}
func boolean(o map[string]json.RawMessage, k string) (bool, error) {
	if bytes.Equal(bytes.TrimSpace(o[k]), []byte("null")) {
		return false, fieldErr(k, "must be boolean")
	}
	var v bool
	if err := json.Unmarshal(o[k], &v); err != nil {
		return false, fieldErr(k, "must be boolean")
	}
	return v, nil
}
func stringsArr(o map[string]json.RawMessage, k string, nullable bool) ([]string, error) {
	r := bytes.TrimSpace(o[k])
	if bytes.Equal(r, []byte("null")) {
		if nullable {
			return nil, nil
		}
		return nil, fieldErr(k, "must be an array of strings")
	}
	var a []string
	if err := json.Unmarshal(r, &a); err != nil || a == nil {
		return nil, fieldErr(k, "must be an array of strings")
	}
	seen := make(map[string]bool, len(a))
	for i, value := range a {
		if value == "" {
			return nil, fieldErr(k, "item %d must be a non-empty string", i)
		}
		if len(value) > 256 {
			return nil, fieldErr(k, "item %d is longer than 256 bytes", i)
		}
		if seen[value] {
			return nil, fieldErr(k, "item %d duplicates queue identifier", i)
		}
		seen[value] = true
	}
	return a, nil
}

// splitPhysicalLine treats CR as a delimiter only when followed by LF.
func splitPhysicalLine(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		token = data[:i]
		if len(token) > 0 && token[len(token)-1] == '\r' {
			token = token[:len(token)-1]
		}
		return i + 1, token, nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// Stream reads one JSON object per physical line, preserving order and line identity.
func Stream(r io.Reader, streamID string, fn func(Envelope) error) error {
	if strings.TrimSpace(streamID) == "" {
		return errors.New("stream ID must be non-empty")
	}
	scanner := bufio.NewScanner(r)
	scanner.Split(splitPhysicalLine)
	scanner.Buffer(make([]byte, 64*1024), MaxLineBytes+2)
	var line uint64
	for scanner.Scan() {
		line++
		rawLine := scanner.Bytes()
		// ScanLines removes CR only when it is part of a CRLF delimiter. A bare
		// CR at EOF remains in rawLine and is correctly counted as content.
		lineBytes := rawLine
		if len(lineBytes) > MaxLineBytes {
			return fmt.Errorf("line %d: content exceeds %d MiB", line, MaxLineBytes/(1024*1024))
		}
		if len(bytes.TrimSpace(lineBytes)) == 0 {
			return fmt.Errorf("line %d: empty event", line)
		}
		e, err := Decode(lineBytes)
		if err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
		raw := append(json.RawMessage(nil), lineBytes...)
		if err := fn(Envelope{ID: OccurrenceID{StreamID: streamID, Line: line}, Line: line, Event: e, Raw: raw}); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("line %d: reading event stream: %w", line+1, err)
	}
	return nil
}
