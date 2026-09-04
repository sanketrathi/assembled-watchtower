package events

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
)

func TestFixtureStream(t *testing.T) {
	data := mustRead(t, "../../data/events.jsonl")
	var got []Envelope
	if err := Stream(bytes.NewReader(data), "fixture", func(e Envelope) error { got = append(got, e); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(got) != 96 {
		t.Fatalf("got %d occurrences", len(got))
	}
	if got[0].ID.Line != 1 || got[95].ID.Line != 96 {
		t.Fatalf("bad line identities")
	}
	if got[0].Raw[0] != '{' {
		t.Fatal("raw payload not retained")
	}
	seen := map[string]int{}
	for _, e := range got {
		seen[e.Event.GetEventID()]++
	}
	if seen["evt_01HXYZ050"] != 2 {
		t.Fatalf("duplicate source IDs were collapsed: %v", seen["evt_01HXYZ050"])
	}
}
func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestStrictBoundary(t *testing.T) {
	base := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`
	cases := []struct{ name, s string }{{"unknown", strings.Replace(base, `"queue_id":"q"`, `"queue_id":"q","extra":1`, 1)}, {"numeric", strings.Replace(base, `"tickets_waiting":0`, `"tickets_waiting":1.2`, 1)}, {"negative", strings.Replace(base, `"tickets_waiting":0`, `"tickets_waiting":-1`, 1)}, {"timestamp", strings.Replace(base, "2026-01-01T00:00:00Z", "nope", 1)}, {"type", strings.Replace(base, "queue_snapshot", "wat", 1)}, {"null", strings.Replace(base, `"queue_id":"q"`, `"queue_id":null`, 1)}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, e := Decode([]byte(tc.s)); e == nil {
				t.Fatal("accepted invalid event")
			}
		})
	}
}
func TestNullableAndStates(t *testing.T) {
	e, err := Decode([]byte(`{"event_id":"e","ts":"2026-01-01T00:00:00+02:00","type":"agent_state_change","agent_id":"a","previous_state":null,"previous_state_duration_sec":null,"new_state":"available","queue_ids":null}`))
	if err != nil {
		t.Fatal(err)
	}
	a := e.(AgentStateChange)
	if a.QueueIDs != nil || a.PreviousState != nil {
		t.Fatal("nullability lost")
	}
	agentEmpty := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"agent_state_change","agent_id":"a","previous_state":null,"new_state":"available","previous_state_duration_sec":null,"queue_ids":[]}`
	if _, err := Decode([]byte(agentEmpty)); err != nil {
		t.Fatalf("empty agent queue_ids rejected: %v", err)
	}
	if a.Timestamp.UTC().Hour() != 22 {
		t.Fatal("timestamp offset not parsed")
	}
	if _, offset := a.Timestamp.Zone(); offset != 2*60*60 {
		t.Fatalf("timestamp offset was not preserved: %d", offset)
	}
}

func TestStreamLineErrors(t *testing.T) {
	err := Stream(strings.NewReader("{}\n{bad\n"), "s", func(Envelope) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("error=%v", err)
	}
}

func TestStrictAdherenceAndChoices(t *testing.T) {
	base := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"adherence_check","agent_id":"a","scheduled_state":"available","actual_state":"on_call","in_violation":false,"violation_started_at":null,"queue_ids":["q"]}`
	for _, value := range []string{`null`, `0`, `"false"`} {
		t.Run("boolean_"+value, func(t *testing.T) {
			if _, err := Decode([]byte(strings.Replace(base, "false", value, 1))); err == nil {
				t.Fatal("accepted invalid required boolean")
			}
		})
	}
	for _, value := range []string{"2026-01-01T00:00:00,1Z", "2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+00:60"} {
		t.Run("timestamp_"+value, func(t *testing.T) {
			input := strings.Replace(base, "2026-01-01T00:00:00Z", value, 1)
			if _, err := Decode([]byte(input)); err == nil {
				t.Fatal("accepted invalid timestamp")
			}
		})
	}
	for _, value := range []string{"0", "18446744073709551615", "18446744073709551616"} {
		t.Run("numeric_"+value, func(t *testing.T) {
			input := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","tickets_waiting":` + value + `,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`
			_, err := Decode([]byte(input))
			if (value == "18446744073709551616") == (err == nil) {
				t.Fatalf("unexpected error=%v", err)
			}
		})
	}
	if _, err := Decode([]byte(strings.Replace(base, `"queue_ids":["q"]`, `"queue_ids":["q","q"]`, 1))); err == nil {
		t.Fatal("accepted duplicate queue IDs")
	}
}
func TestEmptyIdentifiersRejected(t *testing.T) {
	base := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`
	for _, field := range []string{"event_id", "queue_id"} {
		input := strings.Replace(base, `"`+field+`":"`+map[string]string{"event_id": "e", "queue_id": "q"}[field]+`"`, `"`+field+`":""`, 1)
		if _, err := Decode([]byte(input)); err == nil {
			t.Errorf("accepted empty %s", field)
		}
	}
	agent := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"agent_state_change","agent_id":"","previous_state":null,"new_state":"available","previous_state_duration_sec":null,"queue_ids":[]}`
	if _, err := Decode([]byte(agent)); err == nil {
		t.Fatal("accepted empty agent_id")
	}
}

func TestQueueListEmptyItemsRejected(t *testing.T) {
	agent := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"agent_state_change","agent_id":"a","previous_state":null,"new_state":"available","previous_state_duration_sec":null,"queue_ids":[""]}`
	adherence := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"adherence_check","agent_id":"a","scheduled_state":"available","actual_state":"on_call","in_violation":false,"violation_started_at":null,"queue_ids":[""]}`
	for _, input := range []string{agent, adherence} {
		if _, err := Decode([]byte(input)); err == nil {
			t.Fatal("accepted empty queue ID item")
		}
	}
}
func TestDuplicateJSONKeysRejected(t *testing.T) {
	input := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","queue_id":"other","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`
	if _, err := Decode([]byte(input)); err == nil {
		t.Fatal("accepted duplicate JSON key")
	}
}

func TestIdentifierBoundaries(t *testing.T) {
	for _, n := range []int{256, 257} {
		value := strings.Repeat("x", n)
		input := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"` + value + `","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`
		_, err := Decode([]byte(input))
		if (n == 256) != (err == nil) {
			t.Errorf("queue_id length %d error=%v", n, err)
		}
	}
	for _, n := range []int{256, 257} {
		value := strings.Repeat("x", n)
		input := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"adherence_check","agent_id":"a","scheduled_state":"available","actual_state":"on_call","in_violation":false,"violation_started_at":null,"queue_ids":["` + value + `"]}`
		_, err := Decode([]byte(input))
		if (n == 256) != (err == nil) {
			t.Errorf("queue_ids item length %d error=%v", n, err)
		}
	}
}
func TestStreamReaderErrorsIncludeLine(t *testing.T) {
	err := Stream(failingReader{}, "s", func(Envelope) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("error=%v", err)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func FuzzDecode(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Fuzz(func(t *testing.T, data []byte) { _, _ = Decode(data) })
}

func TestWrongTypeMatrix(t *testing.T) {
	cases := []struct {
		sample string
		bad    map[string]string
	}{
		{`{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`, map[string]string{"event_id": "true", "ts": "true", "type": "true", "queue_id": "true", "tickets_waiting": "true", "longest_wait_sec": "true", "sla_target_sec": "true", "agents_available": "true", "agents_on_call": "true", "volume_last_15m": "true", "volume_forecast_next_15m": "true"}},
		{`{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"agent_state_change","agent_id":"a","previous_state":null,"new_state":"available","previous_state_duration_sec":null,"queue_ids":null}`, map[string]string{"event_id": "true", "ts": "true", "type": "true", "agent_id": "true", "previous_state": "true", "new_state": "true", "previous_state_duration_sec": "true", "queue_ids": "true"}},
		{`{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"adherence_check","agent_id":"a","scheduled_state":"available","actual_state":"on_call","in_violation":false,"violation_started_at":null,"queue_ids":[]}`, map[string]string{"event_id": "true", "ts": "true", "type": "true", "agent_id": "true", "scheduled_state": "true", "actual_state": "true", "in_violation": "\"false\"", "violation_started_at": "true", "queue_ids": "true"}},
	}
	for _, tc := range cases {
		for field, raw := range tc.bad {
			var fields map[string]json.RawMessage
			_ = json.Unmarshal([]byte(tc.sample), &fields)
			fields[field] = json.RawMessage(raw)
			data, _ := json.Marshal(fields)
			if _, err := Decode(data); err == nil {
				t.Errorf("accepted wrong type for %s", field)
			}
		}
	}
}

func TestStreamLineLimit(t *testing.T) {
	base := `{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`
	under := base + strings.Repeat(" ", MaxLineBytes-len(base)-1)
	if err := Stream(strings.NewReader(under+"\n"), "s", func(Envelope) error { return nil }); err != nil {
		t.Fatalf("under limit: %v", err)
	}
	exact := base + strings.Repeat(" ", MaxLineBytes-len(base))
	if err := Stream(strings.NewReader(exact+"\n"), "s", func(Envelope) error { return nil }); err != nil {
		t.Fatalf("exact LF limit: %v", err)
	}
	if err := Stream(strings.NewReader(exact+"\r\n"), "s", func(Envelope) error { return nil }); err != nil {
		t.Fatalf("exact CRLF limit: %v", err)
	}
	over := base + strings.Repeat(" ", MaxLineBytes-len(base)+1)
	for name, input := range map[string]string{"LF": over + "\n", "CRLF": over + "\r\n", "EOF": over, "bare-CR-EOF": exact + "\r"} {
		if err := Stream(strings.NewReader(input), "s", func(Envelope) error { return nil }); err == nil || !strings.Contains(err.Error(), "line 1") {
			t.Errorf("over %s error=%v", name, err)
		}
	}
}

func TestRequiredAndWrongTypeMatrices(t *testing.T) {
	samples := []string{
		`{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"queue_snapshot","queue_id":"q","tickets_waiting":0,"longest_wait_sec":0,"sla_target_sec":1,"agents_available":0,"agents_on_call":0,"volume_last_15m":0,"volume_forecast_next_15m":null}`,
		`{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"agent_state_change","agent_id":"a","previous_state":null,"new_state":"available","previous_state_duration_sec":null,"queue_ids":null}`,
		`{"event_id":"e","ts":"2026-01-01T00:00:00Z","type":"adherence_check","agent_id":"a","scheduled_state":"available","actual_state":"on_call","in_violation":false,"violation_started_at":null,"queue_ids":[]}`,
	}
	for _, sample := range samples {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(sample), &fields); err != nil {
			t.Fatal(err)
		}
		for field := range fields {
			input := map[string]json.RawMessage{}
			for k, v := range fields {
				input[k] = v
			}
			delete(input, field)
			data, _ := json.Marshal(input)
			if _, err := Decode(data); err == nil {
				t.Errorf("accepted missing %s in %s", field, fields["type"])
			}
		}
	}
	// Every required scalar rejects null, and representative fields reject wrong JSON types.
	cases := []struct{ sample, field string }{
		{samples[0], "event_id"}, {samples[0], "ts"}, {samples[0], "queue_id"}, {samples[0], "tickets_waiting"},
		{samples[1], "agent_id"}, {samples[1], "new_state"},
		{samples[2], "agent_id"}, {samples[2], "scheduled_state"}, {samples[2], "in_violation"},
	}
	for _, tc := range cases {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal([]byte(tc.sample), &fields)
		fields[tc.field] = json.RawMessage(`null`)
		data, _ := json.Marshal(fields)
		if _, err := Decode(data); err == nil {
			t.Errorf("accepted null %s", tc.field)
		}
	}
	// Nullable fields accept null, while empty arrays are distinct accepted values.
	for _, sample := range []string{samples[1], samples[2]} {
		if _, err := Decode([]byte(sample)); err != nil {
			t.Fatalf("accepted valid nullable/empty-array event: %v", err)
		}
	}
}

func TestPhysicalLineDelimiterHandling(t *testing.T) {
	advance, token, err := splitPhysicalLine([]byte("abc\r\n"), false)
	if err != nil || advance != 5 || string(token) != "abc" {
		t.Fatalf("CRLF split: advance=%d token=%q err=%v", advance, token, err)
	}
	advance, token, err = splitPhysicalLine([]byte("abc\r"), true)
	if err != nil || advance != 4 || string(token) != "abc\r" {
		t.Fatalf("bare CR EOF split: advance=%d token=%q err=%v", advance, token, err)
	}
}
