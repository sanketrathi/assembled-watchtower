package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// DecodeRuleDefinition strictly decodes one canonical definition and applies
// the v1 semantic contract. UnmarshalJSON provides the structural-only hook.
func DecodeRuleDefinition(data []byte) (RuleDefinition, error) {
	var r RuleDefinition
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	d.UseNumber()
	if err := d.Decode(&r); err != nil {
		return RuleDefinition{}, fmt.Errorf("decode rule definition: %w", err)
	}
	var extra json.RawMessage
	if err := d.Decode(&extra); err != io.EOF {
		if err == nil {
			return RuleDefinition{}, fmt.Errorf("decode rule definition: trailing JSON value")
		}
		return RuleDefinition{}, fmt.Errorf("decode rule definition: trailing data: %w", err)
	}
	if err := r.Validate(); err != nil {
		return RuleDefinition{}, fmt.Errorf("validate rule definition: %w", err)
	}
	return r, nil
}
