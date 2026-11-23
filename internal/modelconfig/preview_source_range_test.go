package modelconfig

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestSourceLineStart(t *testing.T) {
	data := []byte("line1\nline2\nline3\n")

	if got, err := sourceLineStart(data, 1); err != nil || got != 0 {
		t.Fatalf("line 1: got %d, err %v", got, err)
	}
	if got, err := sourceLineStart(data, 2); err != nil || got != 6 {
		t.Fatalf("line 2: got %d, err %v", got, err)
	}
	if got, err := sourceLineStart(data, 3); err != nil || got != 12 {
		t.Fatalf("line 3: got %d, err %v", got, err)
	}
	// Missing line: a newline is absent mid-scan.
	short := []byte("line1\nline2")
	if _, err := sourceLineStart(short, 3); err == nil || !strings.Contains(err.Error(), "invalid YAML source location") {
		t.Fatalf("line 3 missing: got err %v", err)
	}
}

func TestSourceColumnOffset(t *testing.T) {
	// 'e' (1 byte) + U+00E9 (2 bytes: C3 A9) + 'x' (1 byte)
	data := []byte{0x65, 0xC3, 0xA9, 0x78}
	if got, err := sourceColumnOffset(data, 0, 1); err != nil || got != 0 {
		t.Fatalf("column 1: got %d, err %v", got, err)
	}
	if got, err := sourceColumnOffset(data, 0, 2); err != nil || got != 1 {
		t.Fatalf("column 2: got %d, err %v", got, err)
	}
	if got, err := sourceColumnOffset(data, 1, 2); err != nil || got != 3 {
		t.Fatalf("column 2 from offset1: got %d, err %v", got, err)
	}
	if _, err := sourceColumnOffset(data, 0, 5); err == nil || !strings.Contains(err.Error(), "invalid YAML source location") {
		t.Fatalf("column beyond data missing: got err %v", err)
	}
}

func TestScalarQuote(t *testing.T) {
	if got, err := scalarQuote(0); err != nil || got != 0 {
		t.Fatalf("plain: got %d, err %v", got, err)
	}
	if got, err := scalarQuote(yaml.SingleQuotedStyle); err != nil || got != '\'' {
		t.Fatalf("single: got %d, err %v", got, err)
	}
	if got, err := scalarQuote(yaml.DoubleQuotedStyle); err != nil || got != '"' {
		t.Fatalf("double: got %d, err %v", got, err)
	}
	if _, err := scalarQuote(yaml.LiteralStyle); err == nil || !strings.Contains(err.Error(), "unsupported command scalar style") {
		t.Fatalf("literal missing: got err %v", err)
	}
}

func TestValidateScalarBytes(t *testing.T) {
	// plain scalar, valid delimiter at end
	if start, end, err := validateScalarBytes([]byte("hello world"), 0, 0, "hello"); err != nil || start != 0 || end != 5 {
		t.Fatalf("plain valid: got %d %d err %v", start, end, err)
	}
	// plain scalar, trailing non-delimiter -> ambiguous
	if _, _, err := validateScalarBytes([]byte("helloX"), 0, 0, "hello"); err == nil || !strings.Contains(err.Error(), "ambiguous command scalar") {
		t.Fatalf("plain ambiguous missing: got err %v", err)
	}
	// plain scalar, value mismatch -> requires unsupported decoding
	if _, _, err := validateScalarBytes([]byte("hello"), 0, 0, "hellox"); err == nil || !strings.Contains(err.Error(), "command scalar requires unsupported decoding") {
		t.Fatalf("plain mismatch missing: got err %v", err)
	}
	// quoted scalar, valid
	if start, end, err := validateScalarBytes([]byte("\"hello\""), 0, '"', "hello"); err != nil || start != 1 || end != 6 {
		t.Fatalf("quoted valid: got %d %d err %v", start, end, err)
	}
	// quoted scalar, missing closing quote -> unsupported command scalar
	if _, _, err := validateScalarBytes([]byte("\"hello"), 0, '"', "hello"); err == nil || !strings.Contains(err.Error(), "unsupported command scalar") {
		t.Fatalf("quoted missing close missing: got err %v", err)
	}
	// quoted scalar, opening quote missing -> unsupported command scalar
	if _, _, err := validateScalarBytes([]byte("hello\""), 0, '"', "hello"); err == nil || !strings.Contains(err.Error(), "unsupported command scalar") {
		t.Fatalf("quoted missing open missing: got err %v", err)
	}
}

func TestCommandSourceRange(t *testing.T) {
	// data:
	//   top:\n
	//     cmd: "hello"\n
	// double-quoted scalar "hello" on line 2; opening quote at byte offset 12 (column 8)
	data := []byte("top:\n  cmd: \"hello\"\n")
	node := &yaml.Node{
		Kind:   yaml.ScalarNode,
		Style:  yaml.DoubleQuotedStyle,
		Tag:    "!!str",
		Value:  "hello",
		Line:   2,
		Column: 8,
	}
	start, end, err := commandSourceRange(data, node)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if start != 13 || end != 18 {
		t.Fatalf("double-quoted: got %d %d", start, end)
	}
	if string(data[start:end]) != "hello" {
		t.Fatalf("double-quoted value: %q", data[start:end])
	}

	// plain scalar on line 1: `cmd: hello`, value starts at byte offset 5 (column 6)
	data2 := []byte("cmd: hello\n")
	node2 := &yaml.Node{
		Kind:   yaml.ScalarNode,
		Style:  0,
		Tag:    "!!str",
		Value:  "hello",
		Line:   1,
		Column: 6,
	}
	start, end, err = commandSourceRange(data2, node2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if start != 5 || end != 10 {
		t.Fatalf("plain: got %d %d", start, end)
	}

	// plain scalar with trailing non-delimiter -> ambiguous command scalar
	data3 := []byte("cmd: helloX\n")
	node3 := &yaml.Node{
		Kind:   yaml.ScalarNode,
		Style:  0,
		Tag:    "!!str",
		Value:  "hello",
		Line:   1,
		Column: 6,
	}
	if _, _, err := commandSourceRange(data3, node3); err == nil || !strings.Contains(err.Error(), "ambiguous command scalar") {
		t.Fatalf("plain ambiguous missing: got err %v", err)
	}

	// unsupported style -> unsupported command scalar style
	node4 := &yaml.Node{
		Kind:   yaml.ScalarNode,
		Style:  yaml.LiteralStyle,
		Tag:    "!!str",
		Value:  "hello",
		Line:   1,
		Column: 6,
	}
	if _, _, err := commandSourceRange(data2, node4); err == nil || !strings.Contains(err.Error(), "unsupported command scalar style") {
		t.Fatalf("unsupported style missing: got err %v", err)
	}

	// value contains newline -> unsupported command scalar
	node5 := &yaml.Node{
		Kind:   yaml.ScalarNode,
		Style:  0,
		Tag:    "!!str",
		Value:  "he\nllo",
		Line:   1,
		Column: 6,
	}
	if _, _, err := commandSourceRange(data2, node5); err == nil || !strings.Contains(err.Error(), "unsupported command scalar") {
		t.Fatalf("value newline missing: got err %v", err)
	}

	// column beyond line -> invalid YAML source location
	node6 := &yaml.Node{
		Kind:   yaml.ScalarNode,
		Style:  0,
		Tag:    "!!str",
		Value:  "hello",
		Line:   1,
		Column: 100,
	}
	if _, _, err := commandSourceRange(data2, node6); err == nil || !strings.Contains(err.Error(), "invalid YAML source location") {
		t.Fatalf("column beyond missing: got err %v", err)
	}
}
