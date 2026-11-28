package modelconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"gopkg.in/yaml.v3"
)

const maxConfigBytes = 2 << 20

// Preview contains only the requested changes, never the source or candidate configuration.
// It is not an authorization to write the file.
type Preview struct {
	TargetID   string          `json:"target_id"`
	ModelID    string          `json:"model_id"`
	Revision   string          `json:"revision"`
	Changes    []SettingChange `json:"changes"`
	Validation string          `json:"validation"`
	candidate  []byte
}

// SettingsSnapshot describes configured values, not effective runtime settings.
type SettingsSnapshot struct {
	TargetID   string          `json:"target_id"`
	ModelID    string          `json:"model_id"`
	Revision   string          `json:"revision"`
	Settings   CommandSettings `json:"settings"`
	Validation string          `json:"validation"`
}

func ReadSettingsDocument(targetID, modelID string, data []byte) (SettingsSnapshot, error) {
	root, model, command, err := findCommandNodes(targetID, modelID, data)
	if err != nil {
		return SettingsSnapshot{}, err
	}
	mapped, err := expandCommandForModel(data, root, model, modelID, command)
	if err != nil {
		return SettingsSnapshot{}, err
	}
	settings, err := readCommandSettings(mapped.text)
	if err != nil {
		return SettingsSnapshot{}, err
	}
	return SettingsSnapshot{
		TargetID: targetID, ModelID: modelID, Revision: sourceRevision(data),
		Settings: settings, Validation: "yaml_syntax_and_supported_command_only",
	}, nil
}

// PreviewDocument edits one exact model command without performing filesystem access.
// The target identifier is a caller-provided identity, not a path.
func PreviewDocument(targetID, modelID string, data []byte, settings CommandSettings) (Preview, error) {
	root, model, command, err := findCommandNodes(targetID, modelID, data)
	if err != nil {
		return Preview{}, err
	}
	mapped, err := expandCommandForModel(data, root, model, modelID, command)
	if err != nil {
		return Preview{}, err
	}
	replacements, changes, err := planCommandRewrite(mapped.text, settings, func(option, value commandToken) bool {
		_, _, optionEditable := commandSourceRangeForToken(mapped, option)
		_, _, valueEditable := commandSourceRangeForToken(mapped, value)
		return optionEditable && valueEditable
	})
	if err != nil {
		return Preview{}, err
	}
	type rawReplacement struct {
		start, end int
		value      string
	}
	rawReplacements := make([]rawReplacement, 0, len(replacements))
	for _, replacement := range replacements {
		start, end, ok := commandSourceRangeForToken(mapped, replacement.token)
		if !ok || start < 0 || end > len(data) ||
			!bytes.Equal(data[start:end], []byte(mapped.text[replacement.token.start:replacement.token.end])) {
			return Preview{}, errors.New("requested setting source is not a directly editable model command value")
		}
		rawReplacements = append(rawReplacements, rawReplacement{start: start, end: end, value: replacement.value})
	}
	candidate := make([]byte, 0, len(data)+len(rawReplacements)*8)
	last := 0
	for _, replacement := range rawReplacements {
		if replacement.start < last {
			return Preview{}, errors.New("overlapping command settings")
		}
		candidate = append(candidate, data[last:replacement.start]...)
		candidate = append(candidate, replacement.value...)
		last = replacement.end
	}
	candidate = append(candidate, data[last:]...)
	if len(candidate) > maxConfigBytes {
		return Preview{}, errors.New("proposed configuration exceeds size limit")
	}
	return Preview{
		TargetID: targetID, ModelID: modelID, Revision: sourceRevision(data),
		Changes: changes, Validation: "yaml_syntax_and_supported_command_only",
		candidate: candidate,
	}, nil
}

func sourceRevision(data []byte) string {
	revision := sha256.Sum256(data)
	return hex.EncodeToString(revision[:])
}

func findCommandNodes(targetID, modelID string, data []byte) (*yaml.Node, *yaml.Node, *yaml.Node, error) {
	if !identity.ValidNodeID(targetID) || targetID == "." || targetID == ".." ||
		modelID == "" || !identity.ValidModelComponent(modelID) {
		return nil, nil, nil, errors.New("invalid target or model identity")
	}
	root, err := parseConfigDocument(data)
	if err != nil {
		return nil, nil, nil, err
	}
	models, err := mappingValue(root, "models")
	if err != nil {
		return nil, nil, nil, err
	}
	model, err := mappingValue(models, modelID)
	if err != nil {
		return nil, nil, nil, err
	}
	command, err := mappingValue(model, "cmd")
	if err != nil {
		return nil, nil, nil, err
	}
	if command.Kind != yaml.ScalarNode || command.Tag != "!!str" || command.Anchor != "" {
		return nil, nil, nil, errors.New("unsupported command representation")
	}
	return root, model, command, nil
}

func parseConfigDocument(data []byte) (*yaml.Node, error) {
	if len(data) == 0 || len(data) > maxConfigBytes || !utf8.Valid(data) {
		return nil, errors.New("invalid configuration size or encoding")
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, errors.New("invalid YAML configuration")
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("configuration must contain one YAML document")
	}
	if len(document.Content) != 1 {
		return nil, errors.New("configuration must be a mapping")
	}
	return document.Content[0], nil
}

func mappingValue(node *yaml.Node, key string) (*yaml.Node, error) {
	result, err := lookupMappingValue(node, key)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, errors.New("configuration target not found")
	}
	return result, nil
}

func lookupMappingValue(node *yaml.Node, key string) (*yaml.Node, error) {
	if err := validateMapping(node); err != nil {
		return nil, err
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1], nil
		}
	}
	return nil, nil
}

func validateMapping(node *yaml.Node) error {
	if node == nil || node.Kind != yaml.MappingNode || node.Tag != "!!map" || node.Anchor != "" {
		return errors.New("unsupported configuration mapping")
	}
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		k := node.Content[i]
		if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || k.Value == "<<" || seen[k.Value] {
			return errors.New("ambiguous configuration mapping")
		}
		seen[k.Value] = true
	}
	return nil
}

// commandSourceRange accepts only byte-exact, single-line scalars so changing
// their contents cannot reformat or alter any other part of the document.
func commandSourceRange(data []byte, node *yaml.Node) (int, int, error) {
	if node.Line < 1 || node.Column < 1 || strings.ContainsAny(node.Value, "\r\n") {
		return 0, 0, errors.New("unsupported command scalar")
	}
	lineStart, err := sourceLineStart(data, node.Line)
	if err != nil {
		return 0, 0, err
	}
	offset, err := sourceColumnOffset(data, lineStart, node.Column)
	if err != nil {
		return 0, 0, err
	}
	quote, err := scalarQuote(node.Style)
	if err != nil {
		return 0, 0, err
	}
	start, end, err := validateScalarBytes(data, offset, quote, node.Value)
	if err != nil {
		return 0, 0, err
	}
	return start, end, nil
}

// sourceLineStart returns the byte offset of the start of the given 1-based line
// by scanning for newlines. It preserves the original bytes.IndexByte line scan.
func sourceLineStart(data []byte, line int) (int, error) {
	lineStart := 0
	for i := 1; i < line; i++ {
		n := bytes.IndexByte(data[lineStart:], '\n')
		if n < 0 {
			return 0, errors.New("invalid YAML source location")
		}
		lineStart += n + 1
	}
	return lineStart, nil
}

// sourceColumnOffset advances offset by the given 1-based column, counting each
// UTF-8 rune as one column. It preserves the original UTF-8 rune column behavior.
func sourceColumnOffset(data []byte, offset, column int) (int, error) {
	for i := 1; i < column; i++ {
		if offset >= len(data) || data[offset] == '\n' {
			return 0, errors.New("invalid YAML source location")
		}
		_, size := utf8.DecodeRune(data[offset:])
		offset += size
	}
	return offset, nil
}

// scalarQuote returns the opening quote byte for the given YAML scalar style.
// Plain style (0) returns 0. Unsupported styles return an error.
func scalarQuote(style yaml.Style) (byte, error) {
	switch style {
	case 0:
		return 0, nil
	case yaml.SingleQuotedStyle:
		return '\'', nil
	case yaml.DoubleQuotedStyle:
		return '"', nil
	default:
		return 0, errors.New("unsupported command scalar style")
	}
}

// validateScalarBytes checks that the scalar bytes at offset match value exactly
// and that the closing quote (when quoted) or delimiter (when plain) is present.
// It returns the byte offset just after the opening quote and just after the
// scalar value.
func validateScalarBytes(data []byte, offset int, quote byte, value string) (int, int, error) {
	if quote != 0 {
		if offset >= len(data) || data[offset] != quote {
			return 0, 0, errors.New("unsupported command scalar")
		}
		offset++
	}
	end := offset + len(value)
	if end > len(data) || !bytes.Equal(data[offset:end], []byte(value)) {
		return 0, 0, errors.New("command scalar requires unsupported decoding")
	}
	if quote != 0 {
		if end >= len(data) || data[end] != quote {
			return 0, 0, errors.New("unsupported command scalar")
		}
	} else if end < len(data) && !strings.ContainsRune(" \t\r\n,}", rune(data[end])) {
		return 0, 0, errors.New("ambiguous command scalar")
	}
	return offset, end, nil
}
