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
	command, _, _, err := findCommand(targetID, modelID, data)
	if err != nil {
		return SettingsSnapshot{}, err
	}
	settings, err := readCommandSettings(command.Value)
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
	command, start, end, err := findCommand(targetID, modelID, data)
	if err != nil {
		return Preview{}, err
	}
	rewritten, changes, err := rewriteCommand(command.Value, settings)
	if err != nil {
		return Preview{}, err
	}
	candidate := make([]byte, 0, len(data)-(end-start)+len(rewritten))
	candidate = append(candidate, data[:start]...)
	candidate = append(candidate, rewritten...)
	candidate = append(candidate, data[end:]...)
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

func findCommand(targetID, modelID string, data []byte) (*yaml.Node, int, int, error) {
	if !identity.ValidNodeID(targetID) || targetID == "." || targetID == ".." ||
		modelID == "" || !identity.ValidModelComponent(modelID) {
		return nil, 0, 0, errors.New("invalid target or model identity")
	}
	root, err := parseConfigDocument(data)
	if err != nil {
		return nil, 0, 0, err
	}
	models, err := mappingValue(root, "models")
	if err != nil {
		return nil, 0, 0, err
	}
	model, err := mappingValue(models, modelID)
	if err != nil {
		return nil, 0, 0, err
	}
	command, err := mappingValue(model, "cmd")
	if err != nil {
		return nil, 0, 0, err
	}
	if command.Kind != yaml.ScalarNode || command.Tag != "!!str" || command.Anchor != "" {
		return nil, 0, 0, errors.New("unsupported command representation")
	}
	start, end, err := commandSourceRange(data, command)
	if err != nil {
		return nil, 0, 0, err
	}
	return command, start, end, nil
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
	lineStart := 0
	for line := 1; line < node.Line; line++ {
		i := bytes.IndexByte(data[lineStart:], '\n')
		if i < 0 {
			return 0, 0, errors.New("invalid YAML source location")
		}
		lineStart += i + 1
	}
	offset := lineStart
	for column := 1; column < node.Column; column++ {
		if offset >= len(data) || data[offset] == '\n' {
			return 0, 0, errors.New("invalid YAML source location")
		}
		_, size := utf8.DecodeRune(data[offset:])
		offset += size
	}
	quote := byte(0)
	switch node.Style {
	case 0:
	case yaml.SingleQuotedStyle:
		quote = '\''
	case yaml.DoubleQuotedStyle:
		quote = '"'
	default:
		return 0, 0, errors.New("unsupported command scalar style")
	}
	if quote != 0 {
		if offset >= len(data) || data[offset] != quote {
			return 0, 0, errors.New("unsupported command scalar")
		}
		offset++
	}
	end := offset + len(node.Value)
	if end > len(data) || !bytes.Equal(data[offset:end], []byte(node.Value)) {
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
