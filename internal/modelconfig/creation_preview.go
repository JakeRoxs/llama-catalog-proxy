package modelconfig

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"unicode"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"gopkg.in/yaml.v3"
)

// CreationPreview is a read-only review, not authorization to write the configuration.
type CreationPreview struct {
	TargetID          string                `json:"target_id"`
	TargetFingerprint string                `json:"target_fingerprint,omitempty"`
	ModelID           string                `json:"model_id"`
	TemplateID        string                `json:"template_id,omitempty"`
	Revision          string                `json:"revision"`
	CandidateRevision string                `json:"candidate_revision"`
	Command           CreationCommandReview `json:"command"`
	Artifact          string                `json:"artifact"`
	Validation        string                `json:"validation"`
	candidate         []byte
}

// PreviewCreationDocument prepares a new entry without filesystem or backend access.
// Template data is always taken from the selected entry in the supplied document.
func PreviewCreationDocument(targetID, modelID, templateID string, data []byte, input CreationCommandInput) (CreationPreview, error) {
	if !identity.ValidNodeID(targetID) || targetID == "." || targetID == ".." ||
		!validCreationModelID(modelID) || templateID != "" && !validCreationModelID(templateID) {
		return CreationPreview{}, errors.New("invalid target or model identity")
	}
	if input.TemplateCommand != "" || len(input.TemplateEnvironment) != 0 {
		return CreationPreview{}, errors.New("template must be selected from the configuration")
	}
	root, err := parseConfigDocument(data)
	if err != nil {
		return CreationPreview{}, err
	}
	models, err := mappingValue(root, "models")
	if err != nil {
		return CreationPreview{}, err
	}
	if root.Style&yaml.FlowStyle != 0 || models.Style&yaml.FlowStyle != 0 || len(models.Content) == 0 {
		return CreationPreview{}, errors.New("new entries require a nonempty block models mapping")
	}
	if _, err := lookupMappingValue(models, modelID); err != nil {
		return CreationPreview{}, err
	}
	if err := creationNamespaceConflicts(root, modelID); err != nil {
		return CreationPreview{}, err
	}
	var selected *yaml.Node
	for i := 0; i < len(models.Content); i += 2 {
		key, entry := models.Content[i], models.Content[i+1]
		if key.Value == modelID {
			return CreationPreview{}, errors.New("model identity already exists")
		}
		aliases, err := lookupMappingValue(entry, "aliases")
		if err != nil {
			return CreationPreview{}, err
		}
		if aliases != nil {
			values, err := creationStringList(aliases)
			if err != nil {
				return CreationPreview{}, errors.New("unsupported model aliases")
			}
			for _, alias := range values {
				if alias == modelID {
					return CreationPreview{}, errors.New("model identity conflicts with an alias")
				}
			}
		}
		filters, err := lookupMappingValue(entry, "filters")
		if err != nil {
			return CreationPreview{}, err
		}
		if filters != nil {
			byID, err := lookupMappingValue(filters, "setParamsByID")
			if err != nil {
				return CreationPreview{}, errors.New("unsupported model filters")
			}
			if byID != nil {
				conflict, err := lookupMappingValue(byID, modelID)
				if err != nil {
					return CreationPreview{}, errors.New("unsupported model filters")
				}
				if conflict != nil {
					return CreationPreview{}, errors.New("model identity conflicts with a filter alias")
				}
			}
		}
		if key.Value == templateID {
			selected = entry
		}
	}
	if templateID != "" {
		if selected == nil {
			return CreationPreview{}, errors.New("template model not found")
		}
		input.TemplateCommand, input.TemplateEnvironment, err = creationTemplateEntry(selected)
		if err != nil {
			return CreationPreview{}, err
		}
	}
	command, err := BuildCreationCommand(input)
	if err != nil {
		return CreationPreview{}, err
	}
	offset, indent, newline, err := creationInsertion(data, root, models)
	if err != nil {
		return CreationPreview{}, err
	}
	var entry strings.Builder
	entry.WriteString(indent + strconv.Quote(modelID) + ":" + newline)
	entry.WriteString(indent + "  cmd: " + strconv.Quote(command.command) + newline)
	if len(command.DeviceEnvironment) != 0 {
		entry.WriteString(indent + "  env:" + newline)
		for _, env := range command.DeviceEnvironment {
			entry.WriteString(indent + "    - " + strconv.Quote(env) + newline)
		}
	}
	if len(data)+entry.Len() > maxConfigBytes {
		return CreationPreview{}, errors.New("proposed configuration exceeds size limit")
	}
	candidate := make([]byte, 0, len(data)+entry.Len())
	candidate = append(candidate, data[:offset]...)
	candidate = append(candidate, entry.String()...)
	candidate = append(candidate, data[offset:]...)
	newRoot, err := parseConfigDocument(candidate)
	if err != nil {
		return CreationPreview{}, errors.New("generated configuration is invalid")
	}
	newModels, err := mappingValue(newRoot, "models")
	if err == nil {
		_, err = mappingValue(newModels, modelID)
	}
	if err != nil {
		return CreationPreview{}, errors.New("generated configuration is invalid")
	}
	return CreationPreview{
		TargetID: targetID, ModelID: modelID, TemplateID: templateID,
		Revision: sourceRevision(data), CandidateRevision: sourceRevision(candidate), Command: command, Artifact: "not_checked",
		Validation: "yaml_syntax_and_supported_command_only", candidate: candidate,
	}, nil
}

func creationNamespaceConflicts(root *yaml.Node, modelID string) error {
	selectors, err := lookupMappingValue(root, "selectors")
	if err != nil {
		return err
	}
	if selectors != nil {
		conflict, err := lookupMappingValue(selectors, modelID)
		if err != nil {
			return errors.New("unsupported selector mapping")
		}
		if conflict != nil {
			return errors.New("model identity conflicts with a selector")
		}
	}
	peers, err := lookupMappingValue(root, "peers")
	if err != nil || peers == nil {
		return err
	}
	if err := validateMapping(peers); err != nil {
		return errors.New("unsupported peer mapping")
	}
	for i := 0; i < len(peers.Content); i += 2 {
		peer, entry := peers.Content[i], peers.Content[i+1]
		models, err := lookupMappingValue(entry, "models")
		if err != nil {
			return errors.New("unsupported peer models")
		}
		if models == nil {
			continue
		}
		values, err := creationStringList(models)
		if err != nil {
			return errors.New("unsupported peer models")
		}
		for _, value := range values {
			if peer.Value+"/"+value == modelID {
				return errors.New("model identity conflicts with a peer model")
			}
		}
	}
	return nil
}

func validCreationModelID(id string) bool {
	if id == "" || id == "<<" || !identity.ValidModelComponent(id) || strings.Contains(id, "${") {
		return false
	}
	for _, c := range id {
		if unicode.IsControl(c) {
			return false
		}
	}
	return true
}

func creationTemplateEntry(entry *yaml.Node) (string, []string, error) {
	command, err := mappingValue(entry, "cmd")
	if err != nil || command.Kind != yaml.ScalarNode || command.Tag != "!!str" || command.Anchor != "" || command.Value == "" {
		return "", nil, errors.New("unsupported template command")
	}
	var env []string
	for i := 0; i < len(entry.Content); i += 2 {
		key, value := entry.Content[i], entry.Content[i+1]
		switch key.Value {
		case "cmd":
		case "env":
			env, err = creationStringList(value)
			if err != nil {
				return "", nil, errors.New("unsupported template environment")
			}
		default:
			return "", nil, errors.New("template contains unsupported model fields")
		}
	}
	return command.Value, env, nil
}

func creationStringList(node *yaml.Node) ([]string, error) {
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" || node.Anchor != "" {
		return nil, errors.New("unsupported string list")
	}
	values := make([]string, 0, len(node.Content))
	for _, item := range node.Content {
		if item.Kind != yaml.ScalarNode || item.Tag != "!!str" || item.Anchor != "" {
			return nil, errors.New("unsupported string list")
		}
		values = append(values, item.Value)
	}
	return values, nil
}

func creationInsertion(data []byte, root, models *yaml.Node) (int, string, string, error) {
	var modelsKey *yaml.Node
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == "models" {
			modelsKey = root.Content[i]
			break
		}
	}
	first := models.Content[0]
	if modelsKey == nil || first.Line <= modelsKey.Line || first.Column <= modelsKey.Column {
		return 0, "", "", errors.New("unsupported models mapping layout")
	}
	for i := 0; i < len(models.Content); i += 2 {
		if models.Content[i].Column != first.Column || models.Content[i].Anchor != "" {
			return 0, "", "", errors.New("unsupported model key layout")
		}
	}
	lineStart := 0
	for line := 1; line < first.Line; line++ {
		index := bytes.IndexByte(data[lineStart:], '\n')
		if index < 0 {
			return 0, "", "", errors.New("invalid YAML source location")
		}
		lineStart += index + 1
	}
	indent := strings.Repeat(" ", first.Column-1)
	if lineStart+len(indent) > len(data) || string(data[lineStart:lineStart+len(indent)]) != indent {
		return 0, "", "", errors.New("unsupported model indentation")
	}
	newline := "\n"
	if lineStart >= 2 && data[lineStart-2] == '\r' {
		newline = "\r\n"
	}
	offset := lineStart
	for line := first.Line - 1; line > modelsKey.Line; line-- {
		end := offset - 1
		start := bytes.LastIndexByte(data[:end], '\n') + 1
		content := strings.TrimSpace(string(data[start:end]))
		if content != "" && !strings.HasPrefix(content, "#") {
			break
		}
		offset = start
	}
	return offset, indent, newline, nil
}
