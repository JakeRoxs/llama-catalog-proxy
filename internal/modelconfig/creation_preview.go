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
	if !validCreationIdentity(targetID, modelID, templateID) {
		return CreationPreview{}, errors.New("invalid target or model identity")
	}
	if input.TemplateCommand != "" || len(input.TemplateEnvironment) != 0 {
		return CreationPreview{}, errors.New("template must be selected from the configuration")
	}
	root, models, err := creationRootAndModels(data)
	if err != nil {
		return CreationPreview{}, err
	}
	if _, err := lookupMappingValue(models, modelID); err != nil {
		return CreationPreview{}, err
	}
	if err := creationNamespaceConflicts(root, modelID); err != nil {
		return CreationPreview{}, err
	}
	selected, err := creationTemplateSelection(models, modelID, templateID)
	if err != nil {
		return CreationPreview{}, err
	}
	var applyErr error
	input, applyErr = applyTemplate(input, selected, templateID)
	if applyErr != nil {
		return CreationPreview{}, applyErr
	}
	command, err := BuildCreationCommand(input)
	if err != nil {
		return CreationPreview{}, err
	}
	offset, indent, newline, err := creationInsertion(data, root, models)
	if err != nil {
		return CreationPreview{}, err
	}
	entry := creationEntry(indent, newline, modelID, command)
	if len(data)+len(entry) > maxConfigBytes {
		return CreationPreview{}, errors.New("proposed configuration exceeds size limit")
	}
	candidate := buildCandidate(data, offset, entry)
	newRoot, err := parseConfigDocument(candidate)
	if err != nil {
		return CreationPreview{}, errors.New("generated configuration is invalid")
	}
	if !candidateContainsModel(newRoot, modelID) {
		return CreationPreview{}, errors.New("generated configuration is invalid")
	}
	return CreationPreview{
		TargetID: targetID, ModelID: modelID, TemplateID: templateID,
		Revision: sourceRevision(data), CandidateRevision: sourceRevision(candidate), Command: command, Artifact: "not_checked",
		Validation: "yaml_syntax_and_supported_command_only", candidate: candidate,
	}, nil
}

// validCreationIdentity validates the target, model, and template identities in
// the exact order required by PreviewCreationDocument.
func validCreationIdentity(targetID, modelID, templateID string) bool {
	if !identity.ValidNodeID(targetID) || targetID == "." || targetID == ".." {
		return false
	}
	if !validCreationModelID(modelID) {
		return false
	}
	if templateID != "" && !validCreationModelID(templateID) {
		return false
	}
	return true
}

// creationRootAndModels parses the configuration and returns the root and the
// block models mapping, rejecting flow-style or empty models mappings.
func creationRootAndModels(data []byte) (*yaml.Node, *yaml.Node, error) {
	root, err := parseConfigDocument(data)
	if err != nil {
		return nil, nil, err
	}
	models, err := mappingValue(root, "models")
	if err != nil {
		return nil, nil, err
	}
	if root.Style&yaml.FlowStyle != 0 || models.Style&yaml.FlowStyle != 0 || len(models.Content) == 0 {
		return nil, nil, errors.New("new entries require a nonempty block models mapping")
	}
	return root, models, nil
}

// creationTemplateSelection walks the models mapping to detect identity conflicts
// against aliases and filters, and to locate the template entry when one is
// requested. It returns the selected template entry (or nil) and an error.
func creationTemplateSelection(models *yaml.Node, modelID, templateID string) (*yaml.Node, error) {
	var selected *yaml.Node
	for i := 0; i < len(models.Content); i += 2 {
		key, entry := models.Content[i], models.Content[i+1]
		if key.Value == modelID {
			return nil, errors.New("model identity already exists")
		}
		if err := creationAliasConflicts(entry, modelID); err != nil {
			return nil, err
		}
		if err := creationFilterConflicts(entry, modelID); err != nil {
			return nil, err
		}
		if key.Value == templateID {
			selected = entry
		}
	}
	return selected, nil
}

// creationAliasConflicts checks whether the model ID conflicts with any alias
// declared on a single candidate model entry.
func creationAliasConflicts(entry *yaml.Node, modelID string) error {
	aliases, err := lookupMappingValue(entry, "aliases")
	if err != nil {
		return err
	}
	if aliases == nil {
		return nil
	}
	values, err := creationStringList(aliases)
	if err != nil {
		return errors.New("unsupported model aliases")
	}
	for _, alias := range values {
		if alias == modelID {
			return errors.New("model identity conflicts with an alias")
		}
	}
	return nil
}

// creationFilterConflicts checks whether the model ID conflicts with a filter
// alias declared on a single candidate model entry.
func creationFilterConflicts(entry *yaml.Node, modelID string) error {
	filters, err := lookupMappingValue(entry, "filters")
	if err != nil {
		return err
	}
	if filters == nil {
		return nil
	}
	byID, err := lookupMappingValue(filters, "setParamsByID")
	if err != nil {
		return errors.New("unsupported model filters")
	}
	if byID == nil {
		return nil
	}
	conflict, err := lookupMappingValue(byID, modelID)
	if err != nil {
		return errors.New("unsupported model filters")
	}
	if conflict != nil {
		return errors.New("model identity conflicts with a filter alias")
	}
	return nil
}

// applyTemplate copies the command and environment from the selected template
// entry into the input when a template is requested.
func applyTemplate(input CreationCommandInput, selected *yaml.Node, templateID string) (CreationCommandInput, error) {
	if templateID != "" {
		if selected == nil {
			return input, errors.New("template model not found")
		}
		var err error
		input.TemplateCommand, input.TemplateEnvironment, err = creationTemplateEntry(selected)
		if err != nil {
			return input, err
		}
	}
	return input, nil
}

// creationEntry renders the YAML entry bytes for a new model command.
func creationEntry(indent, newline, modelID string, command CreationCommandReview) string {
	var entry strings.Builder
	entry.WriteString(indent + strconv.Quote(modelID) + ":" + newline)
	entry.WriteString(indent + "  cmd: " + strconv.Quote(command.command) + newline)
	if len(command.DeviceEnvironment) != 0 {
		entry.WriteString(indent + "  env:" + newline)
		for _, env := range command.DeviceEnvironment {
			entry.WriteString(indent + "    - " + strconv.Quote(env) + newline)
		}
	}
	return entry.String()
}

// buildCandidate assembles the candidate configuration by inserting the entry
// at the given offset into the source bytes.
func buildCandidate(data []byte, offset int, entry string) []byte {
	candidate := make([]byte, 0, len(data)+len(entry))
	candidate = append(candidate, data[:offset]...)
	candidate = append(candidate, entry...)
	candidate = append(candidate, data[offset:]...)
	return candidate
}

// candidateContainsModel reports whether the parsed candidate contains the
// requested model in its models mapping.
func candidateContainsModel(newRoot *yaml.Node, modelID string) bool {
	newModels, err := mappingValue(newRoot, "models")
	if err == nil {
		_, err = mappingValue(newModels, modelID)
	}
	return err == nil
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
	modelsKey, err := creationModelsKey(root)
	if err != nil {
		return 0, "", "", err
	}
	first := models.Content[0]
	if first.Line <= modelsKey.Line || first.Column <= modelsKey.Column {
		return 0, "", "", errors.New("unsupported models mapping layout")
	}
	if err := creationModelKeyLayout(models, first); err != nil {
		return 0, "", "", err
	}
	lineStart, err := sourceLineStart(data, first.Line)
	if err != nil {
		return 0, "", "", err
	}
	indent := strings.Repeat(" ", first.Column-1)
	if lineStart+len(indent) > len(data) || string(data[lineStart:lineStart+len(indent)]) != indent {
		return 0, "", "", errors.New("unsupported model indentation")
	}
	newline := "\n"
	if lineStart >= 2 && data[lineStart-2] == '\r' {
		newline = "\r\n"
	}
	offset, err := creationPrecedingOffset(data, lineStart, first.Line, modelsKey.Line)
	if err != nil {
		return 0, "", "", err
	}
	return offset, indent, newline, nil
}

// creationModelsKey locates the models mapping key in the root document.
func creationModelsKey(root *yaml.Node) (*yaml.Node, error) {
	for i := 0; i < len(root.Content); i += 2 {
		if root.Content[i].Value == "models" {
			return root.Content[i], nil
		}
	}
	return nil, errors.New("unsupported models mapping layout")
}

// creationModelKeyLayout validates that every model key uses the same column
// and has no anchor.
func creationModelKeyLayout(models *yaml.Node, first *yaml.Node) error {
	for i := 0; i < len(models.Content); i += 2 {
		if models.Content[i].Column != first.Column || models.Content[i].Anchor != "" {
			return errors.New("unsupported model key layout")
		}
	}
	return nil
}

// creationPrecedingOffset scans backward over blank and comment lines to find
// the insertion offset just before the first non-blank, non-comment line.
func creationPrecedingOffset(data []byte, lineStart, firstLine, modelsKeyLine int) (int, error) {
	offset := lineStart
	for line := firstLine - 1; line > modelsKeyLine; line-- {
		end := offset - 1
		start := bytes.LastIndexByte(data[:end], '\n') + 1
		content := strings.TrimSpace(string(data[start:end]))
		if content != "" && !strings.HasPrefix(content, "#") {
			break
		}
		offset = start
	}
	return offset, nil
}
