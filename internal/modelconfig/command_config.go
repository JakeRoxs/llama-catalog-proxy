package modelconfig

import (
	"bytes"
	"errors"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

type mappedCommand struct {
	text   string
	source []int
}

type configMacro struct {
	name  string
	value string
}

func commandForModel(data []byte, root, model *yaml.Node, modelID string, command *yaml.Node) (mappedCommand, error) {
	mapped, err := mapCommandScalar(data, command)
	if err != nil {
		return mappedCommand{}, err
	}
	mapped = stripMappedCommandComments(mapped)
	macros, err := commandMacros(root, model, modelID)
	if err != nil {
		return mappedCommand{}, err
	}
	for i := len(macros) - 1; i >= 0; i-- {
		mapped, err = replaceMappedMacro(mapped, macros[i].name, macros[i].value)
		if err != nil {
			return mappedCommand{}, err
		}
	}
	mapped = stripMappedCommandComments(mapped)
	return mapped, nil
}

func commandMacros(root, model *yaml.Node, modelID string) ([]configMacro, error) {
	global, err := macroDefinitions(root, "macros")
	if err != nil {
		return nil, err
	}
	local, err := macroDefinitions(model, "macros")
	if err != nil {
		return nil, err
	}
	result := []configMacro{{name: "MODEL_ID", value: modelID}}
	positions := make(map[string]int, len(global)+len(local)+1)
	positions["MODEL_ID"] = 0
	for _, macro := range global {
		if macro.name == "MODEL_ID" || macro.name == "PORT" || macro.name == "PID" {
			return nil, errors.New("reserved command macro name")
		}
		positions[macro.name] = len(result)
		result = append(result, macro)
	}
	for _, macro := range local {
		if macro.name == "MODEL_ID" || macro.name == "PORT" || macro.name == "PID" {
			return nil, errors.New("reserved command macro name")
		}
		if index, ok := positions[macro.name]; ok {
			result[index] = macro
			continue
		}
		positions[macro.name] = len(result)
		result = append(result, macro)
	}
	return result, nil
}

func macroDefinitions(parent *yaml.Node, key string) ([]configMacro, error) {
	node, err := lookupMappingValue(parent, key)
	if err != nil || node == nil {
		return nil, err
	}
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" || node.Anchor != "" || len(node.Content)%2 != 0 {
		return nil, errors.New("unsupported command macro mapping")
	}
	macros := make([]configMacro, 0, len(node.Content)/2)
	seen := make(map[string]bool, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		name, value := node.Content[i], node.Content[i+1]
		if name.Kind != yaml.ScalarNode || name.Tag != "!!str" || !validMacroName(name.Value) || seen[name.Value] ||
			value.Kind != yaml.ScalarNode || value.Anchor != "" || value.Tag != "!!str" && value.Tag != "!!int" && value.Tag != "!!bool" {
			return nil, errors.New("unsupported command macro definition")
		}
		seen[name.Value] = true
		macros = append(macros, configMacro{name: name.Value, value: value.Value})
	}
	return macros, nil
}

func validMacroName(name string) bool {
	if name == "" || len(name) >= 64 {
		return false
	}
	for i := range len(name) {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func replaceMappedMacro(input mappedCommand, name, value string) (mappedCommand, error) {
	needle := "${" + name + "}"
	if !strings.Contains(input.text, needle) {
		return input, nil
	}
	count := strings.Count(input.text, needle)
	if count > maxCommandBytes || len(value) > maxCommandBytes || len(input.text)+count*(len(value)-len(needle)) > maxCommandBytes {
		return mappedCommand{}, errors.New("expanded command exceeds size limit")
	}
	var text strings.Builder
	var source []int
	for offset := 0; offset < len(input.text); {
		index := strings.Index(input.text[offset:], needle)
		if index < 0 {
			text.WriteString(input.text[offset:])
			source = append(source, input.source[offset:]...)
			break
		}
		index += offset
		text.WriteString(input.text[offset:index])
		source = append(source, input.source[offset:index]...)
		text.WriteString(value)
		for range len(value) {
			source = append(source, -1)
		}
		offset = index + len(needle)
	}
	return mappedCommand{text: text.String(), source: source}, nil
}

func stripMappedCommandComments(input mappedCommand) mappedCommand {
	lines := strings.Split(input.text, "\n")
	var text strings.Builder
	var source []int
	lineStart := 0
	kept := 0
	for _, line := range lines {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			if kept != 0 {
				text.WriteByte('\n')
				source = append(source, -1)
			}
			text.WriteString(line)
			source = append(source, input.source[lineStart:lineStart+len(line)]...)
			kept++
		}
		lineStart += len(line) + 1
	}
	return mappedCommand{text: text.String(), source: source}
}

func mapCommandScalar(data []byte, node *yaml.Node) (mappedCommand, error) {
	if node == nil || node.Kind != yaml.ScalarNode || node.Tag != "!!str" || node.Anchor != "" {
		return mappedCommand{}, errors.New("unsupported command representation")
	}
	if node.Style != yaml.FoldedStyle {
		start, end, err := commandSourceRange(data, node)
		if err != nil {
			return mappedCommand{}, err
		}
		source := make([]int, end-start)
		for i := range source {
			source[i] = start + i
		}
		return mappedCommand{text: node.Value, source: source}, nil
	}
	return mapFoldedCommandScalar(data, node)
}

func mapFoldedCommandScalar(data []byte, node *yaml.Node) (mappedCommand, error) {
	if node.Line < 1 || node.Line > bytes.Count(data, []byte{'\n'})+1 {
		return mappedCommand{}, errors.New("invalid command scalar location")
	}
	lineStart := 0
	for line := 1; line < node.Line; line++ {
		i := bytes.IndexByte(data[lineStart:], '\n')
		if i < 0 {
			return mappedCommand{}, errors.New("invalid command scalar location")
		}
		lineStart += i + 1
	}
	headerEnd := bytes.IndexByte(data[lineStart:], '\n')
	if headerEnd < 0 {
		return mappedCommand{}, errors.New("unsupported folded command scalar")
	}
	header := data[lineStart : lineStart+headerEnd]
	headerIndent := 0
	for headerIndent < len(header) && header[headerIndent] == ' ' {
		headerIndent++
	}
	position := lineStart + headerEnd + 1
	contentIndent := -1
	var decoded strings.Builder
	var sources []int
	var blankAfterContent bool
	for position < len(data) {
		lineEnd := bytes.IndexByte(data[position:], '\n')
		if lineEnd < 0 {
			lineEnd = len(data) - position
		}
		line := data[position : position+lineEnd]
		if bytes.ContainsRune(line, '\r') {
			return mappedCommand{}, errors.New("unsupported folded command line ending")
		}
		indent := 0
		for indent < len(line) && line[indent] == ' ' {
			indent++
		}
		blank := indent == len(line)
		if !blank && indent <= headerIndent {
			break
		}
		if blank {
			blankAfterContent = blankAfterContent || contentIndent >= 0
		} else {
			if blankAfterContent {
				// Blank-line folding changes command token boundaries; fail closed.
				return mappedCommand{}, errors.New("unsupported folded command scalar")
			}
			if contentIndent < 0 {
				contentIndent = indent
			}
			if indent != contentIndent {
				return mappedCommand{}, errors.New("unsupported folded command indentation")
			}
			if decoded.Len() != 0 {
				decoded.WriteByte(' ')
				sources = append(sources, -1)
			}
			decoded.Write(line[contentIndent:])
			for index := contentIndent; index < len(line); index++ {
				sources = append(sources, position+index)
			}
		}
		position += lineEnd
		if position < len(data) {
			position++
		}
	}
	value := decoded.String()
	if node.Value != value && node.Value != value+"\n" {
		return mappedCommand{}, errors.New("unsupported folded command scalar")
	}
	if len(value) == 0 || len(value) > maxCommandBytes || len(sources) != len(value) || !utf8.ValidString(value) {
		return mappedCommand{}, errors.New("unsupported folded command scalar")
	}
	return mappedCommand{text: value, source: sources}, nil
}

func commandSourceRangeForToken(command mappedCommand, token commandToken) (int, int, bool) {
	if token.start < 0 || token.end <= token.start || token.end > len(command.source) {
		return 0, 0, false
	}
	start := command.source[token.start]
	if start < 0 {
		return 0, 0, false
	}
	for i := token.start + 1; i < token.end; i++ {
		if command.source[i] != start+i-token.start {
			return 0, 0, false
		}
	}
	return start, start + token.end - token.start, true
}

func expandCommandForModel(data []byte, root, model *yaml.Node, modelID string, command *yaml.Node) (mappedCommand, error) {
	return commandForModel(data, root, model, modelID, command)
}
