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
	lineStart, err := advanceToFoldedLine(data, 0, node.Line)
	if err != nil {
		return mappedCommand{}, err
	}
	value, sources, err := foldScalarContent(data, lineStart)
	if err != nil {
		return mappedCommand{}, err
	}
	if node.Value != value && node.Value != value+"\n" {
		return mappedCommand{}, errors.New("unsupported folded command scalar")
	}
	if len(value) == 0 || len(value) > maxCommandBytes || len(sources) != len(value) || !utf8.ValidString(value) {
		return mappedCommand{}, errors.New("unsupported folded command scalar")
	}
	return mappedCommand{text: value, source: sources}, nil
}

// advanceToFoldedLine advances lineStart to the first byte of the target line
// (1-indexed) by scanning for newline separators. An error is returned when the
// target line lies beyond the data.
func advanceToFoldedLine(data []byte, lineStart, targetLine int) (int, error) {
	for line := 1; line < targetLine; line++ {
		i := bytes.IndexByte(data[lineStart:], '\n')
		if i < 0 {
			return 0, errors.New("invalid command scalar location")
		}
		lineStart += i + 1
	}
	return lineStart, nil
}

// foldedState accumulates the decoded value and source positions while folding a
// multi-line scalar block.
type foldedState struct {
	contentIndent     int
	blankAfterContent bool
	decoded           strings.Builder
	sources           []int
}

// foldScalarContent folds the content of a folded scalar block starting at
// lineStart (the first byte after the header's terminating newline).
func foldScalarContent(data []byte, lineStart int) (string, []int, error) {
	headerEnd := bytes.IndexByte(data[lineStart:], '\n')
	if headerEnd < 0 {
		return "", nil, errors.New("unsupported folded command scalar")
	}
	header := data[lineStart : lineStart+headerEnd]
	headerIndent := 0
	for headerIndent < len(header) && header[headerIndent] == ' ' {
		headerIndent++
	}
	position := lineStart + headerEnd + 1
	state := foldedState{
		contentIndent: -1,
		sources:       make([]int, 0),
	}
	for position < len(data) {
		lineEnd := bytes.IndexByte(data[position:], '\n')
		if lineEnd < 0 {
			lineEnd = len(data) - position
		}
		line := data[position : position+lineEnd]
		stop, err := state.processFoldedLine(line, position, lineEnd, headerIndent)
		if err != nil {
			return "", nil, err
		}
		if stop {
			break
		}
		position += lineEnd
		if position < len(data) {
			position++
		}
	}
	return state.decoded.String(), state.sources, nil
}

// processFoldedLine processes one content line of a folded scalar. It returns
// stop=true when the content block ends at a less-indented non-blank line.
func (s *foldedState) processFoldedLine(line []byte, position, lineEnd, headerIndent int) (bool, error) {
	if bytes.ContainsRune(line, '\r') {
		return false, errors.New("unsupported folded command line ending")
	}
	indent := 0
	for indent < len(line) && line[indent] == ' ' {
		indent++
	}
	blank := indent == len(line)
	if !blank && indent <= headerIndent {
		return true, nil
	}
	if blank {
		s.blankAfterContent = s.blankAfterContent || s.contentIndent >= 0
		return false, nil
	}
	if s.blankAfterContent {
		// Blank-line folding changes command token boundaries; fail closed.
		return false, errors.New("unsupported folded command scalar")
	}
	if s.contentIndent < 0 {
		s.contentIndent = indent
	}
	if indent != s.contentIndent {
		return false, errors.New("unsupported folded command indentation")
	}
	if s.decoded.Len() != 0 {
		s.decoded.WriteByte(' ')
		s.sources = append(s.sources, -1)
	}
	s.decoded.Write(line[s.contentIndent:])
	for index := s.contentIndent; index < len(line); index++ {
		s.sources = append(s.sources, position+index)
	}
	return false, nil
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
