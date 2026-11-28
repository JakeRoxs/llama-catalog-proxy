// Package modelconfig provides narrowly scoped llama-swap configuration editing.
package modelconfig

import (
	"errors"
	"path"
	"strconv"
	"strings"
)

const maxCommandBytes = 32 << 10

// CommandSettings contains only supported llama-server options. Nil leaves an option unchanged.
type CommandSettings struct {
	ContextSize *int32 `json:"context_size,omitempty"`
	Fit         *bool  `json:"fit,omitempty"`
	FitContext  *int32 `json:"fit_context,omitempty"`
	FitTarget   *int32 `json:"fit_target,omitempty"`
	CacheRAM    *int32 `json:"cache_ram,omitempty"`
}

// SettingChange contains only the edited option and its typed, non-secret values.
type SettingChange struct {
	Setting string `json:"setting"`
	Before  string `json:"before"`
	After   string `json:"after"`
}

type commandToken struct {
	text       string
	start, end int
}

type commandOption struct {
	name     string
	hasValue bool
}

type parsedOption struct {
	name   string
	option commandToken
	value  commandToken
}

type commandReplacement struct {
	token commandToken
	value string
}

// parseStepResult is the outcome of parsing a single command token. ok indicates
// a parsed option to collect; next is the index of the next token to process.
type parseStepResult struct {
	option parsedOption
	ok     bool
	next   int
	err    error
}

var commandOptions = map[string]commandOption{
	"-c": {"context", true}, "--ctx-size": {"context", true},
	"-fit": {"fit", true}, "--fit": {"fit", true},
	"-fitc": {"fit_context", true}, "--fit-ctx": {"fit_context", true},
	"-fitt": {"fit_target", true}, "--fit-target": {"fit_target", true},
	"-cram": {"cache_ram", true}, "--cache-ram": {"cache_ram", true},
	"--port": {"port", true}, "--host": {"host", true},
	"--parallel": {"parallel", true}, "--flash-attn": {"flash_attn", true},
	"--cache-type-k": {"cache_type_k", true}, "--cache-type-v": {"cache_type_v", true},
	"-lv": {"log_level", true}, "-m": {"model", true}, "--model": {"model", true},
	"-ngl": {"gpu_layers", true}, "--gpu-layers": {"gpu_layers", true}, "--n-gpu-layers": {"gpu_layers", true},
	"--reasoning-preserve": {"reasoning_preserve", false},
}

// RewriteCommand changes supported options in a simple direct llama-server
// invocation. It does not execute commands or expose unmodified arguments as
// a preview. Unsupported forms fail closed.
func RewriteCommand(command string, settings CommandSettings) (string, error) {
	rewritten, _, err := rewriteCommand(command, settings)
	return rewritten, err
}

func rewriteCommand(command string, settings CommandSettings) (string, []SettingChange, error) {
	replacements, changes, err := planCommandRewrite(command, settings, nil)
	if err != nil {
		return "", nil, err
	}
	return applyCommandReplacements(command, replacements), changes, nil
}

func planCommandRewrite(command string, settings CommandSettings, editable func(commandToken, commandToken) bool) ([]commandReplacement, []SettingChange, error) {
	requested, err := requestedSettings(settings)
	if err != nil {
		return nil, nil, err
	}
	if len(requested) == 0 {
		return nil, nil, errors.New("at least one command setting is required")
	}
	options, err := parseCommand(command)
	if err != nil {
		return nil, nil, err
	}
	var replacements []commandReplacement
	var changes []SettingChange
	for _, option := range options {
		if value, ok := requested[option.name]; ok {
			replacement, change, err := applyRequestedOption(option, value, editable)
			if err != nil {
				return nil, nil, err
			}
			replacements = append(replacements, replacement)
			changes = append(changes, change)
			delete(requested, option.name)
		}
	}
	if len(requested) != 0 {
		return nil, nil, errors.New("requested option is absent from command")
	}
	return replacements, changes, nil
}

func requestedSettings(settings CommandSettings) (map[string]string, error) {
	requested := make(map[string]string)
	if err := requestedNumericSetting(requested, "context", settings.ContextSize, 0, "context size must be non-negative"); err != nil {
		return nil, err
	}
	if settings.Fit != nil {
		requested["fit"] = "off"
		if *settings.Fit {
			requested["fit"] = "on"
		}
	}
	if err := requestedNumericSetting(requested, "fit_context", settings.FitContext, 0, "fit context must be non-negative"); err != nil {
		return nil, err
	}
	if err := requestedNumericSetting(requested, "fit_target", settings.FitTarget, 0, "fit target must be non-negative"); err != nil {
		return nil, err
	}
	if err := requestedNumericSetting(requested, "cache_ram", settings.CacheRAM, -1, "cache RAM must be at least -1"); err != nil {
		return nil, err
	}
	return requested, nil
}

func requestedNumericSetting(requested map[string]string, key string, value *int32, minValue int32, errMsg string) error {
	if value == nil {
		return nil
	}
	if *value < minValue {
		return errors.New(errMsg)
	}
	requested[key] = strconv.FormatInt(int64(*value), 10)
	return nil
}

func applyRequestedOption(option parsedOption, value string, editable func(commandToken, commandToken) bool) (commandReplacement, SettingChange, error) {
	if editable != nil && !editable(option.option, option.value) {
		return commandReplacement{}, SettingChange{}, errors.New("requested setting is defined by a shared macro and cannot be changed per model")
	}
	if err := validateExistingSetting(option.name, option.value.text); err != nil {
		return commandReplacement{}, SettingChange{}, err
	}
	return commandReplacement{token: option.value, value: value}, SettingChange{Setting: option.name, Before: option.value.text, After: value}, nil
}

func applyCommandReplacements(command string, replacements []commandReplacement) string {
	var result strings.Builder
	last := 0
	for _, change := range replacements {
		result.WriteString(command[last:change.token.start])
		result.WriteString(change.value)
		last = change.token.end
	}
	result.WriteString(command[last:])
	return result.String()
}

func parseCommand(command string) ([]parsedOption, error) {
	tokens, err := tokenizeCommand(command)
	if err != nil {
		return nil, err
	}
	if path.Base(tokens[0].text) != "llama-server" {
		return nil, errors.New("unsupported command executable")
	}
	seen := make(map[string]bool)
	var options []parsedOption
	for i := 1; i < len(tokens); {
		step := parseNextToken(command, tokens, i, seen)
		if step.err != nil {
			return nil, step.err
		}
		if step.ok {
			options = append(options, step.option)
		}
		i = step.next
	}
	return options, nil
}

func parseNextToken(command string, tokens []commandToken, i int, seen map[string]bool) parseStepResult {
	optionToken := tokens[i]
	optionName, attachedValue, attached := strings.Cut(optionToken.text, "=")
	option, ok := commandOptions[optionName]
	if !ok {
		return parseUnknownOption(optionName, attachedValue, attached, tokens, i)
	}
	if seen[option.name] {
		return parseStepResult{next: i + 1, err: errors.New("duplicate command option")}
	}
	seen[option.name] = true
	if !attached && !option.hasValue {
		return parseStepResult{next: i + 1}
	}
	valueToken, next, err := resolveValueToken(optionName, option, attached, attachedValue, optionToken, tokens, i)
	if err != nil {
		return parseStepResult{next: next, err: err}
	}
	if err := validateResolvedValue(command, option, valueToken); err != nil {
		return parseStepResult{next: next, err: err}
	}
	return parseStepResult{option: parsedOption{name: option.name, option: optionToken, value: valueToken}, ok: true, next: next}
}

func parseUnknownOption(optionName string, attachedValue string, attached bool, tokens []commandToken, i int) parseStepResult {
	if !strings.HasPrefix(optionName, "-") || optionName == "-" || optionName == "--" || optionName == "-1" {
		return parseStepResult{next: i + 1, err: errors.New("unsupported command argument")}
	}
	if attached {
		return parseStepResult{next: i + 1}
	}
	next := i + 1
	if i+1 < len(tokens) {
		nextToken := tokens[i+1].text
		if isKnownCommandOption(nextToken) {
			return parseStepResult{next: i + 1, err: errors.New("ambiguous unknown command option value")}
		}
		if !strings.HasPrefix(nextToken, "-") || nextToken == "-1" {
			next = i + 2
		}
	}
	return parseStepResult{next: next}
}

func resolveValueToken(optionName string, option commandOption, attached bool, attachedValue string, optionToken commandToken, tokens []commandToken, i int) (commandToken, int, error) {
	if attached {
		if !option.hasValue || attachedValue == "" {
			return commandToken{}, i + 1, errors.New("invalid or missing command option value")
		}
		return commandToken{text: attachedValue, start: optionToken.start + len(optionName) + 1, end: optionToken.end}, i + 1, nil
	}
	next := i + 1
	if next >= len(tokens) || strings.HasPrefix(tokens[next].text, "-") && tokens[next].text != "-1" {
		return commandToken{}, next, errors.New("invalid or missing command option value")
	}
	return tokens[next], next + 1, nil
}

func validateResolvedValue(command string, option commandOption, valueToken commandToken) error {
	if valueToken.start < len(command) && command[valueToken.start] == '\'' && option.name != "model" {
		return errors.New("unsupported quoted command value")
	}
	if option.name == "gpu_layers" && !validCreationValue(option.name, valueToken.text) {
		return errors.New("unsupported GPU layer setting")
	}
	if valueToken.text == "${PORT}" && option.name != "port" {
		return errors.New("unsupported command expansion")
	}
	return nil
}

func isKnownCommandOption(token string) bool {
	name, _, _ := strings.Cut(token, "=")
	_, ok := commandOptions[name]
	return ok
}

func readCommandSettings(command string) (CommandSettings, error) {
	options, err := parseCommand(command)
	if err != nil {
		return CommandSettings{}, err
	}
	var result CommandSettings
	for _, option := range options {
		switch option.name {
		case "context", "fit_context", "fit_target", "cache_ram", "fit":
			if err := validateExistingSetting(option.name, option.value.text); err != nil {
				return CommandSettings{}, err
			}
			if option.name == "fit" {
				value := option.value.text == "on"
				result.Fit = &value
				continue
			}
			n, err := strconv.ParseInt(option.value.text, 10, 32)
			if err != nil {
				return CommandSettings{}, errors.New("unsupported existing numeric setting")
			}
			value := int32(n)
			switch option.name {
			case "context":
				result.ContextSize = &value
			case "fit_context":
				result.FitContext = &value
			case "fit_target":
				result.FitTarget = &value
			case "cache_ram":
				result.CacheRAM = &value
			}
		}
	}
	return result, nil
}

func safeQuotedCommandValue(value string) bool {
	if !path.IsAbs(value) || path.Clean(value) != value || len(value) > 4096 {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || strings.ContainsRune("/ ._+()-", rune(c))) {
			return false
		}
	}
	return true
}

func validateExistingSetting(name, value string) error {
	if name == "fit" {
		if value == "on" || value == "off" {
			return nil
		}
		return errors.New("unsupported existing fit setting")
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 0 && !(name == "cache_ram" && n == -1) {
		return errors.New("unsupported existing numeric setting")
	}
	return nil
}

func tokenizeCommand(command string) ([]commandToken, error) {
	if len(command) == 0 || len(command) > maxCommandBytes {
		return nil, errors.New("invalid command length")
	}
	var tokens []commandToken
	for i := 0; i < len(command); {
		if command[i] == ' ' || command[i] == '\t' || command[i] == '\r' || command[i] == '\n' {
			i++
			continue
		}
		start := i
		var word string
		if command[i] == '\'' {
			var err error
			word, i, err = tokenizeQuoted(command, i)
			if err != nil {
				return nil, err
			}
		} else {
			word, i = tokenizeWord(command, i)
			if word != "${PORT}" {
				if err := validateWord(word); err != nil {
					return nil, err
				}
			}
		}
		tokens = append(tokens, commandToken{text: word, start: start, end: i})
		if len(tokens) > 128 {
			return nil, errors.New("too many command arguments")
		}
	}
	if len(tokens) == 0 {
		return nil, errors.New("empty command")
	}
	return tokens, nil
}

func tokenizeQuoted(command string, i int) (string, int, error) {
	i++
	end := strings.IndexByte(command[i:], '\'')
	if end < 0 {
		return "", i, errors.New("unsupported command quoting")
	}
	word := command[i : i+end]
	i += end + 1
	if i < len(command) && command[i] != ' ' && command[i] != '\t' && command[i] != '\r' && command[i] != '\n' {
		return "", i, errors.New("unsupported command quoting")
	}
	if !safeQuotedCommandValue(word) {
		return "", i, errors.New("unsupported quoted command value")
	}
	return word, i, nil
}

func tokenizeWord(command string, i int) (string, int) {
	start := i
	for i < len(command) && command[i] != ' ' && command[i] != '\t' && command[i] != '\r' && command[i] != '\n' {
		i++
	}
	return command[start:i], i
}

func validateWord(word string) error {
	for j := range len(word) {
		c := word[j]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || strings.ContainsRune("/._,:+-=", rune(c))) {
			return errors.New("unsupported command character")
		}
	}
	return nil
}
