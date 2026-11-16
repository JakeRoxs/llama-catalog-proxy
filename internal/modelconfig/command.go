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
	requested := make(map[string]string)
	if settings.ContextSize != nil {
		if *settings.ContextSize < 0 {
			return nil, nil, errors.New("context size must be non-negative")
		}
		requested["context"] = strconv.FormatInt(int64(*settings.ContextSize), 10)
	}
	if settings.Fit != nil {
		requested["fit"] = "off"
		if *settings.Fit {
			requested["fit"] = "on"
		}
	}
	if settings.FitContext != nil {
		if *settings.FitContext < 0 {
			return nil, nil, errors.New("fit context must be non-negative")
		}
		requested["fit_context"] = strconv.FormatInt(int64(*settings.FitContext), 10)
	}
	if settings.FitTarget != nil {
		if *settings.FitTarget < 0 {
			return nil, nil, errors.New("fit target must be non-negative")
		}
		requested["fit_target"] = strconv.FormatInt(int64(*settings.FitTarget), 10)
	}
	if settings.CacheRAM != nil {
		if *settings.CacheRAM < -1 {
			return nil, nil, errors.New("cache RAM must be at least -1")
		}
		requested["cache_ram"] = strconv.FormatInt(int64(*settings.CacheRAM), 10)
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
			if editable != nil && !editable(option.option, option.value) {
				return nil, nil, errors.New("requested setting is defined by a shared macro and cannot be changed per model")
			}
			if err := validateExistingSetting(option.name, option.value.text); err != nil {
				return nil, nil, err
			}
			replacements = append(replacements, commandReplacement{token: option.value, value: value})
			changes = append(changes, SettingChange{Setting: option.name, Before: option.value.text, After: value})
			delete(requested, option.name)
		}
	}
	if len(requested) != 0 {
		return nil, nil, errors.New("requested option is absent from command")
	}
	return replacements, changes, nil
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
	for i := 1; i < len(tokens); i++ {
		optionToken := tokens[i]
		optionName, attachedValue, attached := strings.Cut(optionToken.text, "=")
		option, ok := commandOptions[optionName]
		if !ok {
			if !strings.HasPrefix(optionName, "-") || optionName == "-" || optionName == "--" || optionName == "-1" {
				return nil, errors.New("unsupported command argument")
			}
			if attached {
				continue
			}
			if i+1 < len(tokens) {
				next := tokens[i+1].text
				if isKnownCommandOption(next) {
					return nil, errors.New("ambiguous unknown command option value")
				}
				if !strings.HasPrefix(next, "-") || next == "-1" {
					i++
				}
			}
			continue
		}
		if seen[option.name] {
			return nil, errors.New("duplicate command option")
		}
		seen[option.name] = true
		var valueToken commandToken
		if attached {
			if !option.hasValue || attachedValue == "" {
				return nil, errors.New("invalid or missing command option value")
			}
			valueToken = commandToken{text: attachedValue, start: optionToken.start + len(optionName) + 1, end: optionToken.end}
		} else if !option.hasValue {
			continue
		} else {
			i++
			if i >= len(tokens) || strings.HasPrefix(tokens[i].text, "-") && tokens[i].text != "-1" {
				return nil, errors.New("invalid or missing command option value")
			}
			valueToken = tokens[i]
		}
		if valueToken.start < len(command) && command[valueToken.start] == '\'' && option.name != "model" {
			return nil, errors.New("unsupported quoted command value")
		}
		if option.name == "gpu_layers" && !validCreationValue(option.name, valueToken.text) {
			return nil, errors.New("unsupported GPU layer setting")
		}
		if valueToken.text == "${PORT}" && option.name != "port" {
			return nil, errors.New("unsupported command expansion")
		}
		options = append(options, parsedOption{name: option.name, option: optionToken, value: valueToken})
	}
	return options, nil
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
			i++
			end := strings.IndexByte(command[i:], '\'')
			if end < 0 {
				return nil, errors.New("unsupported command quoting")
			}
			word = command[i : i+end]
			i += end + 1
			if i < len(command) && command[i] != ' ' && command[i] != '\t' && command[i] != '\r' && command[i] != '\n' {
				return nil, errors.New("unsupported command quoting")
			}
			if !safeQuotedCommandValue(word) {
				return nil, errors.New("unsupported quoted command value")
			}
		} else {
			for i < len(command) && command[i] != ' ' && command[i] != '\t' && command[i] != '\r' && command[i] != '\n' {
				i++
			}
			word = command[start:i]
			if word != "${PORT}" {
				for j := range len(word) {
					c := word[j]
					if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
						c >= '0' && c <= '9' || strings.ContainsRune("/._,:+-=", rune(c))) {
						return nil, errors.New("unsupported command character")
					}
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
