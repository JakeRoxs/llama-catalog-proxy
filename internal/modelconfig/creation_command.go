package modelconfig

import (
	"errors"
	"path"
	"strconv"
	"strings"
)

// CreationCommandInput describes either an explicit command or a restricted
// existing command to use as a template. ModelPath is the path in both containers.
type CreationCommandInput struct {
	ModelPath           string
	Executable          string
	Settings            CommandSettings
	GPULayers           *int32
	DeviceEnvironment   []string
	TemplateCommand     string
	TemplateEnvironment []string
}

// CreationCommandReview excludes executable and model paths. The command itself
// remains private until it is incorporated into a protected proposal.
type CreationCommandReview struct {
	Options           []CreationOption `json:"options"`
	DeviceEnvironment []string         `json:"device_environment,omitempty"`
	command           string
}

type CreationOption struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// BuildCreationCommand does not inspect the artifact or execute the command.
// The template form copies only supported command options and device variables.
func BuildCreationCommand(input CreationCommandInput) (CreationCommandReview, error) {
	model, err := quoteModelPath(input.ModelPath)
	if err != nil {
		return CreationCommandReview{}, err
	}
	if input.TemplateCommand != "" {
		if input.Executable != "" || input.GPULayers != nil || hasCreationSettings(input.Settings) ||
			len(input.DeviceEnvironment) != 0 {
			return CreationCommandReview{}, errors.New("template and explicit command settings cannot be combined")
		}
		env, err := creationEnvironment(input.TemplateEnvironment)
		if err != nil {
			return CreationCommandReview{}, err
		}
		return creationTemplate(input.TemplateCommand, model, env)
	}
	if len(input.TemplateEnvironment) != 0 {
		return CreationCommandReview{}, errors.New("template environment requires a template command")
	}
	env, err := creationEnvironment(input.DeviceEnvironment)
	if err != nil {
		return CreationCommandReview{}, err
	}
	tokens, err := tokenizeCommand(input.Executable)
	if err != nil || len(tokens) != 1 || path.Base(tokens[0].text) != "llama-server" {
		return CreationCommandReview{}, errors.New("unsupported command executable")
	}
	command := input.Executable + " --port ${PORT} --model " + model
	options := []CreationOption{{Name: "port", Value: "${PORT}"}}
	addNumber := func(name, flag string, value *int32, minimum int32) error {
		if value == nil {
			return nil
		}
		if *value < minimum {
			return errors.New("invalid explicit command setting")
		}
		v := strconv.FormatInt(int64(*value), 10)
		command += " " + flag + " " + v
		options = append(options, CreationOption{Name: name, Value: v})
		return nil
	}
	for _, setting := range []struct {
		name, flag string
		value      *int32
		minimum    int32
	}{
		{"context", "--ctx-size", input.Settings.ContextSize, 0},
		{"fit_context", "--fit-ctx", input.Settings.FitContext, 0},
		{"fit_target", "--fit-target", input.Settings.FitTarget, 0},
		{"cache_ram", "--cache-ram", input.Settings.CacheRAM, -1},
		{"gpu_layers", "--gpu-layers", input.GPULayers, -1},
	} {
		if err := addNumber(setting.name, setting.flag, setting.value, setting.minimum); err != nil {
			return CreationCommandReview{}, err
		}
	}
	if input.Settings.Fit != nil {
		value := "off"
		if *input.Settings.Fit {
			value = "on"
		}
		command += " --fit " + value
		options = append(options, CreationOption{Name: "fit", Value: value})
	}
	return CreationCommandReview{Options: options, DeviceEnvironment: env, command: command}, nil
}

func hasCreationSettings(s CommandSettings) bool {
	return s.ContextSize != nil || s.Fit != nil || s.FitContext != nil || s.FitTarget != nil || s.CacheRAM != nil
}

func quoteModelPath(model string) (string, error) {
	if !path.IsAbs(model) || path.Clean(model) != model || len(model) > 4096 ||
		!strings.EqualFold(path.Ext(model), ".gguf") {
		return "", errors.New("model path must be a clean absolute GGUF path")
	}
	for _, c := range model {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || strings.ContainsRune("/ ._+()-", c)) {
			return "", errors.New("unsupported model path characters")
		}
	}
	return "'" + model + "'", nil
}

func creationEnvironment(values []string) ([]string, error) {
	if len(values) > 4 {
		return nil, errors.New("unsupported device environment")
	}
	seen := make(map[string]bool)
	result := make([]string, 0, len(values))
	for _, value := range values {
		name, devices, ok := strings.Cut(value, "=")
		if !ok || seen[name] || (name != "CUDA_VISIBLE_DEVICES" && name != "HIP_VISIBLE_DEVICES" &&
			name != "ROCR_VISIBLE_DEVICES" && name != "GGML_CUDA_VISIBLE_DEVICES") || len(devices) > 256 {
			return nil, errors.New("unsupported device environment")
		}
		if devices != "-1" {
			for _, device := range strings.Split(devices, ",") {
				if device == "" {
					return nil, errors.New("unsupported device environment")
				}
				for _, c := range device {
					if c < '0' || c > '9' {
						return nil, errors.New("unsupported device environment")
					}
				}
			}
		}
		seen[name] = true
		result = append(result, value)
	}
	return result, nil
}

func creationTemplate(command, model string, env []string) (CreationCommandReview, error) {
	tokens, err := tokenizeCommand(command)
	if err != nil || path.Base(tokens[0].text) != "llama-server" {
		return CreationCommandReview{}, errors.New("unsupported template command")
	}
	seen := make(map[string]bool)
	var options []CreationOption
	var modelToken commandToken
	for i := 1; i < len(tokens); i++ {
		option, ok := commandOptions[tokens[i].text]
		if !ok || seen[option.name] {
			return CreationCommandReview{}, errors.New("unsupported or duplicate template option")
		}
		seen[option.name] = true
		if !option.hasValue {
			options = append(options, CreationOption{Name: option.name, Value: "enabled"})
			continue
		}
		i++
		if i >= len(tokens) || !validCreationValue(option.name, tokens[i].text) {
			return CreationCommandReview{}, errors.New("unsupported template option value")
		}
		if option.name == "model" {
			modelToken = tokens[i]
		} else {
			options = append(options, CreationOption{Name: option.name, Value: tokens[i].text})
		}
	}
	if !seen["model"] || !seen["port"] {
		return CreationCommandReview{}, errors.New("template requires a model and dynamic port")
	}
	rewritten := command[:modelToken.start] + model + command[modelToken.end:]
	if len(rewritten) > maxCommandBytes {
		return CreationCommandReview{}, errors.New("command exceeds size limit")
	}
	return CreationCommandReview{Options: options, DeviceEnvironment: env, command: rewritten}, nil
}

func validCreationValue(name, value string) bool {
	switch name {
	case "model":
		_, err := quoteModelPath(value)
		return err == nil
	case "port":
		return value == "${PORT}"
	case "context", "fit_context", "fit_target", "cache_ram", "fit":
		return validateExistingSetting(name, value) == nil
	case "gpu_layers":
		n, err := strconv.ParseInt(value, 10, 32)
		return err == nil && n >= -1
	case "parallel", "log_level":
		n, err := strconv.ParseInt(value, 10, 32)
		return err == nil && n >= 0
	case "host":
		return value == "localhost" || value == "127.0.0.1" || value == "0.0.0.0"
	case "flash_attn":
		return value == "on" || value == "off" || value == "auto"
	case "cache_type_k", "cache_type_v":
		switch value {
		case "f32", "f16", "bf16", "q8_0", "q4_0", "q4_1", "iq4_nl", "q5_0", "q5_1":
			return true
		}
	}
	return false
}
