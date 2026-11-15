package modelconfig

import (
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestPreviewCreationDocumentExplicitPreservesSource(t *testing.T) {
	source := "# secret: confidential\r\nmodels:\r\n  # existing model comment\r\n  old:\r\n    aliases: [previous]\r\n    cmd: llama-server --port ${PORT} -m /private/old.gguf\r\nother: unchanged\r\n"
	ctx, gpuLayers := int32(4096), int32(24)
	got, err := PreviewCreationDocument("local", "new", "", []byte(source), CreationCommandInput{
		ModelPath: "/private/new(1).gguf", Executable: "/opt/llama-server",
		Settings: CommandSettings{ContextSize: &ctx}, GPULayers: &gpuLayers,
	})
	if err != nil {
		t.Fatal(err)
	}
	insert := "  \"new\":\r\n    cmd: \"/opt/llama-server --port ${PORT} --model '/private/new(1).gguf' --ctx-size 4096 --gpu-layers 24\"\r\n"
	expected := strings.Replace(source, "  # existing model comment\r\n", insert+"  # existing model comment\r\n", 1)
	if string(got.candidate) != expected {
		t.Fatalf("unexpected candidate: %q", got.candidate)
	}
	var decoded struct {
		Models map[string]struct {
			Cmd string `yaml:"cmd"`
		} `yaml:"models"`
	}
	if err := yaml.Unmarshal(got.candidate, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Models["new"].Cmd != "/opt/llama-server --port ${PORT} --model '/private/new(1).gguf' --ctx-size 4096 --gpu-layers 24" {
		t.Fatalf("unexpected command: %q", decoded.Models["new"].Cmd)
	}
	updated := int32(8192)
	if _, err := PreviewDocument("local", "new", got.candidate, CommandSettings{ContextSize: &updated}); err != nil {
		t.Fatalf("created entry does not support existing settings preview: %v", err)
	}
	if got.Revision != sourceRevision([]byte(source)) || got.Artifact != "not_checked" {
		t.Fatalf("incorrect source or artifact evidence: %#v", got)
	}
	public, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"confidential", "/private", "/opt", "llama-server"} {
		if strings.Contains(string(public), private) {
			t.Fatalf("preview leaked configuration: %s", public)
		}
	}
}

func TestPreviewCreationDocumentUsesSelectedTemplate(t *testing.T) {
	source := `models:
  template:
    cmd: "llama-server --port ${PORT} -m /models/old.gguf -ngl 24"
    env: ["CUDA_VISIBLE_DEVICES=0,1"]
  unrelated: {cmd: "other"}
`
	got, err := PreviewCreationDocument("local", "new", "template", []byte(source), CreationCommandInput{
		ModelPath: "/models/new.gguf",
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Models map[string]struct {
			Cmd string   `yaml:"cmd"`
			Env []string `yaml:"env"`
		} `yaml:"models"`
	}
	if err := yaml.Unmarshal(got.candidate, &decoded); err != nil {
		t.Fatal(err)
	}
	entry := decoded.Models["new"]
	if entry.Cmd != "llama-server --port ${PORT} -m '/models/new.gguf' -ngl 24" ||
		len(entry.Env) != 1 || entry.Env[0] != "CUDA_VISIBLE_DEVICES=0,1" {
		t.Fatalf("unexpected template result: %#v", entry)
	}
	insert := "  \"new\":\n    cmd: \"llama-server --port ${PORT} -m '/models/new.gguf' -ngl 24\"\n    env:\n      - \"CUDA_VISIBLE_DEVICES=0,1\"\n"
	expected := strings.Replace(source, "models:\n", "models:\n"+insert, 1)
	if got.TemplateID != "template" || string(got.candidate) != expected {
		t.Fatal("template identity or source bytes not preserved")
	}
}

func TestPreviewCreationDocumentRejectsConflictsAndAmbiguity(t *testing.T) {
	base := "models:\n  old:\n    aliases: [previous]\n    cmd: llama-server --port ${PORT} -m /models/old.gguf\n"
	explicit := CreationCommandInput{ModelPath: "/models/new.gguf", Executable: "llama-server"}
	tests := []struct {
		name, source, model, template string
		input                         CreationCommandInput
	}{
		{"existing model", base, "old", "", explicit},
		{"alias conflict", base, "previous", "", explicit},
		{"selector conflict", base + "selectors: {new: {targets: [old]}}\n", "new", "", explicit},
		{"peer conflict", base + "peers: {remote: {models: [new]}}\n", "remote/new", "", explicit},
		{"filter alias conflict", strings.Replace(base, "    cmd:", "    filters: {setParamsByID: {new: {}}}\n    cmd:", 1), "new", "", explicit},
		{"filter alias without aliases", "models:\n  old:\n    filters: {setParamsByID: {new: {}}}\n    cmd: llama-server --port ${PORT} -m /models/old.gguf\n", "new", "", explicit},
		{"duplicate model", base + "  old: {cmd: x}\n", "new", "", explicit},
		{"duplicate aliases", strings.Replace(base, "    cmd:", "    aliases: [other]\n    cmd:", 1), "new", "", explicit},
		{"alias reference", "aliases: &a [new]\nmodels:\n  old: {aliases: *a, cmd: x}\n", "new", "", explicit},
		{"flow models", "models: {old: {cmd: x}}\n", "new", "", explicit},
		{"empty models", "models: {}\n", "new", "", explicit},
		{"multiple documents", base + "---\nmodels: {}\n", "new", "", explicit},
		{"unsupported template field", base, "new", "old", CreationCommandInput{ModelPath: "/models/new.gguf"}},
		{"unknown template", base, "new", "missing", CreationCommandInput{ModelPath: "/models/new.gguf"}},
		{"untrusted template", base, "new", "", CreationCommandInput{ModelPath: "/models/new.gguf", TemplateCommand: "bad"}},
		{"unsafe id", base, "bad\nkey", "", explicit},
		{"template secret", "models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf', env: [TOKEN=secret]}\n", "new", "old", CreationCommandInput{ModelPath: "/models/new.gguf"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PreviewCreationDocument("local", tt.model, tt.template, []byte(tt.source), tt.input)
			if err == nil || got.candidate != nil {
				t.Fatal("accepted unsafe preview")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("error disclosed template data: %v", err)
			}
		})
	}
}
