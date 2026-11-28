package modelconfig

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildCreationCommandExplicit(t *testing.T) {
	context, layers, fit := int32(8192), int32(24), false
	got, err := BuildCreationCommand(CreationCommandInput{
		ModelPath: "/private models/model(2).gguf", Executable: "/opt/bin/llama-server",
		Settings: CommandSettings{ContextSize: &context, Fit: &fit}, GPULayers: &layers,
		DeviceEnvironment: []string{"CUDA_VISIBLE_DEVICES=0,2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "/opt/bin/llama-server --port ${PORT} --model '/private models/model(2).gguf' --ctx-size 8192 --gpu-layers 24 --fit off"
	if got.command != want {
		t.Fatalf("command = %q", got.command)
	}
	public, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"/private", "/opt/bin", "llama-server"} {
		if strings.Contains(string(public), private) {
			t.Fatalf("review leaked private command: %s", public)
		}
	}
	if !strings.Contains(string(public), `"gpu_layers"`) || !strings.Contains(string(public), "CUDA_VISIBLE_DEVICES=0,2") {
		t.Fatalf("review omitted settings: %s", public)
	}
}

func TestBuildCreationCommandTemplatePreservesSupportedSettings(t *testing.T) {
	template := "llama-server --port ${PORT} --host 0.0.0.0 -m '/private/old model.gguf' -ngl 30 --ctx-size 4096 --fit on --reasoning-preserve"
	got, err := BuildCreationCommand(CreationCommandInput{
		ModelPath: "/models/new(1).gguf", TemplateCommand: template,
		TemplateEnvironment: []string{"CUDA_VISIBLE_DEVICES=1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(template, "'/private/old model.gguf'", "'/models/new(1).gguf'", 1)
	if got.command != want {
		t.Fatalf("command = %q; want %q", got.command, want)
	}
	if _, err := BuildCreationCommand(CreationCommandInput{
		ModelPath: "/models/next.gguf", TemplateCommand: got.command,
	}); err != nil {
		t.Fatalf("generated command cannot be reused as a template: %v", err)
	}
	public, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(public), "/private/") || strings.Contains(string(public), "/models/") {
		t.Fatalf("review leaked paths: %s", public)
	}
}

func TestBuildCreationCommandRejectsUnsafeInput(t *testing.T) {
	bad := int32(-2)
	tests := []CreationCommandInput{
		{ModelPath: "/models/x.gguf", Executable: "wrapper"},
		{ModelPath: "/models/../x.gguf", Executable: "llama-server"},
		{ModelPath: "/models/${SECRET}.gguf", Executable: "llama-server"},
		{ModelPath: "/models/x'evil.gguf", Executable: "llama-server"},
		{ModelPath: "/models/x.gguf", Executable: "llama-server", GPULayers: &bad},
		{ModelPath: "/models/x.gguf", Executable: "llama-server", DeviceEnvironment: []string{"TOKEN=secret"}},
		{ModelPath: "/models/x.gguf", Executable: "llama-server", DeviceEnvironment: []string{"CUDA_VISIBLE_DEVICES=0", "CUDA_VISIBLE_DEVICES=1"}},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf --api-key secret"},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port 8080 -m /m/x.gguf"},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf --model /m/y.gguf"},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf; curl secret"},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf --host secret"},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf", Executable: "llama-server"},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf", TemplateEnvironment: []string{"TOKEN=secret"}},
		{ModelPath: "/models/x.gguf", TemplateCommand: "llama-server --port ${PORT} -m /m/x.gguf", DeviceEnvironment: []string{"CUDA_VISIBLE_DEVICES=0"}},
	}
	for _, input := range tests {
		got, err := BuildCreationCommand(input)
		if err == nil || got.command != "" {
			t.Fatalf("unsafe input accepted: %#v", input)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("error disclosed template data: %v", err)
		}
	}
}

func TestBuildCreationCommandNoImpliedPerformanceSettings(t *testing.T) {
	got, err := BuildCreationCommand(CreationCommandInput{
		ModelPath: "/models/x.gguf", Executable: "llama-server",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.command != "llama-server --port ${PORT} --model '/models/x.gguf'" ||
		len(got.Options) != 1 {
		t.Fatalf("unexpected defaults: %#v", got)
	}
}
