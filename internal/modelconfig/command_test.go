package modelconfig

import (
	"strings"
	"testing"
)

func TestRewriteCommandPreservesUnchangedArguments(t *testing.T) {
	command := "llama-server --port ${PORT} --host 0.0.0.0 --parallel 1 --reasoning-preserve --flash-attn on --fit on --fit-ctx 262144 --fit-target 120 --cache-type-k q4_0 --cache-type-v q4_0 --cache-ram 4096 -lv 4 -m /models/example.gguf"
	fit, ctx, cache := false, int32(8192), int32(-1)
	got, err := RewriteCommand(command, CommandSettings{Fit: &fit, FitContext: &ctx, CacheRAM: &cache})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(command, "--fit on --fit-ctx 262144", "--fit off --fit-ctx 8192", 1)
	want = strings.Replace(want, "--cache-ram 4096", "--cache-ram -1", 1)
	if got != want {
		t.Fatalf("unexpected edited command: %q", got)
	}
}

func TestRewriteCommandRejectsAmbiguousOrUnsafeCommands(t *testing.T) {
	ctx := int32(4096)
	tests := []struct {
		name    string
		command string
	}{
		{"duplicate aliases", "llama-server -c 8192 --ctx-size 1024"},
		{"ambiguous unknown option value", "llama-server --other --ctx-size 8192 -c 4096"},
		{"absent option", "llama-server --port ${PORT}"},
		{"shell operator", "llama-server -c 8192; other"},
		{"command substitution", "llama-server -c $(value)"},
		{"quoted command", "llama-server -m 'model name' -c 8192"},
		{"quoted non-model value", "llama-server --host '/models/model.gguf' -c 8192"},
		{"invalid GPU layers", "llama-server --gpu-layers bad -c 8192"},
		{"wrong executable", "wrapper -c 8192"},
		{"environment expansion", "llama-server -c ${SIZE}"},
		{"invalid numeric", "llama-server -c invalid"},
		{"missing argument", "llama-server -c --port 8080"},
		{"oversized", strings.Repeat("x", maxCommandBytes+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RewriteCommand(tt.command, CommandSettings{ContextSize: &ctx})
			if err == nil || got != "" {
				t.Fatalf("unexpected result: %q, %v", got, err)
			}
		})
	}
}

func TestRewriteCommandPreservesUnknownSafeLlamaFlags(t *testing.T) {
	ctx := int32(16384)
	command := "llama-server --chat-template-file /templates/chat.jinja --metal --fast --other=value --ctx-size=8192"
	got, err := RewriteCommand(command, CommandSettings{ContextSize: &ctx})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Replace(command, "--ctx-size=8192", "--ctx-size=16384", 1)
	if got != want {
		t.Fatalf("unknown pass-through flags changed: got %q want %q", got, want)
	}
}

func TestRewriteCommandRequiresValidExplicitSettings(t *testing.T) {
	bad := int32(-2)
	if _, err := RewriteCommand("llama-server --cache-ram 4", CommandSettings{CacheRAM: &bad}); err == nil {
		t.Fatal("accepted invalid cache RAM")
	}
	if _, err := RewriteCommand("llama-server -c 4", CommandSettings{}); err == nil {
		t.Fatal("accepted empty change")
	}
}
