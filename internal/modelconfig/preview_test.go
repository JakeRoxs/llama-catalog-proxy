package modelconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestPreviewDocumentPreservesAllOtherBytes(t *testing.T) {
	input := `# unrelated token: secret-value
apiKeys: [secret-value]
groups:
  workers: {members: [model-a, model-b]}
models:
  model-a: # owner comment
    aliases: [alias-a]
    env: ["TOKEN=secret-value"]
    cmd: "llama-server -c 8192 --cache-ram 1024 -m /models/a.gguf" # keep this
    metadata: {other: unchanged}
  model-b:
    cmd: other-server
`
	ctx, cache := int32(4096), int32(-1)
	preview, err := PreviewDocument("local", "model-a", []byte(input), CommandSettings{ContextSize: &ctx, CacheRAM: &cache})
	if err != nil {
		t.Fatal(err)
	}
	expected := strings.Replace(input, "-c 8192 --cache-ram 1024", "-c 4096 --cache-ram -1", 1)
	if string(preview.candidate) != expected {
		t.Fatalf("configuration outside target changed: %q", preview.candidate)
	}
	sum := sha256.Sum256([]byte(input))
	if preview.Revision != hex.EncodeToString(sum[:]) || preview.TargetID != "local" || preview.ModelID != "model-a" ||
		len(preview.Changes) != 2 || preview.Changes[0] != (SettingChange{Setting: "context", Before: "8192", After: "4096"}) ||
		preview.Changes[1] != (SettingChange{Setting: "cache_ram", Before: "1024", After: "-1"}) {
		t.Fatalf("unexpected preview: %#v", preview)
	}
	public, err := json.Marshal(preview)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(public), "secret-value") || strings.Contains(string(public), "llama-server") ||
		strings.Contains(string(public), "/models/") {
		t.Fatalf("preview leaked configuration: %s", public)
	}
}

func TestReadSettingsDocumentReportsOnlyExplicitTypedSettings(t *testing.T) {
	input := []byte("secret: confidential\nmodels: {example: {cmd: 'llama-server -m /private/model.gguf -c 0 --fit off --fit-ctx 8192 --fit-target 120 --cache-ram -1'}}\n")
	snapshot, err := ReadSettingsDocument("local", "example", input)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(input)
	if snapshot.Revision != hex.EncodeToString(sum[:]) {
		t.Fatal("snapshot revision does not match source")
	}
	result, err := json.Marshal(snapshot.Settings)
	if err != nil {
		t.Fatal(err)
	}
	if string(result) != `{"context_size":0,"fit":false,"fit_context":8192,"fit_target":120,"cache_ram":-1}` {
		t.Fatalf("unexpected settings: %s", result)
	}
	public, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(public), "confidential") || strings.Contains(string(public), "/private/") ||
		strings.Contains(string(public), "llama-server") {
		t.Fatalf("snapshot leaked configuration: %s", public)
	}
	empty, err := ReadSettingsDocument("local", "example", []byte("models: {example: {cmd: 'llama-server --port 8080'}}"))
	if err != nil {
		t.Fatal(err)
	}
	result, err = json.Marshal(empty.Settings)
	if err != nil || string(result) != "{}" {
		t.Fatalf("absent values must not be inferred: %s, %v", result, err)
	}
}

func TestReadSettingsDocumentRejectsInvalidSettings(t *testing.T) {
	for _, command := range []string{
		"llama-server -c 4 --fit maybe",
		"llama-server -c 4294967296",
		"llama-server --cache-ram -2",
		"llama-server -c 4 --ctx-size 8",
		"llama-server -c 4 --unknown confidential",
	} {
		t.Run(command, func(t *testing.T) {
			_, err := ReadSettingsDocument("local", "example", []byte("models: {example: {cmd: '"+command+"'}}"))
			if err == nil || strings.Contains(err.Error(), "confidential") {
				t.Fatalf("invalid command accepted or leaked: %v", err)
			}
		})
	}
}

func TestPreviewDocumentSupportsPlainAndQuotedScalars(t *testing.T) {
	ctx := int32(32)
	for _, line := range []string{
		"    cmd: llama-server -c 16 # comment\n",
		"    cmd: 'llama-server -c 16' # comment\n",
		"    cmd: \"llama-server -c 16\" # comment\n",
	} {
		input := "models:\n  model:\n" + line
		got, err := PreviewDocument("local", "model", []byte(input), CommandSettings{ContextSize: &ctx})
		if err != nil || string(got.candidate) != strings.Replace(input, "-c 16", "-c 32", 1) {
			t.Fatalf("line %q: candidate=%q error=%v", line, got.candidate, err)
		}
	}
}

func TestPreviewDocumentPreservesUnrelatedAnchorsAndUnicode(t *testing.T) {
	input := "shared: &shared {description: 'keep'}\nmodels: {mödél: {metadata: *shared, cmd: 'llama-server -c 16'}}\n"
	ctx := int32(32)
	got, err := PreviewDocument("local", "mödél", []byte(input), CommandSettings{ContextSize: &ctx})
	if err != nil {
		t.Fatal(err)
	}
	if string(got.candidate) != strings.Replace(input, "-c 16", "-c 32", 1) {
		t.Fatalf("unexpected result: %q", got.candidate)
	}
}

func TestPreviewDocumentRejectsUnsafeOrAmbiguousYAML(t *testing.T) {
	ctx := int32(32)
	tests := map[string]string{
		"duplicate models": "models: {}\nmodels: {model: {cmd: llama-server -c 16}}\n",
		"duplicate model":  "models: {model: {cmd: llama-server -c 16}, model: {cmd: llama-server -c 8}}\n",
		"duplicate cmd":    "models: {model: {cmd: llama-server -c 16, cmd: llama-server -c 8}}\n",
		"aliased model":    "base: &base {cmd: llama-server -c 16}\nmodels: {model: *base}\n",
		"anchored model":   "models: {model: &base {cmd: llama-server -c 16}, other: *base}\n",
		"anchored command": "models: {model: {cmd: &cmd llama-server -c 16}, other: {cmd: *cmd}}\n",
		"merged model":     "base: &base {cmd: llama-server -c 16}\nmodels: {model: {<<: *base, cmd: llama-server -c 8}}\n",
		"missing model":    "models: {other: {cmd: llama-server -c 16}}\n",
		"multiple docs":    "models: {model: {cmd: llama-server -c 16}}\n---\nmodels: {}\n",
		"invalid yaml":     "models: {model: [\n",
		"block command":    "models:\n  model:\n    cmd: |\n      llama-server -c 16\n",
		"escaped scalar":   "models: {model: {cmd: \"llama-server\\t-c 16\"}}\n",
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := PreviewDocument("local", "model", []byte(input), CommandSettings{ContextSize: &ctx}); err == nil {
				t.Fatal("accepted unsupported input")
			}
			if _, err := ReadSettingsDocument("local", "model", []byte(input)); err == nil {
				t.Fatal("read accepted unsupported input")
			}
		})
	}
}

func TestPreviewDocumentBoundsAndBindsInput(t *testing.T) {
	ctx := int32(32)
	input := []byte("models: {model: {cmd: llama-server -c 16}}\n")
	original, err := PreviewDocument("local", "model", input, CommandSettings{ContextSize: &ctx})
	if err != nil {
		t.Fatal(err)
	}
	changed := append(append([]byte(nil), input...), []byte("# external edit\n")...)
	other, err := PreviewDocument("local", "model", changed, CommandSettings{ContextSize: &ctx})
	if err != nil {
		t.Fatal(err)
	}
	if original.Revision == other.Revision {
		t.Fatal("revision did not change")
	}
	for _, data := range [][]byte{nil, []byte(strings.Repeat("x", maxConfigBytes+1)), {0xff}} {
		if _, err := PreviewDocument("local", "model", data, CommandSettings{ContextSize: &ctx}); err == nil {
			t.Fatal("accepted invalid configuration")
		}
	}
	for _, target := range []string{"../target", ".", ".."} {
		if _, err := PreviewDocument(target, "model", input, CommandSettings{ContextSize: &ctx}); err == nil {
			t.Fatalf("accepted target path %q", target)
		}
	}
}
