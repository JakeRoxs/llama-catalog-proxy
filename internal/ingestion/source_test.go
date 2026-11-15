package ingestion

import (
	"strings"
	"testing"
)

func TestParseSource(t *testing.T) {
	want := Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"}
	for _, input := range []SourceInput{
		{Repository: want.Repository, Filename: want.Filename, Revision: want.Revision},
		{URL: "hf://Owner/model-gguf/model.Q4_K_M.gguf", Revision: want.Revision},
		{URL: "https://huggingface.co/Owner/model-gguf/model.Q4_K_M.gguf", Revision: want.Revision},
		{URL: "https://huggingface.co/Owner/model-gguf/resolve/revision-1/model.Q4_K_M.gguf"},
		{URL: "https://www.huggingface.co/Owner/model-gguf/blob/revision-1/model.Q4_K_M.gguf", Revision: want.Revision},
	} {
		got, err := ParseSource(input)
		if err != nil || got != want {
			t.Fatalf("ParseSource(%+v) = %+v, %v; want %+v", input, got, err, want)
		}
	}
}

func TestParseSourceRejectsAmbiguousOrUnsupportedInputs(t *testing.T) {
	for _, input := range []SourceInput{
		{Repository: "owner/repo", Filename: "model.gguf"},
		{Repository: "owner/repo", Filename: "model.gguf", Revision: "refs/pr/1"},
		{Repository: "owner/../repo", Filename: "model.gguf", Revision: "main"},
		{Repository: "owner/repo", Filename: "../model.gguf", Revision: "main"},
		{Repository: "owner/repo", Filename: "model-00001-of-00002.gguf", Revision: "main"},
		{Repository: "owner/repo", Filename: strings.Repeat("x", 251) + ".gguf", Revision: "main"},
		{URL: "https://huggingface.co/owner/repo/resolve/main/subdir/model.gguf"},
		{URL: "https://huggingface.co/owner/repo/resolve/refs/pr/1/model.gguf"},
		{URL: "https://huggingface.co/owner/repo/resolve/main/subdir%2Fmodel.gguf"},
		{URL: "https://huggingface.co/owner/repo/resolve/main/model.gguf", Revision: "other"},
		{URL: "https://huggingface.co/owner/repo/resolve/main/model.gguf", Repository: "owner/repo"},
		{URL: "https://huggingface.co/owner/repo/resolve/main/model.gguf?token=redacted"},
		{URL: "https://huggingface.co/owner/repo/resolve/main/model.gguf#"},
		{URL: "https://user@huggingface.co/owner/repo/resolve/main/model.gguf"},
		{URL: "http://huggingface.co/owner/repo/resolve/main/model.gguf"},
		{URL: "https://huggingface.co.evil.example/owner/repo/resolve/main/model.gguf"},
		{URL: "https://huggingface.co:443/owner/repo/resolve/main/model.gguf"},
		{URL: "hf://owner/repo/model.gguf/extra", Revision: "main"},
	} {
		if result, err := ParseSource(input); err == nil {
			t.Errorf("ParseSource(%+v) unexpectedly accepted %+v", input, result)
		}
	}
}
