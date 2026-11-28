package ingestion

import (
	"net/url"
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

func TestParseSourceURLScheme(t *testing.T) {
	tests := []struct {
		name    string
		parsed  *url.URL
		parts   []string
		input   SourceInput
		want    Source
		wantErr bool
	}{
		{
			name:   "hf scheme two parts",
			parsed: &url.URL{Scheme: "hf", Host: "Owner"},
			parts:  []string{"model-gguf", "model.Q4_K_M.gguf"},
			input:  SourceInput{Revision: "revision-1"},
			want:   Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:   "https huggingface.co three parts",
			parsed: &url.URL{Scheme: "https", Host: "huggingface.co"},
			parts:  []string{"Owner", "model-gguf", "model.Q4_K_M.gguf"},
			input:  SourceInput{Revision: "revision-1"},
			want:   Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:   "https huggingface.co resolve five parts",
			parsed: &url.URL{Scheme: "https", Host: "huggingface.co"},
			parts:  []string{"Owner", "model-gguf", "resolve", "revision-1", "model.Q4_K_M.gguf"},
			input:  SourceInput{},
			want:   Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:   "https huggingface.co blob five parts",
			parsed: &url.URL{Scheme: "https", Host: "huggingface.co"},
			parts:  []string{"Owner", "model-gguf", "blob", "revision-1", "model.Q4_K_M.gguf"},
			input:  SourceInput{Revision: "revision-1"},
			want:   Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:    "unsupported scheme",
			parsed:  &url.URL{Scheme: "ftp", Host: "example.com"},
			parts:   []string{"Owner", "model-gguf"},
			input:   SourceInput{},
			wantErr: true,
		},
		{
			name:    "hf scheme missing host",
			parsed:  &url.URL{Scheme: "hf"},
			parts:   []string{"model-gguf", "model.Q4_K_M.gguf"},
			input:   SourceInput{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repository, filename, revision, err := parseSourceURLScheme(tt.parsed, tt.parts, tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseSourceURLScheme(%v, %v, %+v) = %q/%q/%q/%q, %v; want error", tt.parsed.Scheme, tt.parts, tt.input, owner, repository, filename, revision, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseSourceURLScheme(%v, %v, %+v) = %q/%q/%q/%q, %v; want no error", tt.parsed.Scheme, tt.parts, tt.input, owner, repository, filename, revision, err)
			}
			got := Source{Repository: owner + "/" + repository, Filename: filename, Revision: revision}
			if got != tt.want {
				t.Fatalf("parseSourceURLScheme(%v, %v, %+v) = %+v; want %+v", tt.parsed.Scheme, tt.parts, tt.input, got, tt.want)
			}
		})
	}
}

func TestParseHuggingFaceURL(t *testing.T) {
	tests := []struct {
		name    string
		parts   []string
		input   SourceInput
		want    Source
		wantErr bool
	}{
		{
			name:  "three parts",
			parts: []string{"Owner", "model-gguf", "model.Q4_K_M.gguf"},
			input: SourceInput{Revision: "revision-1"},
			want:  Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:  "resolve five parts matching revision",
			parts: []string{"Owner", "model-gguf", "resolve", "revision-1", "model.Q4_K_M.gguf"},
			input: SourceInput{Revision: "revision-1"},
			want:  Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:  "blob five parts matching revision",
			parts: []string{"Owner", "model-gguf", "blob", "revision-1", "model.Q4_K_M.gguf"},
			input: SourceInput{Revision: "revision-1"},
			want:  Source{Repository: "Owner/model-gguf", Filename: "model.Q4_K_M.gguf", Revision: "revision-1"},
		},
		{
			name:    "revision mismatch",
			parts:   []string{"Owner", "model-gguf", "resolve", "revision-1", "model.Q4_K_M.gguf"},
			input:   SourceInput{Revision: "other"},
			wantErr: true,
		},
		{
			name:    "unsupported path length",
			parts:   []string{"Owner", "model-gguf"},
			input:   SourceInput{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, repository, filename, revision, err := parseHuggingFaceURL(tt.parts, tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseHuggingFaceURL(%v, %+v) = %q/%q/%q/%q, %v; want error", tt.parts, tt.input, owner, repository, filename, revision, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseHuggingFaceURL(%v, %+v) = %q/%q/%q/%q, %v; want no error", tt.parts, tt.input, owner, repository, filename, revision, err)
			}
			got := Source{Repository: owner + "/" + repository, Filename: filename, Revision: revision}
			if got != tt.want {
				t.Fatalf("parseHuggingFaceURL(%v, %+v) = %+v; want %+v", tt.parts, tt.input, got, tt.want)
			}
		})
	}
}

func TestValidateSourceComponents(t *testing.T) {
	tests := []struct {
		name       string
		owner      string
		repository string
		filename   string
		revision   string
		wantErr    bool
	}{
		{
			name:       "valid components",
			owner:      "Owner",
			repository: "model-gguf",
			filename:   "model.Q4_K_M.gguf",
			revision:   "revision-1",
		},
		{
			name:       "invalid owner",
			owner:      "Owner/../Bad",
			repository: "model-gguf",
			filename:   "model.Q4_K_M.gguf",
			revision:   "revision-1",
			wantErr:    true,
		},
		{
			name:       "invalid repository",
			owner:      "Owner",
			repository: "model/../gguf",
			filename:   "model.Q4_K_M.gguf",
			revision:   "revision-1",
			wantErr:    true,
		},
		{
			name:       "invalid revision",
			owner:      "Owner",
			repository: "model-gguf",
			filename:   "model.Q4_K_M.gguf",
			revision:   "refs/pr/1",
			wantErr:    true,
		},
		{
			name:       "invalid filename",
			owner:      "Owner",
			repository: "model-gguf",
			filename:   "model-00001-of-00002.gguf",
			revision:   "revision-1",
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSourceComponents(tt.owner, tt.repository, tt.filename, tt.revision)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("validateSourceComponents(%q, %q, %q, %q) = %v; want error", tt.owner, tt.repository, tt.filename, tt.revision, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateSourceComponents(%q, %q, %q, %q) = %v; want no error", tt.owner, tt.repository, tt.filename, tt.revision, err)
			}
		})
	}
}
