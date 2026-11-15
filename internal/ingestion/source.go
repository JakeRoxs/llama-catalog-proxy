// Package ingestion provides bounded model artifact ingestion primitives.
package ingestion

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

var (
	repositoryPart = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$`)
	revisionPart   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	ggufFilename   = regexp.MustCompile(`(?i)^[a-z0-9][a-z0-9._+()-]*\.gguf$`)
	splitGGUF      = regexp.MustCompile(`(?i)-[0-9]{5}-of-[0-9]{5}\.gguf$`)
)

// SourceInput selects either a URL or a repository and filename. Revision is
// required unless it is present in a Hugging Face blob or resolve URL.
type SourceInput struct {
	URL        string
	Repository string
	Filename   string
	Revision   string
}

// Source describes a local artifact or a requested revision, which may be a
// mutable branch or tag. It does not assert verified artifact identity.
type Source struct {
	Kind       string `json:"kind,omitempty"`
	Repository string `json:"repository,omitempty"`
	Filename   string `json:"filename"`
	Revision   string `json:"revision,omitempty"`
}

// LocalSource identifies an existing local file without asserting its origin.
func LocalSource(filename string) (Source, error) {
	if !validGGUFFilename(filename) {
		return Source{}, errors.New("a supported GGUF basename is required")
	}
	return Source{Kind: "local", Filename: filename}, nil
}

func validGGUFFilename(filename string) bool {
	return len(filename) <= 255 && ggufFilename.MatchString(filename) && !splitGGUF.MatchString(filename)
}

func ParseSource(input SourceInput) (Source, error) {
	var owner, repository, filename, revision string
	if input.URL == "" {
		parts := strings.Split(input.Repository, "/")
		if len(parts) != 2 {
			return Source{}, errors.New("repository must be owner/repository")
		}
		owner, repository, filename, revision = parts[0], parts[1], input.Filename, input.Revision
	} else {
		if input.Repository != "" || input.Filename != "" || len(input.URL) > 2048 {
			return Source{}, errors.New("provide a URL or a repository and filename, not both")
		}
		parsed, err := url.Parse(input.URL)
		if err != nil || strings.Contains(input.URL, "#") || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" ||
			parsed.ForceQuery || parsed.Fragment != "" || parsed.RawFragment != "" {
			return Source{}, errors.New("invalid model source URL")
		}
		path := strings.ToLower(parsed.EscapedPath())
		if strings.Contains(path, "%2f") || strings.Contains(path, "%5c") {
			return Source{}, errors.New("encoded path separators are unsupported")
		}
		parts := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
		switch {
		case parsed.Scheme == "hf" && parsed.Host != "" && len(parts) == 2:
			owner, repository, filename, revision = parsed.Host, parts[0], parts[1], input.Revision
		case parsed.Scheme == "https" &&
			(strings.EqualFold(parsed.Host, "huggingface.co") || strings.EqualFold(parsed.Host, "www.huggingface.co")):
			switch {
			case len(parts) == 3:
				owner, repository, filename, revision = parts[0], parts[1], parts[2], input.Revision
			case len(parts) == 5 && (parts[2] == "blob" || parts[2] == "resolve"):
				owner, repository, filename, revision = parts[0], parts[1], parts[4], parts[3]
				if input.Revision != "" && input.Revision != revision {
					return Source{}, errors.New("source revisions do not match")
				}
			default:
				return Source{}, errors.New("unsupported Hugging Face artifact path")
			}
		default:
			return Source{}, errors.New("unsupported model source URL")
		}
	}
	if !repositoryPart.MatchString(owner) || !repositoryPart.MatchString(repository) {
		return Source{}, errors.New("invalid Hugging Face repository")
	}
	if !revisionPart.MatchString(revision) {
		return Source{}, errors.New("an explicit single-component revision is required")
	}
	if len(filename) > 255 || !ggufFilename.MatchString(filename) {
		return Source{}, errors.New("a GGUF basename is required")
	}
	if splitGGUF.MatchString(filename) {
		return Source{}, errors.New("split GGUF artifacts are unsupported")
	}
	return Source{Repository: owner + "/" + repository, Filename: filename, Revision: revision}, nil
}

func validRecordedSource(source Source, operation string) bool {
	if operation == "adopt" {
		local, err := LocalSource(source.Filename)
		return err == nil && local == source
	}
	if operation != "" && operation != "replace" {
		return false
	}
	remote, err := ParseSource(SourceInput{
		Repository: source.Repository, Filename: source.Filename, Revision: source.Revision,
	})
	return err == nil && remote == source
}
