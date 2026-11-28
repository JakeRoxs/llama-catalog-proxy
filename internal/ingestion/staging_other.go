//go:build !linux

package ingestion

import (
	"context"
	"io"
)

func StageNewArtifact(context.Context, StageOptions, io.Reader) (StageResult, error) {
	return StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}, ErrStageUnsupported
}
