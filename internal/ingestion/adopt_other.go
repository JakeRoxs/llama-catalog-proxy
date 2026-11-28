//go:build !linux

package ingestion

import (
	"context"
	"time"
)

func adoptArtifact(context.Context, StageOptions, time.Duration) (StageResult, error) {
	return StageResult{Publication: PublicationNotPublished, Durability: "not_applicable"}, ErrStageUnsupported
}
