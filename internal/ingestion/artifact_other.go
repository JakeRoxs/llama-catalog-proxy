//go:build !linux

package ingestion

func withRecordedArtifactFile(string, Record, func(ArtifactEvidence, func() (ArtifactEvidence, error)) error) error {
	return ErrStageUnsupported
}
