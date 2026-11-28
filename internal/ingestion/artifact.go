package ingestion

import "errors"

var (
	ErrArtifactRecord      = errors.New("ingestion record is not an eligible completed publication")
	ErrArtifactUnavailable = errors.New("recorded artifact is unavailable or unsafe")
	ErrArtifactChanged     = errors.New("recorded artifact size or identity changed")
	ErrArtifactBusy        = errors.New("artifact directory is busy")
)

// ArtifactEvidence is a point-in-time observation, not verified artifact identity.
type ArtifactEvidence struct {
	RecordID      string `json:"record_id"`
	Source        Source `json:"source"`
	Bytes         int64  `json:"bytes"`
	Presence      string `json:"presence"`
	RecordMatch   string `json:"record_match"`
	Integrity     string `json:"integrity"`
	BackendAccess string `json:"backend_access"`
	Revision      string `json:"revision"`
}

// CheckRecordedArtifact binds a successful record to a current local filesystem
// observation. Callers must recheck the revision before acting and exclude
// uncooperative writers throughout any subsequent configuration change.
func (m *JobManager) CheckRecordedArtifact(directory, id string) (ArtifactEvidence, error) {
	var result ArtifactEvidence
	err := m.WithRecordedArtifact(directory, id, func(evidence ArtifactEvidence, _ func() (ArtifactEvidence, error)) error {
		result = evidence
		return nil
	})
	return result, err
}

// WithRecordedArtifact holds the job manager and a cooperating directory lock
// until fn returns. Recheck must only be called during fn.
func (m *JobManager) WithRecordedArtifact(directory, id string, fn func(ArtifactEvidence, func() (ArtifactEvidence, error)) error) error {
	if fn == nil {
		return ErrArtifactRecord
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped || m.store == nil || m.blocked {
		return ErrRecordStore
	}
	if m.cancel != nil {
		return ErrJobActive
	}
	record, err := m.store.Get(id)
	if err != nil {
		return err
	}
	job := record.Job
	published := job.Result.Publication == PublicationPublished && job.Result.Durability == "confirmed"
	adopted := job.Operation == "adopt" && job.Result.Publication == PublicationExisting && job.Result.Durability == "not_applicable"
	if job.State != "succeeded" || (!published && !adopted) || record.ReviewRequired || job.Result.Bytes <= 0 {
		return ErrArtifactRecord
	}
	return withRecordedArtifactFile(directory, record, fn)
}
