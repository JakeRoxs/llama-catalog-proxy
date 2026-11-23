// Package diagnostics provides opt-in, on-demand capability observations. It
// never calls model upstreams, captures, logs, or lifecycle-management APIs.
package diagnostics

// IndexBackendSnapshot records one diagnostics snapshot into the per-backend
// index. The caller creates the index and duplicate maps and performs any
// pre-validation (nil, empty, or invalid backend) before calling. The helper
// increments the backend count, marks the backend as duplicated when more than
// one snapshot is observed, and stores the snapshot (last write wins). It does
// not validate its input, so a nil snapshot panics; callers must skip invalid
// entries first.
func IndexBackendSnapshot(snapshot *Snapshot, snapshotsByBackend map[string]*Snapshot, counts map[string]int, duplicateBackends map[string]bool) {
	counts[snapshot.Backend]++
	if counts[snapshot.Backend] > 1 {
		duplicateBackends[snapshot.Backend] = true
	}
	snapshotsByBackend[snapshot.Backend] = snapshot
}
