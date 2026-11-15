// Package correlation provides bounded, normalized collection of backend
// activity from llama-swap upstreams. It is independent of the request
// correlation store (correlator.go) and never merges activity records with
// proxy request records. Correlation with proxy request IDs is explicitly
// unsupported: the llama-swap ActivityLogEntry carries no X-Request-ID, so
// activity cannot be joined to a proxy request, and concurrent same-model
// requests are never merged.
package correlation

import (
	"errors"
	"sort"
	"strings"
	"time"
)

// TimedDeviceSample is a normalized GPU sample timestamp with backend/epoch/
// device context. DeviceID is a canonical opaque 'gpu-' identifier, never the
// raw name/UUID/PCI address.
type TimedDeviceSample struct {
	BackendID string    `json:"backend_id"`
	Epoch     int64     `json:"epoch"`
	DeviceID  string    `json:"device_id"` // canonical 'gpu-' ID
	Timestamp time.Time `json:"timestamp"`
}

// AssociationWindow bounds the association look-behind and look-ahead around
// an activity timestamp.
type AssociationWindow struct {
	Before time.Duration `json:"before,omitempty"`
	After  time.Duration `json:"after,omitempty"`
}

// DefaultAssociationWindow returns the default 2s look-behind and 2s
// look-ahead window.
func DefaultAssociationWindow() AssociationWindow {
	return AssociationWindow{Before: 2 * time.Second, After: 2 * time.Second}
}

// ActivityDeviceAssociation is the result of associating one activity with one
// GPU device. It always serializes the collection-contract uncertainty fields
// (kind, attribution, request_id_mapping, upstream_generation,
// clock_alignment_unverified, execution_device_unverified) so the output is
// self-documenting. It never copies ModelID, Route, or other text from the
// ActivityRecord.
type ActivityDeviceAssociation struct {
	Kind                      string    `json:"kind"`                          // "approximate_time_window" or "unavailable"
	Attribution               string    `json:"attribution,omitempty"`         // "shared_resource_only"
	RequestIDMapping          string    `json:"request_id_mapping,omitempty"`  // "unsupported"
	UpstreamGeneration        string    `json:"upstream_generation,omitempty"` // "unknown"
	LifecycleState            string    `json:"lifecycle_state"`
	ClockAlignmentUnverified  bool      `json:"clock_alignment_unverified,omitempty"`
	ExecutionDeviceUnverified bool      `json:"execution_device_unverified,omitempty"`
	BackendID                 string    `json:"backend_id"`
	Epoch                     int64     `json:"epoch"`
	ActivityID                string    `json:"activity_id"`
	DeviceID                  string    `json:"device_id"`
	WindowStart               time.Time `json:"window_start"`
	WindowEnd                 time.Time `json:"window_end"`
	MatchedSampleCount        int       `json:"matched_sample_count,omitempty"`
	FirstSampleTime           time.Time `json:"first_sample_time,omitzero"`
	LastSampleTime            time.Time `json:"last_sample_time,omitzero"`
	// ObservedMinIntervalMS/ObservedMaxIntervalMS are the min/max interval in
	// milliseconds between consecutive distinct matched sample timestamps.
	// Nil when fewer than 2 distinct matched timestamps exist. 0ms reflects
	// sub-millisecond spacing, not a rate.
	ObservedMinIntervalMS *int64 `json:"observed_min_interval_ms,omitempty"`
	ObservedMaxIntervalMS *int64 `json:"observed_max_interval_ms,omitempty"`
}

// AssociateActivities associates activities with GPU device samples within a
// time window. Samples are grouped by backend+epoch+canonical device ID, with
// duplicate timestamps deduplicated. For each activity-device pair from the
// same backend+positive epoch, sample timestamps within
// [activity.Timestamp - Before, activity.Timestamp + After] (inclusive) are
// counted. The function is pure: it does not mutate inputs and is safe for
// concurrent calls.
func AssociateActivities(activities []*ActivityRecord, samples []TimedDeviceSample, window AssociationWindow) ([]ActivityDeviceAssociation, error) {
	// Validate policy before any allocation.
	if window.Before < 0 || window.Before > time.Minute {
		return nil, errors.New("association window before out of range: must be 0..1 minute")
	}
	if window.After < 0 || window.After > time.Minute {
		return nil, errors.New("association window after out of range: must be 0..1 minute")
	}

	// Validate input bounds before allocation.
	if len(activities) > 1000 {
		return nil, errors.New("too many activities: max 1000")
	}
	if len(samples) > 4096 {
		return nil, errors.New("too many samples: max 4096")
	}

	// Validate activities: nil, invalid IDs, zero timestamps, non-positive
	// epochs are rejected with explicit unavailable errors.
	for _, a := range activities {
		if a == nil {
			return nil, errors.New("nil activity: explicit unavailable")
		}
		if !validIdentifier(a.BackendID) {
			return nil, errors.New("invalid backend id: explicit unavailable")
		}
		if !validIdentifier(a.ActivityID) {
			return nil, errors.New("invalid activity id: explicit unavailable")
		}
		if a.Timestamp.IsZero() {
			return nil, errors.New("zero timestamp: explicit unavailable")
		}
		if a.Epoch <= 0 {
			return nil, errors.New("non-positive epoch: explicit unavailable")
		}
	}

	// Validate samples and group by backend+epoch+device ID. Track distinct
	// device counts per backend/epoch pair.
	type sampleKey struct {
		backendID string
		epoch     int64
		deviceID  string
	}

	samplesByKey := make(map[sampleKey][]time.Time)
	devicesByPair := make(map[string]map[int64]map[string]bool) // backendID -> epoch -> deviceID -> true

	for _, s := range samples {
		if !validIdentifier(s.BackendID) {
			return nil, errors.New("invalid backend id in sample: explicit unavailable")
		}
		if !validIdentifier(s.DeviceID) {
			return nil, errors.New("invalid device id in sample: explicit unavailable")
		}
		if !isGPUDeviceID(s.DeviceID) {
			return nil, errors.New("invalid device id: must be canonical gpu- ID: explicit unavailable")
		}
		if s.Timestamp.IsZero() {
			return nil, errors.New("zero timestamp in sample: explicit unavailable")
		}
		if s.Epoch <= 0 {
			return nil, errors.New("non-positive epoch in sample: explicit unavailable")
		}

		if devicesByPair[s.BackendID] == nil {
			devicesByPair[s.BackendID] = make(map[int64]map[string]bool)
		}
		if devicesByPair[s.BackendID][s.Epoch] == nil {
			devicesByPair[s.BackendID][s.Epoch] = make(map[string]bool)
		}
		devicesByPair[s.BackendID][s.Epoch][s.DeviceID] = true
		if len(devicesByPair[s.BackendID][s.Epoch]) > 8 {
			return nil, errors.New("too many devices per backend/epoch: max 8")
		}

		key := sampleKey{s.BackendID, s.Epoch, s.DeviceID}
		ts := s.Timestamp.UTC().Round(0)
		existing := samplesByKey[key]
		found := false
		for _, ets := range existing {
			if ets.Equal(ts) {
				found = true
				break
			}
		}
		if !found {
			samplesByKey[key] = append(samplesByKey[key], ts)
		}
	}

	// Sort timestamps for each device.
	for key := range samplesByKey {
		sort.Slice(samplesByKey[key], func(i, j int) bool {
			return samplesByKey[key][i].Before(samplesByKey[key][j])
		})
	}

	// Process each activity.
	result := make([]ActivityDeviceAssociation, 0)

	for _, a := range activities {
		epochDevices, hasEpoch := devicesByPair[a.BackendID]
		if !hasEpoch {
			continue // no samples for this backend
		}
		deviceMap, hasDevices := epochDevices[a.Epoch]
		if !hasDevices {
			continue // no samples for this backend+epoch
		}

		// Collect and sort device IDs for deterministic output.
		var deviceIDs []string
		for deviceID := range deviceMap {
			deviceIDs = append(deviceIDs, deviceID)
		}
		sort.Strings(deviceIDs)

		for _, deviceID := range deviceIDs {
			timestamps := samplesByKey[sampleKey{a.BackendID, a.Epoch, deviceID}]

			// Base association with always-serialized uncertainty fields.
			assoc := ActivityDeviceAssociation{
				Kind:                      "unavailable",
				Attribution:               "shared_resource_only",
				RequestIDMapping:          "unsupported",
				UpstreamGeneration:        "unknown",
				LifecycleState:            "unknown",
				ClockAlignmentUnverified:  true,
				ExecutionDeviceUnverified: true,
				BackendID:                 a.BackendID,
				Epoch:                     a.Epoch,
				ActivityID:                a.ActivityID,
				DeviceID:                  deviceID,
				WindowStart:               a.Timestamp.Add(-window.Before),
				WindowEnd:                 a.Timestamp.Add(window.After),
			}

			// Identity conflict: no valid association.
			if a.IdentityConflict {
				result = append(result, assoc)
				continue
			}

			// Window: [activity.Timestamp - Before, activity.Timestamp + After] inclusive.
			windowStart := a.Timestamp.Add(-window.Before)
			windowEnd := a.Timestamp.Add(window.After)

			var matched []time.Time
			for _, ts := range timestamps {
				if !ts.Before(windowStart) && !ts.After(windowEnd) {
					matched = append(matched, ts)
				}
			}

			if len(matched) == 0 {
				// No matching samples: explicit unavailable row.
				result = append(result, assoc)
				continue
			}

			// Valid association.
			assoc.Kind = "approximate_time_window"
			assoc.MatchedSampleCount = len(matched)
			assoc.FirstSampleTime = matched[0]
			assoc.LastSampleTime = matched[len(matched)-1]

			if len(matched) >= 2 {
				var minNS, maxNS int64
				for i := 1; i < len(matched); i++ {
					interval := int64(matched[i].Sub(matched[i-1]))
					if i == 1 || interval < minNS {
						minNS = interval
					}
					if i == 1 || interval > maxNS {
						maxNS = interval
					}
				}
				minMS := minNS / 1_000_000
				maxMS := maxNS / 1_000_000
				assoc.ObservedMinIntervalMS = &minMS
				assoc.ObservedMaxIntervalMS = &maxMS
			}

			result = append(result, assoc)
		}
	}

	// Sort result deterministically by backend, epoch, activity ID, device ID.
	sort.Slice(result, func(i, j int) bool {
		if result[i].BackendID != result[j].BackendID {
			return result[i].BackendID < result[j].BackendID
		}
		if result[i].Epoch != result[j].Epoch {
			return result[i].Epoch < result[j].Epoch
		}
		if result[i].ActivityID != result[j].ActivityID {
			return result[i].ActivityID < result[j].ActivityID
		}
		return result[i].DeviceID < result[j].DeviceID
	})

	return result, nil
}

// isGPUDeviceID checks that the device ID is a canonical 'gpu-' prefixed ASCII
// identifier (e.g. "gpu-12345"). The raw name, UUID, driver, and PCI address
// are never used.
func isGPUDeviceID(id string) bool {
	if !strings.HasPrefix(id, "gpu-") {
		return false
	}
	rest := id[4:]
	if len(rest) == 0 || len(rest) > 64 {
		return false
	}
	for _, c := range rest {
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
