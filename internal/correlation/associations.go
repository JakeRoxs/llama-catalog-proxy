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

// sampleKey groups samples by backend ID, epoch, and canonical device ID.
type sampleKey struct {
	backendID string
	epoch     int64
	deviceID  string
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
	if err := validateActivityWindow(window); err != nil {
		return nil, err
	}

	// Validate input bounds before allocation.
	if err := validateActivityInputBounds(activities, samples); err != nil {
		return nil, err
	}

	// Validate activities: nil, invalid IDs, zero timestamps, non-positive
	// epochs are rejected with explicit unavailable errors.
	if err := validateActivityRecords(activities); err != nil {
		return nil, err
	}

	// Validate samples and group by backend+epoch+canonical device ID.
	samplesByKey, devicesByPair, err := groupSamples(samples)
	if err != nil {
		return nil, err
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
		associateActivity(a, samplesByKey, devicesByPair, window, &result)
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

// validateActivityWindow validates the association look-behind / look-ahead
// window bounds.
func validateActivityWindow(window AssociationWindow) error {
	if window.Before < 0 || window.Before > time.Minute {
		return errors.New("association window before out of range: must be 0..1 minute")
	}
	if window.After < 0 || window.After > time.Minute {
		return errors.New("association window after out of range: must be 0..1 minute")
	}
	return nil
}

// validateActivityInputBounds validates the input size bounds upfront.
func validateActivityInputBounds(activities []*ActivityRecord, samples []TimedDeviceSample) error {
	if len(activities) > 1000 {
		return errors.New("too many activities: max 1000")
	}
	if len(samples) > 4096 {
		return errors.New("too many samples: max 4096")
	}
	return nil
}

// validateActivityRecords validates each activity record.
func validateActivityRecords(activities []*ActivityRecord) error {
	for _, a := range activities {
		if err := validateActivity(a); err != nil {
			return err
		}
	}
	return nil
}

// groupSamples validates samples and groups them by backend+epoch+device ID.
// It tracks distinct device counts per backend/epoch pair (max 8).
func groupSamples(samples []TimedDeviceSample) (map[sampleKey][]time.Time, map[string]map[int64]map[string]bool, error) {
	samplesByKey := make(map[sampleKey][]time.Time)
	devicesByPair := make(map[string]map[int64]map[string]bool)

	for _, s := range samples {
		if err := validateSample(s); err != nil {
			return nil, nil, err
		}
		if err := trackDevice(devicesByPair, s.BackendID, s.Epoch, s.DeviceID); err != nil {
			return nil, nil, err
		}
		key := sampleKey{s.BackendID, s.Epoch, s.DeviceID}
		ts := s.Timestamp.UTC().Round(0)
		appendUniqueTimestamp(samplesByKey, key, ts)
	}

	return samplesByKey, devicesByPair, nil
}

// validateSample validates a single TimedDeviceSample. It is pure: it does not
// mutate the sample. Checks are ordered backend ID, device ID, GPU device ID
// format, timestamp, epoch. Returns the first failing check's error.
func validateSample(s TimedDeviceSample) error {
	if !validIdentifier(s.BackendID) {
		return errors.New("invalid backend id in sample: explicit unavailable")
	}
	if !validIdentifier(s.DeviceID) {
		return errors.New("invalid device id in sample: explicit unavailable")
	}
	if !isGPUDeviceID(s.DeviceID) {
		return errors.New("invalid device id: must be canonical gpu- ID: explicit unavailable")
	}
	if s.Timestamp.IsZero() {
		return errors.New("zero timestamp in sample: explicit unavailable")
	}
	if s.Epoch <= 0 {
		return errors.New("non-positive epoch in sample: explicit unavailable")
	}
	return nil
}

// trackDevice records a device for a backend+epoch pair and enforces the
// per-pair device cap of 8. It mutates devicesByPair in place.
func trackDevice(devicesByPair map[string]map[int64]map[string]bool, backendID string, epoch int64, deviceID string) error {
	if devicesByPair[backendID] == nil {
		devicesByPair[backendID] = make(map[int64]map[string]bool)
	}
	if devicesByPair[backendID][epoch] == nil {
		devicesByPair[backendID][epoch] = make(map[string]bool)
	}
	devicesByPair[backendID][epoch][deviceID] = true
	if len(devicesByPair[backendID][epoch]) > 8 {
		return errors.New("too many devices per backend/epoch: max 8")
	}
	return nil
}

// appendUniqueTimestamp appends ts to the key's timestamp list if it is not
// already present. Distinct timestamps are deduplicated; the list is not
// sorted here (callers sort before use).
func appendUniqueTimestamp(samplesByKey map[sampleKey][]time.Time, key sampleKey, ts time.Time) {
	for _, ets := range samplesByKey[key] {
		if ets.Equal(ts) {
			return
		}
	}
	samplesByKey[key] = append(samplesByKey[key], ts)
}

// buildBaseAssociation builds the unavailable base association for one
// activity/device pair: fixed attribution fields, the activity's association
// window, and the verification flags.
func buildBaseAssociation(a *ActivityRecord, deviceID string, window AssociationWindow) ActivityDeviceAssociation {
	return ActivityDeviceAssociation{
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
}

// computeIntervalStats returns the min/max interval in milliseconds between
// consecutive distinct matched sample timestamps. Returns nil, nil when fewer
// than 2 timestamps exist.
func computeIntervalStats(matched []time.Time) (*int64, *int64) {
	if len(matched) < 2 {
		return nil, nil
	}
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
	return &minMS, &maxMS
}

// associateActivity associates one activity with all devices for its
// backend+epoch. It appends one association per device to result.
func associateActivity(a *ActivityRecord, samplesByKey map[sampleKey][]time.Time, devicesByPair map[string]map[int64]map[string]bool, window AssociationWindow, result *[]ActivityDeviceAssociation) {
	for _, deviceID := range sortedDeviceIDs(a, devicesByPair) {
		associateDevice(a, deviceID, samplesByKey, window, result)
	}
}

// sortedDeviceIDs returns the sorted device IDs for an activity's backend+epoch
// pair. It returns nil when no devices exist for that pair, so callers can
// range over the result safely.
func sortedDeviceIDs(a *ActivityRecord, devicesByPair map[string]map[int64]map[string]bool) []string {
	epochDevices, hasEpoch := devicesByPair[a.BackendID]
	if !hasEpoch {
		return nil
	}
	deviceMap, hasDevices := epochDevices[a.Epoch]
	if !hasDevices {
		return nil
	}
	var deviceIDs []string
	for deviceID := range deviceMap {
		deviceIDs = append(deviceIDs, deviceID)
	}
	sort.Strings(deviceIDs)
	return deviceIDs
}

// associateDevice processes one device for an activity and appends the
// association to result. It is pure with respect to its inputs (result is the
// only mutation site).
func associateDevice(a *ActivityRecord, deviceID string, samplesByKey map[sampleKey][]time.Time, window AssociationWindow, result *[]ActivityDeviceAssociation) {
	timestamps := samplesByKey[sampleKey{a.BackendID, a.Epoch, deviceID}]
	assoc := buildBaseAssociation(a, deviceID, window)

	// Identity conflict: no valid association.
	if a.IdentityConflict {
		*result = append(*result, assoc)
		return
	}

	// Window: [activity.Timestamp - Before, activity.Timestamp + After] inclusive.
	// Reuse the base association's window bounds.
	windowStart := assoc.WindowStart
	windowEnd := assoc.WindowEnd

	matched := samplesInWindow(timestamps, windowStart, windowEnd)

	if len(matched) == 0 {
		*result = append(*result, assoc)
		return
	}

	assoc.Kind = "approximate_time_window"
	assoc.MatchedSampleCount = len(matched)
	assoc.FirstSampleTime = matched[0]
	assoc.LastSampleTime = matched[len(matched)-1]

	if len(matched) >= 2 {
		minMS, maxMS := computeIntervalStats(matched)
		assoc.ObservedMinIntervalMS = minMS
		assoc.ObservedMaxIntervalMS = maxMS
	}

	*result = append(*result, assoc)
}

// samplesInWindow returns the timestamps within [windowStart, windowEnd]
// inclusive. The input slice is not mutated.
func samplesInWindow(timestamps []time.Time, windowStart, windowEnd time.Time) []time.Time {
	var matched []time.Time
	for _, ts := range timestamps {
		if !ts.Before(windowStart) && !ts.After(windowEnd) {
			matched = append(matched, ts)
		}
	}
	return matched
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
