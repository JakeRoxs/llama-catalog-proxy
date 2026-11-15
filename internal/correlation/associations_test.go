package correlation

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAssociationDefaultWindow(t *testing.T) {
	w := DefaultAssociationWindow()
	if w.Before != 2*time.Second || w.After != 2*time.Second {
		t.Fatalf("default window = %+v, want 2s/2s", w)
	}
}

func TestAssociationTimestampRangeAndDeclaredWindow(t *testing.T) {
	// UnixNano wraps outside its narrow representable range; timestamps must
	// remain actual instants rather than wrapping into an unrelated century.
	at := time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
	activity := &ActivityRecord{BackendID: "backend", Epoch: 1, ActivityID: "1", Timestamp: at}
	samples := []TimedDeviceSample{
		{BackendID: "backend", Epoch: 1, DeviceID: "gpu-abcd", Timestamp: at},
		{BackendID: "backend", Epoch: 1, DeviceID: "gpu-abcd", Timestamp: at.Add(time.Second)},
	}
	rows, err := AssociateActivities([]*ActivityRecord{activity}, samples, DefaultAssociationWindow())
	if err != nil || len(rows) != 1 {
		t.Fatalf("association count=%d, error=%v", len(rows), err)
	}
	r := rows[0]
	if r.MatchedSampleCount != 2 || r.ObservedMinIntervalMS == nil || *r.ObservedMinIntervalMS != 1000 {
		t.Fatal("lost source timestamps or observed cadence")
	}
	if !r.WindowStart.Equal(at.Add(-2*time.Second)) || !r.WindowEnd.Equal(at.Add(2*time.Second)) || r.LifecycleState != "unknown" {
		t.Fatal("window or lifecycle uncertainty missing")
	}
}

func TestAssociationSameBackendEpochWithinWindow(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 association, got %d", len(result))
	}
	if result[0].Kind != "approximate_time_window" {
		t.Fatalf("kind = %q, want approximate_time_window", result[0].Kind)
	}
	if result[0].MatchedSampleCount != 1 {
		t.Fatalf("matched count = %d, want 1", result[0].MatchedSampleCount)
	}
	if !result[0].FirstSampleTime.Equal(sampleTime) {
		t.Fatalf("first sample time = %v, want %v", result[0].FirstSampleTime, sampleTime)
	}
	if !result[0].LastSampleTime.Equal(sampleTime) {
		t.Fatalf("last sample time = %v, want %v", result[0].LastSampleTime, sampleTime)
	}
	// With 1 sample, interval stats should be nil.
	if result[0].ObservedMinIntervalMS != nil || result[0].ObservedMaxIntervalMS != nil {
		t.Fatal("interval stats should be nil with 1 sample")
	}
	// Uncertainty fields always serialized.
	if result[0].Attribution != "shared_resource_only" {
		t.Fatalf("attribution = %q, want shared_resource_only", result[0].Attribution)
	}
	if result[0].RequestIDMapping != "unsupported" {
		t.Fatalf("request_id_mapping = %q, want unsupported", result[0].RequestIDMapping)
	}
	if result[0].UpstreamGeneration != "unknown" {
		t.Fatalf("upstream_generation = %q, want unknown", result[0].UpstreamGeneration)
	}
	if !result[0].ClockAlignmentUnverified {
		t.Fatal("clock_alignment_unverified should be true")
	}
	if !result[0].ExecutionDeviceUnverified {
		t.Fatal("execution_device_unverified should be true")
	}
}

func TestAssociationDifferentBackend(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-2", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 associations (different backend), got %d", len(result))
	}
}

func TestAssociationDifferentEpoch(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 2, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 associations (different epoch), got %d", len(result))
	}
}

func TestAssociationOutOfOrderAndDuplicates(t *testing.T) {
	activityTime := fixtureTime
	t1 := activityTime.Add(-time.Second)
	t2 := activityTime.Add(time.Second)
	t3 := activityTime.Add(time.Second) // duplicate of t2

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t3}, // duplicate
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t1},
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t2},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 association, got %d", len(result))
	}
	if result[0].MatchedSampleCount != 2 {
		t.Fatalf("matched count = %d, want 2 (deduped)", result[0].MatchedSampleCount)
	}
	if !result[0].FirstSampleTime.Equal(t1) {
		t.Fatalf("first sample time = %v, want %v", result[0].FirstSampleTime, t1)
	}
	if !result[0].LastSampleTime.Equal(t2) {
		t.Fatalf("last sample time = %v, want %v", result[0].LastSampleTime, t2)
	}
	if result[0].ObservedMinIntervalMS == nil || result[0].ObservedMaxIntervalMS == nil {
		t.Fatal("interval stats should be set with 2+ samples")
	}
	// Interval: t2 - t1 = 2 seconds = 2000ms
	if *result[0].ObservedMinIntervalMS != 2000 || *result[0].ObservedMaxIntervalMS != 2000 {
		t.Fatalf("interval stats = %d/%d, want 2000/2000", *result[0].ObservedMinIntervalMS, *result[0].ObservedMaxIntervalMS)
	}
}

func TestAssociationMultipleActivitiesDeviceAmbiguity(t *testing.T) {
	t1 := fixtureTime
	t2 := fixtureTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: t1},
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-2", Timestamp: t2},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t1},
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t2},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 associations, got %d", len(result))
	}
	if result[0].ActivityID != "act-1" || result[1].ActivityID != "act-2" {
		t.Fatalf("unexpected order: %v/%v", result[0].ActivityID, result[1].ActivityID)
	}
}

func TestAssociationOneSampleNilIntervalStats(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 association, got %d", len(result))
	}
	if result[0].ObservedMinIntervalMS != nil || result[0].ObservedMaxIntervalMS != nil {
		t.Fatal("interval stats should be nil with 1 sample")
	}
}

func TestAssociationZeroSamples(t *testing.T) {
	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: fixtureTime},
	}
	samples := []TimedDeviceSample{}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 associations with no samples, got %d", len(result))
	}
}

func TestAssociationIdentityConflict(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime, IdentityConflict: true},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 association, got %d", len(result))
	}
	if result[0].Kind != "unavailable" {
		t.Fatalf("kind = %q, want unavailable (identity conflict)", result[0].Kind)
	}
}

func TestAssociationInputValidation(t *testing.T) {
	// Nil activity.
	_, err := AssociateActivities([]*ActivityRecord{nil}, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for nil activity")
	}

	// Empty backend ID.
	_, err = AssociateActivities([]*ActivityRecord{{BackendID: "", Epoch: 1, ActivityID: "act-1", Timestamp: fixtureTime}}, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for empty backend ID")
	}

	// Empty activity ID.
	_, err = AssociateActivities([]*ActivityRecord{{BackendID: "backend-1", Epoch: 1, ActivityID: "", Timestamp: fixtureTime}}, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for empty activity ID")
	}

	// Zero timestamp.
	_, err = AssociateActivities([]*ActivityRecord{{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: time.Time{}}}, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for zero timestamp")
	}

	// Non-positive epoch.
	_, err = AssociateActivities([]*ActivityRecord{{BackendID: "backend-1", Epoch: 0, ActivityID: "act-1", Timestamp: fixtureTime}}, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for non-positive epoch")
	}

	// Invalid device ID (not gpu- prefix).
	_, err = AssociateActivities(nil, []TimedDeviceSample{{BackendID: "backend-1", Epoch: 1, DeviceID: "invalid", Timestamp: fixtureTime}}, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for invalid device ID")
	}

	// Zero sample timestamp.
	_, err = AssociateActivities(nil, []TimedDeviceSample{{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: time.Time{}}}, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for zero sample timestamp")
	}

	// Non-positive sample epoch.
	_, err = AssociateActivities(nil, []TimedDeviceSample{{BackendID: "backend-1", Epoch: 0, DeviceID: "gpu-1", Timestamp: fixtureTime}}, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for non-positive sample epoch")
	}
}

func TestAssociationPolicyValidation(t *testing.T) {
	// Negative before.
	_, err := AssociateActivities(nil, nil, AssociationWindow{Before: -time.Second, After: 2 * time.Second})
	if err == nil {
		t.Fatal("expected error for negative before")
	}

	// Negative after.
	_, err = AssociateActivities(nil, nil, AssociationWindow{Before: 2 * time.Second, After: -time.Second})
	if err == nil {
		t.Fatal("expected error for negative after")
	}

	// Before > 1 minute.
	_, err = AssociateActivities(nil, nil, AssociationWindow{Before: time.Minute + time.Second, After: 2 * time.Second})
	if err == nil {
		t.Fatal("expected error for before > 1 minute")
	}

	// After > 1 minute.
	_, err = AssociateActivities(nil, nil, AssociationWindow{Before: 2 * time.Second, After: time.Minute + time.Second})
	if err == nil {
		t.Fatal("expected error for after > 1 minute")
	}
}

func TestAssociationBounds(t *testing.T) {
	// Too many activities.
	activities := make([]*ActivityRecord, 1001)
	for i := range activities {
		activities[i] = &ActivityRecord{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: fixtureTime}
	}
	_, err := AssociateActivities(activities, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for too many activities")
	}

	// Too many samples.
	samples := make([]TimedDeviceSample, 4097)
	for i := range samples {
		samples[i] = TimedDeviceSample{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: fixtureTime}
	}
	_, err = AssociateActivities(nil, samples, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for too many samples")
	}

	// Too many devices per backend/epoch (distinct device IDs).
	samples = make([]TimedDeviceSample, 9)
	for i := range samples {
		samples[i] = TimedDeviceSample{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-" + string(rune('1'+i)), Timestamp: fixtureTime}
	}
	_, err = AssociateActivities(nil, samples, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for too many devices per backend/epoch")
	}
}

func TestAssociationWindowBoundaries(t *testing.T) {
	activityTime := fixtureTime
	window := AssociationWindow{Before: 0, After: 0} // point window

	// Sample exactly at activity time should match.
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: activityTime},
	}
	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}

	result, err := AssociateActivities(activities, samples, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Kind != "approximate_time_window" {
		t.Fatalf("expected match at exact timestamp, got %d results", len(result))
	}

	// Sample 1ns before should not match with point window: unavailable row.
	samples = []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: activityTime.Add(-time.Nanosecond)},
	}
	result, err = AssociateActivities(activities, samples, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Kind != "unavailable" || result[0].MatchedSampleCount != 0 {
		t.Fatalf("expected 1 unavailable association for sample before point window, got %d results kind=%q count=%d", len(result), result[0].Kind, result[0].MatchedSampleCount)
	}

	// Sample 1ns after should not match with point window: unavailable row.
	samples = []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: activityTime.Add(time.Nanosecond)},
	}
	result, err = AssociateActivities(activities, samples, window)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].Kind != "unavailable" || result[0].MatchedSampleCount != 0 {
		t.Fatalf("expected 1 unavailable association for sample after point window, got %d results kind=%q count=%d", len(result), result[0].Kind, result[0].MatchedSampleCount)
	}
}

func TestAssociationNoModelIDInOutput(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime, ModelID: "secret-model"},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 association, got %d", len(result))
	}

	// JSON marshaling should not contain the model ID or route.
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("json marshal error: %v", err)
	}
	if strings.Contains(string(encoded), "secret-model") {
		t.Fatal("model ID leaked in output")
	}
	if strings.Contains(string(encoded), "route") {
		t.Fatal("route field should not be in output")
	}
}

func TestAssociationStableOrder(t *testing.T) {
	t1 := fixtureTime
	t2 := fixtureTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-2", Epoch: 1, ActivityID: "act-2", Timestamp: t2},
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: t1},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t1},
		{BackendID: "backend-2", Epoch: 1, DeviceID: "gpu-2", Timestamp: t2},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 associations, got %d", len(result))
	}
	// Should be sorted by backend, then activity ID.
	if result[0].BackendID != "backend-1" || result[1].BackendID != "backend-2" {
		t.Fatalf("unexpected order: %v/%v", result[0].BackendID, result[1].BackendID)
	}
}

func TestAssociationConcurrent(t *testing.T) {
	activityTime := fixtureTime
	sampleTime := activityTime.Add(time.Second)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	var wg sync.WaitGroup
	results := make([][]ActivityDeviceAssociation, 10)
	errs := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = AssociateActivities(activities, samples, DefaultAssociationWindow())
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		if errs[i] != nil {
			t.Fatalf("concurrent call %d error: %v", i, errs[i])
		}
		if len(results[i]) != 1 {
			t.Fatalf("concurrent call %d: expected 1 association, got %d", i, len(results[i]))
		}
		if results[i][0].Kind != "approximate_time_window" {
			t.Fatalf("concurrent call %d: unexpected kind %q", i, results[i][0].Kind)
		}
	}
}

func TestAssociationInvalidDeviceID(t *testing.T) {
	// Device ID not starting with "gpu-".
	_, err := AssociateActivities(nil, []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "invalid-device", Timestamp: fixtureTime},
	}, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for invalid device ID format")
	}

	// Device ID "gpu-" with no digits.
	_, err = AssociateActivities(nil, []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-", Timestamp: fixtureTime},
	}, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for device ID with no digits")
	}
}

func TestAssociationNoMatchingSamples(t *testing.T) {
	activityTime := fixtureTime
	// Sample far outside the window.
	sampleTime := activityTime.Add(10 * time.Minute)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: sampleTime},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 unavailable association, got %d", len(result))
	}
	if result[0].Kind != "unavailable" {
		t.Fatalf("kind = %q, want unavailable", result[0].Kind)
	}
	if result[0].MatchedSampleCount != 0 {
		t.Fatalf("matched count = %d, want 0", result[0].MatchedSampleCount)
	}
}

func TestAssociationSubMillisecondInterval(t *testing.T) {
	activityTime := fixtureTime
	// Two samples 1000ns apart (sub-millisecond).
	t1 := activityTime.Add(-time.Nanosecond * 500)
	t2 := activityTime.Add(time.Nanosecond * 500)

	activities := []*ActivityRecord{
		{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: activityTime},
	}
	samples := []TimedDeviceSample{
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t1},
		{BackendID: "backend-1", Epoch: 1, DeviceID: "gpu-1", Timestamp: t2},
	}

	result, err := AssociateActivities(activities, samples, DefaultAssociationWindow())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 association, got %d", len(result))
	}
	// Interval is 1000ns = 0ms (integer division). Sub-millisecond spacing
	// yields 0ms, not a rate.
	if *result[0].ObservedMinIntervalMS != 0 || *result[0].ObservedMaxIntervalMS != 0 {
		t.Fatalf("interval stats = %d/%d, want 0/0 (sub-millisecond)", *result[0].ObservedMinIntervalMS, *result[0].ObservedMaxIntervalMS)
	}
}

func TestAssociationErrorsAreFinite(t *testing.T) {
	// All error returns must be non-nil and finite (not panics).
	_, err := AssociateActivities(nil, nil, AssociationWindow{Before: -time.Second, After: 2 * time.Second})
	if err == nil {
		t.Fatal("expected finite error for negative window")
	}

	_, err = AssociateActivities([]*ActivityRecord{nil}, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected finite error for nil activity")
	}

	_, err = AssociateActivities(nil, []TimedDeviceSample{{BackendID: "", Epoch: 1, DeviceID: "gpu-1", Timestamp: fixtureTime}}, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected finite error for empty sample backend ID")
	}

	// Ensure no panic on oversized inputs.
	activities := make([]*ActivityRecord, 1001)
	for i := range activities {
		activities[i] = &ActivityRecord{BackendID: "backend-1", Epoch: 1, ActivityID: "act-1", Timestamp: fixtureTime}
	}
	_, err = AssociateActivities(activities, nil, DefaultAssociationWindow())
	if err == nil {
		t.Fatal("expected error for too many activities")
	}
}
