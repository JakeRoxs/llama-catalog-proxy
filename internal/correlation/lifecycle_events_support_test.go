// Package correlation tests for DescribeLifecycleEventSupport.
package correlation

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func TestDescribeLifecycleEventSupportValidBackends(t *testing.T) {
	backends := map[string]config.Backend{
		"test":  {URL: "http://127.0.0.1:8080", Diagnostics: "llama_swap"},
		"other": {URL: "http://127.0.0.1:8081"}, // no Diagnostics -> disabled
	}
	report, err := DescribeLifecycleEventSupport(backends, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.Source != "adapter_capability" {
		t.Fatalf("source = %q, want adapter_capability", report.Source)
	}
	if report.EpochScope != "collector" {
		t.Fatalf("epoch_scope = %q, want collector", report.EpochScope)
	}
	if len(report.BackendStatuses) != 2 {
		t.Fatalf("expected 2 backend statuses, got %d", len(report.BackendStatuses))
	}
	// llama_swap backend: unsupported with transition_payload_not_exposed
	// other backend: disabled
	assertLifecycleBackendStatuses(t, report.BackendStatuses)
	// Explicit unavailability fields
	assertLifecycleUnavailable(t, report)
	// ObservedTransitions is empty, no collection claimed
	if len(report.ObservedTransitions) != 0 {
		t.Fatalf("expected 0 observed transitions, got %d", len(report.ObservedTransitions))
	}
	if report.CollectionPerformed {
		t.Fatal("capability report must not claim transition collection")
	}
	// No URLs, secrets, or model IDs leaked. Backend IDs are configured
	// identifiers (map keys), not secrets.
	output, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	assertNoSecretLeaked(t, output, []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081", "secret-key", "secret-header"})
}

// assertLifecycleBackendStatuses verifies the capability status for each
// configured backend: llama_swap is unsupported without transition payloads,
// all other backends are disabled.
func assertLifecycleBackendStatuses(t *testing.T, statuses map[string]LifecycleBackendEventSupport) {
	t.Helper()
	testStatus, ok := statuses["test"]
	if !ok {
		t.Fatal("missing backend status for test")
	}
	if testStatus.State != lifecycleEventSupportUnsupported {
		t.Fatalf("state = %q, want %q", testStatus.State, lifecycleEventSupportUnsupported)
	}
	if testStatus.Reason != lifecycleEventReasonTransitionPayloadNotExposed {
		t.Fatalf("reason = %q, want %q", testStatus.Reason, lifecycleEventReasonTransitionPayloadNotExposed)
	}
	if testStatus.Explanation != lifecycleEventExplainTransitionPayloadNotExposed {
		t.Fatalf("explanation = %q", testStatus.Explanation)
	}
	otherStatus, ok := statuses["other"]
	if !ok {
		t.Fatal("missing backend status for other")
	}
	if otherStatus.State != lifecycleEventSupportDisabled {
		t.Fatalf("state = %q, want %q", otherStatus.State, lifecycleEventSupportDisabled)
	}
	if otherStatus.Reason != lifecycleEventReasonNotEnabled {
		t.Fatalf("reason = %q, want %q", otherStatus.Reason, lifecycleEventReasonNotEnabled)
	}
	if otherStatus.Explanation != lifecycleEventExplainNotEnabled {
		t.Fatalf("explanation = %q", otherStatus.Explanation)
	}
}

// assertLifecycleUnavailable verifies the explicit unavailability statements
// for the inspected adapter contract.
func assertLifecycleUnavailable(t *testing.T, report LifecycleEventSupportReport) {
	t.Helper()
	if report.SourceTimeUnavailable != lifecycleEventSourceTimeUnavailable {
		t.Fatalf("source_time_unavailable = %q", report.SourceTimeUnavailable)
	}
	if report.SourceEventIDUnavailable != lifecycleEventSourceEventIDUnavailable {
		t.Fatalf("source_event_id_unavailable = %q", report.SourceEventIDUnavailable)
	}
	if report.ReplayUnsupported != lifecycleEventReplayUnsupported {
		t.Fatalf("replay_unsupported = %q", report.ReplayUnsupported)
	}
}

// assertNoSecretLeaked verifies that none of the given secrets appear in the
// serialized report output.
func assertNoSecretLeaked(t *testing.T, output []byte, secrets []string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(string(output), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}

func TestDescribeLifecycleEventSupportInvalidIDsOmitted(t *testing.T) {
	backends := map[string]config.Backend{
		"valid": {URL: "http://127.0.0.1:8080", Diagnostics: "llama_swap"},
		"":      {URL: "http://127.0.0.1:8081"}, // empty ID
	}
	report, err := DescribeLifecycleEventSupport(backends, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.BackendStatuses) != 1 {
		t.Fatalf("expected 1 backend status (invalid IDs omitted), got %d", len(report.BackendStatuses))
	}
	if _, ok := report.BackendStatuses["valid"]; !ok {
		t.Fatal("missing backend status for valid")
	}
	if _, ok := report.BackendStatuses[""]; ok {
		t.Fatal("empty ID should be omitted")
	}
}

func TestDescribeLifecycleEventSupportNonPositiveEpoch(t *testing.T) {
	backends := map[string]config.Backend{
		"test": {URL: "http://127.0.0.1:8080", Diagnostics: "llama_swap"},
	}
	for _, epoch := range []int64{0, -1} {
		_, err := DescribeLifecycleEventSupport(backends, epoch)
		if err == nil {
			t.Fatalf("expected error for epoch %d", epoch)
		}
	}
}

func TestDescribeLifecycleEventSupportNoURLNoSecretLeak(t *testing.T) {
	backends := map[string]config.Backend{
		"test": {URL: "http://127.0.0.1:8080", Diagnostics: "llama_swap", APIKey: "secret-key", Headers: map[string]string{"X-Custom": "secret-header"}},
	}
	report, err := DescribeLifecycleEventSupport(backends, 1)
	if err != nil {
		t.Fatal(err)
	}
	output, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-key", "secret-header", "http://127.0.0.1:8080", "127.0.0.1:8080"} {
		if strings.Contains(string(output), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}

func TestDescribeLifecycleEventSupportExplicitUnknownStates(t *testing.T) {
	backends := map[string]config.Backend{
		"test": {URL: "http://127.0.0.1:8080", Diagnostics: "llama_swap"},
	}
	report, err := DescribeLifecycleEventSupport(backends, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Explicit unknown/unavailable states must be present, not omitted.
	if report.SourceTimeUnavailable == "" {
		t.Fatal("source_time_unavailable should be explicitly set")
	}
	if report.SourceEventIDUnavailable == "" {
		t.Fatal("source_event_id_unavailable should be explicitly set")
	}
	if report.ReplayUnsupported == "" {
		t.Fatal("replay_unsupported should be explicitly set")
	}
	// No reader ran: unknown is null, not a measured empty stream.
	if report.ObservedTransitions != nil || report.CollectionPerformed {
		t.Fatal("unsupported source must not manufacture a zero-transition observation")
	}
	if len(report.ObservedTransitions) != 0 {
		t.Fatalf("expected 0 observed transitions, got %d", len(report.ObservedTransitions))
	}
}
