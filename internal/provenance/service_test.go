package provenance

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
)

func newDiagnosticsSnapshot(backendID string, capabilities map[string]diagnostics.Observation) *diagnostics.Snapshot {
	return &diagnostics.Snapshot{
		Backend:      backendID,
		Adapter:      "llama_swap",
		GeneratedAt:  time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC),
		Capabilities: capabilities,
	}
}

func observation(state diagnostics.State, observedAt time.Time, stale bool, reason string) diagnostics.Observation {
	return diagnostics.Observation{
		State:         state,
		Source:        "/api/version",
		ObservedAt:    observedAt,
		FreshUntil:    observedAt.Add(30 * time.Second),
		ExpiresAt:     observedAt.Add(5 * time.Minute),
		LastAttemptAt: observedAt,
		Reason:        reason,
		Stale:         stale,
	}
}

func observationWithBuild(state diagnostics.State, observedAt time.Time, stale bool, reason string, build *diagnostics.Build) diagnostics.Observation {
	obs := observation(state, observedAt, stale, reason)
	obs.Build = build
	return obs
}

func requireUnknown(t *testing.T, prov *ModelProvenance, name string) {
	t.Helper()
	f, ok := prov.Fields[name]
	if !ok {
		t.Fatalf("field %s should be present (unknown schema)", name)
	}
	if f.Value != nil {
		t.Fatalf("field %s value = %v, want nil", name, f.Value)
	}
	if f.Source != sourceUnknown {
		t.Fatalf("field %s source = %s, want %s", name, f.Source, sourceUnknown)
	}
	if f.Verification != Unknown {
		t.Fatalf("field %s verification = %s, want unknown", name, f.Verification)
	}
	if !f.ObservedAt.IsZero() {
		t.Fatalf("field %s observed_at = %v, want zero", name, f.ObservedAt)
	}
}

func requireDeclared(t *testing.T, prov *ModelProvenance, name string, value any, observedAt time.Time) {
	t.Helper()
	f, ok := prov.Fields[name]
	if !ok {
		t.Fatalf("field %s should be present", name)
	}
	if f.Verification != Declared {
		t.Fatalf("field %s verification = %s, want declared", name, f.Verification)
	}
	if f.Source != sourceCatalog {
		t.Fatalf("field %s source = %s, want %s", name, f.Source, sourceCatalog)
	}
	if f.Value != value {
		t.Fatalf("field %s value = %v, want %v", name, f.Value, value)
	}
	if !f.ObservedAt.Equal(observedAt) {
		t.Fatalf("field %s observed_at = %v, want %v", name, f.ObservedAt, observedAt)
	}
}

func requireVerified(t *testing.T, prov *ModelProvenance, name string, value any, observedAt time.Time) {
	t.Helper()
	f, ok := prov.Fields[name]
	if !ok {
		t.Fatalf("field %s should be present", name)
	}
	if f.Verification != Verified {
		t.Fatalf("field %s verification = %s, want verified", name, f.Verification)
	}
	if f.Source != sourceVersion {
		t.Fatalf("field %s source = %s, want %s", name, f.Source, sourceVersion)
	}
	if f.Value != value {
		t.Fatalf("field %s value = %v, want %v", name, f.Value, value)
	}
	if !f.ObservedAt.Equal(observedAt) {
		t.Fatalf("field %s observed_at = %v, want %v", name, f.ObservedAt, observedAt)
	}
}

func TestBuildUnknownStaticSchema(t *testing.T) {
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	prov := Build(identity, map[string]any{"id": "model"}, nil)

	for _, name := range unknownFields {
		if name == "model_identity" {
			continue // the valid catalog identity below is present
		}
		requireUnknown(t, &prov, name)
	}
	requireDeclared(t, &prov, "model_identity", "model", time.Time{})
	// Missing catalog context and build are explicitly unknown, not omitted.
	requireUnknown(t, &prov, "catalog_context_length")
	requireUnknown(t, &prov, "build_version")
}

func TestBuildCatalogContextDeclared(t *testing.T) {
	model := map[string]any{"id": "model", "context_length": 8192}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	prov := Build(identity, model, nil)

	requireDeclared(t, &prov, "catalog_context_length", 8192, time.Time{})
	// Static unknown fields remain present.
	requireUnknown(t, &prov, "quantization")
	requireUnknown(t, &prov, "usable_context_length")
	// No build without diagnostics.
	if _, ok := prov.Fields["build_version"]; !ok {
		t.Fatal("build_version should be present (unknown) without diagnostics")
	}
}

func TestBuildCatalogContextInvalidValues(t *testing.T) {
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	invalid := []any{
		float64(8192.5), // fractional
		float64(math.NaN()),
		float64(math.Inf(1)),
		float64(math.Inf(-1)),
		float64(-1), // negative
		float64(-8192),
		float64(maxInt),                     // 2^63, above maxInt
		float64(maxInt) * 2,                 // overflow
		json.Number("8192.5"),               // fractional
		json.Number("-1"),                   // negative
		json.Number("99999999999999999999"), // overflow
	}
	for _, v := range invalid {
		model := map[string]any{"id": "model", "context_length": v}
		prov := Build(identity, model, nil)
		requireUnknown(t, &prov, "catalog_context_length")
	}
}

func TestBuildCatalogContextZeroAndMissing(t *testing.T) {
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	for _, v := range []any{0, float64(0), json.Number("0")} {
		model := map[string]any{"id": "model", "context_length": v}
		prov := Build(identity, model, nil)
		requireUnknown(t, &prov, "catalog_context_length")
	}
	// Missing context_length.
	model := map[string]any{"id": "model"}
	prov := Build(identity, model, nil)
	requireUnknown(t, &prov, "catalog_context_length")
}

func TestBuildIntValueBounds(t *testing.T) {
	valid := []any{int(8192), int64(8192), float64(8192), json.Number("8192")}
	for _, v := range valid {
		val, ok := intValue(v)
		if !ok || val != 8192 {
			t.Fatalf("intValue(%v) = (%d, %v), want (8192, true)", v, val, ok)
		}
	}
	// Zero is valid for intValue but not advertised (v > 0).
	if val, ok := intValue(0); !ok || val != 0 {
		t.Fatalf("intValue(0) = (%d, %v), want (0, true)", val, ok)
	}
	invalid := []any{
		float64(8192.5), float64(math.NaN()), float64(math.Inf(1)),
		float64(-1), float64(maxInt) + 1, float64(math.MaxFloat64),
		json.Number("8192.5"), json.Number("-1"), json.Number("99999999999999999999"),
		"not-a-number", []any{1}, map[string]any{"a": 1},
	}
	for _, v := range invalid {
		if val, ok := intValue(v); ok {
			t.Fatalf("intValue(%v) = (%d, true), want invalid", v, val)
		}
	}
}

func TestBuildFreshBuildInfoVerified(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	obs := observationWithBuild(diagnostics.Supported, now, false, "", &diagnostics.Build{
		Version: "v2.0.0",
		Commit:  "deadbeef",
		Date:    "2026-11-11T00:00:00Z",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireVerified(t, &prov, "build_version", "v2.0.0", now)
	requireVerified(t, &prov, "build_commit", "deadbeef", now)
	requireVerified(t, &prov, "build_date", "2026-11-11T00:00:00Z", now)
}

func TestBuildStaleBuildInfoUnknown(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	obs := observationWithBuild(diagnostics.Supported, now.Add(-60*time.Second), true, "", &diagnostics.Build{
		Version: "v1.0.0",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireUnknown(t, &prov, "build_version")
	requireUnknown(t, &prov, "build_commit")
	requireUnknown(t, &prov, "build_date")
}

func TestBuildExpiredBuildInfoUnknown(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	// Expired: expiry is not after the observation time.
	obs := diagnostics.Observation{
		State:         diagnostics.Supported,
		Source:        "/api/version",
		ObservedAt:    now,
		FreshUntil:    now.Add(30 * time.Second),
		ExpiresAt:     now, // not after ObservedAt -> expired
		LastAttemptAt: now,
		Reason:        "",
		Stale:         false,
		Build:         &diagnostics.Build{Version: "v1.0.0"},
	}
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireUnknown(t, &prov, "build_version")
	requireUnknown(t, &prov, "build_commit")
	requireUnknown(t, &prov, "build_date")
}

func TestBuildUnsupportedBuildInfoUnknown(t *testing.T) {
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	obs := observationWithBuild(diagnostics.Unsupported, time.Now(), false, "", &diagnostics.Build{
		Version: "v1.0.0",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireUnknown(t, &prov, "build_version")
}

func TestBuildUnauthorizedBuildInfoUnknown(t *testing.T) {
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	obs := observationWithBuild(diagnostics.Unauthorized, time.Now(), false, "authorization_required", &diagnostics.Build{
		Version: "v1.0.0",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireUnknown(t, &prov, "build_version")
	// Raw reason is not copied.
	if f, ok := prov.Fields["build_version"]; ok && f.Source != sourceUnknown {
		t.Fatalf("build_version source = %s, want %s (raw reason not copied)", f.Source, sourceUnknown)
	}
}

func TestBuildNoBuildInfoUnknown(t *testing.T) {
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	obs := observation(diagnostics.Supported, time.Now(), false, "")
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireUnknown(t, &prov, "build_version")
	requireUnknown(t, &prov, "build_commit")
	requireUnknown(t, &prov, "build_date")
}

func TestBuildZeroObservedAtUnknown(t *testing.T) {
	model := map[string]any{"id": "model"}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	obs := diagnostics.Observation{
		State:  diagnostics.Supported,
		Source: "/api/version",
		Stale:  false,
		Build:  &diagnostics.Build{Version: "v1.0.0"},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})
	prov := Build(identity, model, snap)

	requireUnknown(t, &prov, "build_version")
}

func TestBuildNoMetaLlamaSwapExtraction(t *testing.T) {
	model := map[string]any{
		"id": "model",
		"meta": map[string]any{
			"llamaswap": map[string]any{
				"template_version": strings.Repeat("v", 65),
				"template_hash":    "abc123",
				"quantization":     "command --secret",
				"profile":          "profile with spaces",
				"terse":            "true",
			},
		},
	}
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	prov := Build(identity, model, nil)

	// These fields are unavailable on current APIs, so they are reported as
	// unknown (not extracted from untrusted meta.llamaswap).
	for _, f := range []string{"template_version", "template_hash", "quantization", "requested_profile", "requested_terse"} {
		requireUnknown(t, &prov, f)
	}
	// The untrusted meta values must not be used as evidence.
	if f := prov.Fields["quantization"]; f.Value != nil {
		t.Fatalf("quantization value = %v, want nil (meta not trusted)", f.Value)
	}
}

func TestBuildBoundedDeclaredTemplateAndProfileMetadata(t *testing.T) {
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	hash := strings.Repeat("a", 64)
	model := map[string]any{"id": "model", "meta": map[string]any{"llamaswap": map[string]any{
		"template_version": "chat-template-v2", "template_hash": hash, "quantization": "Q4_K_M", "profile": "reasoning", "terse": true,
		"command": "PRIVATE_COMMAND_SENTINEL", "prompt": "PRIVATE_PROMPT_SENTINEL", "api_key": "PRIVATE_KEY_SENTINEL",
	}}}
	prov := Build(identity, model, nil)
	for field, value := range map[string]any{"template_version": "chat-template-v2", "template_hash": hash, "quantization": "Q4_K_M", "requested_profile": "reasoning", "requested_terse": true} {
		got := prov.Fields[field]
		if got.Value != value || got.Source == sourceUnknown || got.Verification != Declared || !got.ObservedAt.IsZero() {
			t.Fatalf("declared field %s = %#v, want %v from bounded metadata", field, got, value)
		}
	}
	for _, field := range []string{"file_identity", "loaded", "observed_profile", "process_generation"} {
		requireUnknown(t, &prov, field)
	}
	b, err := json.Marshal(prov)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"PRIVATE_COMMAND_SENTINEL", "PRIVATE_PROMPT_SENTINEL", "PRIVATE_KEY_SENTINEL"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("unallowlisted metadata leaked: %s", secret)
		}
	}
}

func TestBuildConflictingContextDeclarationsRemainDeclared(t *testing.T) {
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	model := map[string]any{
		"id": "model", "context_length": 8192,
		"meta": map[string]any{"llamaswap": map[string]any{"context_length": 4096}},
	}
	observedAt := time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)
	prov := Build(identity, model, nil, observedAt)
	requireDeclared(t, &prov, "model_identity", "model", observedAt)
	requireDeclared(t, &prov, "catalog_context_length", 8192, observedAt)
	f := prov.Fields["declared_context_length"]
	if f.Value != 4096 || f.Source != "meta.llamaswap.context_length" || f.Verification != Declared || !f.ObservedAt.Equal(observedAt) {
		t.Fatalf("declared_context_length = %#v", f)
	}
	f = prov.Fields["context_length_consistency"]
	if f.Value != "conflicting" || f.Verification != Conflicting || f.ObservedAt != observedAt {
		t.Fatalf("context consistency = %#v", f)
	}
	requireUnknown(t, &prov, "usable_context_length")
}

func TestBuildRejectsMismatchedModelIdentityEvidence(t *testing.T) {
	identity := ModelIdentity{PublicID: "requested", BackendID: "default", LocalID: "requested"}
	prov := Build(identity, map[string]any{"id": "different", "context_length": 8192}, nil, time.Now())
	if prov.Identity.LocalID != "requested" {
		t.Fatal("requested identity should remain separately represented")
	}
	requireUnknown(t, &prov, "model_identity")
	requireUnknown(t, &prov, "catalog_context_length")
	requireUnknown(t, &prov, "build_version")
}

func TestBuildRejectsMalformedDeclaredMetadata(t *testing.T) {
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	model := map[string]any{"id": "model", "meta": map[string]any{"llamaswap": map[string]any{
		"template_version": strings.Repeat("v", 65), "template_hash": "not-a-digest", "quantization": "command --secret", "profile": "profile with spaces", "terse": "true",
	}}}
	prov := Build(identity, model, nil)
	for _, field := range []string{"template_version", "template_hash", "quantization", "requested_profile", "requested_terse"} {
		requireUnknown(t, &prov, field)
	}
}

func TestBuildCatalogContextStampsAuthenticFetchedAt(t *testing.T) {
	observed := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	model := map[string]any{"id": "model", "context_length": 8192}
	prov := Build(identity, model, nil, observed)

	requireDeclared(t, &prov, "catalog_context_length", 8192, observed)
	if !prov.Fields["catalog_context_length"].ObservedAt.Equal(observed) {
		t.Fatalf("catalog_context_length observed_at = %v, want %v", prov.Fields["catalog_context_length"].ObservedAt, observed)
	}
}

func TestBuildCatalogContextNoTimestampWithoutObservedAt(t *testing.T) {
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	model := map[string]any{"id": "model", "context_length": 8192}
	prov := Build(identity, model, nil)

	requireDeclared(t, &prov, "catalog_context_length", 8192, time.Time{})
	if !prov.Fields["catalog_context_length"].ObservedAt.IsZero() {
		t.Fatalf("catalog_context_length observed_at = %v, want zero", prov.Fields["catalog_context_length"].ObservedAt)
	}
}

func TestBuildDeclaredMetadataStampsAuthenticFetchedAt(t *testing.T) {
	observed := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	hash := strings.Repeat("a", 64)
	model := map[string]any{"id": "model", "meta": map[string]any{"llamaswap": map[string]any{
		"template_version": "chat-template-v2", "template_hash": hash, "quantization": "Q4_K_M", "profile": "reasoning", "terse": true,
	}}}
	prov := Build(identity, model, nil, observed)

	for _, field := range []string{"template_version", "template_hash", "quantization", "requested_profile", "requested_terse"} {
		f := prov.Fields[field]
		if f.Value == nil || f.Verification != Declared {
			t.Fatalf("declared field %s = %#v", field, f)
		}
		if !f.ObservedAt.Equal(observed) {
			t.Fatalf("declared field %s observed_at = %v, want %v", field, f.ObservedAt, observed)
		}
	}
}

func TestBuildDeclaredMetadataNoTimestampWithoutObservedAt(t *testing.T) {
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	hash := strings.Repeat("a", 64)
	model := map[string]any{"id": "model", "meta": map[string]any{"llamaswap": map[string]any{
		"template_version": "chat-template-v2", "template_hash": hash, "quantization": "Q4_K_M", "profile": "reasoning", "terse": true,
	}}}
	prov := Build(identity, model, nil)

	for _, field := range []string{"template_version", "template_hash", "quantization", "requested_profile", "requested_terse"} {
		f := prov.Fields[field]
		if f.Value == nil || f.Verification != Declared {
			t.Fatalf("declared field %s = %#v", field, f)
		}
		if !f.ObservedAt.IsZero() {
			t.Fatalf("declared field %s observed_at = %v, want zero", field, f.ObservedAt)
		}
	}
}

func TestServiceSnapshotStampsCatalogFetchedAt(t *testing.T) {
	observed := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": 8192},
		}, FetchedAt: observed},
	}
	obs := observationWithBuild(diagnostics.Supported, time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC), false, "", &diagnostics.Build{
		Version: "v2.0.0",
		Commit:  "deadbeef",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	prov := result[0]
	requireDeclared(t, prov, "catalog_context_length", 8192, observed)
	if !prov.Fields["catalog_context_length"].ObservedAt.Equal(observed) {
		t.Fatalf("catalog_context_length observed_at = %v, want %v", prov.Fields["catalog_context_length"].ObservedAt, observed)
	}
	requireVerified(t, prov, "build_version", "v2.0.0", time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))
}

func TestServiceSnapshotNoFetchedAtStaysZero(t *testing.T) {
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": 8192},
		}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	prov := result[0]
	requireDeclared(t, prov, "catalog_context_length", 8192, time.Time{})
	if !prov.Fields["catalog_context_length"].ObservedAt.IsZero() {
		t.Fatalf("catalog_context_length observed_at = %v, want zero", prov.Fields["catalog_context_length"].ObservedAt)
	}
}

func TestBuildIdentityValidation(t *testing.T) {
	valid := ModelIdentity{
		PublicID:  "prefix/local",
		BackendID: "backend",
		LocalID:   "local",
		Prefix:    "prefix/",
	}
	if !valid.Valid() {
		t.Fatal("valid identity should be valid")
	}

	invalid := []ModelIdentity{
		{PublicID: "p/local", BackendID: "b", LocalID: "local", Prefix: "x/"}, // publicID != prefix+localID
		{PublicID: string(make([]rune, 257)), BackendID: "b", LocalID: "local", Prefix: ""},
		{PublicID: "p/local", BackendID: string(make([]rune, 257)), LocalID: "local", Prefix: ""},
		{PublicID: "p/local", BackendID: "b", LocalID: string(make([]rune, 257)), Prefix: ""},
		{PublicID: "p/local", BackendID: "b", LocalID: "local", Prefix: string(make([]rune, 257))},
		{PublicID: "\xffinvalid", BackendID: "b", LocalID: "local", Prefix: ""},    // invalid UTF8
		{PublicID: "p/local", BackendID: "b", LocalID: "\xfflocal", Prefix: ""},    // invalid UTF8
		{PublicID: "p/local", BackendID: "b", LocalID: "local", Prefix: "\xffpre"}, // invalid UTF8
	}
	for _, id := range invalid {
		if id.Valid() {
			t.Fatalf("identity %#v should be invalid", id)
		}
		if prov := Build(id, map[string]any{"id": "local"}, nil); len(prov.Fields) != 0 {
			t.Fatalf("Build with invalid identity %#v should return no fields", id)
		}
	}
}

func TestBuildForgedIdentityRejected(t *testing.T) {
	// Caller forges an identity with a public ID that does not compose from
	// prefix + local ID.
	id := ModelIdentity{
		PublicID:  "forged/public",
		BackendID: "default",
		LocalID:   "public",
		Prefix:    "other/",
	}
	prov := Build(id, map[string]any{"id": "public", "context_length": 8192}, nil)
	if len(prov.Fields) != 0 {
		t.Fatal("Build with forged identity should return no fields")
	}
}

// TestBuildInvalidOrMismatchedIdentityReturnsUnknownFieldsOnly verifies that an
// invalid identity or a model ID that does not match the requested identity
// returns exactly the fixed unknown-fields schema: declared-metadata and build
// fields are not declared/verified, they remain unknown. This preserves the
// pre-refactor field presence for invalid/nonmatching IDs.
func TestBuildInvalidOrMismatchedIdentityReturnsUnknownFieldsOnly(t *testing.T) {
	validIdentity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	// Invalid identity: public ID does not compose from prefix + local ID.
	forgedIdentity := ModelIdentity{PublicID: "forged/public", BackendID: "default", LocalID: "public", Prefix: "other/"}

	// Invalid identity yields no fields at all.
	prov := Build(forgedIdentity, map[string]any{"id": "public", "context_length": 8192}, nil)
	if len(prov.Fields) != 0 {
		t.Fatalf("Build with invalid identity should return no fields, got %d", len(prov.Fields))
	}

	// Mismatched model ID (valid identity, wrong catalog id) yields exactly the
	// fixed unknown-fields schema. Declared-metadata and build fields remain
	// unknown (not declared/verified) and no extra keys are present.
	prov = Build(validIdentity, map[string]any{"id": "different", "context_length": 8192}, nil)
	if prov.Identity.LocalID != validIdentity.LocalID {
		t.Fatalf("requested identity should remain separately represented: %#v", prov.Identity)
	}
	if len(prov.Fields) != len(unknownFields) {
		t.Fatalf("field count = %d, want %d (unknownFields only)", len(prov.Fields), len(unknownFields))
	}
	for _, name := range unknownFields {
		requireUnknown(t, &prov, name)
	}
	// Declared metadata must not be declared; build must not be verified.
	for _, name := range declaredMetadataFields {
		f := prov.Fields[name]
		if f.Verification != Unknown || f.Value != nil || f.Source != sourceUnknown || !f.ObservedAt.IsZero() {
			t.Fatalf("declared field %s should be unknown, got %#v", name, f)
		}
	}
	for _, name := range []string{"build_version", "build_commit", "build_date"} {
		f := prov.Fields[name]
		if f.Verification != Unknown || f.Value != nil || f.Source != sourceUnknown || !f.ObservedAt.IsZero() {
			t.Fatalf("build field %s should be unknown, got %#v", name, f)
		}
	}
}

func TestIdentityByteBound(t *testing.T) {
	identity := ModelIdentity{PublicID: "model", BackendID: strings.Repeat("é", 129), LocalID: "model"}
	if identity.Valid() {
		t.Fatal("identity component exceeding byte bound accepted")
	}
}

func TestServiceSnapshotProducesProvenance(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": 8192},
		}},
	}
	obs := observationWithBuild(diagnostics.Supported, now, false, "", &diagnostics.Build{
		Version: "v2.0.0",
		Commit:  "deadbeef",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	prov := result[0]
	if prov.Identity.PublicID != "model" || prov.Identity.BackendID != "default" || prov.Identity.LocalID != "model" {
		t.Fatalf("identity = %#v", prov.Identity)
	}
	requireDeclared(t, prov, "catalog_context_length", 8192, time.Time{})
	requireVerified(t, prov, "build_version", "v2.0.0", now)
	requireVerified(t, prov, "build_commit", "deadbeef", now)
	// Static unknown fields present.
	requireUnknown(t, prov, "quantization")
	requireUnknown(t, prov, "usable_context_length")
}

func TestServiceSnapshotMissingDiagnosticsDeclaredOnly(t *testing.T) {
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": 8192},
		}},
	}
	// No diagnostics snapshots provided.

	result, err := New().Snapshot(context.Background(), backendModels, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	prov := result[0]
	requireDeclared(t, prov, "catalog_context_length", 8192, time.Time{})
	// Build fields retained as explicit unknown (not omitted, no raw reason).
	requireUnknown(t, prov, "build_version")
	requireUnknown(t, prov, "build_commit")
	requireUnknown(t, prov, "build_date")
}

func TestServiceSnapshotHandlesEmptyInputs(t *testing.T) {
	result, err := New().Snapshot(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected 0 snapshots for empty inputs, got %d", len(result))
	}
}

func TestServiceSnapshotDeterministic(t *testing.T) {
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "model"}}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})

	service := New()
	result1, err := service.Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}
	result2, err := service.Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}

	if len(result1) != len(result2) {
		t.Fatal("provenance service is not deterministic")
	}
	if result1[0].Identity.PublicID != result2[0].Identity.PublicID {
		t.Fatal("provenance service is not deterministic")
	}
	// Field map serialization is deterministic (Go sorts map keys).
	if len(result1[0].Fields) != len(result2[0].Fields) {
		t.Fatal("field count not deterministic")
	}
	for k := range result1[0].Fields {
		if _, ok := result2[0].Fields[k]; !ok {
			t.Fatalf("field %s missing in second run", k)
		}
	}
}

func TestServiceSnapshotSkipsLongIdentity(t *testing.T) {
	longID := string(make([]rune, 257))
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": longID}}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 0 {
		t.Fatalf("expected 0 snapshots for long identity, got %d", len(result))
	}
}

func TestServiceSnapshotSkipsInvalidIdentity(t *testing.T) {
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": string(make([]rune, 257))}, // too long
			{"id": "valid"},
		}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot (invalid skipped), got %d", len(result))
	}
	if result[0].Identity.LocalID != "valid" {
		t.Fatalf("identity = %#v", result[0].Identity)
	}
}

func TestServiceSnapshotDuplicatePublicID(t *testing.T) {
	// Same public ID from two backends preserves both backend identities and
	// reports the public alias as ambiguous instead of choosing by input order.
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{{"id": "shared"}}},
		{BackendID: "remote", Prefix: "", Models: []map[string]any{{"id": "shared"}}},
	}
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{})
	snap2 := newDiagnosticsSnapshot("remote", map[string]diagnostics.Observation{})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap, snap2})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("expected both colliding backend identities, got %d", len(result))
	}
	for _, prov := range result {
		field, ok := prov.Fields["public_id_resolution"]
		if !ok || field.Value != "ambiguous_catalog_id" || field.Verification != Conflicting {
			t.Fatalf("public ID resolution = %#v, want ambiguous/conflicting", field)
		}
	}
	if result[0].Identity.BackendID != "default" || result[1].Identity.BackendID != "remote" {
		t.Fatalf("duplicate resolution order = %s, %s", result[0].Identity.BackendID, result[1].Identity.BackendID)
	}
	reversed, err := New().Snapshot(context.Background(), []catalog.BackendModel{backendModels[1], backendModels[0]}, []*diagnostics.Snapshot{snap2, snap})
	if err != nil {
		t.Fatal(err)
	}
	if len(reversed) != 2 || reversed[0].Identity.BackendID != "default" || reversed[1].Identity.BackendID != "remote" {
		t.Fatalf("duplicate resolution depended on input order: %#v", reversed)
	}
}

func TestBuildSanitizesForgedBuildValues(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	obs := observationWithBuild(diagnostics.Supported, now, false, "PRIVATE_REASON_SECRET", &diagnostics.Build{
		Version: "PRIVATE_VERSION_SECRET", Commit: "PRIVATE_COMMIT_SECRET", Date: "PRIVATE_DATE_SECRET",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{"version": obs})
	prov := Build(identity, map[string]any{"id": "model"}, snap)
	for _, field := range []string{"build_version", "build_commit", "build_date"} {
		requireUnknown(t, &prov, field)
	}
	b, err := json.Marshal(prov)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"PRIVATE_VERSION_SECRET", "PRIVATE_COMMIT_SECRET", "PRIVATE_DATE_SECRET", "PRIVATE_REASON_SECRET"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("forged build data leaked %s", secret)
		}
	}
}

func TestBuildUnknownBackendSentinelsStayUnknown(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	identity := ModelIdentity{PublicID: "model", BackendID: "default", LocalID: "model"}
	obs := observationWithBuild(diagnostics.Supported, now, false, "", &diagnostics.Build{
		Version: "unknown", Commit: "unknown", Date: "",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{"version": obs})
	prov := Build(identity, map[string]any{"id": "model"}, snap)
	for _, field := range []string{"build_version", "build_commit", "build_date"} {
		requireUnknown(t, &prov, field)
	}
}

func TestServiceSnapshotDuplicateBackendSnapshotUnknown(t *testing.T) {
	// Two snapshots for the same backend are ambiguous: build is unknown.
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	model1 := map[string]any{"id": "model", "context_length": 8192}
	model2 := map[string]any{"id": "model2"}
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{model1}},
		{BackendID: "default", Prefix: "", Models: []map[string]any{model2}},
	}
	obs := observationWithBuild(diagnostics.Supported, now, false, "", &diagnostics.Build{
		Version: "v2.0.0",
	})
	snap1 := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{"version": obs})
	snap2 := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{"version": obs})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap1, snap2})
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 2 {
		t.Fatalf("expected 2 snapshots, got %d", len(result))
	}
	for _, prov := range result {
		// Duplicate backend -> ambiguous -> build unknown.
		requireUnknown(t, prov, "build_version")
		requireUnknown(t, prov, "build_commit")
		requireUnknown(t, prov, "build_date")
	}
	// Catalog context still declared for the model that advertises it.
	if _, ok := result[0].Fields["catalog_context_length"]; !ok {
		t.Fatal("catalog_context_length should be present")
	}
}

func TestBuildUnknownNoTimestamp(t *testing.T) {
	identity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	prov := Build(identity, map[string]any{"id": "model"}, nil)
	for _, name := range unknownFields {
		f := prov.Fields[name]
		if !f.ObservedAt.IsZero() {
			t.Fatalf("field %s has fake timestamp %v", name, f.ObservedAt)
		}
	}
}

func TestServiceSnapshotProvenanceMatchesModelDiagnostics(t *testing.T) {
	now := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	backendModels := []catalog.BackendModel{
		{BackendID: "default", Prefix: "", Models: []map[string]any{
			{"id": "model", "context_length": 8192},
		}},
	}
	obs := observationWithBuild(diagnostics.Supported, now, false, "", &diagnostics.Build{
		Version: "v2.0.0",
		Commit:  "deadbeef",
	})
	snap := newDiagnosticsSnapshot("default", map[string]diagnostics.Observation{
		"version": obs,
	})

	result, err := New().Snapshot(context.Background(), backendModels, []*diagnostics.Snapshot{snap})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(result))
	}
	prov := result[0]

	// Compare with provenance.Build output directly.
	provIdentity := ModelIdentity{
		PublicID:  "model",
		BackendID: "default",
		LocalID:   "model",
		Prefix:    "",
	}
	expected := Build(provIdentity, backendModels[0].Models[0], snap)
	if len(prov.Fields) != len(expected.Fields) {
		t.Fatalf("provenance fields count = %d, want %d", len(prov.Fields), len(expected.Fields))
	}
	for k, v := range expected.Fields {
		got, ok := prov.Fields[k]
		if !ok {
			t.Fatalf("field %s missing in snapshot provenance", k)
		}
		if k == "public_id_resolution" {
			if got.Value != "unique_catalog_id" || got.Verification != Declared {
				t.Fatalf("catalog resolution = %#v", got)
			}
			continue
		}
		if got.Value != v.Value || got.Source != v.Source || got.Verification != v.Verification {
			t.Fatalf("field %s mismatch: got %#v, want %#v", k, got, v)
		}
	}
}
