// Package provenance provides unified model provenance records that combine
// declared configuration (from catalog metadata) with verified runtime facts
// (from backend diagnostics observations). It separates supplied configuration
// from independently observed state, with per-field verification levels.
// It uses cached data only and never issues upstream requests.
package provenance

import (
	"context"
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"time"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/diagnostics"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
)

// VerificationLevel describes how a provenance field's value was established.
type VerificationLevel string

const (
	// Declared means catalog/configuration-advertised, not independently
	// verified effective runtime state.
	Declared VerificationLevel = "declared"
	// Verified means the value was confirmed by a backend API observation.
	Verified VerificationLevel = "verified"
	// Conflicting means the declared and verified values disagree.
	Conflicting VerificationLevel = "conflicting"
	// Unknown means no usable data is available for the field.
	Unknown VerificationLevel = "unknown"
)

// FieldProvenance records a single provenance field with its value, source,
// observation timestamp, and verification level. A nil value serializes as
// JSON null and represents a field with no usable evidence.
type FieldProvenance struct {
	Value        any               `json:"value"`
	Source       string            `json:"source"`
	ObservedAt   time.Time         `json:"observed_at,omitempty"`
	Verification VerificationLevel `json:"verification"`
}

// ModelIdentity stably and boundedly identifies a model. The public ID is the
// catalog-visible ID (backend prefix + local ID). Backend ID and local ID are
// the stable internal identifiers; prefix collisions are resolved by the
// catalog layer. Public-ID collisions are reported explicitly by Snapshot.
type ModelIdentity struct {
	PublicID  string `json:"public_id"`
	BackendID string `json:"backend_id"`
	LocalID   string `json:"local_id"`
	Prefix    string `json:"prefix,omitempty"`
}

// ModelProvenance is a unified provenance record for a single model. Each
// field records its value, source, observation time when available, and
// verification level. Unsupported fields are explicit unknown/null records.
type ModelProvenance struct {
	Identity ModelIdentity `json:"identity"`
	// Fields is a fixed per-field provenance schema. Unavailable evidence is
	// represented explicitly as unknown/null rather than an omitted entry.
	Fields map[string]FieldProvenance `json:"fields,omitempty"`
}

// Service produces unified model provenance records. It is a pure
// function of its inputs: no state, no upstream requests, no background
// polling.
type Service struct{}

// New returns a Service.
func New() *Service {
	return &Service{}
}

// maxIdentityRunes bounds the length of every identity component so that
// forged or oversized identities cannot be trusted.
const maxIdentityRunes = identity.MaxModelComponentBytes

// Fixed source strings. Arbitrary upstream metadata is not trusted and model
// names/aliases are not used to infer missing provenance.
const (
	sourceUnknown = "unavailable"
	sourceCatalog = "/v1/models"
	sourceVersion = "/api/version"
)

// unknownFields is the fixed, bounded schema of fields that are unavailable on
// the current backend APIs. Each is always reported as unknown (null value,
// unavailable source, unknown verification, no timestamp). Evidence fields
// override these only when supported. No arbitrary input keys are added.
var unknownFields = []string{
	"model_identity",
	"build_version",
	"build_commit",
	"build_date",
	"catalog_context_length",
	"declared_context_length",
	"context_length_consistency",
	"public_id_resolution",
	"quantization",
	"usable_context_length",
	"total_requests",
	"total_input_tokens",
	"total_output_tokens",
	"draft_rate",
	"acceptance_rate",
	"template_version",
	"template_hash",
	"file_identity",
	"loaded",
	"requested_profile",
	"requested_terse",
	"observed_profile",
	"process_generation",
}

var (
	buildVersionPattern  = regexp.MustCompile(`^(v?[0-9]+\.[0-9]+\.[0-9]+([-+][A-Za-z0-9._-]+)?|dev|unknown)$`)
	buildCommitPattern   = regexp.MustCompile(`^([0-9a-fA-F]{7,64}|unknown)$`)
	declaredTokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$`)
	quantizationPattern  = regexp.MustCompile(`^(F16|F32|BF16|Q[2-8]_[A-Z0-9_]{1,24}|IQ[1-9]_[A-Z0-9_]{1,24})$`)
	templateHashPattern  = regexp.MustCompile(`^(sha256:)?[0-9a-fA-F]{64}$`)
)

var declaredMetadataFields = []string{
	"template_version", "template_hash", "quantization", "requested_profile", "requested_terse",
}

// unknownProvenance returns a FieldProvenance representing a field that has no
// usable evidence: a null value, the fixed unavailable source, unknown
// verification, and no observation timestamp.
func unknownProvenance() FieldProvenance {
	return FieldProvenance{
		Value:        nil,
		Source:       sourceUnknown,
		Verification: Unknown,
	}
}

// Build produces a ModelProvenance from a model identity, catalog metadata,
// and backend diagnostics snapshot. It is a pure function of its inputs and a
// linear orchestrator: it validates the identity, initializes the fixed unknown
// schema, then delegates to focused helpers for declared catalog metadata,
// catalog context-length declarations/consistency, and fresh build evidence.
// Only documented sources are used: catalog context_length from /v1/models
// (declared), and build info from /api/version (verified only when the
// observation is supported, fresh, nonexpired, and carries sanitized build
// values with an observed timestamp). Fields unavailable on current APIs are
// always reported as unknown. Callers may forge identities, so Build validates
// the identity before trusting it.
// catalogObservedAt is the authentic time the catalog entry was fetched. When
// nonzero it stamps catalog-derived declared fields; zero or missing means no
// observation timestamp (no time.Now fallback).
func Build(identity ModelIdentity, model map[string]any, backend *diagnostics.Snapshot, catalogObservedAt ...time.Time) ModelProvenance {
	if !identity.Valid() {
		return ModelProvenance{}
	}

	var catalogObserved time.Time
	if len(catalogObservedAt) > 0 {
		catalogObserved = catalogObservedAt[0]
	}

	fields := initializeUnknownFields()

	// The catalog identity evidence must match the requested local ID;
	// otherwise the catalog is not authoritative for this identity.
	modelID, modelIDOK := model["id"].(string)
	if !modelIDOK || modelID != identity.LocalID {
		return ModelProvenance{Identity: identity, Fields: fields}
	}

	// Valid identity and matching model ID: the full schema is now available.
	initializeDeclaredAndBuildFields(fields)

	fields["model_identity"] = FieldProvenance{
		Value: identity.PublicID, Source: sourceCatalog, ObservedAt: catalogObserved, Verification: Declared,
	}

	// Declared catalog metadata from meta.llamaswap.
	declaredContextLength, hasDeclaredContextLength := extractDeclaredMetadata(fields, model, catalogObserved)

	// Catalog context_length declaration and consistency with the declared
	// context length.
	catalogContextLength, hasCatalogContextLength := extractCatalogContext(fields, model, catalogObserved)
	if hasDeclaredContextLength && hasCatalogContextLength {
		value := "consistent"
		level := Declared
		if declaredContextLength != catalogContextLength {
			value, level = "conflicting", Conflicting
		}
		fields["context_length_consistency"] = FieldProvenance{
			Value: value, Source: "catalog_context_declarations", ObservedAt: catalogObserved, Verification: level,
		}
	}

	// Fresh supported build evidence from /api/version.
	extractBuildFields(fields, backend)

	return ModelProvenance{
		Identity: identity,
		Fields:   fields,
	}
}

// initializeUnknownFields returns a field map pre-populated with the fixed
// unknown schema for fields unavailable on current APIs. It is the only
// initialization performed before the identity/modelID checks so that an
// invalid or mismatched identity returns exactly this set.
func initializeUnknownFields() map[string]FieldProvenance {
	fields := make(map[string]FieldProvenance, len(unknownFields))
	for _, name := range unknownFields {
		fields[name] = unknownProvenance()
	}
	return fields
}

// initializeDeclaredAndBuildFields adds the declared catalog metadata and
// build unknown fields to an already-initialized field map. It is called only
// after the identity and modelID checks pass, so the valid path carries the
// full schema while invalid/mismatched identities return only the unknown
// fields.
func initializeDeclaredAndBuildFields(fields map[string]FieldProvenance) {
	for _, name := range declaredMetadataFields {
		fields[name] = unknownProvenance()
	}
	// Build fields are unknown unless a fresh, supported version observation
	// below supplies them.
	fields["build_version"] = unknownProvenance()
	fields["build_commit"] = unknownProvenance()
	fields["build_date"] = unknownProvenance()
}

// extractDeclaredMetadata extracts declared catalog metadata from meta.llamaswap
// and records it as declared provenance. It returns whether a declared context
// length was advertised and its value.
func extractDeclaredMetadata(fields map[string]FieldProvenance, model map[string]any, catalogObserved time.Time) (declaredContextLength int, hasDeclaredContextLength bool) {
	metadata := nestedMap(model, "meta", "llamaswap")
	if metadata == nil {
		return 0, false
	}
	if value := boundedToken(metadata["template_version"]); value != "" {
		fields["template_version"] = declaredMetadataField("meta.llamaswap.template_version", value, catalogObserved)
	}
	if value := boundedString(metadata["template_hash"], 71); templateHashPattern.MatchString(value) {
		fields["template_hash"] = declaredMetadataField("meta.llamaswap.template_hash", value, catalogObserved)
	}
	if value := boundedString(metadata["quantization"], 32); quantizationPattern.MatchString(value) {
		fields["quantization"] = declaredMetadataField("meta.llamaswap.quantization", value, catalogObserved)
	}
	if value := boundedToken(metadata["profile"]); value != "" {
		fields["requested_profile"] = declaredMetadataField("meta.llamaswap.profile", value, catalogObserved)
	}
	if value, ok := metadata["terse"].(bool); ok {
		fields["requested_terse"] = declaredMetadataField("meta.llamaswap.terse", value, catalogObserved)
	}
	if value, ok := intValue(metadata["context_length"]); ok && value > 0 {
		declaredContextLength = value
		hasDeclaredContextLength = true
		fields["declared_context_length"] = declaredMetadataField("meta.llamaswap.context_length", value, catalogObserved)
	}
	return declaredContextLength, hasDeclaredContextLength
}

// extractCatalogContext records the catalog-advertised context_length from
// /v1/models as a declared field and, when a declared context length is also
// present, records the consistency/conflict status. It returns whether a
// catalog context length was advertised and its value.
func extractCatalogContext(fields map[string]FieldProvenance, model map[string]any, catalogObserved time.Time) (catalogContextLength int, hasCatalogContextLength bool) {
	// Catalog context_length from /v1/models is catalog-advertised: it is a
	// declaration, not a usable runtime verified fact. No backend API exposes
	// the usable context length, so it is never verified. Field name is
	// catalog_context_length to distinguish from usable runtime context.
	// Fractional, NaN/Inf, negative, and overflowing values are invalid and
	// must not truncate into a false catalog context. Zero or missing values
	// mean no catalog context is advertised. The catalog fetch timestamp is
	// stamped only when an actual nonzero fetch time is provided; no
	// time.Now fallback is used.
	if cl, ok := model["context_length"]; ok {
		if v, ok := intValue(cl); ok && v > 0 {
			catalogContextLength, hasCatalogContextLength = v, true
			fields["catalog_context_length"] = FieldProvenance{
				Value:        v,
				Source:       sourceCatalog,
				Verification: Declared,
				ObservedAt:   catalogObserved,
			}
		}
	}
	return
}

// extractBuildFields records build info from /api/version as verified fields,
// but only when the observation is supported, fresh, nonexpired, and carries
// sanitized build values with an observed timestamp. Missing, stale, expired,
// unsupported, and unauthorized observations retain explicit unknown fields;
// the raw reason is never recorded. Build fields are always present so that an
// unavailable version is explicitly reported rather than omitted.
func extractBuildFields(fields map[string]FieldProvenance, backend *diagnostics.Snapshot) {
	if backend == nil {
		return
	}
	v, ok := backend.Capabilities["version"]
	if !ok || !isSupportedFreshBuild(v) || v.Build == nil {
		return
	}
	b := v.Build
	if safeBuildVersion(b.Version) {
		fields["build_version"] = FieldProvenance{
			Value:        b.Version,
			Source:       sourceVersion,
			ObservedAt:   v.ObservedAt,
			Verification: Verified,
		}
	}
	if safeBuildCommit(b.Commit) {
		fields["build_commit"] = FieldProvenance{
			Value:        b.Commit,
			Source:       sourceVersion,
			ObservedAt:   v.ObservedAt,
			Verification: Verified,
		}
	}
	if safeBuildDate(b.Date) {
		fields["build_date"] = FieldProvenance{
			Value:        b.Date,
			Source:       sourceVersion,
			ObservedAt:   v.ObservedAt,
			Verification: Verified,
		}
	}
}

func declaredMetadataField(source string, value any, observedAt time.Time) FieldProvenance {
	return FieldProvenance{Value: value, Source: source, Verification: Declared, ObservedAt: observedAt}
}

func boundedToken(value any) string {
	s, ok := value.(string)
	if !ok || !declaredTokenPattern.MatchString(s) {
		return ""
	}
	return s
}

func boundedString(value any, maxBytes int) string {
	s, ok := value.(string)
	if !ok || len(s) > maxBytes || !utf8.ValidString(s) {
		return ""
	}
	return s
}

func nestedMap(source map[string]any, keys ...string) map[string]any {
	current := source
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

func safeBuildVersion(value string) bool {
	return value != "unknown" && len(value) <= 96 && buildVersionPattern.MatchString(value)
}

func safeBuildCommit(value string) bool {
	return value != "unknown" && buildCommitPattern.MatchString(value)
}

func safeBuildDate(value string) bool {
	parsed, err := time.Parse(time.RFC3339, value)
	return err == nil && parsed.Format(time.RFC3339) == value
}

// isSupportedFreshBuild reports whether a version observation is supported,
// fresh, nonexpired, and observed at a nonzero time. It is a pure check of
// the observation's own fields and requires no clock.
func isSupportedFreshBuild(v diagnostics.Observation) bool {
	if v.State != diagnostics.Supported || v.Stale || v.ObservedAt.IsZero() || v.FreshUntil.IsZero() || v.ExpiresAt.IsZero() {
		return false
	}
	return v.FreshUntil.After(v.ObservedAt) && v.ExpiresAt.After(v.ObservedAt)
}

// intValue parses a JSON-decoded value into a finite, nonnegative integral
// value bounded by MaxInt. Fractional floats, NaN/Inf, negatives, and values
// that overflow MaxInt are invalid and return ok=false. json.Number is parsed
// with the same bounds.
func intValue(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return parseInt(n)
	case int64:
		return parseInt64(n)
	case float64:
		return parseFloat64(n)
	case json.Number:
		return parseJSONNumber(n)
	}
	return 0, false
}

// parseInt validates a nonnegative integral value bounded by maxInt.
func parseInt(n int) (int, bool) {
	if n >= 0 && n <= maxInt {
		return n, true
	}
	return 0, false
}

// parseInt64 validates a nonnegative integral value bounded by maxInt64.
func parseInt64(n int64) (int, bool) {
	if n >= 0 && n <= maxInt64 {
		return int(n), true
	}
	return 0, false
}

// parseFloat64 validates a finite, nonnegative integral float64 bounded by
// maxInt. NaN/Inf and negatives are rejected before conversion; the bound
// check happens before int(n) so the conversion never overflows.
func parseFloat64(n float64) (int, bool) {
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return 0, false
	}
	if n < 0 {
		return 0, false
	}
	// Bound: float64 cannot represent maxInt exactly (it rounds to 2^63),
	// so the smallest representable float64 above maxInt is 2^63. Reject
	// anything at or above that to avoid truncating an overflow into a
	// false catalog context. This check happens before int(n) so the
	// conversion never overflows.
	if n >= float64(maxInt) {
		return 0, false
	}
	// Reject fractional values: the value must be integral.
	if n != float64(int(n)) {
		return 0, false
	}
	return int(n), true
}

// parseJSONNumber parses a json.Number with the same bounds as int64.
func parseJSONNumber(n json.Number) (int, bool) {
	if i, err := n.Int64(); err == nil {
		if i >= 0 && i <= maxInt64 {
			return int(i), true
		}
	}
	return 0, false
}

const (
	maxInt   = int(^uint(0) >> 1)
	maxInt64 = int64(maxInt)
)

// Valid reports whether the identity is bounded and well formed. Callers may
// forge identities, so Build and Snapshot validate before trusting them.
func (i ModelIdentity) Valid() bool {
	if !utf8.ValidString(i.PublicID) || !utf8.ValidString(i.BackendID) ||
		!utf8.ValidString(i.LocalID) || !utf8.ValidString(i.Prefix) {
		return false
	}
	if !validIdentityComponent(i.LocalID) {
		return false
	}
	if !validIdentityComponent(i.BackendID) {
		return false
	}
	if !validIdentityComponent(i.Prefix) {
		return false
	}
	if !validIdentityComponent(i.PublicID) {
		return false
	}
	// The public ID is the composed prefix + local ID.
	if i.PublicID != i.Prefix+i.LocalID {
		return false
	}
	return true
}

func validIdentityComponent(value string) bool {
	return identity.ValidModelComponent(value)
}

// Snapshot produces per-model provenance records combining catalog metadata
// with backend diagnostics observations. It uses cached data only and never
// issues upstream requests. Models without a matching diagnostics snapshot
// still appear with declared-only provenance. It is a linear orchestrator:
// cancel the context, index backend snapshots and mark ambiguous duplicates,
// clone and sort catalog models, count canonical public IDs, then assemble
// per-model provenance records through focused helpers below complexity 15.
func (s *Service) Snapshot(ctx context.Context, backendModels []catalog.BackendModel, backendSnapshots []*diagnostics.Snapshot) ([]*ModelProvenance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Index backend snapshots by backend ID and mark backends with duplicate
	// snapshots as ambiguous. Such backends are treated as having no trusted
	// diagnostics (nil for that backend).
	snapByBackend, duplicateBackends := indexBackendSnapshots(backendSnapshots)

	// Defensively clone catalog models and sort each backend's models by local
	// ID, then the backends by backend ID and prefix, for stable output.
	models := cloneAndSortModels(backendModels)

	// Count how many models share each canonical public ID (prefix + local ID)
	// across all backends. Models with invalid catalog identity parts are
	// skipped.
	publicCounts := countPublicIDs(models)

	// Assemble per-model provenance records.
	return assembleModelProvenance(models, snapByBackend, duplicateBackends, publicCounts)
}

// indexBackendSnapshots indexes backend snapshots by backend ID and marks
// backends with more than one snapshot as ambiguous. The last snapshot wins
// for the index, but duplicate backends are overridden to nil diagnostics by
// the caller, so the indexed value for a duplicate backend is not trusted.
func indexBackendSnapshots(backendSnapshots []*diagnostics.Snapshot) (map[string]*diagnostics.Snapshot, map[string]bool) {
	snapByBackend := make(map[string]*diagnostics.Snapshot, len(backendSnapshots))
	duplicateBackends := make(map[string]bool)
	counts := make(map[string]int)
	for _, snap := range backendSnapshots {
		snapByBackend[snap.Backend] = snap
		counts[snap.Backend]++
		if counts[snap.Backend] > 1 {
			duplicateBackends[snap.Backend] = true
		}
	}
	return snapByBackend, duplicateBackends
}

// cloneAndSortModels defensively copies backend models and sorts each
// backend's models by local ID, then the backends by backend ID and prefix,
// for stable, deterministic output. The input slice is not mutated.
func cloneAndSortModels(backendModels []catalog.BackendModel) []catalog.BackendModel {
	models := append([]catalog.BackendModel(nil), backendModels...)
	for i := range models {
		models[i].Models = append([]map[string]any(nil), models[i].Models...)
		sort.SliceStable(models[i].Models, func(a, b int) bool {
			left, _ := models[i].Models[a]["id"].(string)
			right, _ := models[i].Models[b]["id"].(string)
			return left < right
		})
	}
	sort.SliceStable(models, func(i, j int) bool {
		if models[i].BackendID != models[j].BackendID {
			return models[i].BackendID < models[j].BackendID
		}
		return models[i].Prefix < models[j].Prefix
	})
	return models
}

// countPublicIDs counts how many models share each canonical public ID
// (prefix + local ID) across all backends. Models with invalid catalog
// identity parts are skipped.
func countPublicIDs(models []catalog.BackendModel) map[string]int {
	publicCounts := make(map[string]int)
	for _, bm := range models {
		for _, model := range bm.Models {
			localID, _ := model["id"].(string)
			if !validCatalogIdentityParts(bm.BackendID, bm.Prefix, localID) {
				continue
			}
			publicCounts[bm.Prefix+localID]++
		}
	}
	return publicCounts
}

// assembleModelProvenance builds per-model provenance records from the cloned,
// sorted catalog models. It skips models with invalid catalog identities,
// resolves duplicate backends to nil diagnostics, calls Build with the backend
// fetch timestamp, and sets the public ID resolution field on each record.
func assembleModelProvenance(models []catalog.BackendModel, snapByBackend map[string]*diagnostics.Snapshot, duplicateBackends map[string]bool, publicCounts map[string]int) ([]*ModelProvenance, error) {
	var result []*ModelProvenance
	for _, bm := range models {
		for _, model := range bm.Models {
			prov, ok := newModelProvenance(bm, model, snapByBackend, duplicateBackends, publicCounts)
			if !ok {
				continue
			}
			result = append(result, prov)
		}
	}
	return result, nil
}

// PublicIDResolutionField returns the provenance FieldProvenance for the
// public_id_resolution field. It reports ambiguous/conflicting when the public
// ID is shared by more than one model across backends, and unique/declared
// otherwise. The caller owns the map key and the decision of when to set the
// field; this helper only centralizes the value, source, and verification so
// the same logic is not duplicated across packages.
func PublicIDResolutionField(publicID string, publicCounts map[string]int) FieldProvenance {
	if publicCounts[publicID] > 1 {
		return FieldProvenance{Value: "ambiguous_catalog_id", Source: sourceCatalog, Verification: Conflicting}
	}
	return FieldProvenance{Value: "unique_catalog_id", Source: sourceCatalog, Verification: Declared}
}

// newModelProvenance builds a single provenance record for one catalog model.
// It returns false when the model's catalog identity is invalid and must be
// skipped. The public ID resolution reflects whether the canonical public ID
// is shared by more than one model across backends.
func newModelProvenance(bm catalog.BackendModel, model map[string]any, snapByBackend map[string]*diagnostics.Snapshot, duplicateBackends map[string]bool, publicCounts map[string]int) (*ModelProvenance, bool) {
	localID, _ := model["id"].(string)
	if !validCatalogIdentityParts(bm.BackendID, bm.Prefix, localID) {
		return nil, false
	}
	publicID := bm.Prefix + localID
	identity := ModelIdentity{
		PublicID:  publicID,
		BackendID: bm.BackendID,
		LocalID:   localID,
		Prefix:    bm.Prefix,
	}
	if !identity.Valid() {
		return nil, false
	}
	snap := snapByBackend[bm.BackendID]
	if duplicateBackends[bm.BackendID] {
		snap = nil
	}
	prov := Build(identity, model, snap, bm.FetchedAt)
	prov.Fields["public_id_resolution"] = PublicIDResolutionField(publicID, publicCounts)
	return &prov, true
}

func validCatalogIdentityParts(backendID, prefix, localID string) bool {
	return localID != "" && validIdentityComponent(backendID) && validIdentityComponent(prefix) &&
		validIdentityComponent(localID) && len(prefix)+len(localID) <= maxIdentityRunes
}
