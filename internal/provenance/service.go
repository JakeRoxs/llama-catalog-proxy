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
const maxIdentityRunes = 256

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
// and backend diagnostics snapshot. It is a pure function of its inputs.
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

	fields := make(map[string]FieldProvenance, len(unknownFields))
	var declaredContextLength int
	var hasDeclaredContextLength bool
	var catalogContextLength int
	var hasCatalogContextLength bool

	// Fixed schema for fields unknown on current APIs or absent from the catalog.
	// Explicit type-checked declared catalog metadata or fresh backend build
	// evidence may override entries; no arbitrary metadata keys are accepted.
	for _, name := range unknownFields {
		fields[name] = unknownProvenance()
	}
	for _, name := range declaredMetadataFields {
		fields[name] = unknownProvenance()
	}
	// Build fields are unknown unless a fresh, supported version observation
	// below supplies them.
	fields["build_version"] = unknownProvenance()
	fields["build_commit"] = unknownProvenance()
	fields["build_date"] = unknownProvenance()
	modelID, modelIDOK := model["id"].(string)
	if !modelIDOK || modelID != identity.LocalID {
		return ModelProvenance{Identity: identity, Fields: fields}
	}
	fields["model_identity"] = FieldProvenance{
		Value: identity.PublicID, Source: sourceCatalog, ObservedAt: catalogObserved, Verification: Declared,
	}
	if metadata := nestedMap(model, "meta", "llamaswap"); metadata != nil {
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
	}

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

	// Build info from /api/version, verified only when the observation is
	// supported, fresh, nonexpired, and carries sanitized build values with an
	// observed timestamp. Missing, stale, expired, unsupported, and
	// unauthorized observations retain explicit unknown fields; the raw reason
	// is never recorded. Build fields are always present so that an unavailable
	// version is explicitly reported rather than omitted.
	if backend != nil {
		if v, ok := backend.Capabilities["version"]; ok && isSupportedFreshBuild(v) && v.Build != nil {
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
	}

	return ModelProvenance{
		Identity: identity,
		Fields:   fields,
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

func boundedString(value any, max int) string {
	s, ok := value.(string)
	if !ok || len(s) > max || !utf8.ValidString(s) {
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
		if n >= 0 && n <= maxInt {
			return n, true
		}
		return 0, false
	case int64:
		if n >= 0 && n <= maxInt64 {
			return int(n), true
		}
		return 0, false
	case float64:
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
	case json.Number:
		if i, err := n.Int64(); err == nil {
			if i >= 0 && i <= maxInt64 {
				return int(i), true
			}
		}
		return 0, false
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
	return utf8.ValidString(value) && len(value) <= maxIdentityRunes && len([]rune(value)) <= maxIdentityRunes
}

// Snapshot produces per-model provenance records combining catalog metadata
// with backend diagnostics observations. It uses cached data only and never
// issues upstream requests. Models without a matching diagnostics snapshot
// still appear with declared-only provenance.
func (s *Service) Snapshot(ctx context.Context, backendModels []catalog.BackendModel, backendSnapshots []*diagnostics.Snapshot) ([]*ModelProvenance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	snapByBackend := make(map[string]*diagnostics.Snapshot, len(backendSnapshots))
	for _, snap := range backendSnapshots {
		snapByBackend[snap.Backend] = snap
	}

	// Duplicate backend snapshots are ambiguous: it is unknown which
	// observation to trust. Treat such backends as having no diagnostics.
	duplicateBackends := make(map[string]bool)
	counts := make(map[string]int)
	for _, snap := range backendSnapshots {
		counts[snap.Backend]++
		if counts[snap.Backend] > 1 {
			duplicateBackends[snap.Backend] = true
		}
	}

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

	var result []*ModelProvenance
	for _, bm := range models {
		for _, model := range bm.Models {
			localID, _ := model["id"].(string)
			if !validCatalogIdentityParts(bm.BackendID, bm.Prefix, localID) {
				continue
			}
			publicID := bm.Prefix + localID
			identity := ModelIdentity{
				PublicID:  publicID,
				BackendID: bm.BackendID,
				LocalID:   localID,
				Prefix:    bm.Prefix,
			}
			if !identity.Valid() {
				continue
			}
			snap := snapByBackend[bm.BackendID]
			if duplicateBackends[bm.BackendID] {
				snap = nil
			}
			prov := Build(identity, model, snap, bm.FetchedAt)
			if publicCounts[publicID] > 1 {
				prov.Fields["public_id_resolution"] = FieldProvenance{Value: "ambiguous_catalog_id", Source: sourceCatalog, Verification: Conflicting}
			} else {
				prov.Fields["public_id_resolution"] = FieldProvenance{Value: "unique_catalog_id", Source: sourceCatalog, Verification: Declared}
			}
			result = append(result, &prov)
		}
	}

	return result, nil
}

func validCatalogIdentityParts(backendID, prefix, localID string) bool {
	return localID != "" && validIdentityComponent(backendID) && validIdentityComponent(prefix) &&
		validIdentityComponent(localID) && len(prefix)+len(localID) <= maxIdentityRunes
}
