// Package nodecontract defines and validates the version 1 node report.
// It does not authenticate peers, issue requests, or grant permissions.
package nodecontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/identity"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/provenance"
)

const (
	MaxReportBytes  = 1 << 20
	MaxObservations = 1000
	MaxModelAliases = 64
	maxJSONDepth    = 32
	maxFeatures     = 32
	maxCapabilities = 64
)

type Version struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
}

type Source struct {
	NodeID           string `json:"node_id"`
	ProxyInstanceID  string `json:"proxy_instance_id"`
	ConfigGeneration uint64 `json:"config_generation"`
}

type ModelIdentity struct {
	OwnerNodeID string `json:"owner_node_id"`
	BackendID   string `json:"backend_id"`
	LocalID     string `json:"local_id"`
}

// Observation describes catalog membership, not verified weights or runtime state.
type Observation struct {
	Identity   ModelIdentity `json:"identity"`
	Aliases    []string      `json:"aliases,omitempty"`
	State      string        `json:"state"`
	ObservedAt *time.Time    `json:"observed_at,omitempty"`
	ExpiresAt  *time.Time    `json:"expires_at,omitempty"`
	Freshness  string        `json:"freshness"`
	Coverage   string        `json:"coverage"`
}

// BackendReadiness describes the result of a local backend health check.
type BackendReadiness struct {
	BackendID  string     `json:"backend_id"`
	Status     string     `json:"status"`
	ObservedAt *time.Time `json:"observed_at,omitempty"`
	Freshness  string     `json:"freshness"`
}

type Capabilities map[string]string

func (c *Capabilities) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) > maxCapabilities {
		return errors.New("too many advertised capabilities")
	}
	result := make(Capabilities, len(capabilityNames))
	for _, name := range capabilityNames {
		if value, ok := raw[name]; ok {
			var state string
			if err := json.Unmarshal(value, &state); err != nil {
				return errors.New("invalid advertised capability")
			}
			result[name] = state
		}
	}
	*c = result
	return nil
}

type Report struct {
	ProtocolVersion  Version             `json:"protocol_version"`
	RequiredFeatures []string            `json:"required_features"`
	Source           Source              `json:"source"`
	GeneratedAt      time.Time           `json:"generated_at"`
	Capabilities     Capabilities        `json:"capabilities"`
	Coverage         string              `json:"coverage"`
	Observations     []Observation       `json:"observations"`
	PathServices     []string            `json:"path_services,omitempty"`
	BackendReadiness *[]BackendReadiness `json:"backend_readiness,omitempty"`
}

// Receipt is receiver-local metadata and is not part of the source report.
type Receipt struct {
	AuthenticatedNodeID string
	ReceivedAt          time.Time
	Report              Report
}

var capabilityNames = [...]string{
	"catalog_read", "diagnostics_read", "federation_exchange",
	"artifact_read", "artifact_write", "config_write",
}

// DecodeReport validates a report against an already authenticated peer's
// configured identity. The caller must authenticate the peer independently.
func DecodeReport(input io.Reader, authenticatedNodeID string, receivedAt time.Time) (Receipt, error) {
	if !validNodeID(authenticatedNodeID) || receivedAt.IsZero() {
		return Receipt{}, errors.New("invalid authenticated peer identity or receipt time")
	}
	data, err := io.ReadAll(io.LimitReader(input, MaxReportBytes+1))
	if err != nil {
		return Receipt{}, fmt.Errorf("read node report: %w", err)
	}
	if len(data) > MaxReportBytes {
		return Receipt{}, errors.New("node report exceeds size limit")
	}
	if !utf8.Valid(data) {
		return Receipt{}, errors.New("node report is not valid UTF-8")
	}
	if err := validateJSON(data); err != nil {
		return Receipt{}, fmt.Errorf("invalid node report JSON: %w", err)
	}
	var fields struct {
		ProtocolVersion map[string]json.RawMessage `json:"protocol_version"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		return Receipt{}, fmt.Errorf("decode node report: %w", err)
	}
	if fields.ProtocolVersion["major"] == nil || fields.ProtocolVersion["minor"] == nil ||
		bytes.Equal(fields.ProtocolVersion["major"], []byte("null")) ||
		bytes.Equal(fields.ProtocolVersion["minor"], []byte("null")) {
		return Receipt{}, errors.New("protocol version requires major and minor")
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return Receipt{}, fmt.Errorf("decode node report: %w", err)
	}
	if err := report.validate(authenticatedNodeID); err != nil {
		return Receipt{}, err
	}
	return Receipt{AuthenticatedNodeID: authenticatedNodeID, ReceivedAt: receivedAt, Report: report}, nil
}

func (r Report) validate(authenticatedNodeID string) error {
	if err := r.validateProtocolVersion(); err != nil {
		return err
	}
	if err := r.validateRequiredFeatures(); err != nil {
		return err
	}
	if err := r.validateSource(authenticatedNodeID); err != nil {
		return err
	}
	if err := r.validateGeneratedAtAndCoverage(); err != nil {
		return err
	}
	if err := r.validateCapabilityCount(); err != nil {
		return err
	}
	if err := r.validatePathServices(); err != nil {
		return err
	}
	if err := r.validateCapabilities(); err != nil {
		return err
	}
	if err := r.validateObservations(); err != nil {
		return err
	}
	if err := r.validateBackendReadiness(); err != nil {
		return err
	}
	return nil
}

// validateProtocolVersion enforces the version 1 protocol contract.
func (r Report) validateProtocolVersion() error {
	if r.ProtocolVersion.Major != 1 || r.ProtocolVersion.Minor < 0 {
		return errors.New("unsupported node report protocol version")
	}
	return nil
}

// validateRequiredFeatures enforces that version 1 reports declare no
// required extension features.
func (r Report) validateRequiredFeatures() error {
	if r.RequiredFeatures == nil || len(r.RequiredFeatures) > maxFeatures {
		return errors.New("invalid required features")
	}
	// Version 1.0 defines no required extension features.
	if len(r.RequiredFeatures) != 0 {
		return errors.New("unsupported required node report feature")
	}
	return nil
}

// validateSource checks the report source against the authenticated peer.
func (r Report) validateSource(authenticatedNodeID string) error {
	if !validNodeID(r.Source.NodeID) || r.Source.NodeID != authenticatedNodeID {
		return errors.New("node report source does not match authenticated peer")
	}
	if !validInstanceID(r.Source.ProxyInstanceID) || r.Source.ConfigGeneration == 0 {
		return errors.New("invalid proxy incarnation or config generation")
	}
	return nil
}

// validateGeneratedAtAndCoverage checks report timing and coverage.
func (r Report) validateGeneratedAtAndCoverage() error {
	if r.GeneratedAt.IsZero() || !validCoverage(r.Coverage) {
		return errors.New("invalid report generation time or coverage")
	}
	return nil
}

// validateCapabilityCount checks the advertised capability count.
func (r Report) validateCapabilityCount() error {
	if len(r.Capabilities) > maxCapabilities {
		return errors.New("too many advertised capabilities")
	}
	return nil
}

// validateCapabilities checks required advertised states.
func (r Report) validateCapabilities() error {
	for _, name := range capabilityNames {
		if r.Capabilities[name] != "enabled" && r.Capabilities[name] != "disabled" {
			return errors.New("missing or invalid advertised capability")
		}
	}
	if r.Coverage == "complete" && r.Capabilities["catalog_read"] != "enabled" {
		return errors.New("complete report requires catalog capability")
	}
	return nil
}

// validatePathServices checks path service count, validity, and uniqueness.
func (r Report) validatePathServices() error {
	if len(r.PathServices) > 32 {
		return errors.New("too many advertised path services")
	}
	pathServices := make(map[string]bool, len(r.PathServices))
	for _, service := range r.PathServices {
		if !validNodeID(service) || pathServices[service] {
			return errors.New("invalid or duplicate advertised path service")
		}
		pathServices[service] = true
	}
	return nil
}

// validateObservations checks observation identities, aliases, and evidence.
func (r Report) validateObservations() error {
	if r.Observations == nil || len(r.Observations) > MaxObservations {
		return errors.New("invalid number of observations")
	}
	seen := make(map[ModelIdentity]struct{}, len(r.Observations))
	for _, observation := range r.Observations {
		if err := r.validateObservationIdentity(observation); err != nil {
			return err
		}
		if _, exists := seen[observation.Identity]; exists {
			return errors.New("ambiguous duplicate model identity")
		}
		seen[observation.Identity] = struct{}{}
		if err := r.validateAliases(observation.Aliases); err != nil {
			return err
		}
		if err := r.validateObservationEvidence(observation); err != nil {
			return err
		}
		if r.Coverage == "complete" && (observation.Coverage != "complete" || observation.State != "supported") {
			return errors.New("complete report contains incomplete coverage")
		}
	}
	return nil
}

// validateObservationIdentity checks that an observation refers to a local
// model owned by the reporting node.
func (r Report) validateObservationIdentity(observation Observation) error {
	id := observation.Identity
	if id.OwnerNodeID != r.Source.NodeID || id.BackendID == "" || id.LocalID == "" ||
		!(provenance.ModelIdentity{BackendID: id.BackendID, LocalID: id.LocalID, PublicID: id.LocalID}).Valid() {
		return errors.New("invalid or non-local model identity")
	}
	return nil
}

// validateAliases checks observation alias count, validity, and uniqueness.
func (r Report) validateAliases(aliases []string) error {
	if len(aliases) > MaxModelAliases {
		return errors.New("too many model aliases")
	}
	seenAliases := make(map[string]struct{}, len(aliases))
	for _, alias := range aliases {
		if alias == "" || !identity.ValidModelComponent(alias) {
			return errors.New("invalid model alias")
		}
		if _, exists := seenAliases[alias]; exists {
			return errors.New("duplicate model alias")
		}
		seenAliases[alias] = struct{}{}
	}
	return nil
}

// validateObservationEvidence checks an observation's state, coverage,
// freshness, and timing invariants.
func (r Report) validateObservationEvidence(observation Observation) error {
	if !validState(observation.State) || !validCoverage(observation.Coverage) ||
		!validFreshness(observation.Freshness) ||
		(observation.ObservedAt != nil && observation.ObservedAt.IsZero()) ||
		(observation.ExpiresAt != nil && (observation.ExpiresAt.IsZero() || observation.ObservedAt == nil || !observation.ExpiresAt.After(*observation.ObservedAt))) ||
		(observation.ObservedAt == nil && observation.Freshness != "unknown") ||
		((observation.State == "supported" || observation.State == "partial") && observation.ObservedAt == nil) {
		return errors.New("invalid observation evidence")
	}
	return nil
}

// validateBackendReadiness checks backend readiness evidence when present.
func (r Report) validateBackendReadiness() error {
	if r.BackendReadiness == nil {
		return nil
	}
	if r.Capabilities["diagnostics_read"] != "enabled" || len(*r.BackendReadiness) > maxLocalBackends {
		return errors.New("invalid backend readiness scope")
	}
	seenBackends := make(map[string]bool, len(*r.BackendReadiness))
	for _, readiness := range *r.BackendReadiness {
		if readiness.BackendID == "" || !identity.ValidModelComponent(readiness.BackendID) || seenBackends[readiness.BackendID] ||
			(readiness.Status != "ready" && readiness.Status != "unavailable" && readiness.Status != "unknown") ||
			!validFreshness(readiness.Freshness) ||
			(readiness.ObservedAt == nil && (readiness.Status != "unknown" || readiness.Freshness != "unknown")) ||
			(readiness.ObservedAt != nil && (readiness.ObservedAt.IsZero() || readiness.Status == "unknown")) {
			return errors.New("invalid backend readiness evidence")
		}
		seenBackends[readiness.BackendID] = true
	}
	return nil
}

func validNodeID(id string) bool {
	return identity.ValidNodeID(id)
}

func validInstanceID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validCoverage(value string) bool {
	switch value {
	case "complete", "partial", "truncated", "unknown":
		return true
	}
	return false
}

func validFreshness(value string) bool {
	return value == "fresh" || value == "stale" || value == "unknown"
}

func validState(value string) bool {
	switch value {
	case "supported", "unsupported", "disabled", "unavailable", "unauthorized", "unknown", "partial", "ambiguous":
		return true
	}
	return false
}

// validateJSON rejects duplicate keys, including in otherwise ignored fields.
func validateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := readJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func readJSONValue(decoder *json.Decoder, depth int) error {
	if depth > maxJSONDepth {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid JSON object key")
			}
			// encoding/json matches struct fields using Unicode simple folding.
			folded := strings.Map(foldRune, key)
			if _, exists := keys[folded]; exists {
				return errors.New("ambiguous JSON object key")
			}
			keys[folded] = struct{}{}
			if err := readJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := readJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected JSON delimiter")
	}
	_, err = decoder.Token()
	return err
}

func foldRune(r rune) rune {
	minimum := r
	for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
		if next < minimum {
			minimum = next
		}
	}
	return minimum
}
