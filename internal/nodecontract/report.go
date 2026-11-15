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
	State      string        `json:"state"`
	ObservedAt *time.Time    `json:"observed_at,omitempty"`
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
	if r.ProtocolVersion.Major != 1 || r.ProtocolVersion.Minor < 0 {
		return errors.New("unsupported node report protocol version")
	}
	if r.RequiredFeatures == nil || len(r.RequiredFeatures) > maxFeatures {
		return errors.New("invalid required features")
	}
	// Version 1.0 defines no required extension features.
	if len(r.RequiredFeatures) != 0 {
		return errors.New("unsupported required node report feature")
	}
	if !validNodeID(r.Source.NodeID) || r.Source.NodeID != authenticatedNodeID {
		return errors.New("node report source does not match authenticated peer")
	}
	if !validInstanceID(r.Source.ProxyInstanceID) || r.Source.ConfigGeneration == 0 {
		return errors.New("invalid proxy incarnation or config generation")
	}
	if r.GeneratedAt.IsZero() || !validCoverage(r.Coverage) {
		return errors.New("invalid report generation time or coverage")
	}
	if len(r.Capabilities) > maxCapabilities {
		return errors.New("too many advertised capabilities")
	}
	for _, name := range capabilityNames {
		if r.Capabilities[name] != "enabled" && r.Capabilities[name] != "disabled" {
			return errors.New("missing or invalid advertised capability")
		}
	}
	if r.Coverage == "complete" && r.Capabilities["catalog_read"] != "enabled" {
		return errors.New("complete report requires catalog capability")
	}
	if r.Observations == nil || len(r.Observations) > MaxObservations {
		return errors.New("invalid number of observations")
	}
	seen := make(map[ModelIdentity]struct{}, len(r.Observations))
	for _, observation := range r.Observations {
		id := observation.Identity
		if id.OwnerNodeID != r.Source.NodeID || id.BackendID == "" || id.LocalID == "" ||
			!(provenance.ModelIdentity{BackendID: id.BackendID, LocalID: id.LocalID, PublicID: id.LocalID}).Valid() {
			return errors.New("invalid or non-local model identity")
		}
		if _, exists := seen[id]; exists {
			return errors.New("ambiguous duplicate model identity")
		}
		seen[id] = struct{}{}
		if !validState(observation.State) || !validCoverage(observation.Coverage) ||
			!validFreshness(observation.Freshness) ||
			(observation.ObservedAt != nil && observation.ObservedAt.IsZero()) ||
			(observation.ObservedAt == nil && observation.Freshness != "unknown") ||
			((observation.State == "supported" || observation.State == "partial") && observation.ObservedAt == nil) {
			return errors.New("invalid observation evidence")
		}
		if r.Coverage == "complete" && (observation.Coverage != "complete" || observation.State != "supported") {
			return errors.New("complete report contains incomplete coverage")
		}
	}
	if r.BackendReadiness != nil {
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
	min := r
	for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
		if next < min {
			min = next
		}
	}
	return min
}
