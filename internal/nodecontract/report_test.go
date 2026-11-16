package nodecontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

var fixtureTime = time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)

func fixtureReport() Report {
	return Report{
		ProtocolVersion:  Version{Major: 1, Minor: 0},
		RequiredFeatures: []string{},
		Source: Source{
			NodeID:           "node-a",
			ProxyInstanceID:  "0123456789abcdef0123456789abcdef",
			ConfigGeneration: 1,
		},
		GeneratedAt: fixtureTime,
		Capabilities: map[string]string{
			"catalog_read": "enabled", "diagnostics_read": "disabled", "federation_exchange": "enabled",
			"artifact_read": "disabled", "artifact_write": "disabled", "config_write": "disabled",
		},
		Coverage:     "partial",
		PathServices: []string{"comfyui"},
		Observations: []Observation{{
			Identity:   ModelIdentity{OwnerNodeID: "node-a", BackendID: "gpu", LocalID: "model"},
			State:      "supported",
			ObservedAt: &fixtureTime,
			Freshness:  "fresh",
			Coverage:   "complete",
		}},
	}
}

func decodeFixture(t *testing.T, report Report, peer string) (Receipt, error) {
	t.Helper()
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	return DecodeReport(bytes.NewReader(data), peer, fixtureTime.Add(time.Minute))
}

func TestDecodeReportPreservesEvidenceAndReceipt(t *testing.T) {
	report := fixtureReport()
	report.Observations = append(report.Observations, Observation{
		Identity:  ModelIdentity{OwnerNodeID: "node-a", BackendID: "gpu", LocalID: "unknown-model"},
		State:     "unknown",
		Freshness: "unknown",
		Coverage:  "unknown",
	})
	sameName := report.Observations[0]
	sameName.Identity.BackendID = "other"
	report.Observations = append(report.Observations, sameName)
	got, err := decodeFixture(t, report, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Report.Observations[1].ObservedAt != nil {
		t.Fatal("receipt time replaced missing source observation time")
	}
	if !got.ReceivedAt.Equal(fixtureTime.Add(time.Minute)) ||
		!got.Report.Observations[0].ObservedAt.Equal(fixtureTime) ||
		got.AuthenticatedNodeID != "node-a" {
		t.Fatalf("receipt and source evidence = %#v", got)
	}
}

func TestDecodeReportRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Report)
		peer   string
	}{
		{"unsupported major", func(r *Report) { r.ProtocolVersion.Major = 2 }, "node-a"},
		{"negative minor", func(r *Report) { r.ProtocolVersion.Minor = -1 }, "node-a"},
		{"missing required features", func(r *Report) { r.RequiredFeatures = nil }, "node-a"},
		{"unknown required feature", func(r *Report) { r.RequiredFeatures = []string{"future"} }, "node-a"},
		{"mismatched authenticated peer", func(*Report) {}, "node-b"},
		{"invalid node ID", func(r *Report) { r.Source.NodeID = "node/a" }, "node/a"},
		{"oversized node ID", func(r *Report) { r.Source.NodeID = strings.Repeat("a", 129) }, strings.Repeat("a", 129)},
		{"invalid incarnation", func(r *Report) { r.Source.ProxyInstanceID = "not-a-random-id" }, "node-a"},
		{"zero config generation", func(r *Report) { r.Source.ConfigGeneration = 0 }, "node-a"},
		{"missing generated time", func(r *Report) { r.GeneratedAt = time.Time{} }, "node-a"},
		{"missing capability", func(r *Report) { delete(r.Capabilities, "config_write") }, "node-a"},
		{"invalid capability", func(r *Report) { r.Capabilities["config_write"] = "allowed" }, "node-a"},
		{"invalid path service", func(r *Report) { r.PathServices = []string{"../comfyui"} }, "node-a"},
		{"duplicate path service", func(r *Report) { r.PathServices = []string{"comfyui", "comfyui"} }, "node-a"},
		{"invalid path service", func(r *Report) { r.PathServices = []string{"../comfyui"} }, "node-a"},
		{"duplicate path service", func(r *Report) { r.PathServices = []string{"comfyui", "comfyui"} }, "node-a"},
		{"imported model", func(r *Report) { r.Observations[0].Identity.OwnerNodeID = "node-b" }, "node-a"},
		{"empty backend", func(r *Report) { r.Observations[0].Identity.BackendID = "" }, "node-a"},
		{"oversized model", func(r *Report) { r.Observations[0].Identity.LocalID = strings.Repeat("x", 257) }, "node-a"},
		{"duplicate identity", func(r *Report) { r.Observations = append(r.Observations, r.Observations[0]) }, "node-a"},
		{"invalid state", func(r *Report) { r.Observations[0].State = "invented" }, "node-a"},
		{"fresh with no observation time", func(r *Report) { r.Observations[0].ObservedAt = nil }, "node-a"},
		{"supported with unknown observation time", func(r *Report) {
			r.Observations[0].ObservedAt = nil
			r.Observations[0].Freshness = "unknown"
		}, "node-a"},
		{"incomplete observations in complete report", func(r *Report) {
			r.Coverage = "complete"
			r.Observations[0].Coverage = "truncated"
		}, "node-a"},
		{"unavailable observation in complete report", func(r *Report) {
			r.Coverage = "complete"
			r.Observations[0].State = "unavailable"
		}, "node-a"},
		{"disabled catalog in complete report", func(r *Report) {
			r.Coverage = "complete"
			r.Capabilities["catalog_read"] = "disabled"
		}, "node-a"},
		{"missing observations", func(r *Report) { r.Observations = nil }, "node-a"},
		{"too many observations", func(r *Report) { r.Observations = make([]Observation, MaxObservations+1) }, "node-a"},
		{"readiness without capability", func(r *Report) {
			items := []BackendReadiness{{BackendID: "local", Status: "unknown", Freshness: "unknown"}}
			r.BackendReadiness = &items
		}, "node-a"},
		{"duplicate readiness", func(r *Report) {
			r.Capabilities["diagnostics_read"] = "enabled"
			items := []BackendReadiness{{BackendID: "local", Status: "unknown", Freshness: "unknown"}, {BackendID: "local", Status: "unknown", Freshness: "unknown"}}
			r.BackendReadiness = &items
		}, "node-a"},
		{"ready without observation", func(r *Report) {
			r.Capabilities["diagnostics_read"] = "enabled"
			items := []BackendReadiness{{BackendID: "local", Status: "ready", Freshness: "unknown"}}
			r.BackendReadiness = &items
		}, "node-a"},
		{"too many readiness entries", func(r *Report) {
			r.Capabilities["diagnostics_read"] = "enabled"
			items := make([]BackendReadiness, maxLocalBackends+1)
			r.BackendReadiness = &items
		}, "node-a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := fixtureReport()
			tt.change(&report)
			if _, err := decodeFixture(t, report, tt.peer); err == nil {
				t.Fatal("accepted invalid report")
			}
		})
	}
}

func TestDecodeReportVersionCompatibilityAndJSONBoundaries(t *testing.T) {
	report := fixtureReport()
	report.ProtocolVersion.Minor = 9
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	base := string(data)
	tests := []struct {
		name string
		data string
		ok   bool
	}{
		{"future minor and optional field", strings.Replace(base, `"protocol_version"`, `"future_field":{"x":1},"protocol_version"`, 1), true},
		{"future capability value", strings.Replace(base, `"catalog_read":"enabled"`, `"future_capability":{"format":2},"catalog_read":"enabled"`, 1), true},
		{"missing minor", strings.Replace(base, `,"minor":9`, ``, 1), false},
		{"null minor", strings.Replace(base, `"minor":9`, `"minor":null`, 1), false},
		{"duplicate key", strings.Replace(base, `"major":1`, `"major":1,"major":2`, 1), false},
		{"case-folded duplicate key", strings.Replace(base, `"major":1`, `"major":1,"MAJOR":2`, 1), false},
		{"trailing data", base + `{}`, false},
		{"oversized document", base + strings.Repeat(" ", MaxReportBytes), false},
		{"invalid UTF-8", base + string([]byte{0xff}), false},
		{"too deep", strings.Replace(base, `"protocol_version"`, `"future":`+strings.Repeat("[", 34)+`0`+strings.Repeat("]", 34)+`,"protocol_version"`, 1), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeReport(strings.NewReader(tt.data), "node-a", fixtureTime)
			if (err == nil) != tt.ok {
				t.Fatalf("DecodeReport error = %v, want success %v", err, tt.ok)
			}
		})
	}
}

func TestDecodeReportLimitsAndUnknownCapabilities(t *testing.T) {
	report := fixtureReport()
	report.Source.NodeID = strings.Repeat("a", 128)
	report.Observations[0].Identity.OwnerNodeID = report.Source.NodeID
	report.Observations[0].Identity.LocalID = strings.Repeat("m", 256)
	report.Capabilities["future"] = "some future state"
	got, err := decodeFixture(t, report, report.Source.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.Report.Capabilities["future"]; ok {
		t.Fatal("unknown capability was retained")
	}

	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	data = append(data, bytes.Repeat([]byte(" "), MaxReportBytes-len(data))...)
	if _, err := DecodeReport(bytes.NewReader(data), report.Source.NodeID, fixtureTime); err != nil {
		t.Fatalf("report exactly at byte limit rejected: %v", err)
	}
}

func TestDocumentedReportExample(t *testing.T) {
	document, err := os.ReadFile("../../NODE-AGENT-CONTRACT.md")
	if err != nil {
		t.Fatal(err)
	}
	_, after, ok := strings.Cut(string(document), "```json\n")
	if !ok {
		t.Fatal("contract JSON example missing")
	}
	example, _, ok := strings.Cut(after, "\n```")
	if !ok {
		t.Fatal("contract JSON example not terminated")
	}
	if _, err := DecodeReport(strings.NewReader(example), "node-a", fixtureTime); err != nil {
		t.Fatalf("documented report rejected: %v", err)
	}
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestDecodeReportRejectsInvalidReceiptAndReadFailure(t *testing.T) {
	if _, err := DecodeReport(strings.NewReader("{}"), "node-a", time.Time{}); err == nil {
		t.Fatal("accepted missing receipt time")
	}
	if _, err := DecodeReport(errorReader{}, "node-a", fixtureTime); err == nil || !strings.Contains(err.Error(), "read failed") {
		t.Fatalf("read error = %v", err)
	}
	if _, err := DecodeReport(io.LimitReader(strings.NewReader(""), 0), "node-a", fixtureTime); err == nil {
		t.Fatal("accepted empty input")
	}
}
