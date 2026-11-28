package nodecontract

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
)

func TestLocalReportExportsOnlyExplicitlySelectedBackends(t *testing.T) {
	source := fixtureReport().Source
	caches := []catalog.BackendModel{
		{BackendID: "local", Prefix: "local/", FetchedAt: fixtureTime, Models: []map[string]any{
			{"id": "z", "secret_metadata": "not exported", "meta": map[string]any{"llamaswap": map[string]any{"aliases": []any{"z-short"}}}},
			{"id": "z-short", "meta": map[string]any{"llamaswap": map[string]any{"type": "alias", "modelID": "z"}}},
			{"id": "a"},
		}},
		{BackendID: "remote", FetchedAt: fixtureTime, Models: []map[string]any{{"id": "peer-model"}}},
	}

	report, err := LocalReport(source, []string{"local"}, caches, fixtureTime.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage != "partial" || len(report.Observations) != 2 {
		t.Fatalf("report coverage and observations = %#v", report)
	}
	for i, want := range []string{"a", "z"} {
		got := report.Observations[i]
		if got.Identity != (ModelIdentity{OwnerNodeID: source.NodeID, BackendID: "local", LocalID: want}) ||
			got.Freshness != "fresh" || got.ObservedAt == nil || !got.ObservedAt.Equal(fixtureTime) ||
			got.ExpiresAt == nil || !got.ExpiresAt.Equal(fixtureTime.Add(time.Minute)) {
			t.Fatalf("observation %d = %#v", i, got)
		}
		if want == "z" && !reflect.DeepEqual(got.Aliases, []string{"z-short"}) {
			t.Fatalf("model aliases = %#v", got.Aliases)
		}
	}
}

func TestLocalReportFromCatalogUsesOnlySelectedCachedModels(t *testing.T) {
	var requests atomic.Int32
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, `{"data":[{"id":"local-model","sensitive":"must-not-export","meta":{"llamaswap":{"aliases":["local-short"]}}},{"id":"local-short","meta":{"llamaswap":{"type":"alias","modelID":"local-model"}}},{"id":"other"}]}`)
	}))
	defer local.Close()
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, `{"data":[{"id":"remote-model"}]}`)
	}))
	defer remote.Close()
	localURL, err := url.Parse(local.URL)
	if err != nil {
		t.Fatal(err)
	}
	remoteURL, err := url.Parse(remote.URL)
	if err != nil {
		t.Fatal(err)
	}
	service := catalog.New([]catalog.Backend{
		{ID: "local", URL: localURL, Default: true},
		{ID: "remote", URL: remoteURL},
	}, local.Client(), time.Minute, time.Second, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := service.Models(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	before := requests.Load()
	report, err := LocalReportFromCatalog(context.Background(), fixtureReport().Source, []string{"local", "missing"}, service, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != before {
		t.Fatal("local report performed an upstream request")
	}
	if report.Coverage != "partial" || len(report.Observations) != 2 {
		t.Fatalf("unexpected report: %#v", report)
	}
	for _, observation := range report.Observations {
		if observation.Identity.BackendID != "local" || observation.ObservedAt == nil || observation.Freshness != "fresh" {
			t.Fatalf("unexpected observation: %#v", observation)
		}
		if observation.Identity.LocalID == "local-model" && !reflect.DeepEqual(observation.Aliases, []string{"local-short"}) {
			t.Fatalf("cached model aliases = %#v", observation.Aliases)
		}
	}
}

func TestLocalReportFromCatalogTruncatesLargeCache(t *testing.T) {
	var body strings.Builder
	body.WriteString(`{"data":[`)
	for i := 0; i <= MaxObservations; i++ {
		if i > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"id":"model-%04d"}`, i)
	}
	body.WriteString(`]}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body.String())
	}))
	defer server.Close()
	target, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	service := catalog.New([]catalog.Backend{{ID: "local", URL: target, Default: true}}, server.Client(),
		time.Minute, time.Second, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := service.Models(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	report, err := LocalReportFromCatalog(context.Background(), fixtureReport().Source, []string{"local"}, service, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage != "truncated" || len(report.Observations) != MaxObservations {
		t.Fatalf("coverage/observations = %s/%d", report.Coverage, len(report.Observations))
	}
}

func TestLocalReportBoundsEncodedSize(t *testing.T) {
	models := make([]map[string]any, MaxObservations)
	for i := range models {
		models[i] = map[string]any{"id": fmt.Sprintf("%04d%s", i, strings.Repeat("\x01", 252))}
	}
	report, err := LocalReport(fixtureReport().Source, []string{"local"}, []catalog.BackendModel{{
		BackendID: "local", FetchedAt: fixtureTime, Models: models,
	}}, fixtureTime, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage != "truncated" || len(report.Observations) == 0 ||
		len(report.Observations) >= MaxObservations || len(data) > MaxReportBytes {
		t.Fatalf("coverage/rows/bytes = %s/%d/%d", report.Coverage, len(report.Observations), len(data))
	}
	previousCount := len(report.Observations)
	checks := make([]catalog.BackendReadiness, maxLocalBackends)
	for i := range checks {
		checks[i] = catalog.BackendReadiness{BackendID: fmt.Sprintf("%03d%s", i, strings.Repeat("\x01", 253)), Status: "unknown"}
	}
	report, err = AddBackendReadiness(report, checks, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > MaxReportBytes || len(report.Observations) >= previousCount ||
		report.BackendReadiness == nil || len(*report.BackendReadiness) != maxLocalBackends || report.Coverage != "truncated" {
		t.Fatalf("readiness report exceeds bounds or loses evidence: bytes=%d observations=%d", len(data), len(report.Observations))
	}
}

func TestLocalReportReflectsMissingStaleAndInvalidEvidence(t *testing.T) {
	source := fixtureReport().Source
	tests := []struct {
		name        string
		selected    []string
		caches      []catalog.BackendModel
		coverage    string
		freshness   string
		observation int
	}{
		{"missing backend", []string{"missing"}, nil, "unknown", "", 0},
		{"uncached backend", []string{"local"}, []catalog.BackendModel{{BackendID: "local"}}, "unknown", "", 0},
		{"partially missing", []string{"local", "missing"}, []catalog.BackendModel{
			{BackendID: "local", FetchedAt: fixtureTime, Models: []map[string]any{{"id": "a"}}},
		}, "partial", "fresh", 1},
		{"stale", []string{"local"}, []catalog.BackendModel{
			{BackendID: "local", FetchedAt: fixtureTime.Add(-time.Hour), Models: []map[string]any{{"id": "a"}}},
		}, "partial", "stale", 1},
		{"future timestamp", []string{"local"}, []catalog.BackendModel{
			{BackendID: "local", FetchedAt: fixtureTime.Add(time.Hour), Models: []map[string]any{{"id": "a"}}},
		}, "partial", "unknown", 1},
		{"invalid ID", []string{"local"}, []catalog.BackendModel{
			{BackendID: "local", FetchedAt: fixtureTime, Models: []map[string]any{{"id": strings.Repeat("x", 257)}}},
		}, "partial", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report, err := LocalReport(source, tt.selected, tt.caches, fixtureTime, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if report.Coverage != tt.coverage || len(report.Observations) != tt.observation {
				t.Fatalf("coverage/observations = %s/%d", report.Coverage, len(report.Observations))
			}
			if tt.observation > 0 && report.Observations[0].Freshness != tt.freshness {
				t.Fatalf("freshness = %s", report.Observations[0].Freshness)
			}
		})
	}
}

func TestAddBackendReadinessPreservesSourceEvidence(t *testing.T) {
	report := fixtureReport()
	checks := []catalog.BackendReadiness{
		{BackendID: "fresh", Status: "ready", ObservedAt: fixtureTime.Add(-time.Second)},
		{BackendID: "stale", Status: "unavailable", ObservedAt: fixtureTime.Add(-time.Hour)},
		{BackendID: "future", Status: "ready", ObservedAt: fixtureTime.Add(time.Second)},
		{BackendID: "unknown", Status: "unknown"},
	}
	report, err := AddBackendReadiness(report, checks, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if report.Capabilities["diagnostics_read"] != "enabled" || report.BackendReadiness == nil {
		t.Fatal("readiness snapshot absent")
	}
	for i, freshness := range []string{"fresh", "stale", "unknown", "unknown"} {
		got := (*report.BackendReadiness)[i]
		if got.Freshness != freshness || got.Status != checks[i].Status {
			t.Fatalf("readiness %d = %#v", i, got)
		}
		if checks[i].ObservedAt.IsZero() {
			if got.ObservedAt != nil {
				t.Fatal("unknown readiness got a timestamp")
			}
		} else if got.ObservedAt == nil || !got.ObservedAt.Equal(checks[i].ObservedAt) {
			t.Fatal("readiness timestamp changed")
		}
	}
}

func TestLocalReportRejectsAmbiguousIdentityAndBoundsRows(t *testing.T) {
	source := fixtureReport().Source
	duplicate := catalog.BackendModel{BackendID: "local", FetchedAt: fixtureTime, Models: []map[string]any{
		{"id": "same"}, {"id": "same"},
	}}
	if _, err := LocalReport(source, []string{"local"}, []catalog.BackendModel{duplicate}, fixtureTime, time.Minute); err == nil {
		t.Fatal("duplicate model identity accepted")
	}
	if _, err := LocalReport(source, []string{"local", "local"}, nil, fixtureTime, time.Minute); err == nil {
		t.Fatal("duplicate backend accepted")
	}
	models := make([]map[string]any, MaxObservations+1)
	for i := range models {
		models[i] = map[string]any{"id": fmt.Sprintf("model-%04d", i)}
	}
	cache := catalog.BackendModel{BackendID: "local", FetchedAt: fixtureTime, Models: models}
	report, err := LocalReport(source, []string{"local"}, []catalog.BackendModel{cache}, fixtureTime, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if report.Coverage != "truncated" || len(report.Observations) != MaxObservations {
		t.Fatalf("coverage/rows = %s/%d", report.Coverage, len(report.Observations))
	}
	for i, j := 0, len(models)-1; i < j; i, j = i+1, j-1 {
		models[i], models[j] = models[j], models[i]
	}
	reversed, err := LocalReport(source, []string{"local"}, []catalog.BackendModel{cache}, fixtureTime, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for i := range report.Observations {
		if report.Observations[i].Identity != reversed.Observations[i].Identity {
			t.Fatalf("truncation depends on source order at row %d", i)
		}
	}
}
