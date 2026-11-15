package correlation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

var aliasFixtureTime = time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)

type aliasMockClock struct {
	now time.Time
}

func (m *aliasMockClock) Now() time.Time { return m.now }

func newTestAliasCollector(t *testing.T, backends map[string]config.Backend) *AliasCollector {
	t.Helper()
	clock := &aliasMockClock{now: aliasFixtureTime}
	c := NewAliasCollector(backends)
	c.now = func() time.Time { return clock.now }
	return c
}

// newAliasRow builds one /v1/models row with only the typed fields decoded.
func newAliasRow(id, typ, modelID string, aliases ...string) aliasRow {
	return aliasRow{
		ID: id,
		Meta: aliasMeta{
			LLaMaSwap: aliasLLaMaSwap{
				Type:    typ,
				ModelID: modelID,
				Aliases: aliases,
			},
		},
	}
}

// aliasPageBody marshals a /v1/models page.
func aliasPageBody(t *testing.T, rows ...aliasRow) string {
	t.Helper()
	page := aliasModelPage{Data: rows}
	b, err := json.Marshal(page)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// aliasPageBodyRaw marshals an arbitrary raw body.
func aliasPageBodyRaw(t *testing.T, body string) string {
	t.Helper()
	return body
}

func TestAliasConstructionMakesNoRequestsAndSkipsDisabledBackends(t *testing.T) {
	var count atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t, newAliasRow("m", "model", "", "")))
	}))
	defer server.Close()

	backends := map[string]config.Backend{
		"off": {URL: server.URL}, // no Diagnostics -> disabled
		"on":  {URL: server.URL, Diagnostics: "llama_swap"},
		"bad": {URL: "not-a-url", Diagnostics: "llama_swap"},
	}
	c := newTestAliasCollector(t, backends)
	if count.Load() != 0 {
		t.Fatal("construction made requests")
	}
	// Disabled and invalid backends are not in the backends map.
	if _, ok := c.backends["off"]; ok {
		t.Fatal("disabled backend should not be registered")
	}
	if _, ok := c.backends["bad"]; ok {
		t.Fatal("invalid URL backend should not be registered")
	}

	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatalf("enabled backend request count = %d, want 1", count.Load())
	}
	if len(report.Edges) != 1 {
		t.Fatalf("expected 1 edge (only enabled backend), got %d", len(report.Edges))
	}
}

func TestAliasCollectProjectsTypedFieldsOnly(t *testing.T) {
	shortSecret := "SECRET"
	longSecret := strings.Repeat("SECRET-LONG-", 50)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/models" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		// Name, Description, and arbitrary metadata keys carry secrets and
		// must not be decoded or copied.
		raw := `{"data":[{"id":"canon","name":"` + shortSecret + `","description":"` + longSecret + `","meta":{"llamaswap":{"type":"model","aliases":["a"]},"other":"` + longSecret + `"}}]}`
		io.WriteString(w, raw)
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// Canonical model with metadata.aliases (no separate alias row) produces
	// a self edge plus an alias edge: 2 edges total.
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges (canonical self + alias), got %d", len(report.Edges))
	}
	encoded, _ := json.Marshal(report)
	// No secret may appear anywhere in the output.
	for _, secret := range []string{shortSecret, longSecret} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "a")
	// Canonical model: exact self edge.
	canon := byRef[canonRef]
	if canon == nil || canon.State != aliasStateExact || canon.CanonicalRef != canonRef {
		t.Fatalf("canonical self edge wrong: %#v", canon)
	}
	// The raw ID "canon" is not derivable from the opaque Ref.
	if strings.Contains(canon.Ref, "canon") {
		t.Fatalf("Ref should not leak raw model ID: %q", canon.Ref)
	}
	// Ref and canonical_ref are the same opaque hash for a self edge.
	if canon.Ref != canon.CanonicalRef {
		t.Fatalf("self edge ref and canonical_ref should match: %#v", canon)
	}
	// Alias: exact edge to canonical.
	aliasEdge := byRef[aliasRef]
	if aliasEdge == nil || aliasEdge.State != aliasStateExact || aliasEdge.CanonicalRef != canonRef {
		t.Fatalf("alias edge wrong: %#v", aliasEdge)
	}
}

func TestAliasEdgeMapsAliasToCanonical(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Canonical model row with an alias, plus an explicit alias row.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("canon", "model", "", "alias1"),
			newAliasRow("alias1", "alias", "canon"),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(report.Edges))
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "alias1")
	// Canonical model: exact self edge.
	canon := byRef[canonRef]
	if canon == nil || canon.State != aliasStateExact || canon.CanonicalRef != canonRef {
		t.Fatalf("canonical self edge wrong: %#v", canon)
	}
	// Alias: exact edge to canonical.
	aliasEdge := byRef[aliasRef]
	if aliasEdge == nil || aliasEdge.State != aliasStateExact || aliasEdge.CanonicalRef != canonRef {
		t.Fatalf("alias edge wrong: %#v", aliasEdge)
	}
	// Alias chains are not inferred: alias1 resolves directly to canon, and
	// canon resolves to itself; no transitive hop is followed.
	if aliasEdge.CanonicalRef != canonRef {
		t.Fatal("alias should resolve directly to canonical")
	}
}

func TestAliasAliasMapsToTwoTargetsAmbiguous(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Alias A points to X via an explicit alias row, and A is also listed
		// as an alias of model Y. Two different canonical targets -> ambiguous.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("A", "alias", "X"),
			newAliasRow("Y", "model", "", "A"),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(report.Edges))
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	aRef, _ := ModelReference("backend", "A")
	a := byRef[aRef]
	if a == nil || a.State != aliasStateAmbiguous {
		t.Fatalf("alias with two targets should be ambiguous, got %#v", a)
	}
	if a.CanonicalRef != "" {
		t.Fatal("ambiguous edge must not emit a canonical_ref")
	}
	// Y is an explicit model: exact self edge.
	yRef, _ := ModelReference("backend", "Y")
	y := byRef[yRef]
	if y == nil || y.State != aliasStateExact || y.CanonicalRef != yRef {
		t.Fatalf("model self edge wrong: %#v", y)
	}
}

func TestAliasCanonicalAliasCollisionAmbiguous(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Ref A is both an explicit model (self edge) and an alias pointing
		// to X. Collision -> ambiguous, no winner picked.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("A", "model", "", "B"),
			newAliasRow("A", "alias", "X"),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// Ref A (collision: model + alias) plus alias row target B produce
	// 2 edges: A ambiguous, B resolved to A.
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges (collision ref + alias target), got %d", len(report.Edges))
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	aRef, _ := ModelReference("backend", "A")
	bRef, _ := ModelReference("backend", "B")
	a := byRef[aRef]
	if a == nil || a.State != aliasStateAmbiguous {
		t.Fatalf("collision ref A should be ambiguous, got %#v", a)
	}
	if a.CanonicalRef != "" {
		t.Fatal("ambiguous edge must not emit a canonical_ref")
	}
	// Extra alias row target B resolves to A (explicit model), not ambiguous.
	b := byRef[bRef]
	if b == nil || b.State != aliasStateExact || b.CanonicalRef != aRef {
		t.Fatalf("alias target B should resolve to A, got %#v", b)
	}
}

func TestAliasUnknownRows(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Missing type, unknown type, and alias with empty modelID.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("U", "", ""),      // missing type
			newAliasRow("O", "other", ""), // unknown type
			newAliasRow("E", "alias", ""), // alias with no canonical
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) != 3 {
		t.Fatalf("expected 3 edges, got %d", len(report.Edges))
	}
	for _, e := range report.Edges {
		if e.State != aliasStateUnknown {
			t.Fatalf("unknown row should be unknown, got %#v", e)
		}
		if e.CanonicalRef != "" {
			t.Fatalf("unknown edge must not emit canonical_ref: %#v", e)
		}
	}
}

func TestAliasCrossBackendAndEpochGuard(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// X is an explicit canonical model; A is an alias to X.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("X", "model", "", ""),
			newAliasRow("A", "alias", "X"),
		))
	}))
	defer server.Close()

	// Separate collectors per backend so scopes are isolated.
	c1 := newTestAliasCollector(t, map[string]config.Backend{
		"b1": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	c2 := newTestAliasCollector(t, map[string]config.Backend{
		"b2": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report1, err := c1.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	report2, err := c2.Collect(context.Background(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(report1.Edges) != 2 || len(report2.Edges) != 2 {
		t.Fatalf("expected 2 edges each (canonical self + alias), got %d and %d", len(report1.Edges), len(report2.Edges))
	}
	xRef1, _ := ModelReference("b1", "X")
	aRef1, _ := ModelReference("b1", "A")
	xRef2, _ := ModelReference("b2", "X")
	aRef2, _ := ModelReference("b2", "A")
	edgeForRef := func(report AliasReport, ref string) *AliasEdge {
		for i := range report.Edges {
			if report.Edges[i].Ref == ref {
				return &report.Edges[i]
			}
		}
		return nil
	}
	e1 := edgeForRef(report1, aRef1)
	e2 := edgeForRef(report2, aRef2)
	if e1 == nil || e1.BackendID != "b1" || e1.Epoch != 1 {
		t.Fatalf("report1 A edge wrong: %#v", report1.Edges)
	}
	if e1.State != aliasStateExact || e1.CanonicalRef != xRef1 {
		t.Fatalf("report1 should resolve A->X exact: %#v", e1)
	}
	if e2 == nil || e2.BackendID != "b2" || e2.Epoch != 2 {
		t.Fatalf("report2 A edge wrong: %#v", report2.Edges)
	}
	if e2.State != aliasStateExact || e2.CanonicalRef != xRef2 {
		t.Fatalf("report2 should resolve A->X exact: %#v", e2)
	}
	// The canonical self edges are also present and exact.
	x1 := edgeForRef(report1, xRef1)
	x2 := edgeForRef(report2, xRef2)
	if x1 == nil || x1.State != aliasStateExact || x1.CanonicalRef != xRef1 {
		t.Fatalf("report1 X self edge wrong: %#v", x1)
	}
	if x2 == nil || x2.State != aliasStateExact || x2.CanonicalRef != xRef2 {
		t.Fatalf("report2 X self edge wrong: %#v", x2)
	}
	// ResolveActivityAliases is scoped by backend+epoch+ref.
	acts := []*ActivityRecord{
		{BackendID: "b1", Epoch: 1, ActivityID: "1", ReportedModelRef: aRef1},
		{BackendID: "b2", Epoch: 2, ActivityID: "2", ReportedModelRef: aRef2}, // wrong backend
		{BackendID: "b1", Epoch: 2, ActivityID: "3", ReportedModelRef: aRef1}, // wrong epoch
	}
	identities, err := ResolveActivityAliases(acts, report1)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 3 {
		t.Fatalf("expected 3 identities, got %d", len(identities))
	}
	// Only the first matches report1's (b1, epoch 1, ref).
	if identities[0].State != aliasStateExact || identities[0].CanonicalRef != xRef1 {
		t.Fatalf("first should resolve exact: %#v", identities[0])
	}
	if identities[0].ReportedModelRef != aRef1 {
		t.Fatalf("reported_model_ref should be preserved: %q", identities[0].ReportedModelRef)
	}
	// Second (wrong backend) and third (wrong epoch) have no matching edge -> unknown.
	if identities[1].State != aliasStateUnknown || identities[1].CanonicalRef != "" {
		t.Fatalf("second should be unknown: %#v", identities[1])
	}
	if identities[2].State != aliasStateUnknown || identities[2].CanonicalRef != "" {
		t.Fatalf("third (wrong epoch) should be unknown: %#v", identities[2])
	}
}

func TestAliasEdgeChainToAliasIsUnknown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Alias chain A -> B -> C. No chain inference: A resolves only to a
		// declared canonical; B is itself an alias (not a model), so A is
		// unknown and A is never linked to C through B.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("A", "alias", "B"),
			newAliasRow("B", "alias", "C"),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges (one per row ID), got %d", len(report.Edges))
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	aRef, _ := ModelReference("backend", "A")
	bRef, _ := ModelReference("backend", "B")
	a := byRef[aRef]
	b := byRef[bRef]
	if a == nil || a.State != aliasStateUnknown || a.CanonicalRef != "" {
		t.Fatalf("alias to alias should be unknown: %#v", a)
	}
	if b == nil || b.State != aliasStateUnknown || b.CanonicalRef != "" {
		t.Fatalf("alias to alias should be unknown: %#v", b)
	}
	// No chain inference: neither A nor B resolves to C.
	for _, e := range report.Edges {
		cRef, _ := ModelReference("backend", "C")
		if e.CanonicalRef == cRef {
			t.Fatal("alias chain must not be inferred")
		}
	}
}

func TestAliasTrueCanonicalWithOptionalAliasRow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// True canonical model X with an alias A, plus an explicit alias row
		// A -> X. Both resolve to X.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("X", "model", "", "A"),
			newAliasRow("A", "alias", "X"),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(report.Edges))
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	xRef, _ := ModelReference("backend", "X")
	aRef, _ := ModelReference("backend", "A")
	x := byRef[xRef]
	a := byRef[aRef]
	if x == nil || x.State != aliasStateExact || x.CanonicalRef != xRef {
		t.Fatalf("canonical self edge wrong: %#v", x)
	}
	if a == nil || a.State != aliasStateExact || a.CanonicalRef != xRef {
		t.Fatalf("alias edge wrong: %#v", a)
	}
}

// TestAliasCanonicalOnlyNoAliasRow covers a true canonical model that declares
// an alias in metadata.aliases with NO separate alias row
// (IncludeAliasesInList=false). The canonical self edge and the alias edge are
// both produced.
func TestAliasCanonicalOnlyNoAliasRow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// True canonical model X with an alias A declared in metadata.aliases,
		// but no separate alias row.
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("X", "model", "", "A"),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// Canonical self edge + alias edge: 2 edges total.
	if len(report.Edges) != 2 {
		t.Fatalf("expected 2 edges (canonical self + alias), got %d", len(report.Edges))
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	xRef, _ := ModelReference("backend", "X")
	aRef, _ := ModelReference("backend", "A")
	x := byRef[xRef]
	if x == nil || x.State != aliasStateExact || x.CanonicalRef != xRef {
		t.Fatalf("canonical self edge wrong: %#v", x)
	}
	a := byRef[aRef]
	if a == nil || a.State != aliasStateExact || a.CanonicalRef != xRef {
		t.Fatalf("alias edge wrong: %#v", a)
	}
}

func TestAliasEdgeCapDoesNotHideConflict(t *testing.T) {
	// More than aliasMaxEdgesGlobal distinct refs: the collection must be
	// bounded and a conflict within the bound must not be hidden as resolved.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rows := make([]aliasRow, 0, aliasMaxEdgesGlobal+10)
		// Conflict ref: alias K points to two canonicals -> ambiguous.
		rows = append(rows, newAliasRow("K", "alias", "X"))
		rows = append(rows, newAliasRow("K", "alias", "Y"))
		// Filler model rows to exceed the edge cap.
		for i := 0; i < aliasMaxEdgesGlobal; i++ {
			rows = append(rows, newAliasRow(fmt.Sprintf("filler-%d", i), "model", "", ""))
		}
		io.WriteString(w, aliasPageBody(t, rows...))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) > aliasMaxEdgesGlobal {
		t.Fatalf("edge count = %d, exceeds cap %d", len(report.Edges), aliasMaxEdgesGlobal)
	}
	// The report must signal truncation.
	if !report.Truncated {
		t.Fatal("expected report to be truncated by edge cap")
	}
	byRef := make(map[string]*AliasEdge)
	for i := range report.Edges {
		byRef[report.Edges[i].Ref] = &report.Edges[i]
	}
	kRef, _ := ModelReference("backend", "K")
	k := byRef[kRef]
	if k == nil {
		t.Fatal("conflict ref K should be present within the cap")
	}
	if k.State != aliasStateAmbiguous {
		t.Fatalf("conflict ref K must be ambiguous, not resolved: %#v", k)
	}
	if k.CanonicalRef != "" {
		t.Fatal("ambiguous edge must not emit a canonical_ref")
	}
}

func TestAliasOverlongLabelsNotRetained(t *testing.T) {
	longID := strings.Repeat("model-id-", 100) // well over 256 bytes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t,
			newAliasRow(longID, "model", "", "a"),
			newAliasRow("a", "alias", longID),
		))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// The overlong model row is invalid and skipped. The valid alias "a"
	// points to an absent (overlong) canonical, so it is unknown, not
	// resolved; no chain inference is made.
	if len(report.Edges) != 1 {
		t.Fatalf("expected 1 edge (valid alias to absent canonical is unknown), got %d", len(report.Edges))
	}
	e := report.Edges[0]
	if e.State != aliasStateUnknown || e.CanonicalRef != "" {
		t.Fatalf("alias to absent canonical should be unknown: %#v", e)
	}
	// The ref is the opaque hash of the valid short alias; the overlong ID
	// is never retained.
	aRef, _ := ModelReference("backend", "a")
	if e.Ref != aRef {
		t.Fatalf("Ref = %q, want opaque hash of 'a': %q", e.Ref, aRef)
	}
	encoded, _ := json.Marshal(report)
	if strings.Contains(string(encoded), longID) {
		t.Fatal("overlong ID leaked in output")
	}
}

func TestAliasSecretsNeverInJSON(t *testing.T) {
	short := "SHORT-SECRET"
	long := strings.Repeat("LONG-SECRET-", 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Secrets in the raw model label, Name, Description, meta values and
		// meta keys must never appear in the output.
		raw := `{"data":[{"id":"` + short + `","name":"` + long + `","description":"` + short + `","meta":{"llamaswap":{"type":"model","aliases":["` + long + `"]},"` + short + `":"` + long + `"}}]}`
		io.WriteString(w, raw)
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(report)
	for _, secret := range []string{short, long} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
	// Only opaque hashes appear.
	if len(report.Edges) == 0 {
		t.Fatal("expected at least one edge")
	}
	if strings.Contains(report.Edges[0].Ref, short) || strings.Contains(report.Edges[0].Ref, long) {
		t.Fatal("Ref should be an opaque hash, not contain secrets")
	}
}

func TestAliasAuthHeadersAndBasepath(t *testing.T) {
	var gotPath, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t, newAliasRow("m", "model", "", "")))
	}))
	defer server.Close()

	// Backend URL with a path prefix (basepath) and an API key.
	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL + "/api", Diagnostics: "llama_swap", APIKey: "k"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != "/api/v1/models" {
		t.Fatalf("basepath path = %q, want /api/v1/models", gotPath)
	}
	if gotAuth != "Bearer k" {
		t.Fatalf("auth header = %q, want Bearer k", gotAuth)
	}
	if len(report.Edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(report.Edges))
	}
}

func TestAliasRedirectRefused(t *testing.T) {
	var reached atomic.Int32
	tripwire := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached.Add(1)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t, newAliasRow("m", "model", "", "")))
	}))
	defer tripwire.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, tripwire.URL+"/v1/models", http.StatusFound)
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	st := report.BackendStates["backend"]
	if st.State != aliasUnknownState || st.Reason != "redirect_refused" {
		t.Fatalf("redirect not refused: %#v", st)
	}
	if reached.Load() != 0 {
		t.Fatal("redirect was followed despite no-redirect policy")
	}
	if len(report.Edges) != 0 {
		t.Fatalf("expected 0 edges on redirect, got %d", len(report.Edges))
	}
}

func TestAliasContextCancelNoPartialSuccess(t *testing.T) {
	var requestCount atomic.Int32
	started := make(chan struct{}, 1)
	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requestCount.Add(1)
		if n == 1 {
			// First backend responds immediately.
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, aliasPageBody(t, newAliasRow("m1", "model", "", "")))
			return
		}
		// Second backend: signal in flight, then block.
		started <- struct{}{}
		<-block
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t, newAliasRow("m2", "model", "", "")))
	}))
	defer server.Close()

	backends := map[string]config.Backend{
		"b1": {URL: server.URL, Diagnostics: "llama_swap"},
		"b2": {URL: server.URL, Diagnostics: "llama_swap"},
	}
	c := newTestAliasCollector(t, backends)

	done := make(chan error, 1)
	go func() {
		_, err := c.Collect(context.Background(), 1)
		done <- err
	}()
	<-started // second request in flight, gate held

	// A second Collect with a cancelled context must return ctx.Err() without
	// acquiring the gate or producing partial success.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := c.Collect(ctx, 1)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled Collect did not return promptly")
	}
	close(block) // let the in-flight request complete
	if err := <-done; err != nil {
		t.Fatalf("in-flight Collect should complete, got %v", err)
	}
}

func TestAliasBounds(t *testing.T) {
	// Per-backend row cap: 1000 rows, only 1000 processed.
	var call atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := call.Add(1)
		w.Header().Set("Content-Type", "application/json")
		rows := make([]aliasRow, 0, 5)
		for i := 0; i < 5; i++ {
			rows = append(rows, newAliasRow(fmt.Sprintf("m%d", int(n)*5+i), "model", "", ""))
		}
		io.WriteString(w, aliasPageBody(t, rows...))
	}))
	defer server.Close()
	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Edges) != 5 {
		t.Fatalf("expected 5 edges (under cap), got %d", len(report.Edges))
	}
	st := report.BackendStates["backend"]
	if st.Rows != 5 {
		t.Fatalf("rows = %d, want 5", st.Rows)
	}

	// Global edge cap: 4096 edges across backends.
	backends := make(map[string]config.Backend)
	for i := 0; i < 3; i++ {
		backends[fmt.Sprintf("b%d", i)] = config.Backend{URL: server.URL, Diagnostics: "llama_swap"}
	}
	c2 := newTestAliasCollector(t, backends)
	report2, err := c2.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// 3 backends * 5 edges = 15, well under the global cap.
	if len(report2.Edges) != 15 {
		t.Fatalf("expected 15 edges, got %d", len(report2.Edges))
	}
}

func TestAliasStableOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t,
			newAliasRow("z", "model", "", "a"),
			newAliasRow("a", "alias", "z"),
			newAliasRow("m", "model", "", ""),
		))
	}))
	defer server.Close()
	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	r1, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Edges) != len(r2.Edges) {
		t.Fatalf("edge counts differ: %d vs %d", len(r1.Edges), len(r2.Edges))
	}
	for i := range r1.Edges {
		if r1.Edges[i] != r2.Edges[i] {
			t.Fatalf("edge %d differs: %#v vs %#v", i, r1.Edges[i], r2.Edges[i])
		}
	}
}

func TestAliasCloneAndConcurrency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, aliasPageBody(t, newAliasRow("m", "model", "", "a")))
	}))
	defer server.Close()
	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})

	var wg sync.WaitGroup
	reports := make([]AliasReport, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Collect(context.Background(), 1)
			if err != nil {
				t.Errorf("concurrent collect %d error: %v", i, err)
				return
			}
			reports[i] = r
		}(i)
	}
	wg.Wait()
	for i := range reports {
		if len(reports[i].Edges) != 2 {
			t.Fatalf("report %d expected 2 edges (canonical self + alias), got %d", i, len(reports[i].Edges))
		}
	}
	// Mutating one report must not affect another.
	reports[0].Edges[0].State = "mutated"
	reports[0].Edges[1].State = "mutated"
	for i := 1; i < len(reports); i++ {
		for j := range reports[i].Edges {
			if reports[i].Edges[j].State == "mutated" {
				t.Fatal("collect returned aliased edges")
			}
		}
	}
}

// TestAliasLateConflictAfterRowCap covers a conflict alias that appears AFTER
// the 1000-row cap, so it is never inspected. Because the catalog is
// truncated/incomplete, none of the retained edges may claim catalog_resolved:
// the collector marks exact edges unknown.
func TestAliasLateConflictAfterRowCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rows := make([]aliasRow, 0, aliasMaxRowsPerBackend+2)
		// Filler model rows fill the row cap.
		for i := 0; i < aliasMaxRowsPerBackend; i++ {
			rows = append(rows, newAliasRow(fmt.Sprintf("filler-%d", i), "model", "", ""))
		}
		// Late conflict: alias K points to two canonicals. It appears AFTER
		// the 1000-row cap, so it is never inspected.
		rows = append(rows, newAliasRow("K", "alias", "X"))
		rows = append(rows, newAliasRow("K", "alias", "Y"))
		io.WriteString(w, aliasPageBody(t, rows...))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Truncated {
		t.Fatal("expected report to be truncated by row cap")
	}
	// Only the 1000 filler rows are retained; the late conflict K is not.
	if len(report.Edges) != aliasMaxRowsPerBackend {
		t.Fatalf("expected %d retained edges (fillers only), got %d", aliasMaxRowsPerBackend, len(report.Edges))
	}
	// None of the retained edges may claim catalog_resolved: the catalog is
	// incomplete, so the collector marks exact edges unknown.
	for _, e := range report.Edges {
		if e.State == aliasStateExact {
			t.Fatalf("retained edge %q claims catalog_resolved despite truncated catalog: %#v", e.Ref, e)
		}
		if e.CanonicalRef != "" {
			t.Fatalf("retained edge %q emits canonical_ref despite truncated catalog: %#v", e.Ref, e)
		}
	}
}

// TestAliasManyTargetsPerAliasNoWinner covers an alias declared to point at
// many canonicals. No winner is picked: the edge is ambiguous with no
// canonical_ref.
func TestAliasManyTargetsPerAliasNoWinner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rows := make([]aliasRow, 0, 5)
		rows = append(rows, newAliasRow("A", "alias", "X1"))
		rows = append(rows, newAliasRow("A", "alias", "X2"))
		rows = append(rows, newAliasRow("A", "alias", "X3"))
		rows = append(rows, newAliasRow("A", "alias", "X4"))
		rows = append(rows, newAliasRow("A", "alias", "X5"))
		io.WriteString(w, aliasPageBody(t, rows...))
	}))
	defer server.Close()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	// One edge per distinct ref: only A (all alias rows share ID "A").
	if len(report.Edges) != 1 {
		t.Fatalf("expected 1 edge (one distinct ref A), got %d", len(report.Edges))
	}
	e := report.Edges[0]
	if e.State != aliasStateAmbiguous {
		t.Fatalf("alias with many targets should be ambiguous, got %#v", e)
	}
	if e.CanonicalRef != "" {
		t.Fatal("many targets must not pick a winner: canonical_ref must be empty")
	}
}

// ---------- ResolveActivityAliases tests ----------

func TestResolveActivityAliasesExactResolution(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "alias1")
	report := AliasReport{
		Edges: []AliasEdge{
			{BackendID: "backend", Epoch: 1, Ref: aliasRef, CanonicalRef: canonRef, State: aliasStateExact},
			{BackendID: "backend", Epoch: 1, Ref: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
		},
	}
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: aliasRef, ModelID: "alias1"},
		{BackendID: "backend", Epoch: 1, ActivityID: "2", ReportedModelRef: canonRef, ModelID: "canon"},
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(identities))
	}
	// Activity under an alias resolves to the canonical ref.
	if identities[0].State != aliasStateExact || identities[0].CanonicalRef != canonRef {
		t.Fatalf("alias activity should resolve exact: %#v", identities[0])
	}
	if identities[0].ReportedModelRef != aliasRef {
		t.Fatalf("reported_model_ref should be preserved: %q", identities[0].ReportedModelRef)
	}
	// Activity under a canonical model resolves to itself.
	if identities[1].State != aliasStateExact || identities[1].CanonicalRef != canonRef {
		t.Fatalf("canonical activity should resolve exact: %#v", identities[1])
	}
	// Source is fixed.
	if identities[0].Source != aliasSourceCatalogMetadata {
		t.Fatalf("source = %q, want %q", identities[0].Source, aliasSourceCatalogMetadata)
	}
	// The identity type has no ModelID field, so the raw ModelID is never
	// emitted by construction.
}

func TestResolveActivityAliasesAmbiguousOmitsCanonicalRef(t *testing.T) {
	aliasRef, _ := ModelReference("backend", "alias1")
	report := AliasReport{
		Edges: []AliasEdge{
			{BackendID: "backend", Epoch: 1, Ref: aliasRef, State: aliasStateAmbiguous},
		},
	}
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: aliasRef},
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 {
		t.Fatalf("expected 1 identity, got %d", len(identities))
	}
	if identities[0].State != aliasStateAmbiguous {
		t.Fatalf("should be ambiguous, got %#v", identities[0])
	}
	if identities[0].CanonicalRef != "" {
		t.Fatal("ambiguous identity must omit canonical_ref")
	}
}

func TestResolveActivityAliasesUnknown(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	report := AliasReport{Edges: []AliasEdge{
		{BackendID: "backend", Epoch: 1, Ref: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
	}}
	// Activity ref has no matching edge.
	unknownRef, _ := ModelReference("backend", "unknown")
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: unknownRef},
		{BackendID: "backend", Epoch: 1, ActivityID: "2", ReportedModelRef: ""}, // empty ref
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(identities))
	}
	for _, id := range identities {
		if id.State != aliasStateUnknown || id.CanonicalRef != "" {
			t.Fatalf("unknown identity: %#v", id)
		}
	}
}

func TestResolveActivityAliasesBounded(t *testing.T) {
	report := AliasReport{}
	acts := make([]*ActivityRecord, aliasMaxActivities+1)
	for i := range acts {
		acts[i] = &ActivityRecord{BackendID: "b", Epoch: 1, ActivityID: fmt.Sprintf("%d", i)}
	}
	if _, err := ResolveActivityAliases(acts, report); err == nil {
		t.Fatal("expected error for over-bound activities")
	}
}

func TestResolveActivityAliasesPureAndBounded(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "alias1")
	report := AliasReport{
		Edges: []AliasEdge{
			{BackendID: "backend", Epoch: 1, Ref: aliasRef, CanonicalRef: canonRef, State: aliasStateExact},
		},
	}
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: aliasRef, ModelID: "alias1"},
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 1 {
		t.Fatalf("expected 1 identity, got %d", len(identities))
	}
	// Mutating the input activity or report must not affect the output.
	acts[0].ModelID = "mutated"
	acts[0].ReportedModelRef = "mutated"
	report.Edges[0].State = "mutated"
	if identities[0].CanonicalRef != canonRef || identities[0].State != aliasStateExact {
		t.Fatalf("output changed after input mutation: %#v", identities[0])
	}
}

func TestResolveActivityAliasesIdentityConflictIsUnknown(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	report := AliasReport{
		Edges: []AliasEdge{
			{BackendID: "backend", Epoch: 1, Ref: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
		},
	}
	// A conflicting activity identity is never merged and is unknown.
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: canonRef, IdentityConflict: true},
		{BackendID: "backend", Epoch: 1, ActivityID: "2", ReportedModelRef: canonRef},
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(identities))
	}
	if identities[0].State != aliasStateUnknown || identities[0].CanonicalRef != "" {
		t.Fatalf("conflict activity should be unknown: %#v", identities[0])
	}
	if identities[1].State != aliasStateExact || identities[1].CanonicalRef != canonRef {
		t.Fatalf("non-conflict activity should resolve: %#v", identities[1])
	}
}

// TestResolveActivityAliasesReportEdgesBounded covers a report with more
// edges than the cap. ResolveActivityAliases must return a finite error
// rather than silently clipping or allocating unbounded memory. The error
// must not leak caller data.
func TestResolveActivityAliasesReportEdgesBounded(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	// Report with more edges than the cap.
	edges := make([]AliasEdge, 0, aliasMaxEdgesGlobal+10)
	for i := 0; i < aliasMaxEdgesGlobal+10; i++ {
		ref, _ := ModelReference("backend", fmt.Sprintf("ref-%d", i))
		edges = append(edges, AliasEdge{BackendID: "backend", Epoch: 1, Ref: ref, CanonicalRef: canonRef, State: aliasStateExact})
	}
	report := AliasReport{Edges: edges}
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: edges[0].Ref},
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err == nil {
		t.Fatal("expected error for over-bound report edges")
	}
	if identities != nil {
		t.Fatal("expected no identities on error")
	}
	// The error must not leak caller data (refs or canonical refs).
	if strings.Contains(err.Error(), "ref-") || strings.Contains(err.Error(), canonRef) {
		t.Fatal("error must not contain caller data")
	}
}

// TestResolveActivityAliasesCanonicalOnlyAliasResolvesCatalogResolved covers a
// canonical-only fixture: model "canon" declares alias "alias1" in
// metadata.aliases with no separate alias row. The catalog contains a self
// edge for canon and an exact edge alias1 -> canon. An activity reported
// under the alias ref resolves to catalog_resolved.
func TestResolveActivityAliasesCanonicalOnlyAliasResolvesCatalogResolved(t *testing.T) {
	canonRef, _ := ModelReference("backend", "canon")
	aliasRef, _ := ModelReference("backend", "alias1")
	// Canonical-only fixture: model "canon" declares alias "alias1" in
	// metadata.aliases with no separate alias row. The catalog contains a
	// self edge for canon and an exact edge alias1 -> canon.
	report := AliasReport{
		Edges: []AliasEdge{
			{BackendID: "backend", Epoch: 1, Ref: canonRef, CanonicalRef: canonRef, State: aliasStateExact},
			{BackendID: "backend", Epoch: 1, Ref: aliasRef, CanonicalRef: canonRef, State: aliasStateExact},
		},
	}
	acts := []*ActivityRecord{
		{BackendID: "backend", Epoch: 1, ActivityID: "1", ReportedModelRef: aliasRef, ModelID: "alias1"},
		{BackendID: "backend", Epoch: 1, ActivityID: "2", ReportedModelRef: canonRef, ModelID: "canon"},
	}
	identities, err := ResolveActivityAliases(acts, report)
	if err != nil {
		t.Fatal(err)
	}
	if len(identities) != 2 {
		t.Fatalf("expected 2 identities, got %d", len(identities))
	}
	// Activity under the alias resolves to the canonical (catalog_resolved).
	if identities[0].State != aliasStateExact || identities[0].CanonicalRef != canonRef {
		t.Fatalf("alias activity should resolve catalog_resolved: %#v", identities[0])
	}
	if identities[0].ReportedModelRef != aliasRef {
		t.Fatalf("reported_model_ref should be preserved: %q", identities[0].ReportedModelRef)
	}
	if identities[0].Source != aliasSourceCatalogMetadata {
		t.Fatalf("source = %q, want %q", identities[0].Source, aliasSourceCatalogMetadata)
	}
	// Activity under the canonical resolves to itself.
	if identities[1].State != aliasStateExact || identities[1].CanonicalRef != canonRef {
		t.Fatalf("canonical activity should resolve catalog_resolved: %#v", identities[1])
	}
}

func TestAliasCollectNoStdoutSecret(t *testing.T) {
	shortSecret := "SHORT-SECRET"
	longSecret := strings.Repeat("LONG-SECRET-", 100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Valid body with a secret in the raw label and Name, plus a
		// malformed trailing fragment.
		raw := `{"data":[{"id":"` + shortSecret + `","name":"` + longSecret + `","meta":{"llamaswap":{"type":"model"}}}` + longSecret + `}`
		io.WriteString(w, raw)
	}))
	defer server.Close()

	pipeReader, pipeWriter, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe failed: %v", err)
	}
	defer pipeReader.Close()
	defer pipeWriter.Close()
	oldOut := os.Stdout
	os.Stdout = pipeWriter
	defer func() { os.Stdout = oldOut }()

	c := newTestAliasCollector(t, map[string]config.Backend{
		"backend": {URL: server.URL, Diagnostics: "llama_swap"},
	})
	report, err := c.Collect(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	pipeWriter.Close()
	out, _ := io.ReadAll(pipeReader)
	if len(out) != 0 {
		t.Fatal("collector printed backend data or diagnostics to stdout")
	}
	encoded, _ := json.Marshal(report)
	for _, secret := range []string{shortSecret, longSecret} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("secret leaked in output: %q", secret)
		}
	}
}
