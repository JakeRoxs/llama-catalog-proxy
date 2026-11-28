//go:build linux

package proxy

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/catalog"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

// requestFunc is the management HTTP request helper signature.
type requestFunc func(method, path, credential string, bodies ...string) *httptest.ResponseRecorder

// creationFixture bundles the shared temp-dir fixtures for creation tests:
// model file, config target, recorded job, and persistent job manager.
type creationFixture struct {
	data       []byte
	models     string
	records    string
	proposals  string
	backups    string
	targetDir  string
	target     config.ManagementTarget
	source     string
	record     ingestion.Record
	manager    *ingestion.JobManager
	ctx        int32
	input      modelconfig.CreationCommandInput
	previewReq recordedCreationPreviewRequest
	recordID   string
}

// newCreationFixture builds the standard fixtures: model file, config target,
// recorded job, and persistent job manager. It replaces the duplicated setup
// sequence (temp dirs, chmod, model/config files, recorded job, job manager)
// found in both creation tests.
func newCreationFixture(t *testing.T) creationFixture {
	t.Helper()
	models, records, proposals, backups, targetDir := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Chmod(records, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(proposals, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(backups, 0o700); err != nil {
		t.Fatal(err)
	}
	data := []byte("fixture only")
	if err := os.WriteFile(filepath.Join(models, "model.gguf"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	source := "secret: confidential\nmodels:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n"
	target := config.ManagementTarget{Directory: targetDir, Filename: "config.yaml"}
	if err := os.WriteFile(filepath.Join(targetDir, target.Filename), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	sum := sha256.Sum256(data)
	record := ingestion.Record{Version: 1, Job: ingestion.JobStatus{
		ID: "ingest-00000000000000000000000000000001", State: "succeeded",
		Source:    ingestion.Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		StartedAt: now, CompletedAt: now.Add(time.Second),
		Result: ingestion.StageResult{Publication: ingestion.PublicationPublished, Durability: "confirmed",
			Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), Integrity: "calculated_unverified",
			Metadata: &ingestion.GGUFMetadata{}},
	}}
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(records, record.Job.ID+".json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := ingestion.NewPersistentJobManager(context.Background(), "", records, 4)
	if err != nil {
		t.Fatal(err)
	}
	ctx := int32(4096)
	input := modelconfig.CreationCommandInput{Executable: "llama-server", Settings: modelconfig.CommandSettings{ContextSize: &ctx}}
	previewReq := recordedCreationPreviewRequest{
		jobs:           manager,
		modelDirectory: models,
		target:         target,
		targetID:       "local",
		modelID:        "new",
		templateID:     "",
		recordID:       record.Job.ID,
		input:          input,
	}
	return creationFixture{
		data:       data,
		models:     models,
		records:    records,
		proposals:  proposals,
		backups:    backups,
		targetDir:  targetDir,
		target:     target,
		source:     source,
		record:     record,
		manager:    manager,
		ctx:        ctx,
		input:      input,
		previewReq: previewReq,
		recordID:   record.Job.ID,
	}
}

// creationTest holds the shared state for a creation test run so the phased
// validation helpers can be methods with a single receiver parameter instead
// of long parameter lists.
type creationTest struct {
	t              *testing.T
	fx             creationFixture
	reloader       *Reloader
	state          *reloaderState
	request        requestFunc
	previewPath    string
	createPath     string
	body           string
	token          string
	writeToken     string
	previewToken   string
	ingestionToken string
	ack            string
	preview        recordedCreationPreview
	proposal       modelconfig.CreationProposal
	createBody     string
	path           string
	applied        mutationResponse
	catalogPath    string
	catalogRequest string
	backendToken   string
}

// managementRequest returns an HTTP request helper bound to the reloader's
// management handler. It mirrors the previous inline request helper.
func managementRequest(reloader *Reloader) requestFunc {
	return func(method, path, credential string, bodies ...string) *httptest.ResponseRecorder {
		body := ""
		if len(bodies) != 0 {
			body = bodies[0]
		}
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.TLS = &tls.ConnectionState{}
		req.Header.Set("Authorization", "Bearer "+credential)
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		reloader.ManagementHandler().ServeHTTP(response, req)
		return response
	}
}

// assertUnauthorized checks a single management request expects 401.
func assertUnauthorized(t *testing.T, request requestFunc, method, path, credential, body, msg string) {
	t.Helper()
	if response := request(method, path, credential, body); response.Code != http.StatusUnauthorized {
		t.Fatalf("%s: %d %s", msg, response.Code, response.Body.String())
	}
}

// assertBadRequest checks a single management request expects 400.
func assertBadRequest(t *testing.T, request requestFunc, method, path, credential, body, msg string) {
	t.Helper()
	if response := request(method, path, credential, body); response.Code != http.StatusBadRequest {
		t.Fatalf("%s: %d %s", msg, response.Code, response.Body.String())
	}
}

// assertNotFound checks a single management request expects 404.
func assertNotFound(t *testing.T, request requestFunc, method, path, credential, body, msg string) {
	t.Helper()
	if response := request(method, path, credential, body); response.Code != http.StatusNotFound {
		t.Fatalf("%s: %d %s", msg, response.Code, response.Body.String())
	}
}

// assertCatalogFields checks a catalog response carries every expected
// substring with a 200 status.
func assertCatalogFields(t *testing.T, response *httptest.ResponseRecorder, expected []string, msg string) {
	t.Helper()
	if response.Code != http.StatusOK {
		t.Fatalf("%s: expected 200, got %d %s", msg, response.Code, response.Body.String())
	}
	for _, e := range expected {
		if !strings.Contains(response.Body.String(), e) {
			t.Fatalf("%s: body %q does not contain %q", msg, response.Body.String(), e)
		}
	}
}

// previewAndEvidence calls the creation preview and asserts the expected
// preview evidence, that no private data leaks into the JSON, and that the
// target file is unchanged.
func previewAndEvidence(t *testing.T, fx creationFixture) recordedCreationPreview {
	t.Helper()
	got, err := previewRecordedCreation(fx.previewReq)
	if err != nil {
		t.Fatal(err)
	}
	if got.Preview.Artifact != "regular_file_when_checked" || got.Artifact.RecordID != fx.record.Job.ID ||
		got.Artifact.RecordMatch != "unknown" || got.Artifact.Integrity != "not_verified" || got.Artifact.BackendAccess != "unknown" {
		t.Fatalf("unexpected preview evidence: %#v", got)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{fx.models, fx.targetDir, "confidential", "llama-server"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("preview disclosed private data: %s", encoded)
		}
	}
	unchanged, err := os.ReadFile(filepath.Join(fx.targetDir, fx.target.Filename))
	if err != nil || string(unchanged) != fx.source {
		t.Fatalf("preview modified target: %v", err)
	}
	return got
}

// storePhases asserts the stale store is rejected, then stores a valid
// proposal and verifies it. It returns the created proposal.
func storePhases(t *testing.T, fx creationFixture, preview recordedCreationPreview) modelconfig.CreationProposal {
	t.Helper()
	if _, err := storeRecordedCreation(storeRecordedCreationRequest{
		previewRequest:           fx.previewReq,
		store:                    fx.proposals,
		expectedRevision:         strings.Repeat("a", 64),
		expectedArtifactRevision: preview.Artifact.Revision,
		expectedReviewRevision:   preview.ReviewRevision,
	}); !errors.Is(err, modelconfig.ErrProposalStale) {
		t.Fatalf("stale review accepted: %v", err)
	}
	proposal, err := storeRecordedCreation(storeRecordedCreationRequest{
		previewRequest:           fx.previewReq,
		store:                    fx.proposals,
		expectedRevision:         preview.Preview.Revision,
		expectedArtifactRevision: preview.Artifact.Revision,
		expectedReviewRevision:   preview.ReviewRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyRecordedCreation(fx.manager, fx.models, fx.proposals, fx.target, "local", proposal.ID)
	if err != nil || verified.Preview.ModelID != "new" || verified.Artifact.Revision != preview.Artifact.Revision {
		t.Fatalf("proposal verification failed: %#v %v", verified, err)
	}
	return proposal
}

// newReloaderAndRequest builds the reloader state and returns the reloader,
// state, and a management request helper bound to it.
func newReloaderAndRequest(t *testing.T, fx creationFixture, proposal modelconfig.CreationProposal) (*Reloader, *reloaderState, requestFunc) {
	t.Helper()
	token, writeToken, previewToken := strings.Repeat("p", 32), strings.Repeat("w", 32), strings.Repeat("v", 32)
	ingestionConfig := config.IngestionConfig{Directory: fx.models, RecordDirectory: fx.records}
	state := &reloaderState{management: &config.ManagementConfig{
		ProposalStore: &config.ProposalStoreConfig{Directory: fx.proposals, Token: token},
		BackupStore:   &config.BackupStoreConfig{Directory: fx.backups, MaxBackups: 4},
		WriteToken:    writeToken,
		PreviewToken:  previewToken,
		Ingestion:     &ingestionConfig,
		Targets:       map[string]config.ManagementTarget{"local": fx.target},
	}}
	reloader := &Reloader{
		logger:          discardLogger(),
		managementSlots: make(chan struct{}, 2),
		ingestion:       &ingestionRuntime{config: ingestionConfig, jobs: fx.manager},
	}
	reloader.state.Store(state)
	return reloader, state, managementRequest(reloader)
}

// catalogBackendHandler returns an httptest server handler that emulates a
// catalog backend. It records unexpected requests and honors the unavailable
// flag and the current catalog body.
func catalogBackendHandler(backendToken string, unavailable *atomic.Bool, catalogBody *atomic.Value, unexpectedRequest *atomic.Bool) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/models" || req.Header.Get("Authorization") != "Bearer "+backendToken {
			unexpectedRequest.Store(true)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		if unavailable.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(catalogBody.Load().(string)))
	})
}

// collidingBackendHandler returns an httptest server handler that emulates a
// second catalog backend with a colliding ID.
func collidingBackendHandler(unexpectedRequest *atomic.Bool) http.HandlerFunc {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/models" {
			unexpectedRequest.Store(true)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"local/new"}]}`))
	})
}

// previewAuthFlow asserts the preview endpoint rejects invalid credentials,
// invalid bodies, and cross-origin requests.
func (c *creationTest) previewAuthFlow() {
	c.t.Helper()
	for _, invalid := range []string{c.token, c.writeToken, c.ingestionToken, ""} {
		c.assertUnauthorized(http.MethodPost, c.previewPath, invalid, c.body, "wrong preview credential")
	}
	for _, invalid := range []string{
		strings.Replace(c.body, `"target_id":"local"`, `"target_id":"local","TARGET_ID":"local"`, 1),
		strings.TrimSuffix(c.body, "}") + `,"model_path":"/private/model.gguf"}`,
		strings.TrimSuffix(c.body, "}") + `,"unknown":true}`,
	} {
		c.assertBadRequest(http.MethodPost, c.previewPath, c.previewToken, invalid, "invalid preview")
	}
	for _, endpoint := range []struct{ path, credential, payload string }{
		{c.previewPath, c.previewToken, c.body},
		{c.createPath, c.token, c.body},
	} {
		req := httptest.NewRequest(http.MethodPost, endpoint.path, strings.NewReader(endpoint.payload))
		req.TLS = &tls.ConnectionState{}
		req.Header.Set("Authorization", "Bearer "+endpoint.credential)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://other.invalid")
		response := httptest.NewRecorder()
		c.reloader.ManagementHandler().ServeHTTP(response, req)
		if response.Code != http.StatusForbidden {
			c.t.Fatalf("cross-origin creation request accepted: %d", response.Code)
		}
	}
}

// previewSuccess calls the preview endpoint and asserts a successful response
// with no private data leakage.
func (c *creationTest) previewSuccess() recordedCreationPreview {
	c.t.Helper()
	response := c.request(http.MethodPost, c.previewPath, c.previewToken, c.body)
	var preview recordedCreationPreview
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil || response.Code != http.StatusOK ||
		preview.Preview.TargetFingerprint == "" || preview.Preview.CandidateRevision == "" || preview.ReviewRevision == "" {
		c.t.Fatalf("creation preview = %d %s %v", response.Code, response.Body.String(), err)
	}
	for _, private := range []string{c.fx.models, c.fx.targetDir, "confidential", "llama-server"} {
		if strings.Contains(response.Body.String(), private) {
			c.t.Fatalf("creation preview leaked private data: %s", response.Body.String())
		}
	}
	return preview
}

// assertUnauthorized is a method variant of the package-level helper bound to
// the test context.
func (c *creationTest) assertUnauthorized(method, path, credential, body, msg string) {
	c.t.Helper()
	if response := c.request(method, path, credential, body); response.Code != http.StatusUnauthorized {
		c.t.Fatalf("%s: %d %s", msg, response.Code, response.Body.String())
	}
}

// assertBadRequest is a method variant of the package-level helper bound to
// the test context.
func (c *creationTest) assertBadRequest(method, path, credential, body, msg string) {
	c.t.Helper()
	if response := c.request(method, path, credential, body); response.Code != http.StatusBadRequest {
		c.t.Fatalf("%s: %d %s", msg, response.Code, response.Body.String())
	}
}

// assertNotFound is a method variant of the package-level helper bound to
// the test context.
func (c *creationTest) assertNotFound(method, path, credential, body, msg string) {
	c.t.Helper()
	if response := c.request(method, path, credential, body); response.Code != http.StatusNotFound {
		c.t.Fatalf("%s: %d %s", msg, response.Code, response.Body.String())
	}
}

// createValidation asserts the create endpoint rejects invalid credentials,
// stale revisions, and an invalid review digest.
func (c *creationTest) createValidation() {
	c.t.Helper()
	for _, invalid := range []string{c.previewToken, c.writeToken, c.ingestionToken, ""} {
		c.assertUnauthorized(http.MethodPost, c.createPath, invalid, c.createBody, "wrong proposal credential")
	}
	for _, stale := range []string{
		strings.Replace(c.createBody, c.preview.Preview.Revision, strings.Repeat("a", 64), 1),
		strings.Replace(c.createBody, c.preview.Artifact.Revision, strings.Repeat("b", 64), 1),
		strings.Replace(c.createBody, c.preview.ReviewRevision, strings.Repeat("c", 64), 1),
		strings.Replace(c.createBody, "4096", "8192", 1),
	} {
		if response := c.request(http.MethodPost, c.createPath, c.token, stale); response.Code != http.StatusConflict {
			c.t.Fatalf("stale creation review accepted: %d %s", response.Code, response.Body.String())
		}
	}
	if response := c.request(http.MethodPost, c.createPath, c.token, strings.Replace(c.createBody, c.preview.ReviewRevision, "invalid", 1)); response.Code != http.StatusBadRequest {
		c.t.Fatalf("invalid review digest accepted: %d", response.Code)
	}
}

// createWithChangedTarget asserts a changed target binding is rejected, then
// creates a valid proposal and returns it.
func (c *creationTest) createWithChangedTarget() modelconfig.CreationProposal {
	c.t.Helper()
	otherTarget := c.t.TempDir()
	if err := os.WriteFile(filepath.Join(otherTarget, c.fx.target.Filename), []byte(c.fx.source), 0o600); err != nil {
		c.t.Fatal(err)
	}
	c.state.management.Targets["local"] = config.ManagementTarget{Directory: otherTarget, Filename: c.fx.target.Filename}
	if response := c.request(http.MethodPost, c.createPath, c.token, c.createBody); response.Code != http.StatusConflict {
		c.t.Fatalf("changed target binding accepted: %d %s", response.Code, response.Body.String())
	}
	c.state.management.Targets["local"] = c.fx.target
	response := c.request(http.MethodPost, c.createPath, c.token, c.createBody)
	var proposal modelconfig.CreationProposal
	if err := json.Unmarshal(response.Body.Bytes(), &proposal); err != nil || response.Code != http.StatusCreated ||
		proposal.CandidateRevision != c.preview.Preview.CandidateRevision {
		c.t.Fatalf("create proposal = %d %s %v", response.Code, response.Body.String(), err)
	}
	for _, private := range []string{c.fx.models, c.fx.targetDir, "confidential", "llama-server"} {
		if strings.Contains(response.Body.String(), private) {
			c.t.Fatalf("creation proposal leaked private data: %s", response.Body.String())
		}
	}
	return proposal
}

// templateFlow asserts a template-backed preview and proposal are created.
func (c *creationTest) templateFlow() {
	c.t.Helper()
	templateBody := fmt.Sprintf(`{"target_id":"local","model_id":"template-new","template_id":"old","artifact_record_id":%q}`, c.fx.record.Job.ID)
	response := c.request(http.MethodPost, c.previewPath, c.previewToken, templateBody)
	var templatePreview recordedCreationPreview
	if err := json.Unmarshal(response.Body.Bytes(), &templatePreview); err != nil || response.Code != http.StatusOK {
		c.t.Fatalf("template preview = %d %s %v", response.Code, response.Body.String(), err)
	}
	templateCreate := strings.TrimSuffix(templateBody, "}") + fmt.Sprintf(`,"revision":%q,"artifact_revision":%q,"review_revision":%q}`,
		templatePreview.Preview.Revision, templatePreview.Artifact.Revision, templatePreview.ReviewRevision)
	response = c.request(http.MethodPost, c.createPath, c.token, templateCreate)
	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"template_id":"old"`) {
		c.t.Fatalf("template proposal = %d %s", response.Code, response.Body.String())
	}
}

// proposalAndApplyFlow asserts the proposal GET, verify, wrong credential,
// apply authorization, and incomplete acknowledgements, then applies and
// returns the mutation response.
func (c *creationTest) proposalAndApplyFlow() mutationResponse {
	c.t.Helper()
	response := c.request(http.MethodGet, c.path, c.token)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"kind":"create_model"`) ||
		strings.Contains(response.Body.String(), c.fx.models) || strings.Contains(response.Body.String(), "llama-server") {
		c.t.Fatalf("proposal GET failed or leaked data: %d %s", response.Code, response.Body.String())
	}
	response = c.request(http.MethodPost, c.path+"/verify", c.token)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"matched_when_checked"`) {
		c.t.Fatalf("proposal verify failed: %d %s", response.Code, response.Body.String())
	}
	if response := c.request(http.MethodGet, c.path, strings.Repeat("x", 32)); response.Code != http.StatusUnauthorized {
		c.t.Fatalf("wrong credential accepted: %d", response.Code)
	}
	if response := c.request(http.MethodPost, c.path+"/apply", c.token, c.ack); response.Code != http.StatusUnauthorized {
		c.t.Fatalf("proposal token applied creation: %d", response.Code)
	}
	for _, incomplete := range []string{
		`{"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":true}`,
	} {
		if response := c.request(http.MethodPost, c.path+"/apply", c.writeToken, incomplete); response.Code != http.StatusBadRequest {
			c.t.Fatalf("creation applied without all acknowledgements: %d", response.Code)
		}
	}
	response = c.request(http.MethodPost, c.path+"/apply", c.writeToken, c.ack)
	var applied mutationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &applied); err != nil || response.Code != http.StatusOK ||
		applied.Status != "written" || applied.Durability != "confirmed" || applied.BackupID == "" {
		c.t.Fatalf("creation apply = %d %s %v", response.Code, response.Body.String(), err)
	}
	return applied
}

// catalogFlow exercises the catalog snapshot flow: backend setup, auth,
// invalid requests, unknown backend, success observations, unavailable
// transitions, and colliding backend IDs.
func (c *creationTest) catalogFlow() {
	c.t.Helper()
	var catalogBody atomic.Value
	catalogBody.Store(`{"data":[{"id":"new"}]}`)
	var unavailable, unexpectedRequest atomic.Bool
	backend := httptest.NewServer(catalogBackendHandler(c.backendToken, &unavailable, &catalogBody, &unexpectedRequest))
	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		c.t.Fatal(err)
	}
	newCatalogHandler := func() *Handler {
		return &Handler{
			catalog: catalog.New([]catalog.Backend{{ID: "selected", URL: backendURL, Prefix: "local/", Default: true, APIKey: c.backendToken}},
				backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger()),
			catalogTimeout: time.Second, logger: discardLogger(),
		}
	}
	c.state.handler = newCatalogHandler()
	for _, invalid := range []string{c.previewToken, c.writeToken, c.ingestionToken, ""} {
		c.assertUnauthorized(http.MethodPost, c.catalogPath, invalid, c.catalogRequest, "wrong credential checked catalog")
	}
	for _, invalid := range []string{
		`{}`, `{"backend_id":"selected","BACKEND_ID":"selected"}`,
		`{"backend_id":"selected","model_id":"other"}`,
	} {
		c.assertBadRequest(http.MethodPost, c.catalogPath, c.token, invalid, "invalid catalog request")
	}
	c.assertNotFound(http.MethodPost, c.catalogPath, c.token, `{"backend_id":"missing"}`, "unknown backend")
	response := c.request(http.MethodPost, c.catalogPath, c.token, c.catalogRequest)
	assertCatalogFields(c.t, response, []string{
		`"public_model_id":"local/new"`, `"catalog":"listed_when_checked"`,
		`"upstream_freshness":"unknown"`, `"selected_backend_relation":"unknown"`,
		`"loadability":"unknown"`, `"loaded_state":"unknown"`, `"effective_settings":"unknown"`,
	}, "catalog check")
	if strings.Contains(response.Body.String(), c.backendToken) || unexpectedRequest.Load() {
		c.t.Fatal("catalog check disclosed credentials or made an unexpected backend request")
	}
	unavailable.Store(true)
	response = c.request(http.MethodPost, c.catalogPath, c.token, c.catalogRequest)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"catalog":"listed_when_checked"`) ||
		!strings.Contains(response.Body.String(), `"upstream_freshness":"unknown"`) {
		c.t.Fatalf("stale catalog observation = %d %s", response.Code, response.Body.String())
	}
	c.state.handler = newCatalogHandler()
	response = c.request(http.MethodPost, c.catalogPath, c.token, c.catalogRequest)
	if response.Code != http.StatusBadGateway {
		c.t.Fatalf("unavailable catalog reported as absent: %d %s", response.Code, response.Body.String())
	}
	unavailable.Store(false)
	catalogBody.Store(`{"data":[]}`)
	c.state.handler = newCatalogHandler()
	response = c.request(http.MethodPost, c.catalogPath, c.token, c.catalogRequest)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"catalog":"not_listed_when_checked"`) {
		c.t.Fatalf("missing catalog entry = %d %s", response.Code, response.Body.String())
	}
	other := httptest.NewServer(collidingBackendHandler(&unexpectedRequest))
	defer other.Close()
	otherURL, err := url.Parse(other.URL)
	if err != nil {
		c.t.Fatal(err)
	}
	c.state.handler = &Handler{
		catalog: catalog.New([]catalog.Backend{
			{ID: "default", URL: otherURL, Default: true},
			{ID: "selected", URL: backendURL, Prefix: "local/", APIKey: c.backendToken},
		}, backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger()),
		catalogTimeout: time.Second, logger: discardLogger(),
	}
	response = c.request(http.MethodPost, c.catalogPath, c.token, c.catalogRequest)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"catalog":"listed_when_checked"`) ||
		!strings.Contains(response.Body.String(), `"selected_backend_relation":"unknown"`) || unexpectedRequest.Load() {
		c.t.Fatalf("colliding catalog ID attributed to selected backend: %d %s", response.Code, response.Body.String())
	}
}

// rollbackAndTargetRestore applies the backup rollback and asserts the target
// file is restored.
func (c *creationTest) rollbackAndTargetRestore() {
	c.t.Helper()
	response := c.request(http.MethodPost, "/management/v1/backups/"+c.applied.BackupID+"/rollback", c.writeToken, c.ack)
	restored, err := os.ReadFile(filepath.Join(c.fx.targetDir, c.fx.target.Filename))
	if response.Code != http.StatusOK || err != nil || string(restored) != c.fx.source {
		c.t.Fatalf("creation rollback failed: %d %s %v", response.Code, response.Body.String(), err)
	}
}

// changedConfigFlow modifies the target config, asserts the changed config is
// rejected, verifies stale, asserts stale verify, and restores the config.
func (c *creationTest) changedConfigFlow() {
	c.t.Helper()
	if err := os.WriteFile(filepath.Join(c.fx.targetDir, c.fx.target.Filename), []byte(c.fx.source+"# changed\n"), 0o600); err != nil {
		c.t.Fatal(err)
	}
	if response := c.request(http.MethodPost, c.createPath, c.token, c.createBody); response.Code != http.StatusConflict {
		c.t.Fatalf("changed config accepted for proposal: %d %s", response.Code, response.Body.String())
	}
	if _, err := verifyRecordedCreation(c.fx.manager, c.fx.models, c.fx.proposals, c.fx.target, "local", c.proposal.ID); !errors.Is(err, modelconfig.ErrProposalStale) {
		c.t.Fatalf("changed config accepted: %v", err)
	}
	if response := c.request(http.MethodPost, c.path+"/verify", c.token); response.Code != http.StatusConflict {
		c.t.Fatalf("stale proposal verify = %d", response.Code)
	}
	if err := os.WriteFile(filepath.Join(c.fx.targetDir, c.fx.target.Filename), []byte(c.fx.source), 0o600); err != nil {
		c.t.Fatal(err)
	}
}

// changedArtifactFlow replaces the artifact, asserts the artifact change is
// rejected, asserts a changed-artifact create is rejected, asserts applying a
// changed artifact is rejected without creating a backup, and asserts the
// target is unchanged.
func (c *creationTest) changedArtifactFlow() {
	c.t.Helper()
	replacement := filepath.Join(c.fx.models, "replacement")
	if err := os.WriteFile(replacement, []byte(strings.Repeat("x", len(c.fx.data))), 0o600); err != nil {
		c.t.Fatal(err)
	}
	if err := os.Rename(replacement, filepath.Join(c.fx.models, "model.gguf")); err != nil {
		c.t.Fatal(err)
	}
	if _, err := verifyRecordedCreation(c.fx.manager, c.fx.models, c.fx.proposals, c.fx.target, "local", c.proposal.ID); !errors.Is(err, modelconfig.ErrProposalArtifact) {
		c.t.Fatalf("changed artifact accepted: %v", err)
	}
	if response := c.request(http.MethodPost, c.createPath, c.token, c.createBody); response.Code != http.StatusConflict {
		c.t.Fatalf("changed artifact accepted for proposal: %d %s", response.Code, response.Body.String())
	}
	beforeBackups, err := os.ReadDir(c.fx.backups)
	if err != nil {
		c.t.Fatal(err)
	}
	if response := c.request(http.MethodPost, c.path+"/apply", c.writeToken, c.ack); response.Code != http.StatusConflict {
		c.t.Fatalf("changed artifact applied: %d", response.Code)
	}
	afterBackups, err := os.ReadDir(c.fx.backups)
	if err != nil || len(beforeBackups) != len(afterBackups) {
		c.t.Fatalf("changed artifact created a backup: %v", err)
	}
	unchanged, err := os.ReadFile(filepath.Join(c.fx.targetDir, c.fx.target.Filename))
	if err != nil || string(unchanged) != c.fx.source {
		c.t.Fatalf("changed artifact modified target: %v", err)
	}
}

// callerModelPathAndMissingArtifact asserts a caller-selected model path is
// rejected and a missing artifact reports ErrArtifactUnavailable.
func (c *creationTest) callerModelPathAndMissingArtifact() {
	c.t.Helper()
	input := c.fx.input
	input.ModelPath = "/different/path.gguf"
	previewReq := c.fx.previewReq
	previewReq.input = input
	if _, err := previewRecordedCreation(previewReq); err == nil {
		c.t.Fatal("accepted a caller-selected model path")
	}
	input.ModelPath = ""
	if err := os.Remove(filepath.Join(c.fx.models, "model.gguf")); err != nil {
		c.t.Fatal(err)
	}
	previewReq.input = input
	if _, err := previewRecordedCreation(previewReq); !errors.Is(err, ingestion.ErrArtifactUnavailable) {
		c.t.Fatalf("missing artifact accepted: %v", err)
	}
}

// TestPreviewRecordedCreationBindsRecordAndConfiguredPath exercises the full
// creation lifecycle: preview, store, proposal, catalog, rollback, and
// artifact/config change guards.
func TestPreviewRecordedCreationBindsRecordAndConfiguredPath(t *testing.T) {
	fx := newCreationFixture(t)
	defer fx.manager.Stop()

	c := &creationTest{t: t, fx: fx}
	c.preview = previewAndEvidence(t, fx)
	c.proposal = storePhases(t, fx, c.preview)
	c.reloader, c.state, c.request = newReloaderAndRequest(t, fx, c.proposal)

	c.previewPath, c.createPath = "/management/v1/creation/preview", "/management/v1/proposals/creation"
	c.body = fmt.Sprintf(`{"target_id":"local","model_id":"new","artifact_record_id":%q,"executable":"llama-server","settings":{"context_size":4096}}`, fx.record.Job.ID)
	c.token, c.writeToken, c.previewToken, c.ingestionToken = strings.Repeat("p", 32), strings.Repeat("w", 32), strings.Repeat("v", 32), strings.Repeat("i", 32)
	c.ack = `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`

	c.previewAuthFlow()
	c.preview = c.previewSuccess()
	c.createBody = strings.TrimSuffix(c.body, "}") + fmt.Sprintf(`,"revision":%q,"artifact_revision":%q,"review_revision":%q}`,
		c.preview.Preview.Revision, c.preview.Artifact.Revision, c.preview.ReviewRevision)

	c.createValidation()
	c.proposal = c.createWithChangedTarget()
	c.templateFlow()

	c.path = "/management/v1/proposals/" + c.proposal.ID

	c.applied = c.proposalAndApplyFlow()

	c.catalogPath = c.path + "/catalog"
	c.catalogRequest = `{"backend_id":"selected"}`
	c.backendToken = strings.Repeat("b", 32)

	c.catalogFlow()

	c.rollbackAndTargetRestore()
	c.changedConfigFlow()
	c.changedArtifactFlow()
	c.callerModelPathAndMissingArtifact()
}

// TestCreationCatalogPreFetchStateChangedReturns503 verifies the refactor
// regression: when the management configuration changed while the request was
// in flight, the pre-fetch state-change check must return 503 with the exact
// error body instead of an empty default 200, and the catalog fetch must not
// be invoked.
func TestCreationCatalogPreFetchStateChangedReturns503(t *testing.T) {
	fx := newCreationFixture(t)
	defer fx.manager.Stop()

	got, err := previewRecordedCreation(fx.previewReq)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := storeRecordedCreation(storeRecordedCreationRequest{
		previewRequest:           fx.previewReq,
		store:                    fx.proposals,
		expectedRevision:         got.Preview.Revision,
		expectedArtifactRevision: got.Artifact.Revision,
		expectedReviewRevision:   got.ReviewRevision,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Catalog backend that records whether it was contacted. The pre-fetch
	// state-change check must short-circuit before fetchCatalogSnapshot, so
	// modelCatalog (and thus this backend) must never be called.
	var backendCalled atomic.Bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		backendCalled.Store(true)
		http.Error(w, "unexpected", http.StatusBadRequest)
	}))
	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	catalogService := catalog.New([]catalog.Backend{{ID: "selected", URL: backendURL, Prefix: "local/", Default: true, APIKey: "token"}},
		backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger())
	handler := &Handler{
		catalog:        catalogService,
		catalogTimeout: time.Second,
		logger:         discardLogger(),
	}
	mgmt := &config.ManagementConfig{
		ProposalStore: &config.ProposalStoreConfig{Directory: fx.proposals, Token: strings.Repeat("p", 32)},
		BackupStore:   &config.BackupStoreConfig{Directory: fx.backups, MaxBackups: 4},
		Targets:       map[string]config.ManagementTarget{"local": fx.target},
	}
	// stateCurrent is the live reloader state; stateOld is a stale snapshot
	// passed to the request. They are distinct pointers so stateChanged reports
	// a change even though their contents are identical.
	stateCurrent := &reloaderState{handler: handler, management: mgmt}
	stateOld := &reloaderState{handler: handler, management: mgmt}
	reloader := &Reloader{logger: discardLogger()}
	reloader.state.Store(stateCurrent)

	req := httptest.NewRequest(http.MethodPost, "/management/v1/proposals/"+proposal.ID+"/catalog",
		strings.NewReader(`{"backend_id":"selected"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	reloader.serveCreationCatalog(w, req, stateOld, fx.proposals, proposal.ID)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), managementConfigChangedMsg) {
		t.Fatalf("expected %q body, got %s", managementConfigChangedMsg, w.Body.String())
	}
	if backendCalled.Load() {
		t.Fatal("catalog fetch was called despite state change")
	}
}
