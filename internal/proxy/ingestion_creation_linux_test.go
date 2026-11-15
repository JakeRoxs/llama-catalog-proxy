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

func TestPreviewRecordedCreationBindsRecordAndConfiguredPath(t *testing.T) {
	models, records, targetDir, proposals, backups := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
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
	defer manager.Stop()
	ctx := int32(4096)
	input := modelconfig.CreationCommandInput{Executable: "llama-server", Settings: modelconfig.CommandSettings{ContextSize: &ctx}}
	got, err := previewRecordedCreation(manager, models, target, "local", "new", "", record.Job.ID, input)
	if err != nil {
		t.Fatal(err)
	}
	if got.Preview.Artifact != "regular_file_when_checked" || got.Artifact.RecordID != record.Job.ID ||
		got.Artifact.RecordMatch != "unknown" || got.Artifact.Integrity != "not_verified" || got.Artifact.BackendAccess != "unknown" {
		t.Fatalf("unexpected preview evidence: %#v", got)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{models, targetDir, "confidential", "llama-server"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("preview disclosed private data: %s", encoded)
		}
	}
	unchanged, err := os.ReadFile(filepath.Join(targetDir, target.Filename))
	if err != nil || string(unchanged) != source {
		t.Fatalf("preview modified target: %v", err)
	}
	if _, err := storeRecordedCreation(manager, models, proposals, target, "local", "new", "", record.Job.ID,
		strings.Repeat("a", 64), got.Artifact.Revision, got.ReviewRevision, input); !errors.Is(err, modelconfig.ErrProposalStale) {
		t.Fatalf("stale review accepted: %v", err)
	}
	proposal, err := storeRecordedCreation(manager, models, proposals, target, "local", "new", "", record.Job.ID,
		got.Preview.Revision, got.Artifact.Revision, got.ReviewRevision, input)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyRecordedCreation(manager, models, proposals, target, "local", proposal.ID)
	if err != nil || verified.Preview.ModelID != "new" || verified.Artifact.Revision != got.Artifact.Revision {
		t.Fatalf("proposal verification failed: %#v %v", verified, err)
	}
	token, writeToken, previewToken, ingestionToken := strings.Repeat("p", 32), strings.Repeat("w", 32), strings.Repeat("v", 32), strings.Repeat("i", 32)
	ingestionConfig := config.IngestionConfig{Directory: models, RecordDirectory: records}
	state := &reloaderState{management: &config.ManagementConfig{
		ProposalStore: &config.ProposalStoreConfig{Directory: proposals, Token: token},
		BackupStore:   &config.BackupStoreConfig{Directory: backups, MaxBackups: 4},
		WriteToken:    writeToken,
		PreviewToken:  previewToken,
		Ingestion:     &ingestionConfig,
		Targets:       map[string]config.ManagementTarget{"local": target},
	}}
	reloader := &Reloader{
		logger: discardLogger(), managementSlots: make(chan struct{}, 2),
		ingestion: &ingestionRuntime{config: ingestionConfig, jobs: manager},
	}
	reloader.state.Store(state)
	request := func(method, path, credential string, bodies ...string) *httptest.ResponseRecorder {
		t.Helper()
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
	previewPath, createPath := "/management/v1/creation/preview", "/management/v1/proposals/creation"
	body := fmt.Sprintf(`{"target_id":"local","model_id":"new","artifact_record_id":%q,"executable":"llama-server","settings":{"context_size":4096}}`, record.Job.ID)
	for _, invalid := range []string{token, writeToken, ingestionToken, ""} {
		if response := request(http.MethodPost, previewPath, invalid, body); response.Code != http.StatusUnauthorized {
			t.Fatalf("wrong preview credential accepted: %d", response.Code)
		}
	}
	for _, invalid := range []string{
		strings.Replace(body, `"target_id":"local"`, `"target_id":"local","TARGET_ID":"local"`, 1),
		strings.TrimSuffix(body, "}") + `,"model_path":"/private/model.gguf"}`,
		strings.TrimSuffix(body, "}") + `,"unknown":true}`,
	} {
		if response := request(http.MethodPost, previewPath, previewToken, invalid); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid preview accepted: %d %s", response.Code, response.Body.String())
		}
	}
	for _, endpoint := range []struct{ path, credential, payload string }{
		{previewPath, previewToken, body},
		{createPath, token, body},
	} {
		req := httptest.NewRequest(http.MethodPost, endpoint.path, strings.NewReader(endpoint.payload))
		req.TLS = &tls.ConnectionState{}
		req.Header.Set("Authorization", "Bearer "+endpoint.credential)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Origin", "https://other.invalid")
		response := httptest.NewRecorder()
		reloader.ManagementHandler().ServeHTTP(response, req)
		if response.Code != http.StatusForbidden {
			t.Fatalf("cross-origin creation request accepted: %d", response.Code)
		}
	}
	response := request(http.MethodPost, previewPath, previewToken, body)
	var preview recordedCreationPreview
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil || response.Code != http.StatusOK ||
		preview.Preview.TargetFingerprint == "" || preview.Preview.CandidateRevision == "" || preview.ReviewRevision == "" {
		t.Fatalf("creation preview = %d %s %v", response.Code, response.Body.String(), err)
	}
	for _, private := range []string{models, targetDir, "confidential", "llama-server"} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("creation preview leaked private data: %s", response.Body.String())
		}
	}
	createBody := strings.TrimSuffix(body, "}") + fmt.Sprintf(`,"revision":%q,"artifact_revision":%q,"review_revision":%q}`,
		preview.Preview.Revision, preview.Artifact.Revision, preview.ReviewRevision)
	for _, invalid := range []string{previewToken, writeToken, ingestionToken, ""} {
		if response := request(http.MethodPost, createPath, invalid, createBody); response.Code != http.StatusUnauthorized {
			t.Fatalf("wrong proposal credential accepted: %d", response.Code)
		}
	}
	for _, stale := range []string{
		strings.Replace(createBody, preview.Preview.Revision, strings.Repeat("a", 64), 1),
		strings.Replace(createBody, preview.Artifact.Revision, strings.Repeat("b", 64), 1),
		strings.Replace(createBody, preview.ReviewRevision, strings.Repeat("c", 64), 1),
		strings.Replace(createBody, "4096", "8192", 1),
	} {
		if response := request(http.MethodPost, createPath, token, stale); response.Code != http.StatusConflict {
			t.Fatalf("stale creation review accepted: %d %s", response.Code, response.Body.String())
		}
	}
	if response := request(http.MethodPost, createPath, token, strings.Replace(createBody, preview.ReviewRevision, "invalid", 1)); response.Code != http.StatusBadRequest {
		t.Fatalf("invalid review digest accepted: %d", response.Code)
	}
	otherTarget := t.TempDir()
	if err := os.WriteFile(filepath.Join(otherTarget, target.Filename), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	state.management.Targets["local"] = config.ManagementTarget{Directory: otherTarget, Filename: target.Filename}
	if response := request(http.MethodPost, createPath, token, createBody); response.Code != http.StatusConflict {
		t.Fatalf("changed target binding accepted: %d %s", response.Code, response.Body.String())
	}
	state.management.Targets["local"] = target
	response = request(http.MethodPost, createPath, token, createBody)
	if err := json.Unmarshal(response.Body.Bytes(), &proposal); err != nil || response.Code != http.StatusCreated ||
		proposal.CandidateRevision != preview.Preview.CandidateRevision {
		t.Fatalf("create proposal = %d %s %v", response.Code, response.Body.String(), err)
	}
	for _, private := range []string{models, targetDir, "confidential", "llama-server"} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("creation proposal leaked private data: %s", response.Body.String())
		}
	}
	templateBody := fmt.Sprintf(`{"target_id":"local","model_id":"template-new","template_id":"old","artifact_record_id":%q}`, record.Job.ID)
	response = request(http.MethodPost, previewPath, previewToken, templateBody)
	var templatePreview recordedCreationPreview
	if err := json.Unmarshal(response.Body.Bytes(), &templatePreview); err != nil || response.Code != http.StatusOK {
		t.Fatalf("template preview = %d %s %v", response.Code, response.Body.String(), err)
	}
	templateCreate := strings.TrimSuffix(templateBody, "}") + fmt.Sprintf(`,"revision":%q,"artifact_revision":%q,"review_revision":%q}`,
		templatePreview.Preview.Revision, templatePreview.Artifact.Revision, templatePreview.ReviewRevision)
	response = request(http.MethodPost, createPath, token, templateCreate)
	if response.Code != http.StatusCreated || !strings.Contains(response.Body.String(), `"template_id":"old"`) {
		t.Fatalf("template proposal = %d %s", response.Code, response.Body.String())
	}
	path := "/management/v1/proposals/" + proposal.ID
	response = request(http.MethodGet, path, token)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"kind":"create_model"`) ||
		strings.Contains(response.Body.String(), models) || strings.Contains(response.Body.String(), "llama-server") {
		t.Fatalf("proposal GET failed or leaked data: %d %s", response.Code, response.Body.String())
	}
	response = request(http.MethodPost, path+"/verify", token)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"status":"matched_when_checked"`) {
		t.Fatalf("proposal verify failed: %d %s", response.Code, response.Body.String())
	}
	if response := request(http.MethodGet, path, strings.Repeat("x", 32)); response.Code != http.StatusUnauthorized {
		t.Fatalf("wrong credential accepted: %d", response.Code)
	}
	ack := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	if response := request(http.MethodPost, path+"/apply", token, ack); response.Code != http.StatusUnauthorized {
		t.Fatalf("proposal token applied creation: %d", response.Code)
	}
	for _, incomplete := range []string{
		`{"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_exclusive_window":true}`,
		`{"acknowledge_review":true,"acknowledge_disruption":true}`,
	} {
		if response := request(http.MethodPost, path+"/apply", writeToken, incomplete); response.Code != http.StatusBadRequest {
			t.Fatalf("creation applied without all acknowledgements: %d", response.Code)
		}
	}
	response = request(http.MethodPost, path+"/apply", writeToken, ack)
	var applied mutationResponse
	if err := json.Unmarshal(response.Body.Bytes(), &applied); err != nil || response.Code != http.StatusOK ||
		applied.Status != "written" || applied.Durability != "confirmed" || applied.BackupID == "" {
		t.Fatalf("creation apply = %d %s %v", response.Code, response.Body.String(), err)
	}
	var catalogBody atomic.Value
	catalogBody.Store(`{"data":[{"id":"new"}]}`)
	var unavailable, unexpectedRequest atomic.Bool
	backendToken := strings.Repeat("b", 32)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
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
	}))
	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	newCatalogHandler := func() *Handler {
		return &Handler{
			catalog: catalog.New([]catalog.Backend{{ID: "selected", URL: backendURL, Prefix: "local/", Default: true, APIKey: backendToken}},
				backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger()),
			catalogTimeout: time.Second, logger: discardLogger(),
		}
	}
	state.handler = newCatalogHandler()
	catalogPath := path + "/catalog"
	catalogRequest := `{"backend_id":"selected"}`
	for _, invalid := range []string{previewToken, writeToken, ingestionToken, ""} {
		if response := request(http.MethodPost, catalogPath, invalid, catalogRequest); response.Code != http.StatusUnauthorized {
			t.Fatalf("wrong credential checked catalog: %d", response.Code)
		}
	}
	for _, invalid := range []string{
		`{}`, `{"backend_id":"selected","BACKEND_ID":"selected"}`,
		`{"backend_id":"selected","model_id":"other"}`,
	} {
		if response := request(http.MethodPost, catalogPath, token, invalid); response.Code != http.StatusBadRequest {
			t.Fatalf("invalid catalog request accepted: %d", response.Code)
		}
	}
	if response := request(http.MethodPost, catalogPath, token, `{"backend_id":"missing"}`); response.Code != http.StatusNotFound {
		t.Fatalf("unknown backend accepted: %d", response.Code)
	}
	response = request(http.MethodPost, catalogPath, token, catalogRequest)
	for _, expected := range []string{
		`"public_model_id":"local/new"`, `"catalog":"listed_when_checked"`,
		`"upstream_freshness":"unknown"`, `"selected_backend_relation":"unknown"`,
		`"loadability":"unknown"`, `"loaded_state":"unknown"`, `"effective_settings":"unknown"`,
	} {
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), expected) {
			t.Fatalf("catalog check = %d %s, expected %s", response.Code, response.Body.String(), expected)
		}
	}
	if strings.Contains(response.Body.String(), backendToken) || unexpectedRequest.Load() {
		t.Fatal("catalog check disclosed credentials or made an unexpected backend request")
	}
	unavailable.Store(true)
	response = request(http.MethodPost, catalogPath, token, catalogRequest)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"catalog":"listed_when_checked"`) ||
		!strings.Contains(response.Body.String(), `"upstream_freshness":"unknown"`) {
		t.Fatalf("stale catalog observation = %d %s", response.Code, response.Body.String())
	}
	state.handler = newCatalogHandler()
	response = request(http.MethodPost, catalogPath, token, catalogRequest)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("unavailable catalog reported as absent: %d %s", response.Code, response.Body.String())
	}
	unavailable.Store(false)
	catalogBody.Store(`{"data":[]}`)
	state.handler = newCatalogHandler()
	response = request(http.MethodPost, catalogPath, token, catalogRequest)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"catalog":"not_listed_when_checked"`) {
		t.Fatalf("missing catalog entry = %d %s", response.Code, response.Body.String())
	}
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/models" {
			unexpectedRequest.Store(true)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"local/new"}]}`))
	}))
	defer other.Close()
	otherURL, err := url.Parse(other.URL)
	if err != nil {
		t.Fatal(err)
	}
	state.handler = &Handler{
		catalog: catalog.New([]catalog.Backend{
			{ID: "default", URL: otherURL, Default: true},
			{ID: "selected", URL: backendURL, Prefix: "local/", APIKey: backendToken},
		}, backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger()),
		catalogTimeout: time.Second, logger: discardLogger(),
	}
	response = request(http.MethodPost, catalogPath, token, catalogRequest)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"catalog":"listed_when_checked"`) ||
		!strings.Contains(response.Body.String(), `"selected_backend_relation":"unknown"`) || unexpectedRequest.Load() {
		t.Fatalf("colliding catalog ID attributed to selected backend: %d %s", response.Code, response.Body.String())
	}
	written, err := os.ReadFile(filepath.Join(targetDir, target.Filename))
	if err != nil || !strings.Contains(string(written), `"new":`) || string(written) == source {
		t.Fatalf("creation entry not written: %v", err)
	}
	response = request(http.MethodPost, "/management/v1/backups/"+applied.BackupID+"/rollback", writeToken, ack)
	restored, err := os.ReadFile(filepath.Join(targetDir, target.Filename))
	if response.Code != http.StatusOK || err != nil || string(restored) != source {
		t.Fatalf("creation rollback failed: %d %s %v", response.Code, response.Body.String(), err)
	}
	if err := os.WriteFile(filepath.Join(targetDir, target.Filename), []byte(source+"# changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if response := request(http.MethodPost, createPath, token, createBody); response.Code != http.StatusConflict {
		t.Fatalf("changed config accepted for proposal: %d %s", response.Code, response.Body.String())
	}
	if _, err := verifyRecordedCreation(manager, models, proposals, target, "local", proposal.ID); !errors.Is(err, modelconfig.ErrProposalStale) {
		t.Fatalf("changed config accepted: %v", err)
	}
	if response := request(http.MethodPost, path+"/verify", token); response.Code != http.StatusConflict {
		t.Fatalf("stale proposal verify = %d", response.Code)
	}
	if err := os.WriteFile(filepath.Join(targetDir, target.Filename), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	replacement := filepath.Join(models, "replacement")
	if err := os.WriteFile(replacement, []byte(strings.Repeat("x", len(data))), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, filepath.Join(models, "model.gguf")); err != nil {
		t.Fatal(err)
	}
	if _, err := verifyRecordedCreation(manager, models, proposals, target, "local", proposal.ID); !errors.Is(err, modelconfig.ErrProposalArtifact) {
		t.Fatalf("changed artifact accepted: %v", err)
	}
	if response := request(http.MethodPost, createPath, token, createBody); response.Code != http.StatusConflict {
		t.Fatalf("changed artifact accepted for proposal: %d %s", response.Code, response.Body.String())
	}
	beforeBackups, err := os.ReadDir(backups)
	if err != nil {
		t.Fatal(err)
	}
	if response := request(http.MethodPost, path+"/apply", writeToken, ack); response.Code != http.StatusConflict {
		t.Fatalf("changed artifact applied: %d %s", response.Code, response.Body.String())
	}
	afterBackups, err := os.ReadDir(backups)
	if err != nil || len(beforeBackups) != len(afterBackups) {
		t.Fatalf("changed artifact created a backup: %v", err)
	}
	unchanged, err = os.ReadFile(filepath.Join(targetDir, target.Filename))
	if err != nil || string(unchanged) != source {
		t.Fatalf("changed artifact modified target: %v", err)
	}
	input.ModelPath = "/different/path.gguf"
	if _, err := previewRecordedCreation(manager, models, target, "local", "new", "", record.Job.ID, input); err == nil {
		t.Fatal("accepted a caller-selected model path")
	}
	input.ModelPath = ""
	if err := os.Remove(filepath.Join(models, "model.gguf")); err != nil {
		t.Fatal(err)
	}
	if _, err := previewRecordedCreation(manager, models, target, "local", "new", "", record.Job.ID, input); !errors.Is(err, ingestion.ErrArtifactUnavailable) {
		t.Fatalf("missing artifact accepted: %v", err)
	}
}
