//go:build linux

package proxy

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

// ingestionLinuxTestHarness holds shared state for the ingestion management API test.
type ingestionLinuxTestHarness struct {
	t           *testing.T
	models      string
	targets     string
	records     string
	certPath    string
	keyPath     string
	preview     string
	token       string
	replacement string
	hub         string
	path        string
	r           *Reloader
	cfg         config.Config
	server      *httptest.Server
	client      *http.Client
}

// newIngestionLinuxTestHarness creates a fresh harness with temp dirs, a TLS
// management server, and a request helper that asserts no secrets leak.
func newIngestionLinuxTestHarness(t *testing.T) *ingestionLinuxTestHarness {
	t.Helper()
	models, targets, records := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Chmod(records, 0o700); err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, _ := peerTestCertificate(t)
	preview, token, replacement, hub := strings.Repeat("m", 32), strings.Repeat("i", 32), strings.Repeat("n", 32), "fixture-hub-token"
	path := configPath(t)
	harness := &ingestionLinuxTestHarness{
		t:           t,
		models:      models,
		targets:     targets,
		records:     records,
		certPath:    certPath,
		keyPath:     keyPath,
		preview:     preview,
		token:       token,
		replacement: replacement,
		hub:         hub,
		path:        path,
	}
	harness.writeConfig(token, targets)
	r, cfg, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	harness.r = r
	harness.cfg = cfg
	cfg.Management.Ingestion.Token = replacement
	recorder := httptest.NewRecorder()
	plaintext := httptest.NewRequest(http.MethodGet, "http://localhost/management/v1/ingestion/jobs/current", nil)
	plaintext.Header.Set("Authorization", "Bearer "+token)
	r.ManagementHandler().ServeHTTP(recorder, plaintext)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("plaintext management access = %d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	r.ServeHTTP(recorder, plaintext)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("management path on inference listener = %d", recorder.Code)
	}
	server := httptest.NewTLSServer(r.ManagementHandler())
	harness.server = server
	harness.client = server.Client()
	return harness
}

// writeConfig writes the ingestion management config for the given token and target.
func (h *ingestionLinuxTestHarness) writeConfig(ingestionToken, target string) {
	h.t.Helper()
	content := fmt.Sprintf(`
default_backend: local
backends: {local: {url: http://localhost:9292}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  ingestion:
    directory: %q
    record_directory: %q
    max_records: 10
    token: %q
    huggingface_token: %q
    max_bytes: 4096
    max_directory_bytes: 20
    min_free_bytes: 1
    timeout: 1m
    file_mode: "0600"
  targets:
    local: {directory: %q, filename: config.yaml}
`, h.certPath, h.keyPath, h.preview, h.models, h.records, ingestionToken, h.hub, target)
	if err := os.WriteFile(h.path, []byte(content), 0o600); err != nil {
		h.t.Fatal(err)
	}
}

// request performs an authenticated request against the management TLS server and
// asserts the response has no-store cache control and exposes no secrets.
func (h *ingestionLinuxTestHarness) request(method, path, credential, body, origin string) (int, string) {
	h.t.Helper()
	req, err := http.NewRequest(method, h.server.URL+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	if credential != "" {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	response, err := h.client.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		h.t.Fatal("missing no-store header")
	}
	for _, secret := range []string{h.token, h.replacement, h.preview, h.hub, h.models, h.records, h.targets} {
		if strings.Contains(string(data), secret) {
			h.t.Fatal("response exposed a credential or local path")
		}
	}
	return response.StatusCode, string(data)
}

// waitJob polls the job specified by body until it is no longer running or
// canceling, then returns its status.
func (h *ingestionLinuxTestHarness) waitJob(body string) ingestion.JobStatus {
	h.t.Helper()
	var started ingestion.JobStatus
	if err := json.Unmarshal([]byte(body), &started); err != nil || started.ID == "" {
		h.t.Fatalf("invalid started job: %s %v", body, err)
	}
	end := time.Now().Add(3 * time.Second)
	for {
		code, body := h.request(http.MethodGet, "/management/v1/ingestion/jobs/"+started.ID, h.token, "", "")
		var current ingestion.JobStatus
		if err := json.Unmarshal([]byte(body), &current); err != nil || code != http.StatusOK {
			h.t.Fatalf("invalid job status: %d %s %v", code, body, err)
		}
		if current.State != "running" && current.State != "canceling" {
			return current
		}
		if time.Now().After(end) {
			h.t.Fatal("job did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// assertJobStatus checks that a job status matches expected state, publication,
// and error codes.
func (h *ingestionLinuxTestHarness) assertJobStatus(job ingestion.JobStatus, expectedState string, expectedPublication ingestion.PublicationState, expectedErrorCodes []string) {
	h.t.Helper()
	if job.State != expectedState {
		h.t.Fatalf("job state = %s, want %s: %+v", job.State, expectedState, job)
	}
	if job.Result.Publication != expectedPublication {
		h.t.Fatalf("job publication = %s, want %s: %+v", job.Result.Publication, expectedPublication, job)
	}
	if len(job.ErrorCodes) != len(expectedErrorCodes) {
		h.t.Fatalf("job error codes = %v, want %v: %+v", job.ErrorCodes, expectedErrorCodes, job)
	}
	for i, expected := range expectedErrorCodes {
		if job.ErrorCodes[i] != expected {
			h.t.Fatalf("job error code %d = %q, want %q: %+v", i, job.ErrorCodes[i], expected, job)
		}
	}
}

// assertAdoptionResult checks that an adoption job succeeded with local source.
func (h *ingestionLinuxTestHarness) assertAdoptionResult(job ingestion.JobStatus) {
	h.t.Helper()
	if job.State != "succeeded" {
		h.t.Fatalf("adoption state = %s, want succeeded: %+v", job.State, job)
	}
	if job.Operation != "adopt" {
		h.t.Fatalf("adoption operation = %s, want adopt: %+v", job.Operation, job)
	}
	if job.Source.Kind != "local" {
		h.t.Fatalf("adoption source kind = %s, want local: %+v", job.Source.Kind, job)
	}
	if job.Source.Repository != "" {
		h.t.Fatalf("adoption source repository = %q, want empty: %+v", job.Source.Repository, job)
	}
	if job.Source.Revision != "" {
		h.t.Fatalf("adoption source revision = %q, want empty: %+v", job.Source.Revision, job)
	}
	if job.Result.Publication != ingestion.PublicationExisting {
		h.t.Fatalf("adoption publication = %s, want existing: %+v", job.Result.Publication, job)
	}
}

// assertReplacementResult checks that a replacement job failed with artifact_unavailable.
func (h *ingestionLinuxTestHarness) assertReplacementResult(job ingestion.JobStatus) {
	h.t.Helper()
	if job.State != "failed" {
		h.t.Fatalf("replacement state = %s, want failed: %+v", job.State, job)
	}
	if job.Operation != "replace" {
		h.t.Fatalf("replacement operation = %s, want replace: %+v", job.Operation, job)
	}
	if job.Result.Publication != ingestion.PublicationNotPublished {
		h.t.Fatalf("replacement publication = %s, want not_published: %+v", job.Result.Publication, job)
	}
	if len(job.ErrorCodes) != 1 || job.ErrorCodes[0] != "artifact_unavailable" {
		h.t.Fatalf("replacement error codes = %v, want [artifact_unavailable]: %+v", job.ErrorCodes, job)
	}
}

func TestIngestionManagementAPIAndReload(t *testing.T) {
	h := newIngestionLinuxTestHarness(t)
	h.testCredentialPreviewAPI()
	h.testInvalidJobSubmissions()
	jobID := h.testSubmitAndPollJob()
	h.testJobResultAndEndpoints(jobID)
	h.testIngestionRecords(jobID)
	h.testAdoptionInvalid()
	adoptedID := h.testQuotaAndAdoption()
	failedReplacementID := h.testReplacement(adoptedID)
	h.testReload(failedReplacementID)
	h.testInterruptedRecord()
}

func (h *ingestionLinuxTestHarness) testCredentialPreviewAPI() {
	h.t.Helper()
	if status, _ := h.request(http.MethodPost, "/management/v1/settings", h.token, `{"target_id":"local","model_id":"x"}`, ""); status != http.StatusUnauthorized {
		h.t.Fatalf("ingestion credential can access preview API: %d", status)
	}
}

func (h *ingestionLinuxTestHarness) testInvalidJobSubmissions() {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	valid := `{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"reject"}`
	for _, invalid := range []struct {
		name, credential, body, origin string
		status                         int
	}{
		{"no credential", "", valid, "", http.StatusUnauthorized},
		{"preview credential", h.preview, valid, "", http.StatusUnauthorized},
		{"returned config mutated", h.replacement, valid, "", http.StatusUnauthorized},
		{"foreign origin", h.token, valid, "https://evil.example", http.StatusForbidden},
		{"plaintext origin", h.token, valid, strings.Replace(h.server.URL, "https:", "http:", 1), http.StatusForbidden},
		{"no consent", h.token, strings.Replace(valid, `"allow_download":true`, `"allow_download":false`, 1), "", http.StatusBadRequest},
		{"no policy", h.token, strings.Replace(valid, `,"existing_file":"reject"`, "", 1), "", http.StatusBadRequest},
		{"replace unsupported", h.token, strings.Replace(valid, `"reject"`, `"replace"`, 1), "", http.StatusBadRequest},
		{"untrusted URL", h.token, `{"source":{"url":"https://evil.example/model.gguf"},"allow_download":true,"existing_file":"reject"}`, "", http.StatusBadRequest},
		{"duplicate field", h.token, strings.Replace(valid, `"allow_download":true`, `"allow_download":true,"ALLOW_DOWNLOAD":true`, 1), "", http.StatusBadRequest},
		{"unknown field", h.token, strings.Replace(valid, `"existing_file":"reject"`, `"existing_file":"reject","token":"oops"`, 1), "", http.StatusBadRequest},
	} {
		h.t.Run(invalid.name, func(t *testing.T) {
			status, body := h.request(http.MethodPost, jobs, invalid.credential, invalid.body, invalid.origin)
			if status != invalid.status {
				t.Fatalf("status = %d, want %d: %s", status, invalid.status, body)
			}
		})
	}
}

func (h *ingestionLinuxTestHarness) testSubmitAndPollJob() string {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	valid := `{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"reject"}`
	original := []byte("existing")
	if err := os.WriteFile(filepath.Join(h.models, "model.gguf"), original, 0o600); err != nil {
		h.t.Fatal(err)
	}
	status, body := h.request(http.MethodPost, jobs, h.token, valid, h.server.URL)
	if status != http.StatusAccepted {
		h.t.Fatalf("start = %d %s", status, body)
	}
	var job struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &job); err != nil || job.ID == "" {
		h.t.Fatalf("invalid job: %s, %v", body, err)
	}
	_ = h.waitJob(body)
	return job.ID
}

func (h *ingestionLinuxTestHarness) testJobResultAndEndpoints(jobID string) {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	status, body := h.request(http.MethodGet, jobs+"/"+jobID, h.token, "", "")
	if status != http.StatusOK {
		h.t.Fatalf("status = %d %s", status, body)
	}
	if !strings.Contains(body, `"destination_exists"`) || !strings.Contains(body, `"publication":"not_published"`) ||
		!strings.Contains(body, `"repository":"owner/repo"`) {
		h.t.Fatalf("unexpected job result: %s", body)
	}
	contents, err := os.ReadFile(filepath.Join(h.models, "model.gguf"))
	if err != nil || string(contents) != string("existing") {
		h.t.Fatalf("existing file changed: %q, %v", contents, err)
	}
	if status, _ := h.request(http.MethodGet, jobs+"/current", h.token, "", ""); status != http.StatusOK {
		h.t.Fatalf("current job = %d", status)
	}
	if status, _ := h.request(http.MethodGet, jobs+"/current?token=ignored", h.token, "", ""); status != http.StatusForbidden {
		h.t.Fatalf("query-bearing job request = %d", status)
	}
	if status, _ := h.request(http.MethodPost, jobs+"/"+jobID+"/cancel", h.token, "", ""); status != http.StatusConflict {
		h.t.Fatalf("cancel finished job = %d", status)
	}
	if status, _ := h.request(http.MethodGet, jobs+"/missing", h.token, "", ""); status != http.StatusNotFound {
		h.t.Fatalf("missing job = %d", status)
	}
	if status, _ := h.request(http.MethodPost, jobs+"/"+jobID+"/cancel", h.token, "{}", ""); status != http.StatusBadRequest {
		h.t.Fatalf("cancel with body = %d", status)
	}
}

func (h *ingestionLinuxTestHarness) testIngestionRecords(jobID string) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	if status, body := h.request(http.MethodGet, recordsPath, h.token, "", ""); status != http.StatusOK || !strings.Contains(body, jobID) {
		h.t.Fatalf("records = %d %s", status, body)
	}
	if status, body := h.request(http.MethodGet, recordsPath+"/"+jobID, h.token, "", ""); status != http.StatusOK || !strings.Contains(body, `"destination_exists"`) {
		h.t.Fatalf("record = %d %s", status, body)
	}
	if status, _ := h.request(http.MethodPost, recordsPath+"/"+jobID+"/acknowledge", h.token, `{"acknowledge_uncertain_outcome":true}`, ""); status != http.StatusConflict {
		h.t.Fatalf("acknowledged a completed certain outcome: %d", status)
	}
}

func (h *ingestionLinuxTestHarness) testAdoptionInvalid() {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	adoption := `{"source":{"filename":"model.gguf"},"existing_file":"adopt","allow_adoption":true,"acknowledge_unverified_source":true}`
	for _, invalid := range []string{
		strings.Replace(adoption, `"allow_adoption":true`, `"allow_adoption":false`, 1),
		strings.Replace(adoption, `"acknowledge_unverified_source":true`, `"acknowledge_unverified_source":false`, 1),
		strings.Replace(adoption, `"filename":"model.gguf"`, `"filename":"model.gguf","repository":"owner/repo"`, 1),
		strings.Replace(adoption, `"existing_file":"adopt"`, `"existing_file":"adopt","allow_download":true`, 1),
	} {
		if status, _ := h.request(http.MethodPost, jobs, h.token, invalid, ""); status != http.StatusBadRequest {
			h.t.Fatalf("invalid adoption accepted: %d", status)
		}
	}
}

func (h *ingestionLinuxTestHarness) testQuotaAndAdoption() string {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	valid := `{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"reject"}`
	gguf := make([]byte, 24)
	copy(gguf, "GGUF")
	binary.LittleEndian.PutUint32(gguf[4:], 3)
	if err := os.WriteFile(filepath.Join(h.models, "model.gguf"), gguf, 0o600); err != nil {
		h.t.Fatal(err)
	}
	status, body := h.request(http.MethodPost, jobs, h.token, strings.Replace(valid, "model.gguf", "another.gguf", 1), "")
	if status != http.StatusAccepted {
		h.t.Fatalf("quota-limited start = %d %s", status, body)
	}
	quotaJob := h.waitJob(body)
	h.assertJobStatus(quotaJob, "failed", ingestion.PublicationNotPublished, []string{"directory_quota"})

	adoption := `{"source":{"filename":"model.gguf"},"existing_file":"adopt","allow_adoption":true,"acknowledge_unverified_source":true}`
	status, body = h.request(http.MethodPost, jobs, h.token, adoption, "")
	if status != http.StatusAccepted {
		h.t.Fatalf("adoption start = %d %s", status, body)
	}
	adopted := h.waitJob(body)
	h.assertAdoptionResult(adopted)

	if err := os.WriteFile(filepath.Join(h.targets, "config.yaml"), []byte("models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n"), 0o600); err != nil {
		h.t.Fatal(err)
	}
	review, err := previewRecordedCreation(recordedCreationPreviewRequest{
		jobs:           h.r.ingestion.jobs,
		modelDirectory: h.models,
		target:         config.ManagementTarget{Directory: h.targets, Filename: "config.yaml"},
		targetID:       "local",
		modelID:        "new",
		templateID:     "",
		recordID:       adopted.ID,
		input:          modelconfig.CreationCommandInput{Executable: "llama-server"},
	})
	if err != nil || review.Artifact.Source.Kind != "local" || review.Artifact.RecordMatch != "unknown" {
		h.t.Fatalf("adopted artifact cannot be reviewed: %+v %v", review, err)
	}
	return adopted.ID
}

func (h *ingestionLinuxTestHarness) testReplacement(adoptedID string) string {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	replacementBody := `{"source":{"repository":"owner/repo","filename":"bad.gguf","revision":"main"},"allow_download":true,"existing_file":"replace","allow_replacement":true,"acknowledge_exclusive_window":true}`
	for _, invalid := range []string{
		strings.Replace(replacementBody, `"allow_replacement":true`, `"allow_replacement":false`, 1),
		strings.Replace(replacementBody, `"acknowledge_exclusive_window":true`, `"acknowledge_exclusive_window":false`, 1),
	} {
		if status, _ := h.request(http.MethodPost, jobs, h.token, invalid, ""); status != http.StatusBadRequest {
			h.t.Fatalf("replacement without authorization accepted: %d", status)
		}
	}
	if err := os.Symlink(filepath.Join(h.t.TempDir(), "missing"), filepath.Join(h.models, "bad.gguf")); err != nil {
		h.t.Fatal(err)
	}
	status, body := h.request(http.MethodPost, jobs, h.token, replacementBody, "")
	if status != http.StatusAccepted {
		h.t.Fatalf("authorized replacement was not started: %d %s", status, body)
	}
	failedReplacement := h.waitJob(body)
	h.assertReplacementResult(failedReplacement)
	return failedReplacement.ID
}

func (h *ingestionLinuxTestHarness) testReload(failedReplacementID string) {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	h.writeConfig(h.token, h.targets)
	if err := h.r.reload(); err != nil {
		h.t.Fatalf("unchanged ingestion configuration could not reload: %v", err)
	}
	if status, _ := h.request(http.MethodGet, jobs+"/"+failedReplacementID, h.token, "", ""); status != http.StatusOK {
		h.t.Fatalf("ordinary reload lost job: %d", status)
	}
	h.writeConfig(h.replacement, h.targets)
	if err := h.r.reload(); err == nil {
		h.t.Fatal("ingestion credential change accepted without restart")
	}
	if status, _ := h.request(http.MethodGet, jobs+"/current", h.token, "", ""); status != http.StatusOK {
		h.t.Fatalf("failed reload changed active policy: %d", status)
	}
	h.writeConfig(h.token, h.models)
	if err := h.r.reload(); err == nil {
		h.t.Fatal("overlapping target directory accepted")
	}
	h.writeConfig(h.token, h.targets)
}

func (h *ingestionLinuxTestHarness) testInterruptedRecord() {
	h.t.Helper()
	jobs := "/management/v1/ingestion/jobs"
	recordsPath := "/management/v1/ingestion/records"
	valid := `{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"reject"}`
	h.r.Stop()
	interrupted := ingestion.Record{Version: 1, Job: ingestion.JobStatus{
		ID: "ingest-" + strings.Repeat("a", 32), State: "running",
		Source:    ingestion.Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		StartedAt: time.Now().UTC(), Result: ingestion.StageResult{Publication: ingestion.PublicationUnknown, Durability: "unknown"},
	}}
	data, err := json.Marshal(interrupted)
	if err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(h.records, interrupted.Job.ID+".json"), data, 0o600); err != nil {
		h.t.Fatal(err)
	}
	restarted, _, err := NewReloader(h.path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	defer restarted.Stop()
	restartedRequest := func(method, path, body string) *httptest.ResponseRecorder {
		h.t.Helper()
		req := httptest.NewRequest(method, "https://management.example"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+h.token)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		response := httptest.NewRecorder()
		restarted.ManagementHandler().ServeHTTP(response, req)
		return response
	}
	response := restartedRequest(http.MethodGet, recordsPath+"/"+interrupted.Job.ID, "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"state":"interrupted"`) ||
		!strings.Contains(response.Body.String(), `"publication":"unknown"`) {
		h.t.Fatalf("interrupted record = %d %s", response.Code, response.Body.String())
	}
	if response := restartedRequest(http.MethodPost, jobs, valid); response.Code != http.StatusConflict {
		h.t.Fatalf("unreviewed job did not block start: %d %s", response.Code, response.Body.String())
	}
	if response := restartedRequest(http.MethodPost, recordsPath+"/"+interrupted.Job.ID+"/acknowledge", `{"acknowledge_uncertain_outcome":false}`); response.Code != http.StatusBadRequest {
		h.t.Fatalf("implicit acknowledgment accepted: %d", response.Code)
	}
	response = restartedRequest(http.MethodPost, recordsPath+"/"+interrupted.Job.ID+"/acknowledge", `{"acknowledge_uncertain_outcome":true}`)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"acknowledged":true`) ||
		!strings.Contains(response.Body.String(), `"publication":"unknown"`) {
		h.t.Fatalf("acknowledgment = %d %s", response.Code, response.Body.String())
	}
	if response := restartedRequest(http.MethodPost, jobs, valid); response.Code != http.StatusAccepted {
		h.t.Fatalf("start after acknowledgment = %d %s", response.Code, response.Body.String())
	}
}

func TestIngestionRejectsDirectoryAlias(t *testing.T) {
	target, models, records := t.TempDir(), t.TempDir(), t.TempDir()
	alias := filepath.Join(models, "target-alias")
	if err := os.Symlink(target, alias); err != nil {
		t.Fatal(err)
	}
	management := &config.ManagementConfig{
		Ingestion: &config.IngestionConfig{Directory: target, RecordDirectory: records},
		Targets:   map[string]config.ManagementTarget{"local": {Directory: alias}},
	}
	if err := validateIngestionDirectories(management, nil); err == nil {
		t.Fatal("target directory alias accepted")
	}
}

// ingestionRecordsTestHarness holds shared state for the records serving test.
type ingestionRecordsTestHarness struct {
	t          *testing.T
	recordsDir string
	jobManager *ingestion.JobManager
	r          *Reloader
	state      reloaderState
}

func newIngestionRecordsTestHarness(t *testing.T) *ingestionRecordsTestHarness {
	t.Helper()
	recordsDir := t.TempDir()
	if err := os.Chmod(recordsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	jobManager, err := ingestion.NewPersistentJobManager(context.Background(), "token", recordsDir, 10)
	if err != nil {
		t.Fatalf("job manager: %v", err)
	}
	harness := &ingestionRecordsTestHarness{
		t:           t,
		recordsDir:  recordsDir,
		jobManager:  jobManager,
	}
	r := &Reloader{
		logger:    discardLogger(),
		ingestion: &ingestionRuntime{jobs: jobManager},
	}
	r.state.Store(&harness.state)
	harness.r = r
	return harness
}

func (h *ingestionRecordsTestHarness) newRequest(method, path, body string) *http.Request {
	h.t.Helper()
	req := httptest.NewRequest(method, "https://management.example"+path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestServeIngestionRecordsRefactor(t *testing.T) {
	h := newIngestionRecordsTestHarness(t)
	h.t.Run("list empty records", h.testListEmptyRecords)
	h.t.Run("read unknown record is 404", h.testReadUnknownRecord)
	h.t.Run("acknowledge unknown record is 404", h.testAcknowledgeUnknownRecord)
	h.t.Run("acknowledge requires explicit true", h.testAcknowledgeRequiresExplicitTrue)
	h.t.Run("acknowledge invalid JSON is 400", h.testAcknowledgeInvalidJSON)
	h.t.Run("post non-acknowledge path is 404", h.testPostNonAcknowledgePath)
	h.t.Run("get with slash in id is 404", h.testGetWithSlashInID)
	h.t.Run("stopped is 503", h.testStoppedIs503)
}

func (h *ingestionRecordsTestHarness) testListEmptyRecords(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodGet, recordsPath, ""), &h.state, recordsPath)
	if w.Code != http.StatusOK {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("list body = %s", w.Body.String())
	}
	if _, ok := body["records"]; !ok {
		t.Fatalf("list body missing records: %s", w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testReadUnknownRecord(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	jobID := "ingest-" + hex.EncodeToString(make([]byte, 16))
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodGet, recordsPath+"/"+jobID, ""), &h.state, recordsPath)
	if w.Code != http.StatusNotFound {
		t.Fatalf("read unknown = %d %s", w.Code, w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testAcknowledgeUnknownRecord(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	jobID := "ingest-" + hex.EncodeToString(make([]byte, 16))
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodPost, recordsPath+"/"+jobID+"/acknowledge", `{"acknowledge_uncertain_outcome":true}`), &h.state, recordsPath)
	if w.Code != http.StatusNotFound {
		t.Fatalf("ack unknown = %d %s", w.Code, w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testAcknowledgeRequiresExplicitTrue(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	jobID := "ingest-" + hex.EncodeToString(make([]byte, 16))
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodPost, recordsPath+"/"+jobID+"/acknowledge", `{"acknowledge_uncertain_outcome":false}`), &h.state, recordsPath)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ack implicit = %d %s", w.Code, w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testAcknowledgeInvalidJSON(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	jobID := "ingest-" + hex.EncodeToString(make([]byte, 16))
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodPost, recordsPath+"/"+jobID+"/acknowledge", `not json`), &h.state, recordsPath)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("ack bad json = %d %s", w.Code, w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testPostNonAcknowledgePath(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	jobID := "ingest-" + hex.EncodeToString(make([]byte, 16))
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodPost, recordsPath+"/"+jobID, ""), &h.state, recordsPath)
	if w.Code != http.StatusNotFound {
		t.Fatalf("post non-ack = %d %s", w.Code, w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testGetWithSlashInID(t *testing.T) {
	h.t.Helper()
	recordsPath := "/management/v1/ingestion/records"
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodGet, recordsPath+"/a/b", ""), &h.state, recordsPath)
	if w.Code != http.StatusNotFound {
		t.Fatalf("get slash = %d %s", w.Code, w.Body.String())
	}
}

func (h *ingestionRecordsTestHarness) testStoppedIs503(t *testing.T) {
	h.t.Helper()
	h.r.stopped = true
	recordsPath := "/management/v1/ingestion/records"
	w := httptest.NewRecorder()
	h.r.serveIngestionRecords(w, h.newRequest(http.MethodGet, recordsPath, ""), &h.state, recordsPath)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("stopped = %d %s", w.Code, w.Body.String())
	}
	h.r.stopped = false
}
