//go:build linux

package proxy

import (
	"encoding/binary"
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

func TestIngestionManagementAPIAndReload(t *testing.T) {
	models, targets, records := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.Chmod(records, 0o700); err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, _ := peerTestCertificate(t)
	preview, token, replacement, hub := strings.Repeat("m", 32), strings.Repeat("i", 32), strings.Repeat("n", 32), "fixture-hub-token"
	path := configPath(t)
	writeConfig := func(ingestionToken, target string) {
		t.Helper()
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
`, certPath, keyPath, preview, models, records, ingestionToken, hub, target)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	writeConfig(token, targets)
	r, cfg, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
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
	defer server.Close()
	client := server.Client()
	request := func(method, path, credential, body, origin string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
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
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("missing no-store header")
		}
		for _, secret := range []string{token, replacement, preview, hub, models, records, targets} {
			if strings.Contains(string(data), secret) {
				t.Fatal("response exposed a credential or local path")
			}
		}
		return response.StatusCode, string(data)
	}
	jobs := "/management/v1/ingestion/jobs"
	valid := `{"source":{"repository":"owner/repo","filename":"model.gguf","revision":"main"},"allow_download":true,"existing_file":"reject"}`
	if status, _ := request(http.MethodPost, "/management/v1/settings", token, `{"target_id":"local","model_id":"x"}`, ""); status != http.StatusUnauthorized {
		t.Fatalf("ingestion credential can access preview API: %d", status)
	}
	for _, invalid := range []struct {
		name, credential, body, origin string
		status                         int
	}{
		{"no credential", "", valid, "", http.StatusUnauthorized},
		{"preview credential", preview, valid, "", http.StatusUnauthorized},
		{"returned config mutated", replacement, valid, "", http.StatusUnauthorized},
		{"foreign origin", token, valid, "https://evil.example", http.StatusForbidden},
		{"plaintext origin", token, valid, strings.Replace(server.URL, "https:", "http:", 1), http.StatusForbidden},
		{"no consent", token, strings.Replace(valid, `"allow_download":true`, `"allow_download":false`, 1), "", http.StatusBadRequest},
		{"no policy", token, strings.Replace(valid, `,"existing_file":"reject"`, "", 1), "", http.StatusBadRequest},
		{"replace unsupported", token, strings.Replace(valid, `"reject"`, `"replace"`, 1), "", http.StatusBadRequest},
		{"untrusted URL", token, `{"source":{"url":"https://evil.example/model.gguf"},"allow_download":true,"existing_file":"reject"}`, "", http.StatusBadRequest},
		{"duplicate field", token, strings.Replace(valid, `"allow_download":true`, `"allow_download":true,"ALLOW_DOWNLOAD":true`, 1), "", http.StatusBadRequest},
		{"unknown field", token, strings.Replace(valid, `"existing_file":"reject"`, `"existing_file":"reject","token":"oops"`, 1), "", http.StatusBadRequest},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			status, body := request(http.MethodPost, jobs, invalid.credential, invalid.body, invalid.origin)
			if status != invalid.status {
				t.Fatalf("status = %d, want %d: %s", status, invalid.status, body)
			}
		})
	}
	original := []byte("existing")
	if err := os.WriteFile(filepath.Join(models, "model.gguf"), original, 0o600); err != nil {
		t.Fatal(err)
	}
	status, body := request(http.MethodPost, jobs, token, valid, server.URL)
	if status != http.StatusAccepted {
		t.Fatalf("start = %d %s", status, body)
	}
	var job struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &job); err != nil || job.ID == "" {
		t.Fatalf("invalid job: %s, %v", body, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body = request(http.MethodGet, jobs+"/"+job.ID, token, "", "")
		if status != http.StatusOK {
			t.Fatalf("status = %d %s", status, body)
		}
		if strings.Contains(body, `"state":"failed"`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("job did not finish: %s", body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !strings.Contains(body, `"destination_exists"`) || !strings.Contains(body, `"publication":"not_published"`) ||
		!strings.Contains(body, `"repository":"owner/repo"`) {
		t.Fatalf("unexpected job result: %s", body)
	}
	contents, err := os.ReadFile(filepath.Join(models, "model.gguf"))
	if err != nil || string(contents) != string(original) {
		t.Fatalf("existing file changed: %q, %v", contents, err)
	}
	if status, _ := request(http.MethodGet, jobs+"/current", token, "", ""); status != http.StatusOK {
		t.Fatalf("current job = %d", status)
	}
	if status, _ := request(http.MethodGet, jobs+"/current?token=ignored", token, "", ""); status != http.StatusForbidden {
		t.Fatalf("query-bearing job request = %d", status)
	}
	if status, _ := request(http.MethodPost, jobs+"/"+job.ID+"/cancel", token, "", ""); status != http.StatusConflict {
		t.Fatalf("cancel finished job = %d", status)
	}
	if status, _ := request(http.MethodGet, jobs+"/missing", token, "", ""); status != http.StatusNotFound {
		t.Fatalf("missing job = %d", status)
	}
	if status, _ := request(http.MethodPost, jobs+"/"+job.ID+"/cancel", token, "{}", ""); status != http.StatusBadRequest {
		t.Fatalf("cancel with body = %d", status)
	}
	recordsPath := "/management/v1/ingestion/records"
	if status, body := request(http.MethodGet, recordsPath, token, "", ""); status != http.StatusOK || !strings.Contains(body, job.ID) {
		t.Fatalf("records = %d %s", status, body)
	}
	if status, body := request(http.MethodGet, recordsPath+"/"+job.ID, token, "", ""); status != http.StatusOK || !strings.Contains(body, `"destination_exists"`) {
		t.Fatalf("record = %d %s", status, body)
	}
	if status, _ := request(http.MethodPost, recordsPath+"/"+job.ID+"/acknowledge", token, `{"acknowledge_uncertain_outcome":true}`, ""); status != http.StatusConflict {
		t.Fatalf("acknowledged a completed certain outcome: %d", status)
	}
	adoption := `{"source":{"filename":"model.gguf"},"existing_file":"adopt","allow_adoption":true,"acknowledge_unverified_source":true}`
	for _, invalid := range []string{
		strings.Replace(adoption, `"allow_adoption":true`, `"allow_adoption":false`, 1),
		strings.Replace(adoption, `"acknowledge_unverified_source":true`, `"acknowledge_unverified_source":false`, 1),
		strings.Replace(adoption, `"filename":"model.gguf"`, `"filename":"model.gguf","repository":"owner/repo"`, 1),
		strings.Replace(adoption, `"existing_file":"adopt"`, `"existing_file":"adopt","allow_download":true`, 1),
	} {
		if status, _ := request(http.MethodPost, jobs, token, invalid, ""); status != http.StatusBadRequest {
			t.Fatalf("invalid adoption accepted: %d", status)
		}
	}
	gguf := make([]byte, 24)
	copy(gguf, "GGUF")
	binary.LittleEndian.PutUint32(gguf[4:], 3)
	if err := os.WriteFile(filepath.Join(models, "model.gguf"), gguf, 0o600); err != nil {
		t.Fatal(err)
	}
	waitJob := func(body string) ingestion.JobStatus {
		t.Helper()
		var started ingestion.JobStatus
		if err := json.Unmarshal([]byte(body), &started); err != nil || started.ID == "" {
			t.Fatalf("invalid started job: %s %v", body, err)
		}
		end := time.Now().Add(3 * time.Second)
		for {
			code, body := request(http.MethodGet, jobs+"/"+started.ID, token, "", "")
			var current ingestion.JobStatus
			if err := json.Unmarshal([]byte(body), &current); err != nil || code != http.StatusOK {
				t.Fatalf("invalid job status: %d %s %v", code, body, err)
			}
			if current.State != "running" && current.State != "canceling" {
				return current
			}
			if time.Now().After(end) {
				t.Fatal("job did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	status, body = request(http.MethodPost, jobs, token, strings.Replace(valid, "model.gguf", "another.gguf", 1), "")
	if status != http.StatusAccepted {
		t.Fatalf("quota-limited start = %d %s", status, body)
	}
	quotaJob := waitJob(body)
	if quotaJob.State != "failed" || quotaJob.Result.Publication != ingestion.PublicationNotPublished ||
		len(quotaJob.ErrorCodes) != 1 || quotaJob.ErrorCodes[0] != "directory_quota" {
		t.Fatalf("quota-limited job = %+v", quotaJob)
	}
	status, body = request(http.MethodPost, jobs, token, adoption, "")
	if status != http.StatusAccepted {
		t.Fatalf("adoption start = %d %s", status, body)
	}
	adopted := waitJob(body)
	if adopted.State != "succeeded" || adopted.Operation != "adopt" || adopted.Source.Kind != "local" ||
		adopted.Source.Repository != "" || adopted.Source.Revision != "" || adopted.Result.Publication != ingestion.PublicationExisting {
		t.Fatalf("adoption result = %+v", adopted)
	}
	if err := os.WriteFile(filepath.Join(targets, "config.yaml"), []byte("models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	review, err := previewRecordedCreation(r.ingestion.jobs, models, config.ManagementTarget{Directory: targets, Filename: "config.yaml"},
		"local", "new", "", adopted.ID, modelconfig.CreationCommandInput{Executable: "llama-server"})
	if err != nil || review.Artifact.Source.Kind != "local" || review.Artifact.RecordMatch != "unknown" {
		t.Fatalf("adopted artifact cannot be reviewed: %+v %v", review, err)
	}
	replacementBody := `{"source":{"repository":"owner/repo","filename":"bad.gguf","revision":"main"},"allow_download":true,"existing_file":"replace","allow_replacement":true,"acknowledge_exclusive_window":true}`
	for _, invalid := range []string{
		strings.Replace(replacementBody, `"allow_replacement":true`, `"allow_replacement":false`, 1),
		strings.Replace(replacementBody, `"acknowledge_exclusive_window":true`, `"acknowledge_exclusive_window":false`, 1),
	} {
		if status, _ := request(http.MethodPost, jobs, token, invalid, ""); status != http.StatusBadRequest {
			t.Fatalf("replacement without authorization accepted: %d", status)
		}
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "missing"), filepath.Join(models, "bad.gguf")); err != nil {
		t.Fatal(err)
	}
	status, body = request(http.MethodPost, jobs, token, replacementBody, "")
	if status != http.StatusAccepted {
		t.Fatalf("authorized replacement was not started: %d %s", status, body)
	}
	failedReplacement := waitJob(body)
	if failedReplacement.State != "failed" || failedReplacement.Operation != "replace" ||
		failedReplacement.Result.Publication != ingestion.PublicationNotPublished ||
		len(failedReplacement.ErrorCodes) != 1 || failedReplacement.ErrorCodes[0] != "artifact_unavailable" {
		t.Fatalf("unsafe replacement result = %+v", failedReplacement)
	}
	writeConfig(token, targets)
	if err := r.reload(); err != nil {
		t.Fatalf("unchanged ingestion configuration could not reload: %v", err)
	}
	if status, _ := request(http.MethodGet, jobs+"/"+failedReplacement.ID, token, "", ""); status != http.StatusOK {
		t.Fatalf("ordinary reload lost job: %d", status)
	}
	writeConfig(replacement, targets)
	if err := r.reload(); err == nil {
		t.Fatal("ingestion credential change accepted without restart")
	}
	if status, _ := request(http.MethodGet, jobs+"/current", token, "", ""); status != http.StatusOK {
		t.Fatalf("failed reload changed active policy: %d", status)
	}
	writeConfig(token, models)
	if err := r.reload(); err == nil {
		t.Fatal("overlapping target directory accepted")
	}

	writeConfig(token, targets)
	r.Stop()
	interrupted := ingestion.Record{Version: 1, Job: ingestion.JobStatus{
		ID: "ingest-" + strings.Repeat("a", 32), State: "running",
		Source:    ingestion.Source{Repository: "owner/repo", Filename: "model.gguf", Revision: "main"},
		StartedAt: time.Now().UTC(), Result: ingestion.StageResult{Publication: ingestion.PublicationUnknown, Durability: "unknown"},
	}}
	data, err := json.Marshal(interrupted)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(records, interrupted.Job.ID+".json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, _, err := NewReloader(path, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()
	restartedRequest := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, "https://management.example"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
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
		t.Fatalf("interrupted record = %d %s", response.Code, response.Body.String())
	}
	if response := restartedRequest(http.MethodPost, jobs, valid); response.Code != http.StatusConflict {
		t.Fatalf("unreviewed job did not block start: %d %s", response.Code, response.Body.String())
	}
	if response := restartedRequest(http.MethodPost, recordsPath+"/"+interrupted.Job.ID+"/acknowledge", `{"acknowledge_uncertain_outcome":false}`); response.Code != http.StatusBadRequest {
		t.Fatalf("implicit acknowledgment accepted: %d", response.Code)
	}
	response = restartedRequest(http.MethodPost, recordsPath+"/"+interrupted.Job.ID+"/acknowledge", `{"acknowledge_uncertain_outcome":true}`)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"acknowledged":true`) ||
		!strings.Contains(response.Body.String(), `"publication":"unknown"`) {
		t.Fatalf("acknowledgment = %d %s", response.Code, response.Body.String())
	}
	if response := restartedRequest(http.MethodPost, jobs, valid); response.Code != http.StatusAccepted {
		t.Fatalf("start after acknowledgment = %d %s", response.Code, response.Body.String())
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
