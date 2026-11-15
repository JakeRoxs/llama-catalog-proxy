//go:build linux

package proxy

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
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

func TestAdoptedArtifactReviewedCreationWorkflow(t *testing.T) {
	models, records, targetDir, proposals, backups := t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{records, proposals, backups} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, 24)
	copy(data, "GGUF")
	binary.LittleEndian.PutUint32(data[4:], 3)
	if err := os.WriteFile(filepath.Join(models, "model.gguf"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(targetDir, "config.yaml")
	original := "models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n"
	if err := os.WriteFile(targetPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := ingestion.NewPersistentJobManager(context.Background(), "", records, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	var unexpectedRequest atomic.Bool
	var catalogRequests atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/models" || req.Header.Get("Authorization") != "" {
			unexpectedRequest.Store(true)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		catalogRequests.Add(1)
		current, err := os.ReadFile(targetPath)
		if err != nil {
			http.Error(w, "fixture unavailable", http.StatusInternalServerError)
			return
		}
		if strings.Contains(string(current), `"new-model":`) {
			_, _ = io.WriteString(w, `{"data":[{"id":"new-model"}]}`)
		} else {
			_, _ = io.WriteString(w, `{"data":[]}`)
		}
	}))
	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{
		catalog: catalog.New([]catalog.Backend{{ID: "local", URL: backendURL, Default: true}},
			backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger()),
		catalogTimeout: time.Second, logger: discardLogger(),
	}
	ingestionToken, previewToken, proposalToken, writeToken := strings.Repeat("i", 32), strings.Repeat("v", 32), strings.Repeat("p", 32), strings.Repeat("w", 32)
	ingestionConfig := config.IngestionConfig{Directory: models, RecordDirectory: records, Token: ingestionToken, MaxBytes: 1024}
	state := &reloaderState{handler: handler, management: &config.ManagementConfig{
		PreviewToken: previewToken, WriteToken: writeToken, Ingestion: &ingestionConfig,
		ProposalStore: &config.ProposalStoreConfig{Directory: proposals, Token: proposalToken},
		BackupStore:   &config.BackupStoreConfig{Directory: backups, MaxBackups: 4},
		Targets:       map[string]config.ManagementTarget{"local": {Directory: targetDir, Filename: "config.yaml"}},
	}}
	reloader := &Reloader{logger: discardLogger(), managementSlots: make(chan struct{}, 2),
		ingestion: &ingestionRuntime{config: ingestionConfig, mode: 0o600, duration: time.Minute, jobs: manager}}
	reloader.state.Store(state)
	server := httptest.NewTLSServer(reloader.ManagementHandler())
	defer server.Close()
	client := server.Client()
	request := func(method, path, token, body string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data
	}

	status, body := request(http.MethodPost, "/management/v1/ingestion/jobs", ingestionToken,
		`{"source":{"filename":"model.gguf"},"existing_file":"adopt","allow_adoption":true,"acknowledge_unverified_source":true}`)
	var job ingestion.JobStatus
	if err := json.Unmarshal(body, &job); err != nil || status != http.StatusAccepted || job.ID == "" {
		t.Fatalf("adoption start = %d %s %v", status, body, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body = request(http.MethodGet, "/management/v1/ingestion/jobs/"+job.ID, ingestionToken, "")
		if err := json.Unmarshal(body, &job); err != nil || status != http.StatusOK {
			t.Fatalf("adoption status = %d %s %v", status, body, err)
		}
		if job.State != "running" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("adoption did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if job.State != "succeeded" || job.Source.Kind != "local" || job.Result.Publication != ingestion.PublicationExisting {
		t.Fatalf("unexpected adoption result: %+v", job)
	}
	status, body = request(http.MethodGet, "/management/v1/ingestion/records/"+job.ID, ingestionToken, "")
	var record ingestion.Record
	if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK ||
		record.Job.ID != job.ID || record.Job.State != "succeeded" || record.Job.Source.Kind != "local" {
		t.Fatalf("adoption record = %d %s %v", status, body, err)
	}
	input := fmt.Sprintf(`{"target_id":"local","model_id":"new-model","artifact_record_id":%q,"executable":"llama-server"}`, job.ID)
	status, body = request(http.MethodPost, "/management/v1/creation/preview", previewToken, input)
	var preview recordedCreationPreview
	if err := json.Unmarshal(body, &preview); err != nil || status != http.StatusOK || preview.Artifact.Source.Kind != "local" {
		t.Fatalf("creation preview = %d %s %v", status, body, err)
	}
	create := strings.TrimSuffix(input, "}") + fmt.Sprintf(`,"revision":%q,"artifact_revision":%q,"review_revision":%q}`,
		preview.Preview.Revision, preview.Artifact.Revision, preview.ReviewRevision)
	status, body = request(http.MethodPost, "/management/v1/proposals/creation", proposalToken, create)
	var proposal modelconfig.CreationProposal
	if err := json.Unmarshal(body, &proposal); err != nil || status != http.StatusCreated || proposal.ID == "" {
		t.Fatalf("creation proposal = %d %s %v", status, body, err)
	}
	ack := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	status, body = request(http.MethodPost, "/management/v1/proposals/"+proposal.ID+"/apply", writeToken, ack)
	var applied mutationResponse
	if err := json.Unmarshal(body, &applied); err != nil || status != http.StatusOK || applied.BackupID == "" || applied.Status != "written" {
		t.Fatalf("creation apply = %d %s %v", status, body, err)
	}
	catalogPath := "/management/v1/proposals/" + proposal.ID + "/catalog"
	status, body = request(http.MethodPost, catalogPath, proposalToken, `{"backend_id":"local"}`)
	if status != http.StatusOK || !strings.Contains(string(body), `"catalog":"listed_when_checked"`) ||
		!strings.Contains(string(body), `"selected_backend_relation":"unknown"`) {
		t.Fatalf("catalog observation = %d %s", status, body)
	}
	status, body = request(http.MethodPost, "/management/v1/backups/"+applied.BackupID+"/rollback", writeToken, ack)
	current, err := os.ReadFile(targetPath)
	if status != http.StatusOK || err != nil || string(current) != original {
		t.Fatalf("rollback = %d %s %v", status, body, err)
	}
	status, body = request(http.MethodPost, catalogPath, proposalToken, `{"backend_id":"local"}`)
	if status != http.StatusOK || !strings.Contains(string(body), `"catalog":"not_listed_when_checked"`) ||
		unexpectedRequest.Load() || catalogRequests.Load() != 2 {
		t.Fatalf("catalog after rollback = %d %s", status, body)
	}
}
