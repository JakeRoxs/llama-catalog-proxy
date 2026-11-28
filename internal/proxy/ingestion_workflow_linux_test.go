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

type workflowContext struct {
	t              *testing.T
	models         string
	records        string
	targetDir      string
	proposals      string
	backups        string
	targetPath     string
	original       string
	manager        *ingestion.JobManager
	backend        *httptest.Server
	handler        *Handler
	reloader       *Reloader
	server         *httptest.Server
	client         *http.Client
	ingestionToken string
	previewToken   string
	proposalToken  string
	writeToken     string
	unexpected     atomic.Bool
	catalogReq     atomic.Int32
	job            ingestion.JobStatus
}

func newWorkflowContext(t *testing.T) *workflowContext {
	ctx := &workflowContext{t: t}
	ctx.models, ctx.records, ctx.targetDir, ctx.proposals, ctx.backups = t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{ctx.records, ctx.proposals, ctx.backups} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	data := make([]byte, 24)
	copy(data, "GGUF")
	binary.LittleEndian.PutUint32(data[4:], 3)
	if err := os.WriteFile(filepath.Join(ctx.models, "model.gguf"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx.targetPath = filepath.Join(ctx.targetDir, "config.yaml")
	ctx.original = "models:\n  old: {cmd: 'llama-server --port ${PORT} -m /models/old.gguf'}\n"
	if err := os.WriteFile(ctx.targetPath, []byte(ctx.original), 0o600); err != nil {
		t.Fatal(err)
	}
	manager, err := ingestion.NewPersistentJobManager(context.Background(), "", ctx.records, 8)
	if err != nil {
		t.Fatal(err)
	}
	ctx.manager = manager

	ingestionToken, previewToken, proposalToken, writeToken := strings.Repeat("i", 32), strings.Repeat("v", 32), strings.Repeat("p", 32), strings.Repeat("w", 32)
	ctx.ingestionToken = ingestionToken
	ctx.previewToken = previewToken
	ctx.proposalToken = proposalToken
	ctx.writeToken = writeToken

	ctx.backend = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet || req.URL.Path != "/v1/models" || req.Header.Get("Authorization") != "" {
			ctx.unexpected.Store(true)
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		ctx.catalogReq.Add(1)
		current, err := os.ReadFile(ctx.targetPath)
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

	backendURL, err := url.Parse(ctx.backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	ingestionConfig := config.IngestionConfig{Directory: ctx.models, RecordDirectory: ctx.records, Token: ingestionToken,
		MaxBytes: 1024, MaxDirectoryBytes: 1024, MinFreeBytes: 1}
	ctx.handler = &Handler{
		catalog: catalog.New([]catalog.Backend{{ID: "local", URL: backendURL, Default: true}},
			ctx.backend.Client(), time.Nanosecond, time.Second, time.Second, discardLogger()),
		catalogTimeout: time.Second, logger: discardLogger(),
	}
	state := &reloaderState{handler: ctx.handler, management: &config.ManagementConfig{
		PreviewToken: previewToken, WriteToken: writeToken, Ingestion: &ingestionConfig,
		ProposalStore: &config.ProposalStoreConfig{Directory: ctx.proposals, Token: proposalToken},
		BackupStore:   &config.BackupStoreConfig{Directory: ctx.backups, MaxBackups: 4},
		Targets:       map[string]config.ManagementTarget{"local": {Directory: ctx.targetDir, Filename: "config.yaml"}},
	}}
	ctx.reloader = &Reloader{logger: discardLogger(), managementSlots: make(chan struct{}, 2),
		ingestion: &ingestionRuntime{config: ingestionConfig, mode: 0o600, duration: time.Minute, jobs: manager}}
	ctx.reloader.state.Store(state)

	ctx.server = httptest.NewTLSServer(ctx.reloader.ManagementHandler())
	ctx.client = ctx.server.Client()
	return ctx
}

func (ctx *workflowContext) cleanup() {
	ctx.server.Close()
	ctx.backend.Close()
	ctx.manager.Stop()
}

func (ctx *workflowContext) request(method, path, token, body string) (int, []byte) {
	ctx.t.Helper()
	req, err := http.NewRequest(method, ctx.server.URL+path, strings.NewReader(body))
	if err != nil {
		ctx.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	response, err := ctx.client.Do(req)
	if err != nil {
		ctx.t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		ctx.t.Fatal(err)
	}
	return response.StatusCode, data
}

func (ctx *workflowContext) startAdoption() {
	status, body := ctx.request(http.MethodPost, "/management/v1/ingestion/jobs", ctx.ingestionToken,
		`{"source":{"filename":"model.gguf"},"existing_file":"adopt","allow_adoption":true,"acknowledge_unverified_source":true}`)
	var job ingestion.JobStatus
	if err := json.Unmarshal(body, &job); err != nil || status != http.StatusAccepted || job.ID == "" {
		ctx.t.Fatalf("adoption start = %d %s %v", status, body, err)
	}
	ctx.job = job
}

func (ctx *workflowContext) waitForAdoption() {
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body := ctx.request(http.MethodGet, "/management/v1/ingestion/jobs/"+ctx.job.ID, ctx.ingestionToken, "")
		if err := json.Unmarshal(body, &ctx.job); err != nil || status != http.StatusOK {
			ctx.t.Fatalf("adoption status = %d %s %v", status, body, err)
		}
		if ctx.job.State != "running" {
			break
		}
		if time.Now().After(deadline) {
			ctx.t.Fatal("adoption did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ctx *workflowContext) verifyAdoption() {
	if ctx.job.State != "succeeded" || ctx.job.Source.Kind != "local" || ctx.job.Result.Publication != ingestion.PublicationExisting {
		ctx.t.Fatalf("unexpected adoption result: %+v", ctx.job)
	}
	status, body := ctx.request(http.MethodGet, "/management/v1/ingestion/records/"+ctx.job.ID, ctx.ingestionToken, "")
	var record ingestion.Record
	if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK ||
		record.Job.ID != ctx.job.ID || record.Job.State != "succeeded" || record.Job.Source.Kind != "local" {
		ctx.t.Fatalf("adoption record = %d %s %v", status, body, err)
	}
}

func (ctx *workflowContext) createPreview() recordedCreationPreview {
	input := fmt.Sprintf(`{"target_id":"local","model_id":"new-model","artifact_record_id":%q,"executable":"llama-server"}`, ctx.job.ID)
	status, body := ctx.request(http.MethodPost, "/management/v1/creation/preview", ctx.previewToken, input)
	var preview recordedCreationPreview
	if err := json.Unmarshal(body, &preview); err != nil || status != http.StatusOK || preview.Artifact.Source.Kind != "local" {
		ctx.t.Fatalf("creation preview = %d %s %v", status, body, err)
	}
	return preview
}

func (ctx *workflowContext) createProposal(preview recordedCreationPreview) modelconfig.CreationProposal {
	input := fmt.Sprintf(`{"target_id":"local","model_id":"new-model","artifact_record_id":%q,"executable":"llama-server"}`, ctx.job.ID)
	create := strings.TrimSuffix(input, "}") + fmt.Sprintf(`,"revision":%q,"artifact_revision":%q,"review_revision":%q}`,
		preview.Preview.Revision, preview.Artifact.Revision, preview.ReviewRevision)
	status, body := ctx.request(http.MethodPost, "/management/v1/proposals/creation", ctx.proposalToken, create)
	var proposal modelconfig.CreationProposal
	if err := json.Unmarshal(body, &proposal); err != nil || status != http.StatusCreated || proposal.ID == "" {
		ctx.t.Fatalf("creation proposal = %d %s %v", status, body, err)
	}
	return proposal
}

func (ctx *workflowContext) applyProposal(proposal modelconfig.CreationProposal) mutationResponse {
	ack := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	status, body := ctx.request(http.MethodPost, "/management/v1/proposals/"+proposal.ID+"/apply", ctx.writeToken, ack)
	var applied mutationResponse
	if err := json.Unmarshal(body, &applied); err != nil || status != http.StatusOK || applied.BackupID == "" || applied.Status != "written" {
		ctx.t.Fatalf("creation apply = %d %s %v", status, body, err)
	}
	return applied
}

func (ctx *workflowContext) verifyCatalogListed(proposalID string) {
	catalogPath := "/management/v1/proposals/" + proposalID + "/catalog"
	status, body := ctx.request(http.MethodPost, catalogPath, ctx.proposalToken, `{"backend_id":"local"}`)
	if status != http.StatusOK || !strings.Contains(string(body), `"catalog":"listed_when_checked"`) ||
		!strings.Contains(string(body), `"selected_backend_relation":"unknown"`) {
		ctx.t.Fatalf("catalog observation = %d %s", status, body)
	}
}

func (ctx *workflowContext) rollbackAndVerify(applied mutationResponse) {
	ack := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	status, body := ctx.request(http.MethodPost, "/management/v1/backups/"+applied.BackupID+"/rollback", ctx.writeToken, ack)
	current, err := os.ReadFile(ctx.targetPath)
	if status != http.StatusOK || err != nil || string(current) != ctx.original {
		ctx.t.Fatalf("rollback = %d %s %v", status, body, err)
	}
}

func (ctx *workflowContext) verifyCatalogNotListed(proposalID string) {
	catalogPath := "/management/v1/proposals/" + proposalID + "/catalog"
	status, body := ctx.request(http.MethodPost, catalogPath, ctx.proposalToken, `{"backend_id":"local"}`)
	if status != http.StatusOK || !strings.Contains(string(body), `"catalog":"not_listed_when_checked"`) ||
		ctx.unexpected.Load() || ctx.catalogReq.Load() != 2 {
		ctx.t.Fatalf("catalog after rollback = %d %s", status, body)
	}
}

func TestAdoptedArtifactReviewedCreationWorkflow(t *testing.T) {
	ctx := newWorkflowContext(t)
	defer ctx.cleanup()

	ctx.startAdoption()
	ctx.waitForAdoption()
	ctx.verifyAdoption()

	preview := ctx.createPreview()
	proposal := ctx.createProposal(preview)
	applied := ctx.applyProposal(proposal)
	ctx.verifyCatalogListed(proposal.ID)
	ctx.rollbackAndVerify(applied)
	ctx.verifyCatalogNotListed(proposal.ID)
}
