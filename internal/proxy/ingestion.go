package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
)

// S1192: duplicated HTTP error message literals replaced by package-level
// constants. Names are unique within the proxy package; the existing
// `managementConfigChangedMsg` ("management configuration changed") covers a
// different message and is not reused here.
const (
	ingestionManagementConfigChangedOrServiceStoppedMsg = "management configuration changed or service stopped"
	ingestionRecordsUnavailableMsg                      = "ingestion records unavailable"
)

type ingestionRuntime struct {
	config   config.IngestionConfig
	duration time.Duration
	mode     os.FileMode
	jobs     *ingestion.JobManager
}

func newIngestionRuntime(cfg config.Config) (*ingestionRuntime, error) {
	if cfg.Management == nil || cfg.Management.Ingestion == nil {
		return nil, nil
	}
	if err := validateIngestionDirectories(cfg.Management, cfg.Benchmark); err != nil {
		return nil, err
	}
	options := *cfg.Management.Ingestion
	duration, err := time.ParseDuration(options.Timeout)
	if err != nil || duration <= 0 {
		return nil, errors.New("invalid ingestion timeout")
	}
	var mode os.FileMode
	switch options.FileMode {
	case "0600":
		mode = 0o600
	case "0640":
		mode = 0o640
	default:
		return nil, errors.New("invalid ingestion file mode")
	}
	jobs, err := ingestion.NewPersistentJobManager(context.Background(), options.HuggingFaceToken, options.RecordDirectory, options.MaxRecords)
	if err != nil {
		return nil, err
	}
	return &ingestionRuntime{config: options, duration: duration, mode: mode, jobs: jobs}, nil
}

// validateIngestionDirectoryMetadata verifies the ingestion directory exists, is a
// regular directory (not a symlink), and is not group/world writable.
func validateIngestionDirectoryMetadata(directory string) error {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("ingestion directory must exist and not be a symlink or group/world writable")
	}
	return nil
}

// validateIngestionDirectoryTargets verifies the ingestion directory is separated
// from every management target.
func validateIngestionDirectoryTargets(directory string, targets map[string]config.ManagementTarget) error {
	if !separateProposalDirectory(directory, targets) {
		return errors.New("ingestion directory overlaps or cannot be separated from management targets")
	}
	return nil
}

// validateIngestionRecordDirectory verifies the ingestion directory and record
// directory are separated from each other and from every management target.
func validateIngestionRecordDirectory(directory, records string, targets map[string]config.ManagementTarget) error {
	if !separateDirectories(directory, records) || !separateProposalDirectory(records, targets) {
		return errors.New("ingestion record directory overlaps or cannot be separated from models or management targets")
	}
	return nil
}

// validateIngestionStoreDirectories verifies the ingestion directory is separated
// from the proposal, backup, and benchmark stores.
func validateIngestionStoreDirectories(directory string, management *config.ManagementConfig, benchmark *config.BenchmarkConfig) error {
	if management.ProposalStore != nil && !separateDirectories(directory, management.ProposalStore.Directory) ||
		management.BackupStore != nil && !separateDirectories(directory, management.BackupStore.Directory) ||
		benchmark != nil && !separateDirectories(directory, benchmark.ReportDirectory) {
		return errors.New("ingestion directory overlaps or cannot be separated from management or benchmark stores")
	}
	return nil
}

// validateIngestionRecordStoreDirectories verifies the ingestion record directory
// is separated from the proposal, backup, and benchmark stores.
func validateIngestionRecordStoreDirectories(records string, management *config.ManagementConfig, benchmark *config.BenchmarkConfig) error {
	if management.ProposalStore != nil && !separateDirectories(records, management.ProposalStore.Directory) ||
		management.BackupStore != nil && !separateDirectories(records, management.BackupStore.Directory) ||
		benchmark != nil && !separateDirectories(records, benchmark.ReportDirectory) {
		return errors.New("ingestion record directory overlaps or cannot be separated from management or benchmark stores")
	}
	return nil
}

func validateIngestionDirectories(management *config.ManagementConfig, benchmark *config.BenchmarkConfig) error {
	directory := management.Ingestion.Directory
	records := management.Ingestion.RecordDirectory
	if err := validateIngestionDirectoryMetadata(directory); err != nil {
		return err
	}
	if err := validateIngestionDirectoryTargets(directory, management.Targets); err != nil {
		return err
	}
	if err := validateIngestionRecordDirectory(directory, records, management.Targets); err != nil {
		return err
	}
	if err := validateIngestionStoreDirectories(directory, management, benchmark); err != nil {
		return err
	}
	if err := validateIngestionRecordStoreDirectories(records, management, benchmark); err != nil {
		return err
	}
	return nil
}

// ingestionRoute classifies the top-level ingestion management route.
type ingestionRoute int

const (
	ingestionRouteRecords ingestionRoute = iota
	ingestionRouteStart
	ingestionRouteJob
)

// ingestionJobAction is the operation on a specific job.
type ingestionJobAction int

const (
	ingestionJobStatus ingestionJobAction = iota
	ingestionJobCancel
)

// ingestionJobRequest is a parsed job-level ingestion request.
type ingestionJobRequest struct {
	action ingestionJobAction
	id     string
}

// errInvalidIngestionJobRequest is returned by parseIngestionRoute when the
// request is invalid. The parser writes the exact 400/404 disposition, so the
// caller returns immediately.
var errInvalidIngestionJobRequest = errors.New("invalid ingestion job request")

// parseIngestionRoute classifies the ingestion management route. For the
// records and start routes the caller dispatches and returns. For the job
// route it returns a parsed ingestionJobRequest; invalid requests receive
// their exact 400/404 disposition from the parser and the caller returns.
func parseIngestionRoute(w http.ResponseWriter, req *http.Request) (ingestionRoute, *ingestionJobRequest, error) {
	const recordsPath = "/management/v1/ingestion/records"
	if req.URL.Path == recordsPath || strings.HasPrefix(req.URL.Path, recordsPath+"/") {
		return ingestionRouteRecords, nil, nil
	}
	const jobsPath = "/management/v1/ingestion/jobs"
	if req.URL.Path == jobsPath && req.Method == http.MethodPost {
		return ingestionRouteStart, nil, nil
	}
	if !strings.HasPrefix(req.URL.Path, jobsPath+"/") {
		http.NotFound(w, req)
		return ingestionRouteJob, nil, errInvalidIngestionJobRequest
	}
	id := strings.TrimPrefix(req.URL.Path, jobsPath+"/")
	cancel := req.Method == http.MethodPost && strings.HasSuffix(id, "/cancel")
	if cancel {
		id = strings.TrimSuffix(id, "/cancel")
		if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
			http.Error(w, "request body not allowed", http.StatusBadRequest)
			return ingestionRouteJob, nil, errInvalidIngestionJobRequest
		}
		return ingestionRouteJob, &ingestionJobRequest{action: ingestionJobCancel, id: id}, nil
	}
	if req.Method != http.MethodGet || strings.Contains(id, "/") {
		http.NotFound(w, req)
		return ingestionRouteJob, nil, errInvalidIngestionJobRequest
	}
	return ingestionRouteJob, &ingestionJobRequest{action: ingestionJobStatus, id: id}, nil
}

func (r *Reloader) serveIngestion(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.URL.RawQuery != "" || req.URL.ForceQuery || !sameIngestionOrigin(req) {
		http.Error(w, "invalid request origin or query", http.StatusForbidden)
		return
	}
	if r.ingestion == nil {
		http.NotFound(w, req)
		return
	}
	const recordsPath = "/management/v1/ingestion/records"
	route, jobReq, err := parseIngestionRoute(w, req)
	if err != nil {
		return
	}
	if route == ingestionRouteRecords {
		r.serveIngestionRecords(w, req, state, recordsPath)
		return
	}
	if route == ingestionRouteStart {
		r.startIngestionJob(w, req, state)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, ingestionManagementConfigChangedOrServiceStoppedMsg, http.StatusServiceUnavailable)
		return
	}
	if jobReq.action == ingestionJobCancel {
		err := r.cancelIngestionJob(jobReq.id)
		r.lifecycleMu.Unlock()
		if err != nil {
			if errors.Is(err, ingestion.ErrJobNotFound) {
				http.NotFound(w, req)
			} else {
				http.Error(w, "ingestion job already finished", http.StatusConflict)
			}
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	job, err := r.statusIngestionJob(jobReq.id)
	r.lifecycleMu.Unlock()
	if err != nil {
		http.NotFound(w, req)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, job)
}

// cancelIngestionJob cancels a job while the caller holds r.lifecycleMu. It
// does not acquire or release the lock and returns the cancel error.
func (r *Reloader) cancelIngestionJob(id string) error {
	return r.ingestion.jobs.Cancel(id)
}

// statusIngestionJob returns the job status while the caller holds r.lifecycleMu.
// It does not acquire or release the lock.
func (r *Reloader) statusIngestionJob(id string) (ingestion.JobStatus, error) {
	var job ingestion.JobStatus
	var err error
	if id == "current" {
		job, err = r.ingestion.jobs.Current()
	} else {
		job, err = r.ingestion.jobs.Status(id)
	}
	return job, err
}

// ingestionRecordAction classifies the kind of ingestion records request.
type ingestionRecordAction int

const (
	ingestionRecordList ingestionRecordAction = iota
	ingestionRecordRead
	ingestionRecordAcknowledge
)

// ingestionRecordRequest is a parsed, typed ingestion records request.
type ingestionRecordRequest struct {
	action ingestionRecordAction
	id     string
}

// errInvalidIngestionRecordRequest is returned by parseIngestionRecordRequest
// when the request is invalid. The parser writes the exact 400/404 response,
// so the caller simply returns.
var errInvalidIngestionRecordRequest = errors.New("invalid ingestion records request")

// parseIngestionRecordRequest extracts the typed ingestion records request
// from the URL path and HTTP method. It preserves the original path trimming,
// the POST-only /acknowledge rule, the exact acknowledgement JSON requirement,
// and the method/slash/id validation. Invalid requests receive their exact
// 400/404 disposition from the parser; the caller returns immediately.
func parseIngestionRecordRequest(w http.ResponseWriter, req *http.Request, path string) (*ingestionRecordRequest, error) {
	id := strings.TrimPrefix(req.URL.Path, path)
	acknowledge := req.Method == http.MethodPost && strings.HasPrefix(id, "/") && strings.HasSuffix(id, "/acknowledge")
	if acknowledge {
		id = strings.TrimSuffix(strings.TrimPrefix(id, "/"), "/acknowledge")
		var input struct {
			Acknowledge bool `json:"acknowledge_uncertain_outcome"`
		}
		if err := decodeBenchmarkJSON(req, &input); err != nil || !input.Acknowledge {
			http.Error(w, "explicit acknowledgment is required", http.StatusBadRequest)
			return nil, errInvalidIngestionRecordRequest
		}
	} else if req.Method == http.MethodGet && strings.HasPrefix(id, "/") {
		id = strings.TrimPrefix(id, "/")
	} else if req.Method != http.MethodGet || id != "" {
		http.NotFound(w, req)
		return nil, errInvalidIngestionRecordRequest
	}
	if strings.Contains(id, "/") || (id == "" && req.URL.Path != path) || (acknowledge && id == "") {
		http.NotFound(w, req)
		return nil, errInvalidIngestionRecordRequest
	}
	action := ingestionRecordList
	if id != "" {
		action = ingestionRecordRead
	}
	if acknowledge {
		action = ingestionRecordAcknowledge
	}
	return &ingestionRecordRequest{action: action, id: id}, nil
}

// ingestionRecordOperationResult is the outcome of a locked ingestion records
// operation. The caller holds r.lifecycleMu when invoking the operation; this
// helper does not acquire or release the lock.
type ingestionRecordOperationResult struct {
	record  ingestion.Record
	records []ingestion.Record
	err     error
}

// performIngestionRecordOperation executes the list/read/acknowledge operation
// while the caller holds r.lifecycleMu. It does not acquire or release the
// lock, so it must only be called from a locked section.
func (r *Reloader) performIngestionRecordOperation(action ingestionRecordAction, id string) ingestionRecordOperationResult {
	var result ingestionRecordOperationResult
	switch action {
	case ingestionRecordList:
		result.records, result.err = r.ingestion.jobs.Records()
	case ingestionRecordAcknowledge:
		result.record, result.err = r.ingestion.jobs.Acknowledge(id)
	default:
		result.record, result.err = r.ingestion.jobs.Record(id)
	}
	return result
}

// mapIngestionRecordError maps an ingestion records operation error to the
// exact HTTP response used by serveIngestionRecords. It logs warnings for
// unexpected and listing failures and preserves the 404/409/503 disposition.
func mapIngestionRecordError(w http.ResponseWriter, req *http.Request, logger *slog.Logger, err error, isList bool) {
	switch {
	case errors.Is(err, ingestion.ErrRecordNotFound):
		http.NotFound(w, req)
	case errors.Is(err, ingestion.ErrRecordReview):
		http.Error(w, "record is not available for acknowledgment", http.StatusConflict)
	default:
		if isList {
			logger.Warn("ingestion record listing failed", "error", err)
		} else {
			logger.Warn("ingestion record access failed", "error", err)
		}
		http.Error(w, ingestionRecordsUnavailableMsg, http.StatusServiceUnavailable)
	}
}

func (r *Reloader) serveIngestionRecords(w http.ResponseWriter, req *http.Request, state *reloaderState, path string) {
	parsed, err := parseIngestionRecordRequest(w, req, path)
	if err != nil {
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, ingestionManagementConfigChangedOrServiceStoppedMsg, http.StatusServiceUnavailable)
		return
	}
	result := r.performIngestionRecordOperation(parsed.action, parsed.id)
	r.lifecycleMu.Unlock()
	if result.err != nil {
		mapIngestionRecordError(w, req, r.logger, result.err, parsed.action == ingestionRecordList)
		return
	}
	if parsed.action == ingestionRecordList {
		writeJSON(r.logger, w, http.StatusOK, map[string]any{"records": result.records})
		return
	}
	writeJSON(r.logger, w, http.StatusOK, result.record)
}

func sameIngestionOrigin(req *http.Request) bool {
	origins := req.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	origin, err := url.Parse(origins[0])
	return err == nil && origin.Scheme == "https" && origin.Host == req.Host &&
		origin.User == nil && origin.Path == "" && origin.RawQuery == "" && !origin.ForceQuery &&
		origin.Fragment == "" && !strings.Contains(origins[0], "#")
}

func (r *Reloader) startIngestionJob(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	var input struct {
		Source struct {
			URL        string `json:"url"`
			Repository string `json:"repository"`
			Filename   string `json:"filename"`
			Revision   string `json:"revision"`
		} `json:"source"`
		AllowDownload               bool   `json:"allow_download"`
		ExistingFile                string `json:"existing_file"`
		AllowAdoption               bool   `json:"allow_adoption"`
		AcknowledgeUnverifiedSource bool   `json:"acknowledge_unverified_source"`
		AllowReplacement            bool   `json:"allow_replacement"`
		AcknowledgeExclusiveWindow  bool   `json:"acknowledge_exclusive_window"`
	}
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, "invalid JSON request", http.StatusBadRequest)
		return
	}
	var source ingestion.Source
	var err error
	switch input.ExistingFile {
	case "reject":
		if !input.AllowDownload || input.AllowAdoption || input.AcknowledgeUnverifiedSource ||
			input.AllowReplacement || input.AcknowledgeExclusiveWindow {
			http.Error(w, "download authorization and reject-existing policy are required", http.StatusBadRequest)
			return
		}
	case "replace":
		if !input.AllowDownload || !input.AllowReplacement || !input.AcknowledgeExclusiveWindow ||
			input.AllowAdoption || input.AcknowledgeUnverifiedSource {
			http.Error(w, "download, replacement, and exclusive-window authorization are required", http.StatusBadRequest)
			return
		}
	case "adopt":
		if !input.AllowAdoption || !input.AcknowledgeUnverifiedSource || input.AllowDownload ||
			input.AllowReplacement || input.AcknowledgeExclusiveWindow || input.Source.URL != "" ||
			input.Source.Repository != "" || input.Source.Revision != "" {
			http.Error(w, "adoption and unverified-source acknowledgment are required", http.StatusBadRequest)
			return
		}
		source, err = ingestion.LocalSource(input.Source.Filename)
	default:
		http.Error(w, "an explicit existing-file policy is required", http.StatusBadRequest)
		return
	}
	if input.ExistingFile != "adopt" {
		source, err = ingestion.ParseSource(ingestion.SourceInput{
			URL: input.Source.URL, Repository: input.Source.Repository,
			Filename: input.Source.Filename, Revision: input.Source.Revision,
		})
	}
	if err != nil {
		http.Error(w, "invalid or unsupported ingestion source", http.StatusBadRequest)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, ingestionManagementConfigChangedOrServiceStoppedMsg, http.StatusServiceUnavailable)
		return
	}
	var benchmark *config.BenchmarkConfig
	if r.benchmark != nil {
		benchmark = &r.benchmark.config
	}
	if err := validateIngestionDirectories(state.management, benchmark); err != nil {
		r.lifecycleMu.Unlock()
		r.logger.Warn("ingestion directory validation failed", "error", err)
		http.Error(w, "ingestion directory unavailable or unsafe", http.StatusServiceUnavailable)
		return
	}
	job, err := r.ingestion.jobs.Start(ingestion.StageOptions{
		Directory: r.ingestion.config.Directory, Source: source, MaxBytes: r.ingestion.config.MaxBytes,
		MaxDirectoryBytes: r.ingestion.config.MaxDirectoryBytes, MinFreeBytes: r.ingestion.config.MinFreeBytes,
		FileMode: r.ingestion.mode, AllowDownload: input.AllowDownload,
		AdoptExisting: input.ExistingFile == "adopt", AllowAdoption: input.AllowAdoption,
		ReplaceExisting: input.ExistingFile == "replace", AllowReplacement: input.AllowReplacement,
		AcknowledgeExclusiveWindow: input.AcknowledgeExclusiveWindow,
	}, r.ingestion.duration)
	r.lifecycleMu.Unlock()
	if err != nil {
		switch {
		case errors.Is(err, ingestion.ErrJobActive):
			http.Error(w, "ingestion job already active", http.StatusConflict)
		case errors.Is(err, ingestion.ErrRecordReview), errors.Is(err, ingestion.ErrRecordStoreFull):
			http.Error(w, "ingestion records require review or retention cleanup", http.StatusConflict)
		case errors.Is(err, ingestion.ErrJobStopped):
			http.Error(w, "ingestion service stopped", http.StatusServiceUnavailable)
		case errors.Is(err, ingestion.ErrRecordStore):
			r.logger.Warn("ingestion record store unavailable", "error", err)
			http.Error(w, ingestionRecordsUnavailableMsg, http.StatusServiceUnavailable)
		case errors.Is(err, ingestion.ErrStageInvalid):
			http.Error(w, "invalid ingestion request", http.StatusBadRequest)
		default:
			r.logger.Error("ingestion job could not start", "error", err)
			http.Error(w, "ingestion job could not start", http.StatusInternalServerError)
		}
		return
	}
	writeJSON(r.logger, w, http.StatusAccepted, job)
}
