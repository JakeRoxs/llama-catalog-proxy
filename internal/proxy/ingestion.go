package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/ingestion"
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

func validateIngestionDirectories(management *config.ManagementConfig, benchmark *config.BenchmarkConfig) error {
	directory := management.Ingestion.Directory
	records := management.Ingestion.RecordDirectory
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("ingestion directory must exist and not be a symlink or group/world writable")
	}
	if !separateProposalDirectory(directory, management.Targets) {
		return errors.New("ingestion directory overlaps or cannot be separated from management targets")
	}
	if !separateDirectories(directory, records) || !separateProposalDirectory(records, management.Targets) {
		return errors.New("ingestion record directory overlaps or cannot be separated from models or management targets")
	}
	if management.ProposalStore != nil && !separateDirectories(directory, management.ProposalStore.Directory) ||
		management.BackupStore != nil && !separateDirectories(directory, management.BackupStore.Directory) ||
		benchmark != nil && !separateDirectories(directory, benchmark.ReportDirectory) {
		return errors.New("ingestion directory overlaps or cannot be separated from management or benchmark stores")
	}
	if management.ProposalStore != nil && !separateDirectories(records, management.ProposalStore.Directory) ||
		management.BackupStore != nil && !separateDirectories(records, management.BackupStore.Directory) ||
		benchmark != nil && !separateDirectories(records, benchmark.ReportDirectory) {
		return errors.New("ingestion record directory overlaps or cannot be separated from management or benchmark stores")
	}
	return nil
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
	if req.URL.Path == recordsPath || strings.HasPrefix(req.URL.Path, recordsPath+"/") {
		r.serveIngestionRecords(w, req, state, recordsPath)
		return
	}
	const jobsPath = "/management/v1/ingestion/jobs"
	if req.URL.Path == jobsPath && req.Method == http.MethodPost {
		r.startIngestionJob(w, req, state)
		return
	}
	if !strings.HasPrefix(req.URL.Path, jobsPath+"/") {
		http.NotFound(w, req)
		return
	}
	id := strings.TrimPrefix(req.URL.Path, jobsPath+"/")
	cancel := req.Method == http.MethodPost && strings.HasSuffix(id, "/cancel")
	if cancel {
		id = strings.TrimSuffix(id, "/cancel")
		if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
			http.Error(w, "request body not allowed", http.StatusBadRequest)
			return
		}
	} else if req.Method != http.MethodGet || strings.Contains(id, "/") {
		http.NotFound(w, req)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, "management configuration changed or service stopped", http.StatusServiceUnavailable)
		return
	}
	if cancel {
		err := r.ingestion.jobs.Cancel(id)
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
	var job ingestion.JobStatus
	var err error
	if id == "current" {
		job, err = r.ingestion.jobs.Current()
	} else {
		job, err = r.ingestion.jobs.Status(id)
	}
	r.lifecycleMu.Unlock()
	if err != nil {
		http.NotFound(w, req)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, job)
}

func (r *Reloader) serveIngestionRecords(w http.ResponseWriter, req *http.Request, state *reloaderState, path string) {
	id := strings.TrimPrefix(req.URL.Path, path)
	acknowledge := req.Method == http.MethodPost && strings.HasPrefix(id, "/") && strings.HasSuffix(id, "/acknowledge")
	if acknowledge {
		id = strings.TrimSuffix(strings.TrimPrefix(id, "/"), "/acknowledge")
		var input struct {
			Acknowledge bool `json:"acknowledge_uncertain_outcome"`
		}
		if err := decodeBenchmarkJSON(req, &input); err != nil || !input.Acknowledge {
			http.Error(w, "explicit acknowledgment is required", http.StatusBadRequest)
			return
		}
	} else if req.Method == http.MethodGet && strings.HasPrefix(id, "/") {
		id = strings.TrimPrefix(id, "/")
	} else if req.Method != http.MethodGet || id != "" {
		http.NotFound(w, req)
		return
	}
	if strings.Contains(id, "/") || (id == "" && req.URL.Path != path) || (acknowledge && id == "") {
		http.NotFound(w, req)
		return
	}
	r.lifecycleMu.Lock()
	if r.stopped || r.state.Load() != state {
		r.lifecycleMu.Unlock()
		http.Error(w, "management configuration changed or service stopped", http.StatusServiceUnavailable)
		return
	}
	if id == "" {
		records, err := r.ingestion.jobs.Records()
		r.lifecycleMu.Unlock()
		if err != nil {
			r.logger.Warn("ingestion record listing failed", "error", err)
			http.Error(w, "ingestion records unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(r.logger, w, http.StatusOK, map[string]any{"records": records})
		return
	}
	var record ingestion.Record
	var err error
	if acknowledge {
		record, err = r.ingestion.jobs.Acknowledge(id)
	} else {
		record, err = r.ingestion.jobs.Record(id)
	}
	r.lifecycleMu.Unlock()
	if err != nil {
		switch {
		case errors.Is(err, ingestion.ErrRecordNotFound):
			http.NotFound(w, req)
		case errors.Is(err, ingestion.ErrRecordReview):
			http.Error(w, "record is not available for acknowledgment", http.StatusConflict)
		default:
			r.logger.Warn("ingestion record access failed", "error", err)
			http.Error(w, "ingestion records unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	writeJSON(r.logger, w, http.StatusOK, record)
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
		http.Error(w, "management configuration changed or service stopped", http.StatusServiceUnavailable)
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
			http.Error(w, "ingestion records unavailable", http.StatusServiceUnavailable)
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
