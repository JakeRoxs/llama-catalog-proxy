package proxy

import (
	"bytes"
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/benchmark"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

//go:embed benchmark_ui/index.html benchmark_ui/style.css benchmark_ui/app.js
var benchmarkAssets embed.FS

const maxBenchmarkJSONBytes = 8192

type benchmarkRuntime struct {
	config         config.BenchmarkConfig
	diagnosticsKey string
	client         *benchmark.APIClient
	jobs           *benchmark.JobManager
}

func newBenchmarkRuntime(cfg config.Config, logger *slog.Logger) (*benchmarkRuntime, error) {
	if cfg.Benchmark == nil {
		return nil, nil
	}
	directory := cfg.Benchmark.ReportDirectory
	if err := validateBenchmarkDirectory(directory); err != nil {
		return nil, err
	}
	if cfg.Management != nil {
		if err := validateBenchmarkManagementDirectories(directory, cfg.Management); err != nil {
			return nil, err
		}
	}
	client, err := benchmark.NewAPIClient(cfg.Benchmark.ProxyOrigin, cfg.DiagnosticsAPIKey)
	if err != nil {
		return nil, err
	}
	jobs, err := benchmark.NewJobManager(context.Background(), directory, logger)
	if err != nil {
		return nil, err
	}
	return &benchmarkRuntime{config: *cfg.Benchmark, diagnosticsKey: cfg.DiagnosticsAPIKey, client: client, jobs: jobs}, nil
}

func benchmarkContainsPath(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// validateBenchmarkManagementDirectories verifies that the benchmark report
// directory does not overlap any management directory (targets, proposal store,
// or backup store). It is a no-op when management is nil.
func validateBenchmarkManagementDirectories(directory string, management *config.ManagementConfig) error {
	if management == nil {
		return nil
	}
	resolved, err := filepath.EvalSymlinks(directory)
	if err != nil {
		return errors.New("benchmark report directory could not be resolved")
	}
	var paths []string
	for _, target := range management.Targets {
		paths = append(paths, target.Directory)
	}
	if management.ProposalStore != nil {
		paths = append(paths, management.ProposalStore.Directory)
	}
	if management.BackupStore != nil {
		paths = append(paths, management.BackupStore.Directory)
	}
	for _, path := range paths {
		other, err := filepath.EvalSymlinks(path)
		if err != nil {
			return errors.New("management directory could not be resolved for benchmark isolation")
		}
		if benchmarkContainsPath(resolved, other) || benchmarkContainsPath(other, resolved) {
			return errors.New("benchmark report directory overlaps a management directory")
		}
	}
	return nil
}

func (r *Reloader) serveBenchmark(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if req.URL.RawQuery != "" || !sameBenchmarkOrigin(req) {
		http.Error(w, "invalid request origin or query", http.StatusForbidden)
		return
	}
	if req.Method == http.MethodGet && r.serveBenchmarkAsset(w, req) {
		return
	}
	if !publicBenchmarkReadOnlyRequest(r.benchmark.config, req) && !benchmarkBearerAuthorized(req, r.benchmark.config.Token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch {
	case req.URL.Path == "/benchmark/v1/jobs" && req.Method == http.MethodPost:
		r.startBenchmarkJob(w, req)
	case strings.HasPrefix(req.URL.Path, "/benchmark/v1/jobs/"):
		r.benchmarkJob(w, req)
	case req.URL.Path == "/benchmark/v1/runs" && req.Method == http.MethodGet:
		runs, truncated, err := benchmark.ListRuns(r.benchmark.config.ReportDirectory)
		if err != nil {
			r.logger.Warn("benchmark report listing failed", "error", err)
			http.Error(w, "benchmark report listing failed", http.StatusInternalServerError)
			return
		}
		writeJSON(r.logger, w, http.StatusOK, map[string]any{"runs": runs, "truncated": truncated})
	case strings.HasPrefix(req.URL.Path, "/benchmark/v1/runs/") && req.Method == http.MethodGet:
		id := strings.TrimPrefix(req.URL.Path, "/benchmark/v1/runs/")
		record, err := benchmark.ReadRun(r.benchmark.config.ReportDirectory, id)
		if err != nil {
			r.benchmarkReportError(w, err)
			return
		}
		writeJSON(r.logger, w, http.StatusOK, record)
	case req.URL.Path == "/benchmark/v1/compare" && req.Method == http.MethodPost:
		r.compareBenchmarkRuns(w, req)
	default:
		http.NotFound(w, req)
	}
}

func (r *Reloader) serveBenchmarkAsset(w http.ResponseWriter, req *http.Request) bool {
	var path, contentType string
	switch req.URL.Path {
	case "/benchmark":
		http.Redirect(w, req, "/benchmark/", http.StatusTemporaryRedirect)
		return true
	case "/benchmark/":
		path, contentType = "benchmark_ui/index.html", "text/html; charset=utf-8"
	case "/benchmark/style.css":
		path, contentType = "benchmark_ui/style.css", "text/css; charset=utf-8"
	case "/benchmark/app.js":
		path, contentType = "benchmark_ui/app.js", "text/javascript; charset=utf-8"
	default:
		return false
	}
	data, err := benchmarkAssets.ReadFile(path)
	if err != nil {
		r.logger.Error("benchmark UI asset unavailable", "error", err)
		http.Error(w, "benchmark UI unavailable", http.StatusInternalServerError)
		return true
	}
	if path == "benchmark_ui/index.html" {
		publicReadOnly := `data-public-read-only="false"`
		if r.benchmark.config.PublicReadOnly {
			publicReadOnly = `data-public-read-only="true"`
		}
		data = bytes.Replace(data, []byte(`data-public-read-only="false"`), []byte(publicReadOnly), 1)
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if _, err := w.Write(data); err != nil {
		r.logger.Warn("benchmark UI response failed", "error", err)
	}
	return true
}

func benchmarkBearerAuthorized(req *http.Request, token string) bool {
	authorization := req.Header.Values("Authorization")
	return len(authorization) == 1 && strings.HasPrefix(authorization[0], "Bearer ") &&
		subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(authorization[0], "Bearer ")), []byte(token)) == 1
}

func publicBenchmarkReadOnlyRequest(config config.BenchmarkConfig, req *http.Request) bool {
	return config.PublicReadOnly && req.Method == http.MethodGet && req.URL.Path == "/benchmark/v1/runs" &&
		req.URL.RawQuery == "" && req.URL.RawPath == "" && req.URL.EscapedPath() == req.URL.Path
}

func sameBenchmarkOrigin(req *http.Request) bool {
	origins := req.Header.Values("Origin")
	if len(origins) == 0 {
		return true
	}
	if len(origins) != 1 {
		return false
	}
	origin, err := url.Parse(origins[0])
	return err == nil && origin != nil && (origin.Scheme == "http" || origin.Scheme == "https") &&
		origin.Host == req.Host && origin.User == nil && origin.Path == "" && origin.RawQuery == "" && origin.Fragment == ""
}

func decodeBenchmarkJSON(req *http.Request, dst any) error {
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" || req.Body == nil {
		return errors.New("JSON request required")
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxBenchmarkJSONBytes+1))
	if err != nil || len(data) > maxBenchmarkJSONBytes {
		return errors.New("request is unavailable or too large")
	}
	if err := validateManagementJSON(data); err != nil {
		return errors.New("invalid JSON request")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return errors.New("invalid JSON request")
	}
	return nil
}

func (r *Reloader) startBenchmarkJob(w http.ResponseWriter, req *http.Request) {
	options, err := decodeBenchmarkJobRequest(req, r.benchmark.client)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	job, err := r.benchmark.jobs.Start(options)
	if err != nil {
		if errors.Is(err, benchmark.ErrJobActive) {
			http.Error(w, "benchmark job already active", http.StatusConflict)
		} else if errors.Is(err, benchmark.ErrJobStopped) {
			http.Error(w, "benchmark service stopped", http.StatusServiceUnavailable)
		} else {
			http.Error(w, "invalid or unauthorized benchmark request", http.StatusBadRequest)
		}
		return
	}
	writeJSON(r.logger, w, http.StatusAccepted, job)
}

// benchmarkJobRequest is the decoded JSON body for starting a benchmark job.
type benchmarkJobRequest struct {
	Backend           string               `json:"backend"`
	Model             string               `json:"model"`
	Temperature       *float64             `json:"temperature"`
	TopP              *float64             `json:"top_p"`
	MaxOutput         *int                 `json:"max_output_tokens"`
	Repetitions       *int                 `json:"repetitions"`
	CacheState        benchmark.CacheState `json:"cache_state"`
	TimeBudgetSeconds *int                 `json:"time_budget_seconds"`
	TokenBudget       *int                 `json:"token_budget"`
	AllowInterference bool                 `json:"allow_interference"`
}

// decodeBenchmarkJobRequest decodes the job request body and constructs the
// benchmark RunOptions with defaults and validated overrides. Any returned
// error is a client error (HTTP 400) suitable for http.Error with err.Error().
func decodeBenchmarkJobRequest(req *http.Request, client *benchmark.APIClient) (benchmark.RunOptions, error) {
	var input benchmarkJobRequest
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		return benchmark.RunOptions{}, err
	}
	options := benchmark.RunOptions{
		Client:            client,
		Backend:           input.Backend,
		Model:             input.Model,
		TopP:              1,
		MaxOutput:         64,
		Repetitions:       1,
		CacheState:        benchmark.CacheUnknown,
		TimeBudget:        benchmark.MaxRunDuration,
		TokenBudget:       benchmark.MaxTotalTokenBudget,
		AllowInterference: input.AllowInterference,
	}
	if input.Temperature != nil {
		options.Temperature = *input.Temperature
	}
	if input.TopP != nil {
		options.TopP = *input.TopP
	}
	if input.MaxOutput != nil {
		options.MaxOutput = *input.MaxOutput
	}
	if input.Repetitions != nil {
		options.Repetitions = *input.Repetitions
	}
	if input.CacheState != "" {
		options.CacheState = input.CacheState
	}
	if input.TimeBudgetSeconds != nil {
		if *input.TimeBudgetSeconds < 1 || *input.TimeBudgetSeconds > int(benchmark.MaxRunDuration/time.Second) {
			return benchmark.RunOptions{}, errors.New("invalid time budget")
		}
		options.TimeBudget = time.Duration(*input.TimeBudgetSeconds) * time.Second
	}
	if input.TokenBudget != nil {
		options.TokenBudget = *input.TokenBudget
	}
	if options.MaxOutput < 1 || options.Repetitions < 1 || options.TokenBudget < 1 {
		return benchmark.RunOptions{}, errors.New("benchmark limits must be positive")
	}
	if options.Repetitions > benchmark.MaxRepetitions {
		return benchmark.RunOptions{}, errors.New("repetitions exceeds maximum allowed value")
	}
	return options, nil
}

func (r *Reloader) benchmarkJob(w http.ResponseWriter, req *http.Request) {
	id := strings.TrimPrefix(req.URL.Path, "/benchmark/v1/jobs/")
	if req.Method == http.MethodPost && strings.HasSuffix(id, "/cancel") {
		id = strings.TrimSuffix(id, "/cancel")
		if err := r.benchmark.jobs.Cancel(id); err != nil {
			http.Error(w, "benchmark job unavailable or finished", http.StatusConflict)
			return
		}
		writeJSON(r.logger, w, http.StatusAccepted, map[string]string{"job_id": id, "state": "cancel_requested"})
		return
	}
	if req.Method != http.MethodGet {
		http.NotFound(w, req)
		return
	}
	var job benchmark.JobStatus
	var err error
	if id == "current" {
		job, err = r.benchmark.jobs.Current()
	} else {
		job, err = r.benchmark.jobs.Status(id)
	}
	if err != nil {
		http.Error(w, "benchmark job unavailable", http.StatusNotFound)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, job)
}

func (r *Reloader) compareBenchmarkRuns(w http.ResponseWriter, req *http.Request) {
	var input struct {
		Baseline  string `json:"baseline"`
		Candidate string `json:"candidate"`
	}
	if err := decodeBenchmarkJSON(req, &input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	baseline, err := benchmark.ReadRun(r.benchmark.config.ReportDirectory, input.Baseline)
	if err != nil {
		r.benchmarkReportError(w, err)
		return
	}
	candidate, err := benchmark.ReadRun(r.benchmark.config.ReportDirectory, input.Candidate)
	if err != nil {
		r.benchmarkReportError(w, err)
		return
	}
	comparison, err := benchmark.Compare(baseline, candidate)
	if err != nil {
		r.logger.Warn("benchmark comparison failed", "error", err)
		http.Error(w, "benchmark reports cannot be compared", http.StatusBadRequest)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, comparison)
}

func (r *Reloader) benchmarkReportError(w http.ResponseWriter, err error) {
	if errors.Is(err, benchmark.ErrRunNotFound) {
		http.Error(w, "benchmark report not found", http.StatusNotFound)
		return
	}
	r.logger.Warn("benchmark report read failed", "error", err)
	http.Error(w, "benchmark report could not be read", http.StatusInternalServerError)
}
