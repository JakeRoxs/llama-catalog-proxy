package benchmark

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type RunSummary struct {
	RunID     string    `json:"run_id"`
	StartedAt time.Time `json:"started_at"`
	Status    RunStatus `json:"status"`
	Backend   string    `json:"backend"`
	Model     string    `json:"model"`
}

var ErrRunNotFound = errors.New("benchmark run not found")

func ListRuns(directory string) ([]RunSummary, bool, error) {
	const maxEntries, maxRuns = 2048, 100
	file, err := os.Open(directory)
	if err != nil {
		return nil, false, errors.New("benchmark report directory could not be opened")
	}
	defer file.Close()
	entries, err := file.ReadDir(maxEntries + 1)
	if err != nil && err != io.EOF {
		return nil, false, errors.New("benchmark report directory could not be listed")
	}
	if len(entries) > maxEntries {
		return nil, false, errors.New("benchmark report directory exceeds the listing limit")
	}
	type candidate struct {
		id      string
		modTime time.Time
	}
	var candidates []candidate
	for _, entry := range entries {
		id := strings.TrimSuffix(entry.Name(), ".json")
		if !strings.HasSuffix(entry.Name(), ".json") || !runIDPattern.MatchString(id) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil, false, errors.New("benchmark report directory contains an invalid run")
		}
		candidates = append(candidates, candidate{id: id, modTime: info.ModTime()})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].modTime.Equal(candidates[j].modTime) {
			return candidates[i].id < candidates[j].id
		}
		return candidates[i].modTime.After(candidates[j].modTime)
	})
	truncated := len(candidates) > maxRuns
	if truncated {
		candidates = candidates[:maxRuns]
	}
	runs := make([]RunSummary, 0, len(candidates))
	for _, candidate := range candidates {
		record, err := ReadRun(directory, candidate.id)
		if err != nil {
			return nil, false, err
		}
		runs = append(runs, RunSummary{
			RunID: record.RunID, StartedAt: record.StartedAt, Status: record.Status,
			Backend: record.Requested.Backend, Model: record.Requested.Model,
		})
	}
	return runs, truncated, nil
}

func WriteRun(directory string, record RunRecord) (string, error) {
	if err := record.Validate(); err != nil {
		return "", fmt.Errorf("invalid benchmark run: %w", err)
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return "", errors.New("could not encode benchmark run")
	}
	if len(data) > MaxReportBytes {
		return "", errors.New("benchmark report exceeds the size limit")
	}
	name := record.RunID + ".json"
	if err := writeNewReport(directory, name, data); err != nil {
		return "", err
	}
	return filepath.Join(directory, name), nil
}

func ReadRun(directory, runID string) (RunRecord, error) {
	if !runIDPattern.MatchString(runID) {
		return RunRecord{}, ErrRunNotFound
	}
	path := filepath.Join(directory, runID+".json")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return RunRecord{}, ErrRunNotFound
	}
	if err != nil {
		return RunRecord{}, errors.New("benchmark run report could not be opened")
	}
	if !info.Mode().IsRegular() || info.Size() > MaxReportBytes {
		return RunRecord{}, errors.New("benchmark run report is invalid or too large")
	}
	file, err := os.Open(path)
	if err != nil {
		return RunRecord{}, errors.New("benchmark run report could not be opened")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, MaxReportBytes+1))
	if err != nil || len(data) > MaxReportBytes {
		return RunRecord{}, errors.New("benchmark run report could not be read")
	}
	var record RunRecord
	if err := json.Unmarshal(data, &record); err != nil || record.RunID != runID {
		return RunRecord{}, errors.New("benchmark run report is invalid")
	}
	if err := record.Validate(); err != nil {
		return RunRecord{}, errors.New("benchmark run report failed validation")
	}
	return record, nil
}

func WriteComparison(directory string, report ComparisonReport) (string, error) {
	if report.SchemaVersion != SchemaVersion ||
		!runIDPattern.MatchString(report.BaselineRunID) ||
		!runIDPattern.MatchString(report.CandidateRunID) ||
		len(report.Workloads) != len(syntheticWorkloads) {
		return "", errors.New("invalid benchmark comparison")
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", errors.New("could not encode benchmark comparison")
	}
	if len(data) > MaxReportBytes {
		return "", errors.New("benchmark comparison exceeds the size limit")
	}
	name := "comparison-" + report.BaselineRunID + "-" + report.CandidateRunID + ".json"
	if err := writeNewReport(directory, name, data); err != nil {
		return "", err
	}
	return filepath.Join(directory, name), nil
}

func writeNewReport(directory, name string, data []byte) error {
	if filepath.Base(name) != name || name == "." || name == string(filepath.Separator) {
		return errors.New("invalid benchmark report filename")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return errors.New("benchmark report directory could not be created")
	}
	directoryInfo, err := os.Lstat(directory)
	if err != nil || !directoryInfo.IsDir() || directoryInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("benchmark report directory must be a real directory")
	}
	if err := os.Chmod(directory, 0700); err != nil {
		return errors.New("benchmark report directory permissions could not be restricted")
	}

	temp, err := os.CreateTemp(directory, ".benchmark-report-*")
	if err != nil {
		return errors.New("temporary benchmark report could not be created")
	}
	tempName := temp.Name()
	defer os.Remove(tempName)
	if err := temp.Chmod(0600); err != nil {
		_ = temp.Close()
		return errors.New("benchmark report permissions could not be restricted")
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return errors.New("benchmark report could not be written")
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return errors.New("benchmark report could not be synchronized")
	}
	if err := temp.Close(); err != nil {
		return errors.New("benchmark report could not be closed")
	}
	target := filepath.Join(directory, name)
	if err := os.Link(tempName, target); err != nil {
		return errors.New("benchmark report could not be committed without overwriting an existing file")
	}
	if err := os.Remove(tempName); err != nil {
		return errors.New("temporary benchmark report could not be removed")
	}
	dir, err := os.Open(directory)
	if err != nil {
		return errors.New("benchmark report directory could not be opened")
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return errors.New("benchmark report directory could not be synchronized")
	}
	return nil
}
