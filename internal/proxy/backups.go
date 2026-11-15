package proxy

import (
	"errors"
	"net/http"
	"strings"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func (r *Reloader) serveBackups(w http.ResponseWriter, req *http.Request, state *reloaderState) {
	const base = "/management/v1/backups"
	if req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return
	}
	if req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		http.Error(w, "backup operations do not accept a request body", http.StatusBadRequest)
		return
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return
	}
	management := state.management
	directory := management.BackupStore.Directory
	if !separateProposalDirectory(directory, management.Targets) ||
		management.ProposalStore != nil && !separateDirectories(directory, management.ProposalStore.Directory) {
		writeBackupError(w, modelconfig.ErrBackupStore)
		return
	}
	var result any
	var err error
	if req.URL.Path == base && req.Method == http.MethodGet {
		var backups []modelconfig.BackupSummary
		backups, err = modelconfig.ListBackups(directory)
		result = struct {
			Backups []modelconfig.BackupSummary `json:"backups"`
		}{Backups: backups}
	} else if strings.HasPrefix(req.URL.Path, base+"/") &&
		(req.Method == http.MethodGet || req.Method == http.MethodDelete) {
		id := strings.TrimPrefix(req.URL.Path, base+"/")
		if req.Method == http.MethodGet {
			result, err = modelconfig.ReadBackupInfo(directory, id)
		} else {
			r.lifecycleMu.Lock()
			if r.stopped || r.state.Load() != state {
				r.lifecycleMu.Unlock()
				http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
				return
			}
			err = modelconfig.DeleteBackup(directory, id)
			r.lifecycleMu.Unlock()
		}
	} else {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err != nil {
		writeBackupError(w, err)
		return
	}
	if req.Method != http.MethodDelete && r.state.Load() != state {
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
		return
	}
	if req.Method == http.MethodDelete {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, result)
}

func writeBackupError(w http.ResponseWriter, err error) {
	if errors.Is(err, modelconfig.ErrBackupNotFound) {
		http.Error(w, "backup not found", http.StatusNotFound)
		return
	}
	http.Error(w, "backup store unavailable or invalid; operation may not be durable", http.StatusServiceUnavailable)
}
