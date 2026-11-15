package proxy

import (
	"bytes"
	"crypto/subtle"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const maxManagementRequest = 16 << 10

func (r *Reloader) ManagementTLSConfig() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			certificate := r.state.Load().managementCertificate
			if certificate == nil {
				return nil, errors.New("management listener disabled")
			}
			return certificate, nil
		},
	}
}

func (r *Reloader) ManagementHandler() http.Handler {
	return http.HandlerFunc(r.serveManagement)
}

func (r *Reloader) serveManagement(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.TLS == nil {
		http.Error(w, "TLS required", http.StatusForbidden)
		return
	}
	state := r.state.Load()
	management := state.management
	isProposal := req.URL.Path == "/management/v1/proposals" || strings.HasPrefix(req.URL.Path, "/management/v1/proposals/")
	isBackup := req.URL.Path == "/management/v1/backups" || strings.HasPrefix(req.URL.Path, "/management/v1/backups/")
	isIngestion := req.URL.Path == "/management/v1/ingestion" || strings.HasPrefix(req.URL.Path, "/management/v1/ingestion/")
	isWrite := strings.HasPrefix(req.URL.Path, "/management/v1/proposals/") && strings.HasSuffix(req.URL.Path, "/apply") ||
		strings.HasPrefix(req.URL.Path, "/management/v1/backups/") && strings.HasSuffix(req.URL.Path, "/rollback")
	auth := req.Header.Values("Authorization")
	token := ""
	if management != nil {
		if isWrite {
			token = management.WriteToken
		} else if isProposal && management.ProposalStore != nil {
			token = management.ProposalStore.Token
		} else if isBackup && management.BackupStore != nil {
			token = management.BackupStore.Token
		} else if isIngestion && management.Ingestion != nil {
			token = management.Ingestion.Token
		} else if !isProposal && !isBackup && !isIngestion {
			token = management.PreviewToken
		}
	}
	if management == nil || len(auth) != 1 || !strings.HasPrefix(auth[0], "Bearer ") ||
		token == "" || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth[0], "Bearer ")), []byte(token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if isWrite {
		r.serveMutation(w, req, state)
		return
	}
	if isIngestion {
		r.serveIngestion(w, req, state)
		return
	}
	if isProposal {
		r.serveProposals(w, req, state)
		return
	}
	if isBackup {
		r.serveBackups(w, req, state)
		return
	}
	if req.URL.Path == "/management/v1/creation/preview" {
		select {
		case r.managementSlots <- struct{}{}:
			defer func() { <-r.managementSlots }()
		default:
			http.Error(w, "management service busy", http.StatusServiceUnavailable)
			return
		}
		r.serveCreation(w, req, state, false)
		return
	}
	if (req.URL.Path != "/management/v1/preview" && req.URL.Path != "/management/v1/settings") || req.URL.RawQuery != "" {
		http.NotFound(w, req)
		return
	}
	if req.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	contentType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || contentType != "application/json" || req.Header.Get("Content-Encoding") != "" {
		http.Error(w, "JSON request required", http.StatusUnsupportedMediaType)
		return
	}
	select {
	case r.managementSlots <- struct{}{}:
		defer func() { <-r.managementSlots }()
	default:
		http.Error(w, "management service busy", http.StatusServiceUnavailable)
		return
	}
	if req.Body == nil {
		http.Error(w, "request body required", http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(io.LimitReader(req.Body, maxManagementRequest+1))
	if err != nil {
		http.Error(w, "request could not be read", http.StatusBadRequest)
		return
	}
	if len(data) > maxManagementRequest {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := validateManagementJSON(data); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	type targetInput struct {
		TargetID string `json:"target_id"`
		ModelID  string `json:"model_id"`
	}
	var input targetInput
	var settings modelconfig.CommandSettings
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if req.URL.Path == "/management/v1/preview" {
		var previewInput struct {
			targetInput
			Settings modelconfig.CommandSettings `json:"settings"`
		}
		if err := decoder.Decode(&previewInput); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		input, settings = previewInput.targetInput, previewInput.Settings
	} else {
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
	}
	target, ok := management.Targets[input.TargetID]
	if !ok {
		http.NotFound(w, req)
		return
	}
	var result any
	if req.URL.Path == "/management/v1/preview" {
		result, err = modelconfig.PreviewTarget(target.Directory, target.Filename, input.TargetID, input.ModelID, settings)
	} else {
		result, err = modelconfig.ReadSettingsTarget(target.Directory, target.Filename, input.TargetID, input.ModelID)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if r.state.Load() != state {
		http.Error(w, "management configuration changed", http.StatusServiceUnavailable)
		return
	}
	writeJSON(r.logger, w, http.StatusOK, result)
}

func validateManagementJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := readManagementJSONValue(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func readManagementJSONValue(decoder *json.Decoder, depth int) error {
	if depth > 8 {
		return errors.New("JSON nesting exceeds limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		keys := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			folded := strings.Map(func(r rune) rune {
				min := r
				for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
					if next < min {
						min = next
					}
				}
				return min
			}, name)
			if keys[folded] {
				return errors.New("duplicate object key")
			}
			keys[folded] = true
			if err := readManagementJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case json.Delim('['):
		for decoder.More() {
			if err := readManagementJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	return nil
}
