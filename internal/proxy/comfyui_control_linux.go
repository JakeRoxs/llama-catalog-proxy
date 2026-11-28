package proxy

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var errComfyUIControlUnavailable = errors.New("private ComfyUI control socket unavailable")

type comfyUIControl struct {
	server      *http.Server
	coordinator *comfyUICoordinator
	path        string
	socketInfo  os.FileInfo
	cancel      context.CancelFunc
	done        chan error
	slots       chan struct{}
	once        sync.Once
	closeErr    error
}

type comfyUIControlRequest struct {
	Version int    `json:"version"`
	LeaseID string `json:"lease_id,omitempty"`
}

type comfyUIControlResponse struct {
	Version          int                   `json:"version"`
	State            string                `json:"state"`
	LeaseID          string                `json:"lease_id,omitempty"`
	OperationLeaseID string                `json:"operation_lease_id,omitempty"`
	Submissions      int                   `json:"submissions"`
	Competing        int                   `json:"competing"`
	Receipt          *ComfyUIReleaseResult `json:"receipt,omitempty"`
	Error            string                `json:"error,omitempty"`
}

// This constructor is intentionally not called by the application yet. A private
// socket alone cannot establish exclusivity over external scheduler load paths.
func newComfyUIControl(socketPath string, coordinator *comfyUICoordinator, operationTimeout time.Duration) (*comfyUIControl, error) {
	if coordinator == nil || operationTimeout < 100*time.Millisecond || operationTimeout > 30*time.Minute ||
		!filepath.IsAbs(socketPath) || filepath.Clean(socketPath) != socketPath {
		return nil, errComfyUIControlUnavailable
	}
	directory := filepath.Dir(socketPath)
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return nil, errComfyUIControlUnavailable
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	resolved, err := filepath.EvalSymlinks(directory)
	if !ok || stat.Uid != uint32(os.Geteuid()) || err != nil || resolved != directory {
		return nil, errComfyUIControlUnavailable
	}
	if _, err := os.Lstat(socketPath); !errors.Is(err, os.ErrNotExist) {
		// Do not guess whether an existing socket is stale or remove another file.
		return nil, errComfyUIControlUnavailable
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, errComfyUIControlUnavailable
	}
	listener.SetUnlinkOnClose(false)
	socketInfo, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		return nil, errComfyUIControlUnavailable
	}
	cleanup := func() error {
		current, err := os.Lstat(socketPath)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil || !os.SameFile(socketInfo, current) {
			return errComfyUIControlUnavailable
		}
		return os.Remove(socketPath)
	}
	if socketInfo.Mode()&os.ModeSocket == 0 {
		_ = listener.Close()
		return nil, errComfyUIControlUnavailable
	}
	if os.Chmod(socketPath, 0o600) != nil {
		_ = listener.Close()
		_ = cleanup()
		return nil, errComfyUIControlUnavailable
	}
	ctx, cancel := context.WithCancel(context.Background())
	control := &comfyUIControl{
		coordinator: coordinator, path: socketPath, socketInfo: socketInfo,
		cancel: cancel, done: make(chan error, 1), slots: make(chan struct{}, 8),
	}
	control.server = &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { control.serve(w, req, operationTimeout) }),
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: operationTimeout + 5*time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 4096,
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	coordinator.blockAdmission()
	go func() {
		err := control.server.Serve(listener)
		coordinator.blockAdmission()
		cancel()
		control.done <- err
	}()
	return control, nil
}

func (c *comfyUIControl) Close() error {
	c.once.Do(func() {
		c.coordinator.blockAdmission()
		c.cancel()
		closeErr := c.server.Close()
		serveErr := <-c.done
		current, err := os.Lstat(c.path)
		if err == nil {
			if os.SameFile(c.socketInfo, current) {
				err = os.Remove(c.path)
			} else {
				err = errComfyUIControlUnavailable
			}
		} else if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
		if closeErr != nil || (serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed)) || err != nil {
			c.closeErr = errComfyUIControlUnavailable
		}
	})
	return c.closeErr
}

func (c *comfyUIControl) respond(w http.ResponseWriter, status int, receipt *ComfyUIReleaseResult, code, operationID string) {
	state := c.coordinator.snapshot()
	w.Header().Set(contentTypeHeader, "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(comfyUIControlResponse{
		Version: 1, State: state.State, LeaseID: state.LeaseID, Submissions: state.Submissions,
		Competing: state.Competing, Receipt: receipt, Error: code, OperationLeaseID: operationID,
	})
}

func (c *comfyUIControl) serve(w http.ResponseWriter, req *http.Request, timeout time.Duration) {
	if (req.Host != "localhost" && req.Host != "comfyui-control") || req.Header.Get("Origin") != "" ||
		req.URL.RawQuery != "" || req.URL.ForceQuery || req.URL.RawPath != "" {
		c.respond(w, http.StatusBadRequest, nil, "invalid_request", "")
		return
	}
	if req.URL.Path == "/v1/lease/status" {
		if req.Method != http.MethodGet || req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
			c.respond(w, http.StatusBadRequest, nil, "invalid_status_request", "")
			return
		}
		c.respond(w, http.StatusOK, nil, "", "")
		return
	}
	switch req.URL.Path {
	case "/v1/lease/acquire", "/v1/lease/activate", "/v1/lease/release", "/v1/lease/invalidate", "/v1/lease/reconcile":
	default:
		c.respond(w, http.StatusNotFound, nil, "unknown_operation", "")
		return
	}
	if req.Method != http.MethodPost || len(req.Header.Values(contentTypeHeader)) != 1 ||
		req.Header.Get(contentTypeHeader) != "application/json" || req.Header.Get("Content-Encoding") != "" {
		c.respond(w, http.StatusBadRequest, nil, "invalid_operation_request", "")
		return
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	default:
		c.respond(w, http.StatusServiceUnavailable, nil, "control_busy", "")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 2048))
	decoder.DisallowUnknownFields()
	var input comfyUIControlRequest
	err := decoder.Decode(&input)
	if err == nil {
		err = decoder.Decode(new(any))
		if err == io.EOF {
			err = nil
		} else if err == nil {
			err = errors.New("extra JSON value")
		}
	}
	if err != nil || input.Version != 1 {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.respond(w, http.StatusRequestEntityTooLarge, nil, "request_too_large", "")
		} else {
			c.respond(w, http.StatusBadRequest, nil, "invalid_protocol_body", "")
		}
		return
	}
	if req.URL.Path == "/v1/lease/reconcile" {
		// A local client's assertion is not proof that external load paths are closed.
		c.respond(w, http.StatusNotImplemented, nil, "exclusive_reconciliation_not_bound", "")
		return
	}
	operationID := input.LeaseID
	if req.URL.Path == "/v1/lease/acquire" {
		if input.LeaseID != "" {
			c.respond(w, http.StatusBadRequest, nil, "unexpected_lease_id", "")
			return
		}
		operationID, err = c.coordinator.beginLease()
	} else {
		decoded, decodeErr := hex.DecodeString(input.LeaseID)
		if decodeErr != nil || len(decoded) != 16 {
			c.respond(w, http.StatusBadRequest, nil, "invalid_lease_id", "")
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		switch req.URL.Path {
		case "/v1/lease/activate":
			err = c.coordinator.activate(input.LeaseID)
		case "/v1/lease/invalidate":
			err = c.coordinator.invalidate(input.LeaseID)
		case "/v1/lease/release":
			var receipt ComfyUIReleaseResult
			receipt, err = c.coordinator.release(ctx, input.LeaseID)
			if err == nil {
				c.respond(w, http.StatusOK, &receipt, "", operationID)
				return
			}
		}
	}
	if err != nil {
		switch {
		case errors.Is(err, errComfyUIStaleLease):
			c.respond(w, http.StatusConflict, nil, "stale_lease", "")
		case errors.Is(err, errComfyUIAdmissionBlocked):
			c.respond(w, http.StatusConflict, nil, "admission_blocked", "")
		default:
			c.respond(w, http.StatusServiceUnavailable, nil, "release_unknown", "")
		}
		return
	}
	c.respond(w, http.StatusOK, nil, "", operationID)
}
