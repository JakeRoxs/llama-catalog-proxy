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

// errHandled is a sentinel returned by dispatch helpers when they have already
// sent an HTTP response and the caller must stop processing.
var errHandled = errors.New("response already sent")

// isValidControlRequest reports whether the request passes the gateway checks:
// private host, no origin, no query, no forced query, no raw path.
func isValidControlRequest(req *http.Request) bool {
	return (req.Host == "localhost" || req.Host == "comfyui-control") &&
		req.Header.Get("Origin") == "" &&
		req.URL.RawQuery == "" &&
		!req.URL.ForceQuery &&
		req.URL.RawPath == ""
}

// handleStatusEndpoint responds to the /v1/lease/status endpoint. It returns
// true when the request was handled and processing should stop.
func (c *comfyUIControl) handleStatusEndpoint(w http.ResponseWriter, req *http.Request) bool {
	if req.URL.Path != "/v1/lease/status" {
		return false
	}
	if req.Method != http.MethodGet || req.ContentLength != 0 || len(req.TransferEncoding) != 0 {
		c.respond(w, http.StatusBadRequest, nil, "invalid_status_request", "")
		return true
	}
	c.respond(w, http.StatusOK, nil, "", "")
	return true
}

// knownOperation reports whether the path is a recognized control operation.
func knownOperation(path string) (string, bool) {
	switch path {
	case "/v1/lease/acquire", "/v1/lease/activate", "/v1/lease/release", "/v1/lease/invalidate", "/v1/lease/reconcile":
		return path, true
	default:
		return "", false
	}
}

// isValidOperationRequest reports whether the request passes operation
// validation: POST method, exactly one Content-Type header of
// application/json, no content encoding.
func isValidOperationRequest(req *http.Request) bool {
	return req.Method == http.MethodPost &&
		len(req.Header.Values(contentTypeHeader)) == 1 &&
		req.Header.Get(contentTypeHeader) == "application/json" &&
		req.Header.Get("Content-Encoding") == ""
}

// acquireSlot attempts to reserve one of the bounded concurrency slots. It
// returns true when a slot was acquired; the caller must release it.
func (c *comfyUIControl) acquireSlot() bool {
	select {
	case c.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseSlot frees a previously acquired concurrency slot.
func (c *comfyUIControl) releaseSlot() {
	<-c.slots
}

// decodeRequestBody decodes and validates the request body. It responds to the
// caller directly on error and returns errHandled to stop processing.
func (c *comfyUIControl) decodeRequestBody(w http.ResponseWriter, req *http.Request) (*comfyUIControlRequest, error) {
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
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.respond(w, http.StatusRequestEntityTooLarge, nil, "request_too_large", "")
		} else {
			c.respond(w, http.StatusBadRequest, nil, "invalid_protocol_body", "")
		}
		return nil, errHandled
	}
	if input.Version != 1 {
		c.respond(w, http.StatusBadRequest, nil, "invalid_protocol_body", "")
		return nil, errHandled
	}
	return &input, nil
}

// handleReconcile responds to reconcile requests, which are not bound to
// exclusive local admission. It returns true when handled.
func (c *comfyUIControl) handleReconcile(w http.ResponseWriter, req *http.Request) bool {
	if req.URL.Path != "/v1/lease/reconcile" {
		return false
	}
	// A local client's assertion is not proof that external load paths are closed.
	c.respond(w, http.StatusNotImplemented, nil, "exclusive_reconciliation_not_bound", "")
	return true
}

// dispatchOperation routes the operation to the coordinator. It responds to the
// caller directly when a terminal response is sent and returns errHandled. On
// success it returns the operation lease ID to echo in the response.
func (c *comfyUIControl) dispatchOperation(w http.ResponseWriter, req *http.Request, input *comfyUIControlRequest, operation string, timeout time.Duration) (string, error) {
	operationID := input.LeaseID
	if req.URL.Path == "/v1/lease/acquire" {
		if input.LeaseID != "" {
			c.respond(w, http.StatusBadRequest, nil, "unexpected_lease_id", "")
			return "", errHandled
		}
		var err error
		operationID, err = c.coordinator.beginLease()
		if err != nil {
			return "", err
		}
	} else {
		decoded, decodeErr := hex.DecodeString(input.LeaseID)
		if decodeErr != nil || len(decoded) != 16 {
			c.respond(w, http.StatusBadRequest, nil, "invalid_lease_id", "")
			return "", errHandled
		}
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer cancel()
		var err error
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
				return "", errHandled
			}
		}
		if err != nil {
			return "", err
		}
	}
	return operationID, nil
}

// respondError maps a coordinator error to an HTTP response.
func (c *comfyUIControl) respondError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errComfyUIStaleLease):
		c.respond(w, http.StatusConflict, nil, "stale_lease", "")
	case errors.Is(err, errComfyUIAdmissionBlocked):
		c.respond(w, http.StatusConflict, nil, "admission_blocked", "")
	default:
		c.respond(w, http.StatusServiceUnavailable, nil, "release_unknown", "")
	}
}

func (c *comfyUIControl) serve(w http.ResponseWriter, req *http.Request, timeout time.Duration) {
	if !isValidControlRequest(req) {
		c.respond(w, http.StatusBadRequest, nil, "invalid_request", "")
		return
	}
	if c.handleStatusEndpoint(w, req) {
		return
	}
	operation, ok := knownOperation(req.URL.Path)
	if !ok {
		c.respond(w, http.StatusNotFound, nil, "unknown_operation", "")
		return
	}
	if !isValidOperationRequest(req) {
		c.respond(w, http.StatusBadRequest, nil, "invalid_operation_request", "")
		return
	}
	if !c.acquireSlot() {
		c.respond(w, http.StatusServiceUnavailable, nil, "control_busy", "")
		return
	}
	defer c.releaseSlot()
	input, err := c.decodeRequestBody(w, req)
	if err != nil {
		return
	}
	if c.handleReconcile(w, req) {
		return
	}
	operationID, err := c.dispatchOperation(w, req, input, operation, timeout)
	if err == errHandled {
		return
	}
	if err != nil {
		c.respondError(w, err)
		return
	}
	c.respond(w, http.StatusOK, nil, "", operationID)
}
