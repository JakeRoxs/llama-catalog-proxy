package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func comfyUIControlFixture(t *testing.T, c *comfyUICoordinator, timeout time.Duration) (*comfyUIControl, *http.Client) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(directory, "c.sock")
	control, err := newComfyUIControl(socketPath, c, timeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = control.Close() })
	return control, comfyUIControlClient(t, socketPath)
}

func comfyUIControlClient(t *testing.T, socketPath string) *http.Client {
	t.Helper()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func controlOperation(t *testing.T, client *http.Client, operation, leaseID string, status int) comfyUIControlResponse {
	t.Helper()
	method := http.MethodPost
	body := `{"version":1}`
	if leaseID != "" {
		body = `{"version":1,"lease_id":"` + leaseID + `"}`
	}
	if operation == "status" {
		method, body = http.MethodGet, ""
	}
	req, err := http.NewRequest(method, "http://localhost/v1/lease/"+operation, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result comfyUIControlResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != status || result.Version != 1 || response.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("operation=%s status=%d want=%d response=%+v", operation, response.StatusCode, status, result)
	}
	return result
}

func TestComfyUIControlLifecycleAndRestart(t *testing.T) {
	c, control, client, calls := newComfyUICoordinatorFixture(t, time.Second)
	assertFailClosedStartup(t, client, calls)
	assertCoordinatorExclusivityEstablished(t, c)
	acquired := assertAcquireActivateRelease(t, client, calls)
	assertDuplicateReleaseIsCallback(t, client, acquired, calls)
	newLease := assertNewAcquireUsesNewLeaseID(t, client, acquired)
	assertReplyNotRelabeledWithCurrentLease(t, control, newLease, acquired)
	assertStaleReleaseRejected(t, client, acquired)
	assertShutdownAndSocketCleanup(t, control, c)
	assertRestartStartsFresh(t, c, control, calls, newLease)
}

// newComfyUICoordinatorFixture builds a coordinator and control fixture, asserting
// the private socket contract. It returns the coordinator, control, HTTP client,
// and the verifier call counter shared by the lifecycle phases.
func newComfyUICoordinatorFixture(t *testing.T, timeout time.Duration) (*comfyUICoordinator, *comfyUIControl, *http.Client, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		calls.Add(1)
		return confirmedCoordinatorEvidence(), nil
	})
	control, client := comfyUIControlFixture(t, c, timeout)
	assertPrivateSocket(t, control)
	return c, control, client, &calls
}

// assertPrivateSocket verifies the control socket is a private (0o600) socket file.
func assertPrivateSocket(t *testing.T, control *comfyUIControl) {
	t.Helper()
	info, err := os.Lstat(control.path)
	if err != nil || info.Mode().Perm() != 0o600 || info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("private socket mode=%v err=%v", info, err)
	}
}

// assertFailClosedStartup verifies startup rejects mutations before the owner
// establishes exclusivity and that the verifier is not invoked by the control plane.
func assertFailClosedStartup(t *testing.T, client *http.Client, calls *atomic.Int32) {
	t.Helper()
	if result := controlOperation(t, client, "status", "", 200); result.State != "unknown" || result.LeaseID != "" || calls.Load() != 0 {
		t.Fatalf("startup was not fail-closed: %+v calls=%d", result, calls.Load())
	}
	controlOperation(t, client, "acquire", "", http.StatusConflict)
	result := controlOperation(t, client, "reconcile", "", http.StatusNotImplemented)
	if result.Error != "exclusive_reconciliation_not_bound" || calls.Load() != 0 {
		t.Fatal("remote reconciliation inferred exclusive admission or called /free")
	}
}

// assertCoordinatorExclusivityEstablished records the future owner's direct
// reconciliation, which must establish exclusivity before internal admission.
func assertCoordinatorExclusivityEstablished(t *testing.T, c *comfyUICoordinator) {
	t.Helper()
	// The future owner binding must establish exclusivity before this internal call.
	if _, err := c.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// assertAcquireActivateRelease drives acquire, activate, and release, returning the
// acquire response for use by later phases.
func assertAcquireActivateRelease(t *testing.T, client *http.Client, calls *atomic.Int32) comfyUIControlResponse {
	t.Helper()
	acquired := controlOperation(t, client, "acquire", "", 200)
	if acquired.State != "acquiring" || len(acquired.LeaseID) != 32 || acquired.OperationLeaseID != acquired.LeaseID {
		t.Fatalf("acquire response=%+v", acquired)
	}
	controlOperation(t, client, "activate", acquired.LeaseID, 200)
	released := controlOperation(t, client, "release", acquired.LeaseID, 200)
	if released.State != "released" || released.Receipt == nil || released.Receipt.State != "confirmed" || released.OperationLeaseID != acquired.OperationLeaseID {
		t.Fatalf("release response=%+v", released)
	}
	return acquired
}

// assertDuplicateReleaseIsCallback verifies a duplicate release is counted as a
// control callback without re-verifying.
func assertDuplicateReleaseIsCallback(t *testing.T, client *http.Client, acquired comfyUIControlResponse, calls *atomic.Int32) {
	t.Helper()
	controlOperation(t, client, "release", acquired.LeaseID, 200)
	if calls.Load() != 2 {
		t.Fatal("duplicate control callback repeated verification")
	}
}

// assertNewAcquireUsesNewLeaseID verifies re-acquisition after release obtains a
// distinct lease ID.
func assertNewAcquireUsesNewLeaseID(t *testing.T, client *http.Client, acquired comfyUIControlResponse) comfyUIControlResponse {
	t.Helper()
	newLease := controlOperation(t, client, "acquire", "", 200)
	if newLease.LeaseID == acquired.LeaseID {
		t.Fatal("new control acquisition reused old ID")
	}
	return newLease
}

// assertReplyNotRelabeledWithCurrentLease verifies a control reply preserves the
// operation lease ID instead of relabeling it with the current lease snapshot.
func assertReplyNotRelabeledWithCurrentLease(t *testing.T, control *comfyUIControl, newLease, acquired comfyUIControlResponse) {
	t.Helper()
	// Replies distinguish a completed operation from a newer current lease snapshot.
	response := httptest.NewRecorder()
	cached := confirmedCoordinatorEvidence()
	control.respond(response, http.StatusOK, &cached, "", acquired.OperationLeaseID)
	var concurrentReply comfyUIControlResponse
	if err := json.Unmarshal(response.Body.Bytes(), &concurrentReply); err != nil ||
		concurrentReply.LeaseID != newLease.LeaseID || concurrentReply.OperationLeaseID != acquired.OperationLeaseID {
		t.Fatalf("operation evidence was relabeled with current lease: %+v err=%v", concurrentReply, err)
	}
}

// assertStaleReleaseRejected verifies a release against a superseded lease is rejected.
func assertStaleReleaseRejected(t *testing.T, client *http.Client, acquired comfyUIControlResponse) {
	t.Helper()
	if stale := controlOperation(t, client, "release", acquired.LeaseID, 409); stale.Error != "stale_lease" {
		t.Fatalf("stale control callback=%+v", stale)
	}
}

// assertShutdownAndSocketCleanup verifies shutdown clears admission state and
// removes the socket file.
func assertShutdownAndSocketCleanup(t *testing.T, control *comfyUIControl, c *comfyUICoordinator) {
	t.Helper()
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if state := c.snapshot(); state.State != "unknown" || state.LeaseID != "" {
		t.Fatalf("shutdown retained admission or lease: %+v", state)
	}
	if _, err := os.Lstat(control.path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket not removed after close: %v", err)
	}
}

// assertRestartStartsFresh verifies a restart on the same socket path begins with
// no retained state and rejects releases against stale leases.
func assertRestartStartsFresh(t *testing.T, c *comfyUICoordinator, control *comfyUIControl, calls *atomic.Int32, newLease comfyUIControlResponse) {
	t.Helper()
	restarted, err := newComfyUIControl(control.path, c, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	client := comfyUIControlClient(t, control.path)
	if state := controlOperation(t, client, "status", "", 200); state.State != "unknown" || state.LeaseID != "" || calls.Load() != 2 {
		t.Fatalf("restart reused receipts or called verifier: %+v calls=%d", state, calls.Load())
	}
	controlOperation(t, client, "release", newLease.LeaseID, 409)
}

func TestComfyUIControlRestartRetainsOutstandingReservations(t *testing.T) {
	var calls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		calls.Add(1)
		return confirmedCoordinatorEvidence(), nil
	})
	if _, err := c.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	finish, err := c.reserveCompeting()
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	_, client := comfyUIControlFixture(t, c, time.Second)
	state := controlOperation(t, client, "status", "", 200)
	if state.State != "unknown" || state.Competing != 1 || calls.Load() != 1 {
		t.Fatalf("transport startup discarded outstanding work: %+v calls=%d", state, calls.Load())
	}
	if _, err := c.reconcile(context.Background()); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("transport restart allowed reconciliation over outstanding work")
	}
	finish()
	if state := controlOperation(t, client, "status", "", 200); state.State != "unknown" || state.Competing != 0 {
		t.Fatalf("reservation completion reopened admission without evidence: %+v", state)
	}
}

func TestComfyUIControlRejectsUnsafeFiles(t *testing.T) {
	for _, mode := range []os.FileMode{0o755, 0o770} {
		directory := t.TempDir()
		if err := os.Chmod(directory, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := newComfyUIControl(filepath.Join(directory, "c.sock"), newComfyUICoordinator(nil), time.Second); err == nil {
			t.Fatalf("unsafe directory permissions accepted: %o", mode)
		}
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "keep")
	if err := os.WriteFile(path, []byte("unrelated file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newComfyUIControl(path, newComfyUICoordinator(nil), time.Second); err == nil {
		t.Fatal("existing file overwritten by control socket")
	}
	link := filepath.Join(directory, "link")
	if err := os.Symlink(directory, link); err != nil {
		t.Fatal(err)
	}
	if _, err := newComfyUIControl(filepath.Join(link, "c.sock"), newComfyUICoordinator(nil), time.Second); err == nil {
		t.Fatal("symlink directory accepted")
	}
	control, _ := comfyUIControlFixture(t, newComfyUICoordinator(nil), time.Second)
	if _, err := newComfyUIControl(control.path, newComfyUICoordinator(nil), time.Second); err == nil {
		t.Fatal("existing socket replaced")
	}
	if err := os.Remove(control.path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(control.path, []byte("replacement file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := control.Close(); !errors.Is(err, errComfyUIControlUnavailable) {
		t.Fatalf("changed socket path was not reported: %v", err)
	}
	if data, err := os.ReadFile(control.path); err != nil || string(data) != "replacement file" {
		t.Fatalf("cleanup deleted an unrelated replacement: %q %v", data, err)
	}
}

func TestComfyUIControlValidatesProtocolBeforeMutation(t *testing.T) {
	var calls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		calls.Add(1)
		return confirmedCoordinatorEvidence(), nil
	})
	_, client := comfyUIControlFixture(t, c, time.Second)
	for _, test := range []struct {
		name, endpoint, body string
		modify               func(*http.Request)
		status               int
	}{
		{"unknown version", "acquire", `{"version":2}`, nil, 400},
		{"unknown field", "acquire", `{"version":1,"exclusive":true}`, nil, 400},
		{"trailing JSON", "acquire", `{"version":1} {}`, nil, 400},
		{"oversize", "acquire", `{"version":1,"lease_id":"` + strings.Repeat("x", 2050) + `"}`, nil, 413},
		{"invalid ID", "release", `{"version":1,"lease_id":"bad"}`, nil, 400},
		{"unexpected ID", "acquire", `{"version":1,"lease_id":"` + strings.Repeat("a", 32) + `"}`, nil, 400},
		{"query", "acquire?exclusive=true", `{"version":1}`, nil, 400},
		{"origin", "acquire", `{"version":1}`, func(r *http.Request) { r.Header.Set("Origin", "https://browser.example") }, 400},
		{"host", "acquire", `{"version":1}`, func(r *http.Request) { r.Host = "public.example" }, 400},
		{"encoding", "acquire", `{"version":1}`, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, 400},
		{"unknown operation", "load", `{"version":1}`, nil, 404},
		{"wrong method", "activate", `{"version":1}`, func(r *http.Request) { r.Method = http.MethodGet }, 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(http.MethodPost, "http://localhost/v1/lease/"+test.endpoint, strings.NewReader(test.body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			if test.modify != nil {
				test.modify(request)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode != test.status || calls.Load() != 0 || c.snapshot().State != "unknown" {
				t.Fatalf("unsafe protocol request: status=%d calls=%d state=%s", response.StatusCode, calls.Load(), c.snapshot().State)
			}
		})
	}
}

func TestComfyUIControlTimeoutAndCapacityStayBlocked(t *testing.T) {
	var calls atomic.Int32
	c := newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
		if calls.Add(1) > 1 {
			<-ctx.Done()
			return ComfyUIReleaseResult{}, errors.New("private owner credential must not be echoed")
		}
		return confirmedCoordinatorEvidence(), nil
	})
	control, client := comfyUIControlFixture(t, c, 100*time.Millisecond)
	for i := 0; i < cap(control.slots); i++ {
		control.slots <- struct{}{}
	}
	controlOperation(t, client, "status", "", 200)
	if result := controlOperation(t, client, "acquire", "", 503); result.Error != "control_busy" {
		t.Fatalf("mutation concurrency cap not enforced: %+v", result)
	}
	for i := 0; i < cap(control.slots); i++ {
		<-control.slots
	}
	if _, err := c.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease := controlOperation(t, client, "acquire", "", 200)
	controlOperation(t, client, "activate", lease.OperationLeaseID, 200)
	result := controlOperation(t, client, "release", lease.OperationLeaseID, 503)
	data, err := json.Marshal(result)
	if err != nil || result.State != "unknown" || result.Error != "release_unknown" || strings.Contains(string(data), "credential") {
		t.Fatalf("timed out release reopened admission or leaked internals: %+v err=%v", result, err)
	}
	controlOperation(t, client, "release", lease.OperationLeaseID, 409)
	if calls.Load() != 2 {
		t.Fatal("timeout automatically retried verification")
	}
}

func TestComfyUIControlShutdownCancelsVerification(t *testing.T) {
	var calls atomic.Int32
	entered, cancelled := make(chan struct{}), make(chan struct{})
	c := newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			return ComfyUIReleaseResult{}, ctx.Err()
		}
		return confirmedCoordinatorEvidence(), nil
	})
	control, client := comfyUIControlFixture(t, c, time.Second)
	if _, err := c.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease := controlOperation(t, client, "acquire", "", 200)
	controlOperation(t, client, "activate", lease.LeaseID, 200)
	done := make(chan struct{})
	go func() {
		defer close(done)
		request, _ := http.NewRequest(http.MethodPost, "http://localhost/v1/lease/release", strings.NewReader(`{"version":1,"lease_id":"`+lease.LeaseID+`"}`))
		request.Header.Set("Content-Type", "application/json")
		if response, err := client.Do(request); err == nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("control release verifier did not start")
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not cancel verifier")
	}
	<-done
	if state := c.snapshot(); state.State != "unknown" || state.LeaseID != "" || calls.Load() != 2 {
		t.Fatalf("shutdown reopened admission: %+v calls=%d", state, calls.Load())
	}
}
