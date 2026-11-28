//go:build linux

package proxy

import (
	"bytes"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

const testedLlamaSwapRevision = "85cbb9b59ba71d7cbd5c73b3a9e345118f0cb217"

type reloadModelEvent struct {
	Event   string `json:"event"`
	PID     int    `json:"pid"`
	Context int    `json:"context"`
}

func TestLlamaSwapReloadWithManagedWrite(t *testing.T) {
	t.Helper()
	binary := os.Getenv("LLAMA_SWAP_TEST_BINARY")
	if binary == "" {
		t.Skip("set LLAMA_SWAP_TEST_BINARY to a local llama-swap reference build")
	}
	var err error
	binary, err = filepath.Abs(binary)
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("CATALOG_PROXY_RELOAD_TEST_NETNS") != "1" {
		runIsolatedReloadTest(t, binary)
		return
	}
	runNativeReloadTest(t, binary)
}

func runIsolatedReloadTest(t *testing.T, binary string) {
	t.Helper()
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	parentNamespace, err := os.Open("/proc/self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	defer parentNamespace.Close()
	isolated := exec.Command("unshare", "--user", "--map-root-user", "--net",
		testBinary, "-test.run=^TestLlamaSwapReloadWithManagedWrite$", "-test.v", "-test.timeout=90s")
	isolated.Env = append(os.Environ(), "CATALOG_PROXY_RELOAD_TEST_NETNS=1")
	isolated.ExtraFiles = []*os.File{parentNamespace}
	output, err := isolated.CombinedOutput()
	if err != nil {
		t.Fatalf("isolated reload test failed (requires unshare, ip, and unprivileged user/network namespaces): %v\n%s", err, output)
	}
	t.Logf("isolated reload test:\n%s", output)
}

func runNativeReloadTest(t *testing.T, binary string) {
	t.Helper()
	parentNamespace := os.NewFile(3, "parent network namespace")
	if parentNamespace == nil {
		t.Fatal("parent network namespace descriptor missing")
	}
	defer parentNamespace.Close()
	selfNS, selfErr := os.Stat("/proc/self/ns/net")
	parentNS, parentErr := parentNamespace.Stat()
	if selfErr != nil || parentErr != nil {
		t.Fatalf("inspect network namespaces: self=%v parent=%v", selfErr, parentErr)
	}
	if os.SameFile(selfNS, parentNS) {
		t.Fatal("reload test requires a separate network namespace")
	}
	if output, err := exec.Command("ip", "link", "set", "dev", "lo", "up").CombinedOutput(); err != nil {
		t.Fatalf("enable isolated loopback: %v: %s", err, output)
	}
	binary = validateReferenceBinary(t, binary)
	modelBinary := buildFakeModel(t)
	for _, watching := range []bool{false, true} {
		name := "watch_disabled"
		if watching {
			name = "watch_enabled"
		}
		t.Run(name, func(t *testing.T) {
			ctx, err := startReloadHarness(t, binary, modelBinary, watching)
			if err != nil {
				t.Fatalf("start reload harness: %v", err)
			}
			runReloadHarness(ctx, watching)
		})
	}
}

func validateReferenceBinary(t *testing.T, binary string) string {
	t.Helper()
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatalf("read llama-swap build information: %v", err)
	}
	var revision, modified string
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value
		}
	}
	if info.Path != "github.com/mostlygeek/llama-swap" || revision != testedLlamaSwapRevision || modified != "false" {
		t.Fatalf("reference binary must be the clean inspected llama-swap revision %s", testedLlamaSwapRevision)
	}
	return binary
}

func buildFakeModel(t *testing.T) string {
	t.Helper()
	modelBinary := filepath.Join(t.TempDir(), "llama-server")
	build := exec.Command("go", "build", "-mod=readonly", "-o", modelBinary, "./testdata/llama-server")
	build.Env = append(os.Environ(), "GOPROXY=off", "GOTOOLCHAIN=local")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake model: %v: %s", err, output)
	}
	return modelBinary
}

// reloadHarness owns the per-test subprocess, HTTP client, and temp directories
// needed by the reload scenario and its cleanup.
type reloadHarness struct {
	t           *testing.T
	command     *exec.Cmd
	done        chan struct{}
	logFile     *os.File
	eventsPath  string
	client      *http.Client
	baseURL     string
	reloader    *Reloader
	writeToken  string
	targetDir   string
	proposalDir string
	backupDir   string
	modelBinary string
}

// startReloadHarness launches the llama-swap reference binary, waits for its
// loopback listener, and registers cleanup that stops the process and any
// leftover test models.
func startReloadHarness(t *testing.T, binary, modelBinary string, watching bool) (*reloadHarness, error) {
	t.Helper()
	targetDir, proposalDir, backupDir := t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{proposalDir, backupDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			return nil, err
		}
	}
	targetPath := filepath.Join(targetDir, "config.yaml")
	source := fmt.Sprintf("logToStdout: none\nperformance: {disabled: true}\nmodels:\n  m:\n    cmd: %s --host 127.0.0.1 --port ${PORT} -c 1024\n", modelBinary)
	if err := os.WriteFile(targetPath, []byte(source), 0o600); err != nil {
		return nil, err
	}
	eventsPath := filepath.Join(t.TempDir(), "model-events.jsonl")
	args := []string{"-listen", "127.0.0.1:0", "-config", targetPath}
	if watching {
		args = append(args, "-watch-config")
	}
	command := exec.Command(binary, args...)
	command.Dir = t.TempDir()
	command.Env = append(os.Environ(), "CATALOG_PROXY_RELOAD_TEST_EVENTS="+eventsPath)
	logFile, err := os.Create(filepath.Join(t.TempDir(), "upstream.log"))
	if err != nil {
		return nil, err
	}
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		return nil, err
	}
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	ctx := &reloadHarness{
		t:           t,
		command:     command,
		done:        done,
		logFile:     logFile,
		eventsPath:  eventsPath,
		client:      &http.Client{Timeout: 2 * time.Second},
		targetDir:   targetDir,
		proposalDir: proposalDir,
		backupDir:   backupDir,
		modelBinary: modelBinary,
	}
	t.Cleanup(func() {
		cleanupReloadHarness(ctx)
	})
	return ctx, nil
}

// waitForListener blocks until the llama-swap process exposes a healthy
// loopback listener and records its address on the harness.
func waitForListener(ctx *reloadHarness) {
	ctx.t.Helper()
	var address string
	waitReloadCondition(ctx.t, 15*time.Second, "llama-swap listener", func() bool {
		select {
		case <-ctx.done:
			return false
		default:
		}
		address = ownedLoopbackListener(ctx.command.Process.Pid)
		if address == "" {
			return false
		}
		return isListenerReady(ctx, address)
	})
	ctx.baseURL = "http://" + address
}

// isListenerReady reports whether the captured loopback address is healthy.
func isListenerReady(ctx *reloadHarness, address string) bool {
	ctx.t.Helper()
	if address == "" {
		return false
	}
	response, err := ctx.client.Get("http://" + address + "/health")
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.StatusCode == http.StatusOK
}

// runReloadHarness executes the reload scenario for a single watching mode.
func runReloadHarness(ctx *reloadHarness, watching bool) {
	ctx.t.Helper()
	initial := waitForInitialModel(ctx)
	proposal, certPath, keyPath := createProposal(ctx)
	reloader, writeToken := createProxyReloader(ctx, proposal, certPath, keyPath)
	ctx.reloader = reloader
	ctx.writeToken = writeToken
	applied := applyProposal(ctx, proposal)
	if !watching {
		verifyUnwatchedWrite(ctx, applied, initial)
		return
	}
	verifyWatchedWrite(ctx, applied, initial)
}

// waitForInitialModel waits for the listener, records the base URL, and returns
// the initial fake-model identity after the watcher baseline is established.
func waitForInitialModel(ctx *reloadHarness) reloadModelEvent {
	ctx.t.Helper()
	waitForListener(ctx)
	initial := requestReloadIdentity(ctx.t, ctx.client, ctx.baseURL)
	if initial.PID <= 1 || initial.Context != 1024 {
		ctx.t.Fatalf("unexpected initial fake model: %+v", initial)
	}
	// The watcher establishes its baseline asynchronously after server startup.
	time.Sleep(2200 * time.Millisecond)
	return initial
}

// createProposal builds the preview and proposal for the context-size change and
// persists the proposal to the proposal directory.
func createProposal(ctx *reloadHarness) (modelconfig.Proposal, string, string) {
	ctx.t.Helper()
	size := int32(4096)
	settings := modelconfig.CommandSettings{ContextSize: &size}
	preview, err := modelconfig.PreviewTarget(ctx.targetDir, "config.yaml", "local", "m", settings)
	if err != nil {
		ctx.t.Fatal(err)
	}
	proposal, err := modelconfig.NewProposal(ctx.targetDir, "config.yaml", preview, settings)
	if err != nil {
		ctx.t.Fatal(err)
	}
	if err := modelconfig.SaveProposal(ctx.proposalDir, proposal); err != nil {
		ctx.t.Fatal(err)
	}
	certPath, keyPath, _ := peerTestCertificate(ctx.t)
	return proposal, certPath, keyPath
}

// createProxyReloader writes the proxy management config and starts the reloader.
func createProxyReloader(ctx *reloadHarness, proposal modelconfig.Proposal, certPath, keyPath string) (*Reloader, string) {
	ctx.t.Helper()
	previewToken, proposalToken := strings.Repeat("m", 32), strings.Repeat("p", 32)
	backupToken, writeToken := strings.Repeat("b", 32), strings.Repeat("w", 32)
	proxyConfig := fmt.Sprintf(`default_backend: local
backends: {local: {url: %q}}
management:
  listen: "127.0.0.1:9294"
  tls_cert_file: %q
  tls_key_file: %q
  preview_token: %q
  proposal_store: {directory: %q, token: %q}
  backup_store: {directory: %q, token: %q, max_backups: 3}
  write_token: %q
  targets:
    local: {directory: %q, filename: config.yaml}
`, ctx.baseURL, certPath, keyPath, previewToken, ctx.proposalDir, proposalToken, ctx.backupDir, backupToken, writeToken, ctx.targetDir)
	proxyConfigPath := configPath(ctx.t)
	if err := os.WriteFile(proxyConfigPath, []byte(proxyConfig), 0o600); err != nil {
		ctx.t.Fatal(err)
	}
	reloader, _, err := NewReloader(proxyConfigPath, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		ctx.t.Fatal(err)
	}
	return reloader, writeToken
}

// applyProposal applies the proposal through the management endpoint and
// returns the applied result.
func applyProposal(ctx *reloadHarness, proposal modelconfig.Proposal) mutationResponse {
	ctx.t.Helper()
	applied := managedWrite(ctx, "/management/v1/proposals/"+proposal.ID+"/apply")
	if applied.BackupID == "" {
		ctx.t.Fatal("managed apply did not return a backup ID")
	}
	return applied
}

// managedWrite posts a managed-write request and fails the test if the response
// is not a confirmed written mutation.
func managedWrite(ctx *reloadHarness, path string) mutationResponse {
	ctx.t.Helper()
	body := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
	req := httptest.NewRequest(http.MethodPost, "https://management"+path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+ctx.writeToken)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	ctx.reloader.ManagementHandler().ServeHTTP(recorder, req)
	result, ok := validManagedWrite(recorder)
	if !ok {
		ctx.t.Fatalf("managed write = %d %s", recorder.Code, recorder.Body.String())
	}
	return result
}

// validManagedWrite reports whether a management recorder holds a confirmed
// written mutation.
func validManagedWrite(recorder *httptest.ResponseRecorder) (mutationResponse, bool) {
	var result mutationResponse
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &result) != nil {
		return mutationResponse{}, false
	}
	if result.Status != "written" || result.Durability != "confirmed" ||
		result.Observed != "unknown" || result.Effective != "unknown" {
		return mutationResponse{}, false
	}
	return result, true
}

// mutateRollback rolls back the applied backup through the management endpoint.
func mutateRollback(ctx *reloadHarness, backupID string) {
	ctx.t.Helper()
	managedWrite(ctx, "/management/v1/backups/"+backupID+"/rollback")
}

// verifyUnwatchedWrite asserts that an unwatched write leaves the running model
// unchanged and produces no stopped event.
func verifyUnwatchedWrite(ctx *reloadHarness, applied mutationResponse, initial reloadModelEvent) {
	ctx.t.Helper()
	_ = applied
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got := requestReloadIdentity(ctx.t, ctx.client, ctx.baseURL)
		if got.PID != initial.PID || got.Context != initial.Context {
			ctx.t.Fatalf("unwatched write changed the running fake model: %+v", got)
		}
		time.Sleep(250 * time.Millisecond)
	}
	if hasReloadEvent(ctx.eventsPath, "stopped", initial.PID) {
		ctx.t.Fatal("unwatched write stopped the model")
	}
}

// verifyWatchedWrite asserts that a watched write reloads the model and that a
// rollback restores the original model.
func verifyWatchedWrite(ctx *reloadHarness, applied mutationResponse, initial reloadModelEvent) {
	ctx.t.Helper()
	waitForModelShutdown(ctx, initial.PID, "old fake model shutdown")
	waitForModelAbsent(ctx)
	assertCatalogModelPresent(ctx.t, ctx.client, ctx.baseURL)
	if !runningModelAbsent(ctx.client, ctx.baseURL) {
		ctx.t.Fatal("catalog inspection unexpectedly loaded the fake model")
	}
	updated := assertReloadIdentity(ctx, initial.PID, 4096)
	mutateRollback(ctx, applied.BackupID)
	waitForModelShutdown(ctx, updated.PID, "updated fake model shutdown")
	restored := assertReloadIdentity(ctx, updated.PID, 1024)
	_ = restored
}

// assertReloadIdentity fetches the model identity and fails if it does not match
// the expected PID and context size.
func assertReloadIdentity(ctx *reloadHarness, expectedPID, expectedContext int) reloadModelEvent {
	ctx.t.Helper()
	got := requestReloadIdentity(ctx.t, ctx.client, ctx.baseURL)
	if got.PID <= 1 || got.PID == expectedPID || got.Context != expectedContext {
		ctx.t.Fatalf("unexpected fake model: %+v", got)
	}
	return got
}

// waitForModelShutdown blocks until the given pid emits a stopped event and is
// no longer alive.
func waitForModelShutdown(ctx *reloadHarness, pid int, description string) {
	ctx.t.Helper()
	waitReloadCondition(ctx.t, 15*time.Second, description, func() bool {
		return hasReloadEvent(ctx.eventsPath, "stopped", pid) && reloadModelStopped(pid, ctx.modelBinary)
	})
}

// waitForModelAbsent blocks until the model is absent from the running list.
func waitForModelAbsent(ctx *reloadHarness) {
	ctx.t.Helper()
	waitReloadCondition(ctx.t, 10*time.Second, "model absent from running list", func() bool {
		return runningModelAbsent(ctx.client, ctx.baseURL)
	})
}

// cleanupReloadHarness stops the llama-swap process, removes leftover test
// models, closes the log file, and releases the HTTP client and reloader.
func cleanupReloadHarness(ctx *reloadHarness) {
	ctx.t.Helper()
	select {
	case <-ctx.done:
	default:
		if err := ctx.command.Process.Signal(syscall.SIGTERM); err != nil && err != os.ErrProcessDone {
			ctx.t.Errorf("stop test llama-swap: %v", err)
		}
		select {
		case <-ctx.done:
		case <-time.After(5 * time.Second):
			if err := ctx.command.Process.Kill(); err != nil && err != os.ErrProcessDone {
				ctx.t.Errorf("kill test llama-swap: %v", err)
			}
			<-ctx.done
		}
	}
	cleanupTestModels(ctx)
	if err := ctx.logFile.Close(); err != nil {
		ctx.t.Errorf("close test log: %v", err)
	}
	ctx.client.CloseIdleConnections()
	if ctx.reloader != nil {
		ctx.reloader.Stop()
	}
}

// cleanupTestModels stops any test models that were started by the harness.
func cleanupTestModels(ctx *reloadHarness) {
	ctx.t.Helper()
	for _, event := range readReloadEvents(ctx.eventsPath) {
		if event.Event != "started" || event.PID <= 1 {
			continue
		}
		stopTestModel(ctx, event.PID)
	}
}

// stopTestModel signals a test model and waits for it to exit, escalating to
// SIGKILL if it does not stop gracefully. Initial liveness is queried before
// waiting/polling, matching the original cleanup behavior.
func stopTestModel(ctx *reloadHarness, pid int) {
	ctx.t.Helper()
	alive, err := reloadModelAlive(pid, ctx.modelBinary)
	if err != nil {
		ctx.t.Errorf("inspect test model %d: %v", pid, err)
		return
	}
	if !alive {
		return
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
		ctx.t.Errorf("stop test model %d: %v", pid, err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for alive && err == nil && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		alive, err = reloadModelAlive(pid, ctx.modelBinary)
	}
	if err != nil {
		ctx.t.Errorf("inspect test model %d: %v", pid, err)
	} else if alive {
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
			ctx.t.Errorf("kill test model %d: %v", pid, err)
		}
		ctx.t.Errorf("test model %d did not stop gracefully", pid)
	}
}

func waitReloadCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func requestReloadIdentity(t *testing.T, client *http.Client, baseURL string) reloadModelEvent {
	t.Helper()
	response, err := client.Get(baseURL + "/upstream/m/identity")
	if err != nil {
		t.Fatalf("request fake model: %v", err)
	}
	defer response.Body.Close()
	var result reloadModelEvent
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&result) != nil {
		t.Fatalf("fake model returned status %d or invalid identity", response.StatusCode)
	}
	return result
}

func readReloadEvents(path string) []reloadModelEvent {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var events []reloadModelEvent
	for _, line := range bytes.Split(data, []byte("\n")) {
		var event reloadModelEvent
		if len(line) > 0 && json.Unmarshal(line, &event) == nil {
			events = append(events, event)
		}
	}
	return events
}

func hasReloadEvent(path, name string, pid int) bool {
	for _, event := range readReloadEvents(path) {
		if event.Event == name && event.PID == pid {
			return true
		}
	}
	return false
}

func reloadModelAlive(pid int, binary string) (bool, error) {
	exe, err := os.Stat(fmt.Sprintf("/proc/%d/exe", pid))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	model, err := os.Stat(binary)
	if err != nil {
		return false, err
	}
	return os.SameFile(exe, model), nil
}

func reloadModelStopped(pid int, binary string) bool {
	alive, err := reloadModelAlive(pid, binary)
	return err == nil && !alive
}

func runningModelAbsent(client *http.Client, baseURL string) bool {
	response, err := client.Get(baseURL + "/running")
	if err != nil {
		return false
	}
	defer response.Body.Close()
	var result struct {
		Running *[]struct {
			Model string `json:"model"`
		} `json:"running"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil || result.Running == nil {
		return false
	}
	for _, model := range *result.Running {
		if model.Model == "m" {
			return false
		}
	}
	return true
}

func assertCatalogModelPresent(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	response, err := client.Get(baseURL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result) != nil {
		t.Fatalf("catalog status %d or invalid response", response.StatusCode)
	}
	for _, model := range result.Data {
		if model.ID == "m" {
			return
		}
	}
	t.Fatal("unloaded model absent from catalog")
}

// Discover the child-owned ephemeral listener without connecting to another process.
func ownedLoopbackListener(pid int) string {
	fds, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return ""
	}
	sockets := make(map[string]bool)
	for _, fd := range fds {
		link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", pid, fd.Name()))
		if err == nil && strings.HasPrefix(link, "socket:[") && strings.HasSuffix(link, "]") {
			sockets[strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")] = true
		}
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", pid))
	if err != nil {
		return ""
	}
	var address string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" || !sockets[fields[9]] {
			continue
		}
		host, portText, ok := strings.Cut(fields[1], ":")
		port, err := strconv.ParseUint(portText, 16, 16)
		if !ok || err != nil || host != "0100007F" || port == 0 || address != "" {
			return ""
		}
		address = fmt.Sprintf("127.0.0.1:%d", port)
	}
	return address
}
