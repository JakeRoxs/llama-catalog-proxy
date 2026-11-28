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
		return
	}
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
	modelBinary := filepath.Join(t.TempDir(), "llama-server")
	build := exec.Command("go", "build", "-mod=readonly", "-o", modelBinary, "./testdata/llama-server")
	build.Env = append(os.Environ(), "GOPROXY=off", "GOTOOLCHAIN=local")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake model: %v: %s", err, output)
	}
	for _, watching := range []bool{false, true} {
		name := "watch_disabled"
		if watching {
			name = "watch_enabled"
		}
		t.Run(name, func(t *testing.T) {
			testLlamaSwapReload(t, binary, modelBinary, watching)
		})
	}
}

func testLlamaSwapReload(t *testing.T, binary, modelBinary string, watching bool) {
	t.Helper()
	targetDir, proposalDir, backupDir := t.TempDir(), t.TempDir(), t.TempDir()
	for _, directory := range []string{proposalDir, backupDir} {
		if err := os.Chmod(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	targetPath := filepath.Join(targetDir, "config.yaml")
	source := fmt.Sprintf("logToStdout: none\nperformance: {disabled: true}\nmodels:\n  m:\n    cmd: %s --host 127.0.0.1 --port ${PORT} -c 1024\n", modelBinary)
	if err := os.WriteFile(targetPath, []byte(source), 0o600); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = logFile, logFile
	if err := command.Start(); err != nil {
		_ = logFile.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = command.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
		default:
			if err := command.Process.Signal(syscall.SIGTERM); err != nil && err != os.ErrProcessDone {
				t.Errorf("stop test llama-swap: %v", err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				if err := command.Process.Kill(); err != nil && err != os.ErrProcessDone {
					t.Errorf("kill test llama-swap: %v", err)
				}
				<-done
			}
		}
		for _, event := range readReloadEvents(eventsPath) {
			if event.Event != "started" || event.PID <= 1 {
				continue
			}
			alive, err := reloadModelAlive(event.PID, modelBinary)
			if err != nil {
				t.Errorf("inspect test model %d: %v", event.PID, err)
				continue
			}
			if alive {
				if err := syscall.Kill(event.PID, syscall.SIGTERM); err != nil && err != syscall.ESRCH {
					t.Errorf("stop test model %d: %v", event.PID, err)
				}
				deadline := time.Now().Add(3 * time.Second)
				for alive && err == nil && time.Now().Before(deadline) {
					time.Sleep(50 * time.Millisecond)
					alive, err = reloadModelAlive(event.PID, modelBinary)
				}
				if err != nil {
					t.Errorf("inspect test model %d: %v", event.PID, err)
				} else if alive {
					if err := syscall.Kill(event.PID, syscall.SIGKILL); err != nil && err != syscall.ESRCH {
						t.Errorf("kill test model %d: %v", event.PID, err)
					}
					t.Errorf("test model %d did not stop gracefully", event.PID)
				}
			}
		}
		if err := logFile.Close(); err != nil {
			t.Errorf("close test log: %v", err)
		}
	})
	client := &http.Client{Timeout: 2 * time.Second}
	defer client.CloseIdleConnections()
	var address string
	waitReloadCondition(t, 15*time.Second, "llama-swap listener", func() bool {
		select {
		case <-done:
			return false
		default:
		}
		address = ownedLoopbackListener(command.Process.Pid)
		if address == "" {
			return false
		}
		response, err := client.Get("http://" + address + "/health")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	})
	baseURL := "http://" + address
	initial := requestReloadIdentity(t, client, baseURL)
	if initial.PID <= 1 || initial.Context != 1024 {
		t.Fatalf("unexpected initial fake model: %+v", initial)
	}
	// The watcher establishes its baseline asynchronously after server startup.
	time.Sleep(2200 * time.Millisecond)

	size := int32(4096)
	settings := modelconfig.CommandSettings{ContextSize: &size}
	preview, err := modelconfig.PreviewTarget(targetDir, "config.yaml", "local", "m", settings)
	if err != nil {
		t.Fatal(err)
	}
	proposal, err := modelconfig.NewProposal(targetDir, "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	if err := modelconfig.SaveProposal(proposalDir, proposal); err != nil {
		t.Fatal(err)
	}
	certPath, keyPath, _ := peerTestCertificate(t)
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
`, baseURL, certPath, keyPath, previewToken, proposalDir, proposalToken, backupDir, backupToken, writeToken, targetDir)
	proxyConfigPath := configPath(t)
	if err := os.WriteFile(proxyConfigPath, []byte(proxyConfig), 0o600); err != nil {
		t.Fatal(err)
	}
	reloader, _, err := NewReloader(proxyConfigPath, discardLogger(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reloader.Stop()
	mutate := func(path string) mutationResponse {
		t.Helper()
		body := `{"acknowledge_review":true,"acknowledge_disruption":true,"acknowledge_exclusive_window":true}`
		req := httptest.NewRequest(http.MethodPost, "https://management"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+writeToken)
		req.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		reloader.ManagementHandler().ServeHTTP(recorder, req)
		var result mutationResponse
		if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &result) != nil ||
			result.Status != "written" || result.Durability != "confirmed" ||
			result.Observed != "unknown" || result.Effective != "unknown" {
			t.Fatalf("managed write = %d %s", recorder.Code, recorder.Body.String())
		}
		return result
	}
	applied := mutate("/management/v1/proposals/" + proposal.ID + "/apply")
	if applied.BackupID == "" {
		t.Fatal("managed apply did not return a backup ID")
	}
	if !watching {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			got := requestReloadIdentity(t, client, baseURL)
			if got.PID != initial.PID || got.Context != initial.Context {
				t.Fatalf("unwatched write changed the running fake model: %+v", got)
			}
			time.Sleep(250 * time.Millisecond)
		}
		if hasReloadEvent(eventsPath, "stopped", initial.PID) {
			t.Fatal("unwatched write stopped the model")
		}
		return
	}
	waitReloadCondition(t, 15*time.Second, "old fake model shutdown", func() bool {
		return hasReloadEvent(eventsPath, "stopped", initial.PID) && reloadModelStopped(initial.PID, modelBinary)
	})
	waitReloadCondition(t, 10*time.Second, "model absent from running list", func() bool {
		return runningModelAbsent(client, baseURL)
	})
	assertCatalogModelPresent(t, client, baseURL)
	if !runningModelAbsent(client, baseURL) {
		t.Fatal("catalog inspection unexpectedly loaded the fake model")
	}
	updated := requestReloadIdentity(t, client, baseURL)
	if updated.PID <= 1 || updated.PID == initial.PID || updated.Context != 4096 {
		t.Fatalf("watched write did not start a new fake model with the changed argument: %+v", updated)
	}
	mutate("/management/v1/backups/" + applied.BackupID + "/rollback")
	waitReloadCondition(t, 15*time.Second, "updated fake model shutdown", func() bool {
		return hasReloadEvent(eventsPath, "stopped", updated.PID) && reloadModelStopped(updated.PID, modelBinary)
	})
	restored := requestReloadIdentity(t, client, baseURL)
	if restored.PID <= 1 || restored.PID == updated.PID || restored.Context != 1024 {
		t.Fatalf("watched rollback did not start a new fake model with the original argument: %+v", restored)
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
