package proxy

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/auditlog"
)

func newTestAuditStore(t *testing.T, nodeID string) *auditlog.Store {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := auditlog.Open(directory, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestManagementHandlerAuditsMachineTokenAsUnknownHuman(t *testing.T) {
	store := newTestAuditStore(t, "node-a")
	r := &Reloader{logger: discardLogger(), auditStore: store}
	r.state.Store(&reloaderState{
		peer:       &config.PeerConfig{NodeID: "node-a"},
		management: &config.ManagementConfig{PreviewToken: strings.Repeat("p", 32), DelegationToken: strings.Repeat("d", 32)},
		auditStore: store,
	})
	request := httptest.NewRequest(http.MethodGet, "https://catalog.example/management/v1/not-known", nil)
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
	response := httptest.NewRecorder()
	r.ManagementHandler().ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown management path status = %d", response.Code)
	}
	events, err := store.ListRecent(10)
	if err != nil || len(events) != 1 {
		t.Fatalf("management audit events = %+v, %v", events, err)
	}
	event := events[0]
	if event.PrincipalType != "machine_token" || event.UserIdentityState != "unknown" || event.VerifiedUserSubject != "" || event.Outcome != "failed" {
		t.Fatalf("machine token was misattributed: %+v", event)
	}
}
