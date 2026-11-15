//go:build linux

package proxy

import (
	"bytes"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestDelegatedOperationLookupAcrossTLSHandlers(t *testing.T) {
	owner := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, owner)
	owner.r.logger = discardLogger()
	owner.r.peerSlots = make(chan struct{}, 2)
	ownerServer := httptest.NewTLSServer(owner.r.PeerHandler())
	defer ownerServer.Close()
	caPath := filepath.Join(t.TempDir(), "owner-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ownerServer.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	caller, peerToken := strings.Repeat("c", 32), strings.Repeat("p", 32)
	peer := &config.PeerConfig{NodeID: "requester", Peers: map[string]config.PeerGrant{
		"owner": {URL: ownerServer.URL, OutboundToken: peerToken, CAFile: caPath, RouteManagement: true},
	}}
	clients, _, err := newPeerClients(peer)
	if err != nil {
		t.Fatal(err)
	}
	defer closePeerClients(clients)
	state := &reloaderState{
		peer: peer, peerClients: clients,
		management: &config.ManagementConfig{DelegationToken: caller, PreviewToken: caller},
	}
	receiver := &Reloader{logger: discardLogger(), managementSlots: make(chan struct{}, 2)}
	receiver.state.Store(state)
	receiverServer := httptest.NewTLSServer(receiver.ManagementHandler())
	defer receiverServer.Close()
	path := receivedOperationPath + "owner/" + owner.input.ProposalID + "/" + owner.input.OperationID
	get := func(token, origin string) (int, []byte, http.Header) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, receiverServer.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		response, err := receiverServer.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, data, response.Header
	}
	status, data, headers := get(caller, "")
	var record modelconfig.DelegatedOperation
	if status != http.StatusOK || json.Unmarshal(data, &record) != nil ||
		record.OperationID != owner.input.OperationID || record.Outcome != "unknown" {
		t.Fatalf("lookup status=%d record=%+v body=%q", status, record, data)
	}
	if headers.Get("Cache-Control") != "no-store" || headers.Get("X-Content-Type-Options") != "nosniff" ||
		bytes.Contains(data, []byte(caller)) || bytes.Contains(data, []byte(peerToken)) ||
		bytes.Contains(data, []byte("confidential")) || bytes.Contains(data, []byte(owner.target)) {
		t.Fatalf("unsafe lookup response: %s", data)
	}
	if status, _, _ := get(peerToken, ""); status != http.StatusUnauthorized {
		t.Fatalf("peer credential accepted as caller: %d", status)
	}
	if status, _, _ := get(caller, "https://different.example"); status != http.StatusForbidden {
		t.Fatalf("cross-origin lookup accepted: %d", status)
	}
	if _, err := modelconfig.FinishDelegatedOperation(owner.operations, owner.input.ProposalID, owner.input.OperationID,
		owner.r.instanceID, modelconfig.MutationResult{}, modelconfig.ErrTargetWrite, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, data, _ = get(caller, "")
	if status != http.StatusOK || json.Unmarshal(data, &record) != nil || record.Outcome != "not_written" {
		t.Fatalf("terminal lookup status=%d record=%+v body=%q", status, record, data)
	}
	updated := *owner.state
	updated.peer = copyPeerConfig(owner.state.peer)
	updated.peer.Peers["requester"] = config.PeerGrant{Token: peerToken, CatalogRead: true}
	owner.r.lifecycleMu.Lock()
	owner.r.state.Store(&updated)
	owner.r.lifecycleMu.Unlock()
	if status, _, _ := get(caller, ""); status != http.StatusServiceUnavailable {
		t.Fatalf("revoked owner grant returned %d", status)
	}
	got, err := os.ReadFile(filepath.Join(owner.target, "config.yaml"))
	if err != nil || !bytes.Equal(got, owner.source) {
		t.Fatalf("lookup changed the target: %v", err)
	}
	ownerRequest := httptest.NewRequest(http.MethodGet, "https://owner.example"+peerOperationPath+owner.input.ProposalID+"/"+owner.input.OperationID, nil)
	ownerRequest.Header.Set("Authorization", "Bearer "+peerToken)
	recorder := httptest.NewRecorder()
	owner.r.PeerHandler().ServeHTTP(recorder, ownerRequest)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("owner lookup did not enforce the revoked peer grant: %d", recorder.Code)
	}
	receivingRequest := httptest.NewRequest(http.MethodGet, "https://receiver.example"+path, nil)
	receivingRequest.Header.Set("Authorization", "Bearer "+caller)
	recorder = httptest.NewRecorder()
	receiver.ManagementHandler().ServeHTTP(recorder, receivingRequest)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("receiving lookup did not report revoked owner grant: %d", recorder.Code)
	}
}

func TestReceivingDelegatedOperationRejectsInvalidPaths(t *testing.T) {
	r := &Reloader{managementSlots: make(chan struct{}, 1)}
	for _, path := range []string{
		receivedOperationPath + "owner/invalid/" + strings.Repeat("a", 32),
		receivedOperationPath + "owner/" + strings.Repeat("a", 32) + "/" + strings.Repeat("b", 32) + "/extra",
		receivedOperationPath + "owner/" + strings.Repeat("a", 32) + "/" + strings.Repeat("b", 32) + "?",
		receivedOperationPath + "ow%6eer/" + strings.Repeat("a", 32) + "/" + strings.Repeat("b", 32),
	} {
		req := httptest.NewRequest(http.MethodGet, "https://receiver.example"+path, nil)
		recorder := httptest.NewRecorder()
		r.serveReceivingDelegatedOperation(recorder, req)
		if recorder.Code != http.StatusNotFound {
			t.Fatalf("invalid path accepted: %s (%d)", path, recorder.Code)
		}
	}
}

func TestDelegatedOperationHandlersRejectInvalidHTTP(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	f.r.logger = discardLogger()
	f.r.peerSlots = make(chan struct{}, 1)
	path := peerOperationPath + f.input.ProposalID + "/" + f.input.OperationID
	for _, tt := range []struct {
		name   string
		method string
		target string
		body   string
		tls    bool
		want   int
	}{
		{"plaintext", http.MethodGet, path, "", false, http.StatusForbidden},
		{"wrong method", http.MethodPost, path, "", true, http.StatusMethodNotAllowed},
		{"body", http.MethodGet, path, "unexpected", true, http.StatusBadRequest},
		{"query", http.MethodGet, path + "?x=1", "", true, http.StatusNotFound},
		{"empty query", http.MethodGet, path + "?", "", true, http.StatusNotFound},
		{"extra segment", http.MethodGet, path + "/extra", "", true, http.StatusNotFound},
		{"invalid ID", http.MethodGet, peerOperationPath + "invalid/" + f.input.OperationID, "", true, http.StatusNotFound},
		{"encoded path", http.MethodGet, peerOperationPath + "%61" + f.input.ProposalID[1:] + "/" + f.input.OperationID, "", true, http.StatusNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "https://owner.example"+tt.target, strings.NewReader(tt.body))
			if !tt.tls {
				req.TLS = nil
			}
			req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
			recorder := httptest.NewRecorder()
			f.r.servePeerDelegatedOperation(recorder, req)
			if recorder.Code != tt.want || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d, want=%d", recorder.Code, tt.want)
			}
		})
	}
	f.r.peerSlots <- struct{}{}
	req := httptest.NewRequest(http.MethodGet, "https://owner.example"+path, nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
	recorder := httptest.NewRecorder()
	f.r.servePeerDelegatedOperation(recorder, req)
	<-f.r.peerSlots
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unbounded owner lookup accepted: %d", recorder.Code)
	}
}
