//go:build linux

package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestForwardDelegatedApplyLostResponseAndReplay(t *testing.T) {
	owner := newDelegatedApplyFixture(t)
	review, err := modelconfig.LoadDelegatedProposal(owner.proposals, owner.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	owner.r.logger = discardLogger()
	owner.r.peerSlots = make(chan struct{}, 2)
	var mode, calls, redirects atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	redirect := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirects.Add(1)
	}))
	defer redirect.Close()
	peerToken, caller := strings.Repeat("p", 32), strings.Repeat("c", 32)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, peerOperationPath) {
			owner.r.servePeerDelegatedOperation(w, req)
			return
		}
		calls.Add(1)
		if req.Method != http.MethodPost || req.URL.Path != peerSettingsApplyPath ||
			req.Header.Get("Authorization") != "Bearer "+peerToken ||
			req.Header.Get("Cookie") != "" || req.Header.Get("X-Unsafe") != "" ||
			req.Header.Get("Idempotency-Key") != "" {
			t.Errorf("unexpected peer request: %s %s", req.Method, req.URL.Path)
		}
		response := httptest.NewRecorder()
		owner.r.servePeerDelegatedSettingsApply(response, req)
		if mode.Load() == 12 {
			close(entered)
			<-release
		}
		switch mode.Load() {
		case 1:
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
		case 2, 3, 4, 5, 6, 7, 8, 9:
			var operation modelconfig.DelegatedOperation
			if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
				t.Error(err)
				return
			}
			switch mode.Load() {
			case 2:
				operation.ReviewRevision = strings.Repeat("f", 64)
			case 3:
				operation.Owner.ConfigGeneration++
			case 4:
				operation.RequesterNode = "another-requester"
			case 5:
				operation.BackendID = "another-backend"
			case 6:
				operation.TargetID = "another-target"
			case 7:
				operation.ModelID = "another-model"
			case 8:
				operation.OperationID = strings.Repeat("f", 32)
			case 9:
				operation.ProposalExpiresAt = operation.ProposalExpiresAt.Add(-time.Nanosecond)
			}
			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(operation); err != nil {
				t.Error(err)
			}
		case 10:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, strings.Repeat(" ", maxManagementRequest+1))
		case 11:
			http.Redirect(w, req, redirect.URL, http.StatusTemporaryRedirect)
		default:
			for key, values := range response.Header() {
				w.Header()[key] = values
			}
			w.WriteHeader(response.Code)
			_, _ = w.Write(response.Body.Bytes())
		}
	}))
	defer server.Close()
	caPath := filepath.Join(t.TempDir(), "peer-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	peer := &config.PeerConfig{NodeID: "requester", Peers: map[string]config.PeerGrant{
		"owner": {URL: server.URL, OutboundToken: peerToken, CAFile: caPath, RouteManagement: true},
	}}
	clients, _, err := newPeerClients(peer)
	if err != nil {
		t.Fatal(err)
	}
	defer closePeerClients(clients)
	state := &reloaderState{peer: peer, peerClients: clients, management: &config.ManagementConfig{DelegationToken: caller}}
	receiver := &Reloader{managementSlots: make(chan struct{}, 2)}
	receiver.state.Store(state)
	request := httptest.NewRequest(http.MethodPost, "https://receiver.example/apply", nil)
	request.Header.Set("Authorization", "Bearer "+caller)
	request.Header.Set("Cookie", "not-forwarded")
	request.Header.Set("X-Unsafe", "not-forwarded")
	apply := func(req *http.Request, s *reloaderState, proposal modelconfig.DelegatedProposal, op string, reviewAck, disruptionAck, exclusiveAck bool) (modelconfig.DelegatedOperation, error) {
		return receiver.forwardDelegatedSettingsApply(req, s, proposal, op, reviewAck, disruptionAck, exclusiveAck)
	}
	wrong := request.Clone(request.Context())
	wrong.Header.Set("Authorization", "Bearer "+peerToken)
	plaintext := request.Clone(request.Context())
	plaintext.TLS = nil
	canceled, cancel := context.WithCancel(request.Context())
	cancel()
	for _, tt := range []struct {
		name          string
		req           *http.Request
		state         *reloaderState
		review        modelconfig.DelegatedProposal
		op            string
		reviewAck     bool
		disruptionAck bool
		exclusiveAck  bool
		want          error
	}{
		{"wrong caller", wrong, state, review, owner.input.OperationID, true, true, true, errDelegationUnauthorized},
		{"plaintext", plaintext, state, review, owner.input.OperationID, true, true, true, errDelegationUnauthorized},
		{"canceled", request.WithContext(canceled), state, review, owner.input.OperationID, true, true, true, context.Canceled},
		{"stale state", request, &reloaderState{}, review, owner.input.OperationID, true, true, true, errDelegationChanged},
		{"missing review", request, state, review, owner.input.OperationID, false, true, true, errDelegationAcknowledgment},
		{"missing disruption", request, state, review, owner.input.OperationID, true, false, true, errDelegationAcknowledgment},
		{"missing exclusive window", request, state, review, owner.input.OperationID, true, true, false, errDelegationAcknowledgment},
		{"invalid operation", request, state, review, "../invalid", true, true, true, modelconfig.ErrProposalInvalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := apply(tt.req, tt.state, tt.review, tt.op, tt.reviewAck, tt.disruptionAck, tt.exclusiveAck); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	tampered := review
	tampered.Proposal.ModelID = "other"
	if _, err := apply(request, state, tampered, owner.input.OperationID, true, true, true); !errors.Is(err, modelconfig.ErrDelegatedProposalInvalid) {
		t.Fatalf("tampered review accepted: %v", err)
	}
	clients["owner"].routeManagement = false
	if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationPeerUnavailable) {
		t.Fatalf("disabled route accepted: %v", err)
	}
	clients["owner"].routeManagement = true
	if calls.Load() != 0 {
		t.Fatal("preflight rejection reached owner")
	}
	revoked := *owner.state
	revoked.peer = copyPeerConfig(owner.state.peer)
	revoked.peer.Peers["requester"] = config.PeerGrant{Token: peerToken, CatalogRead: true}
	owner.r.lifecycleMu.Lock()
	owner.r.state.Store(&revoked)
	owner.r.lifecycleMu.Unlock()
	if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationOutcomeUnknown) {
		t.Fatalf("revoked owner grant before apply: %v", err)
	}
	owner.r.lifecycleMu.Lock()
	owner.r.state.Store(owner.state)
	owner.r.instanceID = strings.Repeat("b", 32)
	owner.r.lifecycleMu.Unlock()
	if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationOutcomeUnknown) {
		t.Fatalf("restarted owner accepted stale review: %v", err)
	}
	owner.r.lifecycleMu.Lock()
	owner.r.instanceID = review.Owner.ProxyInstanceID
	owner.r.lifecycleMu.Unlock()
	if _, err := modelconfig.LoadDelegatedOperation(owner.operations, review.Proposal.ID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("rejected owner request reserved an operation: %v", err)
	}

	mode.Store(1)
	before := calls.Load()
	if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationOutcomeUnknown) {
		t.Fatalf("lost response was not unknown: %v", err)
	}
	if calls.Load() != before+1 {
		t.Fatalf("apply retried automatically: %d additional calls", calls.Load()-before)
	}
	stored, err := receiver.forwardDelegatedOperation(request, state, "owner", review.Proposal.ID, owner.input.OperationID)
	if err != nil || stored.Outcome != "written" {
		t.Fatalf("lookup did not recover owner outcome: %+v, %v", stored, err)
	}
	written, err := os.ReadFile(filepath.Join(owner.target, "config.yaml"))
	if err != nil || bytes.Equal(written, owner.source) {
		t.Fatalf("expected owner write: %v", err)
	}
	mode.Store(0)
	record, err := apply(request, state, review, owner.input.OperationID, true, true, true)
	if err != nil || record.Outcome != "written" || record.OperationID != owner.input.OperationID {
		t.Fatalf("exact replay: %+v, %v", record, err)
	}
	for _, m := range []int32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		mode.Store(m)
		if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationOutcomeUnknown) {
			t.Fatalf("invalid response mode %d: %v", m, err)
		}
	}
	if redirects.Load() != 0 {
		t.Fatal("apply followed a redirect")
	}
	mode.Store(12)
	done := make(chan error, 1)
	go func() {
		_, err := apply(request, state, review, owner.input.OperationID, true, true, true)
		done <- err
	}()
	<-entered
	if receiver.lifecycleMu.TryLock() {
		receiver.lifecycleMu.Unlock()
		close(release)
		<-done
		t.Fatal("receiving configuration changed during apply forwarding")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight replay: %v", err)
	}
	mode.Store(0)
	updated := *owner.state
	updated.peer = copyPeerConfig(owner.state.peer)
	updated.peer.Peers["requester"] = config.PeerGrant{Token: peerToken, CatalogRead: true}
	owner.r.lifecycleMu.Lock()
	owner.r.state.Store(&updated)
	owner.r.lifecycleMu.Unlock()
	if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationOutcomeUnknown) {
		t.Fatalf("revoked owner grant: %v", err)
	}
	server.Close()
	if _, err := apply(request, state, review, owner.input.OperationID, true, true, true); !errors.Is(err, errDelegationOutcomeUnknown) {
		t.Fatalf("owner outage was not unknown: %v", err)
	}
	backups, err := modelconfig.ListBackups(owner.state.management.BackupStore.Directory)
	if err != nil || len(backups) != 1 {
		t.Fatalf("replay created extra backups: %d, %v", len(backups), err)
	}
	current, err := os.ReadFile(filepath.Join(owner.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, written) {
		t.Fatalf("replay changed target: %v", err)
	}
	unregistered := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsApplyPath, nil)
	unregistered.Header.Set("Authorization", "Bearer "+peerToken)
	recorder := httptest.NewRecorder()
	owner.r.PeerHandler().ServeHTTP(recorder, unregistered)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("apply route did not enforce the revoked peer grant: %d", recorder.Code)
	}
}

func TestRegisteredDelegatedSettingsManagementRoundTrip(t *testing.T) {
	owner := newDelegatedApplyFixture(t)
	owner.r.peerSlots = make(chan struct{}, 1)
	ownerServer := httptest.NewTLSServer(owner.r.PeerHandler())
	defer ownerServer.Close()
	caPath := filepath.Join(t.TempDir(), "owner-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ownerServer.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	peerToken, caller := strings.Repeat("p", 32), strings.Repeat("c", 32)
	peer := &config.PeerConfig{NodeID: "requester", Peers: map[string]config.PeerGrant{
		"owner": {URL: ownerServer.URL, OutboundToken: peerToken, CAFile: caPath, RouteManagement: true},
	}}
	clients, _, err := newPeerClients(peer)
	if err != nil {
		t.Fatal(err)
	}
	defer closePeerClients(clients)
	state := &reloaderState{peer: peer, peerClients: clients, management: &config.ManagementConfig{DelegationToken: caller}}
	receiver := &Reloader{logger: discardLogger(), managementSlots: make(chan struct{}, 1)}
	receiver.state.Store(state)
	receiverServer := httptest.NewTLSServer(receiver.ManagementHandler())
	defer receiverServer.Close()
	client := receiverServer.Client()

	review, err := modelconfig.LoadDelegatedProposal(owner.proposals, owner.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	applyBody, err := json.Marshal(struct {
		Review                     modelconfig.DelegatedProposal `json:"review"`
		OperationID                string                        `json:"operation_id"`
		AcknowledgeReview          bool                          `json:"acknowledge_review"`
		AcknowledgeDisruption      bool                          `json:"acknowledge_disruption"`
		AcknowledgeExclusiveWindow bool                          `json:"acknowledge_exclusive_window"`
	}{review, owner.input.OperationID, true, true, true})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, receiverServer.URL+receivedSettingsApplyPath, bytes.NewReader(applyBody))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+caller)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var operation modelconfig.DelegatedOperation
	if err := json.NewDecoder(response.Body).Decode(&operation); err != nil {
		response.Body.Close()
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || operation.Outcome != "written" || operation.OperationID != owner.input.OperationID {
		t.Fatalf("registered apply status=%d operation=%+v", response.StatusCode, operation)
	}

	lookupURL := receiverServer.URL + receivedOperationPath + "owner/" + owner.input.ProposalID + "/" + owner.input.OperationID
	lookup, err := http.NewRequest(http.MethodGet, lookupURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	lookup.Header.Set("Authorization", "Bearer "+caller)
	response, err = client.Do(lookup)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var recovered modelconfig.DelegatedOperation
	if err := json.NewDecoder(response.Body).Decode(&recovered); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || recovered.Outcome != "written" || recovered.RequesterNode != "requester" {
		t.Fatalf("registered lookup status=%d operation=%+v", response.StatusCode, recovered)
	}
	if _, err := os.ReadFile(filepath.Join(owner.target, "config.yaml")); err != nil {
		t.Fatal(err)
	}
}

func TestPeerDelegatedApplyRejectsInvalidRequests(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.logger = discardLogger()
	f.r.peerSlots = make(chan struct{}, 1)
	data, err := json.Marshal(f.input)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range []struct {
		name   string
		method string
		path   string
		body   string
		edit   func(*http.Request)
		want   int
	}{
		{"plaintext", http.MethodPost, peerSettingsApplyPath, string(data), func(r *http.Request) { r.TLS = nil }, http.StatusForbidden},
		{"wrong method", http.MethodGet, peerSettingsApplyPath, "", nil, http.StatusMethodNotAllowed},
		{"query", http.MethodPost, peerSettingsApplyPath + "?", string(data), nil, http.StatusNotFound},
		{"encoded path", http.MethodPost, "/peer/v1/management/settings/%61pply", string(data), nil, http.StatusNotFound},
		{"wrong token", http.MethodPost, peerSettingsApplyPath, string(data), func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
		}, http.StatusUnauthorized},
		{"duplicate token", http.MethodPost, peerSettingsApplyPath, string(data), func(r *http.Request) {
			r.Header.Add("Authorization", "Bearer "+strings.Repeat("p", 32))
		}, http.StatusUnauthorized},
		{"unknown field", http.MethodPost, peerSettingsApplyPath, `{"extra":true}`, nil, http.StatusBadRequest},
		{"duplicate field", http.MethodPost, peerSettingsApplyPath, `{"backend_id":"backend","BACKEND_ID":"backend"}`, nil, http.StatusBadRequest},
		{"oversize", http.MethodPost, peerSettingsApplyPath, string(data) + strings.Repeat(" ", 8192), nil, http.StatusBadRequest},
		{"missing acknowledgement", http.MethodPost, peerSettingsApplyPath, `{}`, nil, http.StatusServiceUnavailable},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "https://owner.example"+tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
			req.Header.Set("Content-Type", "application/json")
			if tt.edit != nil {
				tt.edit(req)
			}
			recorder := httptest.NewRecorder()
			f.r.servePeerDelegatedSettingsApply(recorder, req)
			if recorder.Code != tt.want || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d, want=%d", recorder.Code, tt.want)
			}
		})
	}
	if _, err := modelconfig.LoadDelegatedOperation(f.operations, f.input.ProposalID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("invalid request reserved operation: %v", err)
	}
	current, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, f.source) {
		t.Fatalf("invalid request changed target: %v", err)
	}
}

func TestPeerDelegatedApplyRechecksRevocationAfterBodyRead(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.logger = discardLogger()
	f.r.peerSlots = make(chan struct{}, 1)
	data, err := json.Marshal(f.input)
	if err != nil {
		t.Fatal(err)
	}
	body := &delayedReviewBody{
		reader: strings.NewReader(string(data)), started: make(chan struct{}), release: make(chan struct{}),
	}
	req := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsApplyPath, body)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		f.r.servePeerDelegatedSettingsApply(recorder, req)
		close(done)
	}()
	<-body.started
	revoked := *f.state
	revoked.peer = copyPeerConfig(f.state.peer)
	revoked.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
	f.r.lifecycleMu.Lock()
	f.r.state.Store(&revoked)
	f.r.lifecycleMu.Unlock()
	close(body.release)
	<-done
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("revoked in-flight request status=%d", recorder.Code)
	}
	if _, err := modelconfig.LoadDelegatedOperation(f.operations, f.input.ProposalID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("revoked in-flight request reserved an operation: %v", err)
	}
	current, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, f.source) {
		t.Fatalf("revoked in-flight request changed target: %v", err)
	}
}
