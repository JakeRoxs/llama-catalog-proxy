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

type delegatedApplyTransportHarness struct {
	t         *testing.T
	owner     delegatedApplyFixture
	review    modelconfig.DelegatedProposal
	peerToken string
	caller    string
	state     *reloaderState
	receiver  *Reloader
	clients   map[string]*peerClient
	request   *http.Request
	server    *httptest.Server
	redirect  *httptest.Server
	written   []byte
	mode      atomic.Int32
	calls     atomic.Int32
	redirects atomic.Int32
	entered   chan struct{}
	release   chan struct{}
}

type applyAcknowledgments struct {
	review, disruption, exclusive bool
}

func TestForwardDelegatedApplyLostResponseAndReplay(t *testing.T) {
	h := newDelegatedApplyTransportHarness(t)
	h.checkApplyPreflight()
	h.checkLostResponseReplay()
	h.checkApplyDuringLifecycleChange()
}

func newDelegatedApplyTransportHarness(t *testing.T) *delegatedApplyTransportHarness {
	t.Helper()
	h := &delegatedApplyTransportHarness{
		t: t, owner: newDelegatedApplyFixture(t), peerToken: strings.Repeat("p", 32),
		caller: strings.Repeat("c", 32), entered: make(chan struct{}), release: make(chan struct{}),
	}
	var err error
	h.review, err = modelconfig.LoadDelegatedProposal(h.owner.proposals, h.owner.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	h.owner.r.logger = discardLogger()
	h.owner.r.peerSlots = make(chan struct{}, 2)
	h.redirect = httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { h.redirects.Add(1) }))
	t.Cleanup(h.redirect.Close)
	h.server = httptest.NewTLSServer(http.HandlerFunc(h.serveOwnerRequest))
	t.Cleanup(h.server.Close)
	h.receiver, h.state, h.clients = newManagementPeerAccess(t, h.server, h.peerToken, h.caller, 2)
	h.request = httptest.NewRequest(http.MethodPost, "https://receiver.example/apply", nil)
	h.request.Header.Set("Authorization", "Bearer "+h.caller)
	h.request.Header.Set("Cookie", "not-forwarded")
	h.request.Header.Set("X-Unsafe", "not-forwarded")
	return h
}

func newManagementPeerAccess(t *testing.T, ownerServer *httptest.Server, peerToken, caller string, slots int) (*Reloader, *reloaderState, map[string]*peerClient) {
	t.Helper()
	caPath := filepath.Join(t.TempDir(), "peer-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ownerServer.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	peer := &config.PeerConfig{NodeID: "requester", Peers: map[string]config.PeerGrant{
		"owner": {URL: ownerServer.URL, OutboundToken: peerToken, CAFile: caPath, RouteManagement: true},
	}}
	clients, _, err := newPeerClients(peer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePeerClients(clients) })
	state := &reloaderState{peer: peer, peerClients: clients, management: &config.ManagementConfig{DelegationToken: caller}}
	receiver := &Reloader{logger: discardLogger(), managementSlots: make(chan struct{}, slots)}
	receiver.state.Store(state)
	return receiver, state, clients
}

func (h *delegatedApplyTransportHarness) serveOwnerRequest(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, peerOperationPath) {
		h.owner.r.servePeerDelegatedOperation(w, req)
		return
	}
	h.calls.Add(1)
	if req.Method != http.MethodPost || req.URL.Path != peerSettingsApplyPath ||
		req.Header.Get("Authorization") != "Bearer "+h.peerToken || req.Header.Get("Cookie") != "" ||
		req.Header.Get("X-Unsafe") != "" || req.Header.Get("Idempotency-Key") != "" {
		h.t.Errorf("unexpected peer request: %s %s", req.Method, req.URL.Path)
	}
	response := httptest.NewRecorder()
	h.owner.r.servePeerDelegatedSettingsApply(response, req)
	if h.mode.Load() == 12 {
		close(h.entered)
		<-h.release
	}
	writeApplyTransportResponse(h.t, w, req, h.mode.Load(), response, h.redirect.URL)
}

func writeApplyTransportResponse(t *testing.T, w http.ResponseWriter, req *http.Request, mode int32, response *httptest.ResponseRecorder, redirectURL string) {
	t.Helper()
	switch mode {
	case 1:
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	case 2, 3, 4, 5, 6, 7, 8, 9:
		writeTamperedApplyOperation(t, w, response, mode)
	case 10:
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, strings.Repeat(" ", maxManagementRequest+1))
	case 11:
		http.Redirect(w, req, redirectURL, http.StatusTemporaryRedirect)
	default:
		for key, values := range response.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(response.Code)
		_, _ = w.Write(response.Body.Bytes())
	}
}

func writeTamperedApplyOperation(t *testing.T, w http.ResponseWriter, response *httptest.ResponseRecorder, mode int32) {
	t.Helper()
	var operation modelconfig.DelegatedOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
		t.Error(err)
		return
	}
	switch mode {
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
}

func (h *delegatedApplyTransportHarness) apply(req *http.Request, state *reloaderState, review modelconfig.DelegatedProposal, op string, ack applyAcknowledgments) (modelconfig.DelegatedOperation, error) {
	return h.receiver.forwardDelegatedSettingsApply(req, state, review, op, ack.review, ack.disruption, ack.exclusive)
}

func (h *delegatedApplyTransportHarness) applyApproved() (modelconfig.DelegatedOperation, error) {
	return h.apply(h.request, h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{true, true, true})
}

func (h *delegatedApplyTransportHarness) checkApplyPreflight() {
	h.t.Helper()
	wrong := h.request.Clone(h.request.Context())
	wrong.Header.Set("Authorization", "Bearer "+h.peerToken)
	plaintext := h.request.Clone(h.request.Context())
	plaintext.TLS = nil
	canceled, cancel := context.WithCancel(h.request.Context())
	cancel()
	cases := []struct {
		name   string
		req    *http.Request
		state  *reloaderState
		review modelconfig.DelegatedProposal
		op     string
		ack    applyAcknowledgments
		want   error
	}{
		{"wrong caller", wrong, h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{true, true, true}, errDelegationUnauthorized},
		{"plaintext", plaintext, h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{true, true, true}, errDelegationUnauthorized},
		{"canceled", h.request.WithContext(canceled), h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{true, true, true}, context.Canceled},
		{"stale state", h.request, &reloaderState{}, h.review, h.owner.input.OperationID, applyAcknowledgments{true, true, true}, errDelegationChanged},
		{"missing review", h.request, h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{false, true, true}, errDelegationAcknowledgment},
		{"missing disruption", h.request, h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{true, false, true}, errDelegationAcknowledgment},
		{"missing exclusive window", h.request, h.state, h.review, h.owner.input.OperationID, applyAcknowledgments{true, true, false}, errDelegationAcknowledgment},
		{"invalid operation", h.request, h.state, h.review, "../invalid", applyAcknowledgments{true, true, true}, modelconfig.ErrProposalInvalid},
	}
	for _, tt := range cases {
		h.t.Run(tt.name, func(t *testing.T) {
			if _, err := h.apply(tt.req, tt.state, tt.review, tt.op, tt.ack); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	tampered := h.review
	tampered.Proposal.ModelID = "other"
	if _, err := h.apply(h.request, h.state, tampered, h.owner.input.OperationID, applyAcknowledgments{true, true, true}); !errors.Is(err, modelconfig.ErrDelegatedProposalInvalid) {
		h.t.Fatalf("tampered review accepted: %v", err)
	}
	h.clients["owner"].routeManagement = false
	if _, err := h.applyApproved(); !errors.Is(err, errDelegationPeerUnavailable) {
		h.t.Fatalf("disabled route accepted: %v", err)
	}
	h.clients["owner"].routeManagement = true
	if h.calls.Load() != 0 {
		h.t.Fatal("preflight rejection reached owner")
	}
}

func (h *delegatedApplyTransportHarness) checkLostResponseReplay() {
	h.t.Helper()
	h.revokeOwnerGrant()
	if _, err := h.applyApproved(); !errors.Is(err, errDelegationOutcomeUnknown) {
		h.t.Fatalf("revoked owner grant before apply: %v", err)
	}
	h.owner.r.lifecycleMu.Lock()
	h.owner.r.state.Store(h.owner.state)
	h.owner.r.instanceID = strings.Repeat("b", 32)
	h.owner.r.lifecycleMu.Unlock()
	if _, err := h.applyApproved(); !errors.Is(err, errDelegationOutcomeUnknown) {
		h.t.Fatalf("restarted owner accepted stale review: %v", err)
	}
	h.owner.r.lifecycleMu.Lock()
	h.owner.r.instanceID = h.review.Owner.ProxyInstanceID
	h.owner.r.lifecycleMu.Unlock()
	if _, err := modelconfig.LoadDelegatedOperation(h.owner.operations, h.review.Proposal.ID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		h.t.Fatalf("rejected owner request reserved an operation: %v", err)
	}
	h.mode.Store(1)
	before := h.calls.Load()
	if _, err := h.applyApproved(); !errors.Is(err, errDelegationOutcomeUnknown) {
		h.t.Fatalf("lost response was not unknown: %v", err)
	}
	if h.calls.Load() != before+1 {
		h.t.Fatalf("apply retried automatically: %d additional calls", h.calls.Load()-before)
	}
	stored, err := h.receiver.forwardDelegatedOperation(h.request, h.state, "owner", h.review.Proposal.ID, h.owner.input.OperationID)
	if err != nil || stored.Outcome != "written" {
		h.t.Fatalf("lookup did not recover owner outcome: %+v, %v", stored, err)
	}
	written, err := os.ReadFile(filepath.Join(h.owner.target, "config.yaml"))
	if err != nil || bytes.Equal(written, h.owner.source) {
		h.t.Fatalf("expected owner write: %v", err)
	}
	h.written = written
	h.mode.Store(0)
	record, err := h.applyApproved()
	if err != nil || record.Outcome != "written" || record.OperationID != h.owner.input.OperationID {
		h.t.Fatalf("exact replay: %+v, %v", record, err)
	}
	for _, m := range []int32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		h.mode.Store(m)
		if _, err := h.applyApproved(); !errors.Is(err, errDelegationOutcomeUnknown) {
			h.t.Fatalf("invalid response mode %d: %v", m, err)
		}
	}
	if h.redirects.Load() != 0 {
		h.t.Fatal("apply followed a redirect")
	}
}

func (h *delegatedApplyTransportHarness) checkApplyDuringLifecycleChange() {
	h.t.Helper()
	h.mode.Store(12)
	done := make(chan error, 1)
	go func() { _, err := h.applyApproved(); done <- err }()
	<-h.entered
	if h.receiver.lifecycleMu.TryLock() {
		h.receiver.lifecycleMu.Unlock()
		close(h.release)
		<-done
		h.t.Fatal("receiving configuration changed during apply forwarding")
	}
	close(h.release)
	if err := <-done; err != nil {
		h.t.Fatalf("in-flight replay: %v", err)
	}
	h.mode.Store(0)
	h.revokeOwnerGrant()
	if _, err := h.applyApproved(); !errors.Is(err, errDelegationOutcomeUnknown) {
		h.t.Fatalf("revoked owner grant: %v", err)
	}
	h.server.Close()
	if _, err := h.applyApproved(); !errors.Is(err, errDelegationOutcomeUnknown) {
		h.t.Fatalf("owner outage was not unknown: %v", err)
	}
	backups, err := modelconfig.ListBackups(h.owner.state.management.BackupStore.Directory)
	if err != nil || len(backups) != 1 {
		h.t.Fatalf("replay created extra backups: %d, %v", len(backups), err)
	}
	current, err := os.ReadFile(filepath.Join(h.owner.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, h.written) {
		h.t.Fatalf("replay changed target: %v", err)
	}
	unregistered := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsApplyPath, nil)
	unregistered.Header.Set("Authorization", "Bearer "+h.peerToken)
	recorder := httptest.NewRecorder()
	h.owner.r.PeerHandler().ServeHTTP(recorder, unregistered)
	if recorder.Code != http.StatusUnauthorized {
		h.t.Fatalf("apply route did not enforce revoked peer grant: %d", recorder.Code)
	}
}

func (h *delegatedApplyTransportHarness) revokeOwnerGrant() {
	revoked := *h.owner.state
	revoked.peer = copyPeerConfig(h.owner.state.peer)
	revoked.peer.Peers["requester"] = config.PeerGrant{Token: h.peerToken, CatalogRead: true}
	h.owner.r.lifecycleMu.Lock()
	h.owner.r.state.Store(&revoked)
	h.owner.r.lifecycleMu.Unlock()
}

func TestRegisteredDelegatedSettingsManagementRoundTrip(t *testing.T) {
	owner := newDelegatedApplyFixture(t)
	owner.r.peerSlots = make(chan struct{}, 1)
	ownerServer := httptest.NewTLSServer(owner.r.PeerHandler())
	defer ownerServer.Close()
	peerToken, caller := strings.Repeat("p", 32), strings.Repeat("c", 32)
	receiver, _, _ := newManagementPeerAccess(t, ownerServer, peerToken, caller, 1)
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
