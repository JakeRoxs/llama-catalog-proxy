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

type tlsProposalHarness struct {
	owner     delegatedApplyFixture
	mode      *atomic.Int32
	calls     *atomic.Int32
	redirects *atomic.Int32
	entered   chan struct{}
	release   chan struct{}
	server    *httptest.Server
	caPath    string
	peerToken string
	caller    string
	clients   map[string]*peerClient
	state     *reloaderState
	receiver  *Reloader
	request   *http.Request
	wrong     *http.Request
	plaintext *http.Request
	canceled  context.Context
	input     delegatedSettingsRequest
	redirect  *httptest.Server
	cleanup   func()
}

type proposalHarnessServerConfig struct {
	owner     delegatedApplyFixture
	mode      *atomic.Int32
	calls     *atomic.Int32
	redirects *atomic.Int32
	entered   chan struct{}
	release   chan struct{}
	peerToken string
	redirect  *httptest.Server
}

func newTLSProposalHarness(t *testing.T) (tlsProposalHarness, func()) {
	t.Helper()
	owner := newDelegatedApplyFixture(t)
	_, input := loadDelegatedProposalRequest(t, owner)
	owner.r.logger = discardLogger()
	owner.r.peerSlots = make(chan struct{}, 2)
	var mode, calls, redirects atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	redirect := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirects.Add(1)
	}))
	peerToken, caller := strings.Repeat("p", 32), strings.Repeat("c", 32)
	serverConfig := proposalHarnessServerConfig{
		owner: owner, mode: &mode, calls: &calls, redirects: &redirects,
		entered: entered, release: release, peerToken: peerToken, redirect: redirect,
	}
	server := httptest.NewTLSServer(newProposalHarnessHandler(t, serverConfig))
	caPath := writeProposalPeerCA(t, server)
	peer, clients := newProposalPeerClient(t, server, peerToken, caPath)
	state := &reloaderState{peer: peer, peerClients: clients, management: &config.ManagementConfig{DelegationToken: caller}}
	receiver := &Reloader{managementSlots: make(chan struct{}, 2)}
	receiver.state.Store(state)
	request := httptest.NewRequest(http.MethodPost, "https://receiver.example/proposal", nil)
	request.Header.Set("Authorization", "Bearer "+caller)
	request.Header.Set("Cookie", "not-forwarded")
	request.Header.Set("X-Unsafe", "not-forwarded")
	wrong := request.Clone(request.Context())
	wrong.Header.Set("Authorization", "Bearer "+peerToken)
	plaintext := request.Clone(request.Context())
	plaintext.TLS = nil
	canceled, cancel := context.WithCancel(request.Context())
	cancel()
	h := tlsProposalHarness{
		owner:     owner,
		mode:      &mode,
		calls:     &calls,
		redirects: &redirects,
		entered:   entered,
		release:   release,
		server:    server,
		caPath:    caPath,
		peerToken: peerToken,
		caller:    caller,
		clients:   clients,
		state:     state,
		receiver:  receiver,
		request:   request,
		wrong:     wrong,
		plaintext: plaintext,
		canceled:  canceled,
		input:     input,
		redirect:  redirect,
	}
	cleanup := func() {
		server.Close()
		redirect.Close()
		closePeerClients(clients)
	}
	return h, cleanup
}

func loadDelegatedProposalRequest(t *testing.T, owner delegatedApplyFixture) (modelconfig.DelegatedProposal, delegatedSettingsRequest) {
	t.Helper()
	proposal, err := modelconfig.LoadDelegatedProposal(owner.proposals, owner.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	input := delegatedSettingsRequest{
		Owner: proposal.Owner, Backend: proposal.BackendID, Model: proposal.Proposal.ModelID,
		Revision: proposal.Proposal.Revision, Settings: proposal.Proposal.Settings,
	}
	return proposal, input
}

func writeProposalPeerCA(t *testing.T, server *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peer-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func newProposalPeerClient(t *testing.T, server *httptest.Server, peerToken, caPath string) (*config.PeerConfig, map[string]*peerClient) {
	t.Helper()
	peer := &config.PeerConfig{NodeID: "requester", Peers: map[string]config.PeerGrant{
		"owner": {URL: server.URL, OutboundToken: peerToken, CAFile: caPath, RouteManagement: true},
	}}
	clients, _, err := newPeerClients(peer)
	if err != nil {
		t.Fatal(err)
	}
	return peer, clients
}

func newProposalHarnessHandler(t *testing.T, cfg proposalHarnessServerConfig) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == peerSettingsApplyPath {
			cfg.owner.r.servePeerDelegatedSettingsApply(w, req)
			return
		}
		cfg.calls.Add(1)
		assertProposalHarnessRequest(t, req, cfg.peerToken)
		response := httptest.NewRecorder()
		cfg.owner.r.servePeerDelegatedSettingsProposal(response, req)
		if cfg.mode.Load() == 12 {
			close(cfg.entered)
			<-cfg.release
		}
		writeProposalHarnessResponse(t, w, req, response, cfg.mode.Load(), cfg.redirect.URL)
	})
}

func assertProposalHarnessRequest(t *testing.T, req *http.Request, peerToken string) {
	t.Helper()
	if req.Method != http.MethodPost || req.URL.Path != peerSettingsProposalPath ||
		req.Header.Get("Authorization") != "Bearer "+peerToken ||
		req.Header.Get("Cookie") != "" || req.Header.Get("X-Unsafe") != "" ||
		req.Header.Get("Idempotency-Key") != "" {
		t.Errorf("unexpected proposal request: %s %s", req.Method, req.URL.Path)
	}
}

func writeProposalHarnessResponse(t *testing.T, w http.ResponseWriter, req *http.Request, response *httptest.ResponseRecorder, mode int32, redirectURL string) {
	t.Helper()
	switch mode {
	case 1:
		closeProposalHarnessConnection(t, w)
	case 2, 3, 4, 5, 6, 7, 8, 11:
		writeForgedProposal(t, w, response, mode)
	case 9:
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, response.Body.String()+strings.Repeat(" ", maxManagementRequest))
	case 10:
		http.Redirect(w, req, redirectURL, http.StatusTemporaryRedirect)
	default:
		copyProposalHarnessResponse(w, response)
	}
}

func closeProposalHarnessConnection(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	connection, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	_ = connection.Close()
}

func writeForgedProposal(t *testing.T, w http.ResponseWriter, response *httptest.ResponseRecorder, mode int32) {
	t.Helper()
	var proposal modelconfig.DelegatedProposal
	if err := json.Unmarshal(response.Body.Bytes(), &proposal); err != nil {
		t.Error(err)
		return
	}
	lifetime := delegatedProposalLifetime
	switch mode {
	case 2:
		proposal.Owner.ConfigGeneration++
	case 3:
		proposal.RequesterNode = "another-requester"
	case 4:
		proposal.BackendID = "another-backend"
	case 5:
		proposal.Proposal.ModelID = "another-model"
	case 6:
		proposal.Proposal.Revision = strings.Repeat("b", 64)
	case 7:
		size := int32(8192)
		proposal.Proposal.Settings.ContextSize = &size
		proposal.Proposal.Changes[0].After = "8192"
	case 8:
		lifetime += time.Minute
	case 11:
		proposal.Proposal.CreatedAt = time.Now().Add(time.Minute)
	}
	forged, err := modelconfig.NewDelegatedProposal(proposal.Owner, proposal.RequesterNode, proposal.BackendID, proposal.Proposal, lifetime)
	if err != nil {
		t.Error(err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(forged); err != nil {
		t.Error(err)
	}
}

func copyProposalHarnessResponse(w http.ResponseWriter, response *httptest.ResponseRecorder) {
	for key, values := range response.Header() {
		w.Header()[key] = values
	}
	w.WriteHeader(response.Code)
	_, _ = w.Write(response.Body.Bytes())
}

func TestForwardDelegatedProposalAcrossTLS(t *testing.T) {
	h, cleanup := newTLSProposalHarness(t)
	defer cleanup()
	assertProposalPreflightRejections(t, h)
	assertProposalTransportFailures(t, h)
	assertProposalInFlightAndSuccess(t, h)
	assertProposalUnregisteredRouteAndOutage(t, h)
}

func assertProposalPreflightRejections(t *testing.T, h tlsProposalHarness) {
	for _, tc := range []struct {
		name  string
		req   *http.Request
		state *reloaderState
		want  error
	}{
		{"wrong caller", h.wrong, h.state, errDelegationUnauthorized},
		{"plaintext", h.plaintext, h.state, errDelegationUnauthorized},
		{"canceled", h.request.WithContext(h.canceled), h.state, context.Canceled},
		{"stale state", h.request, &reloaderState{}, errDelegationChanged},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertProposalRejected(t, h, tc.req, tc.state, h.input, tc.want)
		})
	}

	h.clients["owner"].routeManagement = false
	assertProposalRejected(t, h, h.request, h.state, h.input, errDelegationPeerUnavailable)
	h.clients["owner"].routeManagement = true

	rotated := *h.state
	rotated.management = copyManagementConfig(h.state.management)
	rotated.management.DelegationToken = strings.Repeat("n", 32)
	h.receiver.lifecycleMu.Lock()
	h.receiver.state.Store(&rotated)
	h.receiver.lifecycleMu.Unlock()
	assertProposalRejected(t, h, h.request, &rotated, h.input, errDelegationUnauthorized)
	h.receiver.lifecycleMu.Lock()
	h.receiver.state.Store(h.state)
	h.receiver.lifecycleMu.Unlock()

	oversized := h.input
	oversized.Model = strings.Repeat("a", maxBenchmarkJSONBytes)
	assertProposalRejected(t, h, h.request, h.state, oversized, modelconfig.ErrDelegatedProposalInvalid)
	if h.calls.Load() != 0 {
		t.Fatal("rejected request reached owner")
	}
}

func assertProposalTransportFailures(t *testing.T, h tlsProposalHarness) {
	revoked := *h.owner.state
	revoked.peer = copyPeerConfig(h.owner.state.peer)
	revoked.peer.Peers["requester"] = config.PeerGrant{Token: h.peerToken, CatalogRead: true}
	h.owner.r.lifecycleMu.Lock()
	h.owner.r.state.Store(&revoked)
	h.owner.r.lifecycleMu.Unlock()
	assertProposalRejected(t, h, h.request, h.state, h.input, errDelegationProposalUnknown)
	h.owner.r.lifecycleMu.Lock()
	h.owner.r.state.Store(h.owner.state)
	h.owner.r.lifecycleMu.Unlock()

	assertStaleProposalBindingsRejected(t, h)
	assertProposalStoredCount(t, h.owner.proposals, 1, "rejected owner request persisted a review")

	h.mode.Store(1)
	before := h.calls.Load()
	assertProposalRejected(t, h, h.request, h.state, h.input, errDelegationProposalUnknown)
	if h.calls.Load() != before+1 {
		t.Fatalf("proposal request automatically retried: %d additional calls", h.calls.Load()-before)
	}
	assertProposalStoredCount(t, h.owner.proposals, 2, "lost response did not leave exactly one stored review")

	for _, mode := range []int32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		h.mode.Store(mode)
		assertProposalRejected(t, h, h.request, h.state, h.input, errDelegationProposalUnknown)
	}
	if h.redirects.Load() != 0 {
		t.Fatal("proposal request followed a redirect")
	}
}

func assertStaleProposalBindingsRejected(t *testing.T, h tlsProposalHarness) {
	staleOwner := h.input
	staleOwner.Owner.ConfigGeneration++
	assertProposalRejected(t, h, h.request, h.state, staleOwner, errDelegationProposalUnknown)

	staleSource := h.input
	staleSource.Revision = strings.Repeat("b", 64)
	assertProposalRejected(t, h, h.request, h.state, staleSource, errDelegationProposalUnknown)
}

func assertProposalStoredCount(t *testing.T, store string, count int, message string) {
	t.Helper()
	ids, err := modelconfig.ListDelegatedProposals(store)
	if err != nil || len(ids) != count {
		t.Fatalf("%s: %v, %v", message, ids, err)
	}
}

func assertProposalInFlightAndSuccess(t *testing.T, h tlsProposalHarness) {
	h.mode.Store(12)
	done := make(chan error, 1)
	go func() {
		_, err := h.receiver.forwardDelegatedSettingsProposal(h.request, h.state, h.input)
		done <- err
	}()
	<-h.entered
	if h.receiver.lifecycleMu.TryLock() {
		h.receiver.lifecycleMu.Unlock()
		close(h.release)
		<-done
		t.Fatal("receiving configuration changed during proposal forwarding")
	}
	close(h.release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight proposal: %v", err)
	}

	h.mode.Store(0)
	proposal, err := h.receiver.forwardDelegatedSettingsProposal(h.request, h.state, h.input)
	if err != nil || proposal.Owner != h.input.Owner || proposal.RequesterNode != "requester" ||
		proposal.Proposal.TargetID != "swap" || proposal.Proposal.ModelID != h.input.Model ||
		proposal.Proposal.Revision != h.input.Revision ||
		proposal.ExpiresAt.Sub(proposal.Proposal.CreatedAt) != delegatedProposalLifetime {
		t.Fatalf("proposal = %+v, %v", proposal, err)
	}
	assertProposalValidated(t, h, &proposal)
	operation, err := h.receiver.forwardDelegatedSettingsApply(h.request, h.state, proposal, strings.Repeat("f", 32), true, true, true)
	if err != nil || operation.Outcome != "written" || operation.ProposalID != proposal.Proposal.ID {
		t.Fatalf("created review could not be applied: %+v, %v", operation, err)
	}
}

func assertProposalUnregisteredRouteAndOutage(t *testing.T, h tlsProposalHarness) {
	unregistered := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsProposalPath, nil)
	unregistered.Header.Set("Authorization", "Bearer "+h.peerToken)
	recorder := httptest.NewRecorder()
	h.owner.r.PeerHandler().ServeHTTP(recorder, unregistered)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("proposal route did not reject an empty request: %d", recorder.Code)
	}

	h.server.Close()
	if _, err := h.receiver.forwardDelegatedSettingsProposal(h.request, h.state, h.input); !errors.Is(err, errDelegationProposalUnknown) {
		t.Fatalf("owner outage was not unknown: %v", err)
	}
}

// assertProposalRejected asserts that forwarding the proposal returns exactly
// the expected error. It is used for every rejection mode: unauthorized callers,
// plaintext, canceled contexts, stale state, peer unavailable, revoked tokens,
// oversized input, lost responses, and untrusted mode mutations.
func assertProposalRejected(t *testing.T, h tlsProposalHarness, req *http.Request, state *reloaderState, input delegatedSettingsRequest, want error) {
	t.Helper()
	if _, err := h.receiver.forwardDelegatedSettingsProposal(req, state, input); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

// assertProposalValidated validates a successfully returned proposal: it matches
// the request identity, is persisted by the owner with the same review revision,
// exposes no confidential or private data, and leaves the target file unchanged.
func assertProposalValidated(t *testing.T, h tlsProposalHarness, proposal *modelconfig.DelegatedProposal) {
	t.Helper()
	if proposal.Owner != h.input.Owner || proposal.RequesterNode != "requester" ||
		proposal.Proposal.TargetID != "swap" || proposal.Proposal.ModelID != h.input.Model || proposal.Proposal.Revision != h.input.Revision ||
		proposal.ExpiresAt.Sub(proposal.Proposal.CreatedAt) != delegatedProposalLifetime {
		t.Fatalf("proposal = %+v", proposal)
	}
	stored, err := modelconfig.LoadDelegatedProposal(h.owner.proposals, proposal.Proposal.ID)
	if err != nil || stored.ReviewRevision != proposal.ReviewRevision {
		t.Fatalf("owner did not persist returned proposal: %v", err)
	}
	data, err := json.Marshal(proposal)
	if err != nil || bytes.Contains(data, []byte(h.caller)) || bytes.Contains(data, []byte(h.peerToken)) ||
		bytes.Contains(data, []byte("confidential")) || bytes.Contains(data, []byte("/private/")) || bytes.Contains(data, []byte(h.owner.target)) {
		t.Fatalf("proposal exposed confidential data: %v", err)
	}
	current, err := os.ReadFile(filepath.Join(h.owner.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, h.owner.source) {
		t.Fatalf("proposal creation changed target: %v", err)
	}
}

func TestForwardDelegatedProposalRecoveryAcrossTLS(t *testing.T) {
	owner := newDelegatedApplyFixture(t)
	owner.r.peerSlots = make(chan struct{}, 1)
	proposal, input := loadDelegatedProposalRequest(t, owner)
	peerToken, caller := strings.Repeat("p", 32), strings.Repeat("c", 32)
	server := httptest.NewTLSServer(owner.r.PeerHandler())
	defer server.Close()
	caPath := writeProposalPeerCA(t, server)
	peer, clients := newProposalPeerClient(t, server, peerToken, caPath)
	defer closePeerClients(clients)
	state := &reloaderState{peer: peer, peerClients: clients, management: &config.ManagementConfig{DelegationToken: caller}}
	receiver := &Reloader{logger: discardLogger(), managementSlots: make(chan struct{}, 1)}
	receiver.state.Store(state)
	receiverServer := httptest.NewTLSServer(receiver.ManagementHandler())
	defer receiverServer.Close()
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, receiverServer.URL+receivedSettingsRecoveryPath, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+caller)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Cookie", "must-not-forward")
	response, err := receiverServer.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var recovered modelconfig.DelegatedProposal
	if err := json.NewDecoder(response.Body).Decode(&recovered); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || recovered.Proposal.ID != proposal.Proposal.ID || len(recovered.RecoveryAudit) != 1 {
		t.Fatalf("TLS recovery status=%d proposal=%+v", response.StatusCode, recovered)
	}
	if event := recovered.RecoveryAudit[0]; event.RequesterNode != "requester" ||
		event.CredentialScope != modelconfig.DelegatedSettingsCredentialScope || event.Outcome != "recovered" {
		t.Fatalf("TLS recovery audit = %+v", event)
	}
	stored, err := modelconfig.LoadDelegatedProposal(owner.proposals, proposal.Proposal.ID)
	if err != nil || len(stored.RecoveryAudit) != 1 {
		t.Fatalf("owner did not durably record recovery: %+v, %v", stored.RecoveryAudit, err)
	}
	if _, err := modelconfig.LoadDelegatedOperation(owner.operations, proposal.Proposal.ID); !errors.Is(err, modelconfig.ErrProposalNotFound) {
		t.Fatalf("read-only recovery changed operation ledger: %v", err)
	}
}

func TestPeerDelegatedProposalRejectsInvalidRequests(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.logger = discardLogger()
	f.r.peerSlots = make(chan struct{}, 1)
	proposal, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	input := delegatedSettingsRequest{
		Owner: proposal.Owner, Backend: proposal.BackendID, Model: proposal.Proposal.ModelID,
		Revision: proposal.Proposal.Revision, Settings: proposal.Proposal.Settings,
	}
	data, err := json.Marshal(input)
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
		{"plaintext", http.MethodPost, peerSettingsProposalPath, string(data), func(r *http.Request) { r.TLS = nil }, http.StatusForbidden},
		{"method", http.MethodGet, peerSettingsProposalPath, "", nil, http.StatusMethodNotAllowed},
		{"query", http.MethodPost, peerSettingsProposalPath + "?", string(data), nil, http.StatusNotFound},
		{"encoded path", http.MethodPost, "/peer/v1/management/settings/%70roposals", string(data), nil, http.StatusNotFound},
		{"wrong token", http.MethodPost, peerSettingsProposalPath, string(data), func(r *http.Request) {
			r.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 32))
		}, http.StatusUnauthorized},
		{"duplicate token", http.MethodPost, peerSettingsProposalPath, string(data), func(r *http.Request) {
			r.Header.Add("Authorization", "Bearer "+strings.Repeat("p", 32))
		}, http.StatusUnauthorized},
		{"unknown field", http.MethodPost, peerSettingsProposalPath, `{"extra":true}`, nil, http.StatusBadRequest},
		{"duplicate field", http.MethodPost, peerSettingsProposalPath, `{"model_id":"model","MODEL_ID":"model"}`, nil, http.StatusBadRequest},
		{"oversize", http.MethodPost, peerSettingsProposalPath, string(data) + strings.Repeat(" ", maxBenchmarkJSONBytes), nil, http.StatusBadRequest},
		{"compressed", http.MethodPost, peerSettingsProposalPath, string(data), func(r *http.Request) {
			r.Header.Set("Content-Encoding", "gzip")
		}, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "https://owner.example"+tt.path, strings.NewReader(tt.body))
			req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
			req.Header.Set("Content-Type", "application/json")
			if tt.edit != nil {
				tt.edit(req)
			}
			recorder := httptest.NewRecorder()
			f.r.servePeerDelegatedSettingsProposal(recorder, req)
			if recorder.Code != tt.want || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status=%d, want=%d", recorder.Code, tt.want)
			}
		})
	}
	f.r.peerSlots <- struct{}{}
	req := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsProposalPath, strings.NewReader(string(data)))
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	f.r.servePeerDelegatedSettingsProposal(recorder, req)
	<-f.r.peerSlots
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("unbounded proposal requests accepted: %d", recorder.Code)
	}
	ids, err := modelconfig.ListDelegatedProposals(f.proposals)
	if err != nil || len(ids) != 1 || ids[0] != f.input.ProposalID {
		t.Fatalf("invalid requests changed proposal store: %v, %v", ids, err)
	}
	current, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, f.source) {
		t.Fatalf("invalid requests changed target: %v", err)
	}
}

func TestPeerDelegatedProposalRechecksAfterBodyRead(t *testing.T) {
	for _, change := range []string{"revocation", "cancellation"} {
		t.Run(change, func(t *testing.T) {
			verifyPeerDelegatedProposalRechecksAfterBodyRead(t, change)
		})
	}
}

// verifyPeerDelegatedProposalRechecksAfterBodyRead exercises one revocation or cancellation change
// and asserts the owner rejects the request after the body read without persisting a proposal.
// Delayed body synchronization, cancel/revoke sequence, lifecycle locking, the release channel,
// the response status, and the persisted-proposal assertion are preserved exactly.
func verifyPeerDelegatedProposalRechecksAfterBodyRead(t *testing.T, change string) {
	f := newDelegatedApplyFixture(t)
	f.r.logger = discardLogger()
	f.r.peerSlots = make(chan struct{}, 1)
	proposal, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	input := delegatedSettingsRequest{
		Owner: proposal.Owner, Backend: proposal.BackendID, Model: proposal.Proposal.ModelID,
		Revision: proposal.Proposal.Revision, Settings: proposal.Proposal.Settings,
	}
	data, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	body := &delayedReviewBody{
		reader: strings.NewReader(string(data)), started: make(chan struct{}), release: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsProposalPath, body).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		f.r.servePeerDelegatedSettingsProposal(recorder, req)
		close(done)
	}()
	<-body.started
	if change == "cancellation" {
		cancel()
	} else {
		revoked := *f.state
		revoked.peer = copyPeerConfig(f.state.peer)
		revoked.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
		f.r.lifecycleMu.Lock()
		f.r.state.Store(&revoked)
		f.r.lifecycleMu.Unlock()
	}
	close(body.release)
	<-done
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("%s during request returned %d", change, recorder.Code)
	}
	ids, err := modelconfig.ListDelegatedProposals(f.proposals)
	if err != nil || len(ids) != 1 || ids[0] != f.input.ProposalID {
		t.Fatalf("%s persisted a proposal: %v, %v", change, ids, err)
	}
}
