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

func TestForwardDelegatedProposalAcrossTLS(t *testing.T) {
	owner := newDelegatedApplyFixture(t)
	existing, err := modelconfig.LoadDelegatedProposal(owner.proposals, owner.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	input := delegatedSettingsRequest{
		Owner: existing.Owner, Backend: existing.BackendID, Model: existing.Proposal.ModelID,
		Revision: existing.Proposal.Revision, Settings: existing.Proposal.Settings,
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
		if req.URL.Path == peerSettingsApplyPath {
			owner.r.servePeerDelegatedSettingsApply(w, req)
			return
		}
		calls.Add(1)
		if req.Method != http.MethodPost || req.URL.Path != peerSettingsProposalPath ||
			req.Header.Get("Authorization") != "Bearer "+peerToken ||
			req.Header.Get("Cookie") != "" || req.Header.Get("X-Unsafe") != "" ||
			req.Header.Get("Idempotency-Key") != "" {
			t.Errorf("unexpected proposal request: %s %s", req.Method, req.URL.Path)
		}
		response := httptest.NewRecorder()
		owner.r.servePeerDelegatedSettingsProposal(response, req)
		if mode.Load() == 12 {
			close(entered)
			<-release
		}
		switch m := mode.Load(); m {
		case 1:
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
		case 2, 3, 4, 5, 6, 7, 8, 11:
			var proposal modelconfig.DelegatedProposal
			if err := json.Unmarshal(response.Body.Bytes(), &proposal); err != nil {
				t.Error(err)
				return
			}
			lifetime := delegatedProposalLifetime
			switch m {
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
		case 9:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, response.Body.String()+strings.Repeat(" ", maxManagementRequest))
		case 10:
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
	for _, tt := range []struct {
		name  string
		req   *http.Request
		state *reloaderState
		want  error
	}{
		{"wrong caller", wrong, state, errDelegationUnauthorized},
		{"plaintext", plaintext, state, errDelegationUnauthorized},
		{"canceled", request.WithContext(canceled), state, context.Canceled},
		{"stale state", request, &reloaderState{}, errDelegationChanged},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := receiver.forwardDelegatedSettingsProposal(tt.req, tt.state, input); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	clients["owner"].routeManagement = false
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, input); !errors.Is(err, errDelegationPeerUnavailable) {
		t.Fatalf("disabled route accepted: %v", err)
	}
	clients["owner"].routeManagement = true
	rotated := *state
	rotated.management = copyManagementConfig(state.management)
	rotated.management.DelegationToken = strings.Repeat("n", 32)
	receiver.lifecycleMu.Lock()
	receiver.state.Store(&rotated)
	receiver.lifecycleMu.Unlock()
	if _, err := receiver.forwardDelegatedSettingsProposal(request, &rotated, input); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("revoked receiving credential accepted: %v", err)
	}
	receiver.lifecycleMu.Lock()
	receiver.state.Store(state)
	receiver.lifecycleMu.Unlock()
	oversized := input
	oversized.Model = strings.Repeat("a", maxBenchmarkJSONBytes)
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, oversized); !errors.Is(err, modelconfig.ErrDelegatedProposalInvalid) {
		t.Fatalf("oversized request accepted: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("rejected request reached owner")
	}
	revoked := *owner.state
	revoked.peer = copyPeerConfig(owner.state.peer)
	revoked.peer.Peers["requester"] = config.PeerGrant{Token: peerToken, CatalogRead: true}
	owner.r.lifecycleMu.Lock()
	owner.r.state.Store(&revoked)
	owner.r.lifecycleMu.Unlock()
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, input); !errors.Is(err, errDelegationProposalUnknown) {
		t.Fatalf("revoked owner grant: %v", err)
	}
	owner.r.lifecycleMu.Lock()
	owner.r.state.Store(owner.state)
	owner.r.lifecycleMu.Unlock()
	staleOwner := input
	staleOwner.Owner.ConfigGeneration++
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, staleOwner); !errors.Is(err, errDelegationProposalUnknown) {
		t.Fatalf("stale owner incarnation: %v", err)
	}
	staleSource := input
	staleSource.Revision = strings.Repeat("b", 64)
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, staleSource); !errors.Is(err, errDelegationProposalUnknown) {
		t.Fatalf("stale source revision: %v", err)
	}
	ids, err := modelconfig.ListDelegatedProposals(owner.proposals)
	if err != nil || len(ids) != 1 {
		t.Fatalf("rejected owner request persisted a review: %v, %v", ids, err)
	}
	mode.Store(1)
	before := calls.Load()
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, input); !errors.Is(err, errDelegationProposalUnknown) {
		t.Fatalf("lost response was not unknown: %v", err)
	}
	if calls.Load() != before+1 {
		t.Fatalf("proposal request automatically retried: %d additional calls", calls.Load()-before)
	}
	ids, err = modelconfig.ListDelegatedProposals(owner.proposals)
	if err != nil || len(ids) != 2 {
		t.Fatalf("lost response did not leave exactly one stored review: %v, %v", ids, err)
	}
	for _, m := range []int32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11} {
		mode.Store(m)
		if _, err := receiver.forwardDelegatedSettingsProposal(request, state, input); !errors.Is(err, errDelegationProposalUnknown) {
			t.Fatalf("untrusted response mode %d: %v", m, err)
		}
	}
	if redirects.Load() != 0 {
		t.Fatal("proposal request followed a redirect")
	}
	mode.Store(12)
	done := make(chan error, 1)
	go func() {
		_, err := receiver.forwardDelegatedSettingsProposal(request, state, input)
		done <- err
	}()
	<-entered
	if receiver.lifecycleMu.TryLock() {
		receiver.lifecycleMu.Unlock()
		close(release)
		<-done
		t.Fatal("receiving configuration changed during proposal forwarding")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight proposal: %v", err)
	}
	mode.Store(0)
	proposal, err := receiver.forwardDelegatedSettingsProposal(request, state, input)
	if err != nil || proposal.Owner != input.Owner || proposal.RequesterNode != "requester" ||
		proposal.Proposal.TargetID != "swap" || proposal.Proposal.ModelID != input.Model || proposal.Proposal.Revision != input.Revision ||
		proposal.ExpiresAt.Sub(proposal.Proposal.CreatedAt) != delegatedProposalLifetime {
		t.Fatalf("proposal = %+v, %v", proposal, err)
	}
	stored, err := modelconfig.LoadDelegatedProposal(owner.proposals, proposal.Proposal.ID)
	if err != nil || stored.ReviewRevision != proposal.ReviewRevision {
		t.Fatalf("owner did not persist returned proposal: %v", err)
	}
	data, err := json.Marshal(proposal)
	if err != nil || bytes.Contains(data, []byte(caller)) || bytes.Contains(data, []byte(peerToken)) ||
		bytes.Contains(data, []byte("confidential")) || bytes.Contains(data, []byte("/private/")) || bytes.Contains(data, []byte(owner.target)) {
		t.Fatalf("proposal exposed confidential data: %v", err)
	}
	current, err := os.ReadFile(filepath.Join(owner.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, owner.source) {
		t.Fatalf("proposal creation changed target: %v", err)
	}
	operation, err := receiver.forwardDelegatedSettingsApply(request, state, proposal, strings.Repeat("f", 32), true, true, true)
	if err != nil || operation.Outcome != "written" || operation.ProposalID != proposal.Proposal.ID {
		t.Fatalf("created review could not be applied: %+v, %v", operation, err)
	}
	unregistered := httptest.NewRequest(http.MethodPost, "https://owner.example"+peerSettingsProposalPath, nil)
	unregistered.Header.Set("Authorization", "Bearer "+peerToken)
	recorder := httptest.NewRecorder()
	owner.r.PeerHandler().ServeHTTP(recorder, unregistered)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("proposal route unexpectedly registered: %d", recorder.Code)
	}
	server.Close()
	if _, err := receiver.forwardDelegatedSettingsProposal(request, state, input); !errors.Is(err, errDelegationProposalUnknown) {
		t.Fatalf("owner outage was not unknown: %v", err)
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
		})
	}
}
