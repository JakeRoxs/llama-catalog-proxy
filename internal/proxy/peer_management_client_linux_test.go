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

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestForwardDelegatedOperationUsesScopedPeerTransport(t *testing.T) {
	fixture := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, fixture)
	operation, err := modelconfig.LoadDelegatedOperation(fixture.operations, fixture.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	caller, outbound := strings.Repeat("c", 32), strings.Repeat("t", 32)
	var mode, calls, redirects atomic.Int32
	entered, release, cancelEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	redirect := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirects.Add(1)
	}))
	defer redirect.Close()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if req.Method != http.MethodGet ||
			req.URL.Path != "/peer/v1/management/operations/"+fixture.input.ProposalID+"/"+fixture.input.OperationID ||
			req.URL.RawQuery != "" || req.Header.Get("Authorization") != "Bearer "+outbound ||
			req.Header.Get("X-Unsafe") != "" || req.Header.Get("Cookie") != "" {
			t.Errorf("unexpected outbound request: method=%s path=%s", req.Method, req.URL.Path)
		}
		if mode.Load() == 7 {
			http.Redirect(w, req, redirect.URL, http.StatusFound)
			return
		}
		if mode.Load() == 8 {
			http.Error(w, "secret remote error", http.StatusForbidden)
			return
		}
		if mode.Load() == 11 {
			close(entered)
			<-release
		}
		if mode.Load() == 13 {
			close(cancelEntered)
			<-req.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		record := operation
		switch mode.Load() {
		case 1:
			record.Owner.NodeID = "different"
		case 2:
			record.RequesterNode = "other"
		case 3:
			record.OperationID = strings.Repeat("f", 32)
		case 4:
			_, _ = io.WriteString(w, strings.Repeat("x", maxManagementRequest+1))
			return
		case 5:
			w.Header().Set("Content-Type", "text/plain")
		case 6:
			w.Header().Set("Content-Encoding", "gzip")
		case 12:
			record.CallerScope = "individual"
		}
		data, err := json.Marshal(record)
		if err != nil {
			t.Error(err)
			return
		}
		if mode.Load() == 9 {
			data = append(bytes.TrimSuffix(data, []byte("}")), []byte(`,"outcome":"written"}`)...)
		}
		if mode.Load() == 10 {
			data = append(bytes.TrimSuffix(data, []byte("}")), []byte(`,"unexpected":"secret"}`)...)
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	caPath := filepath.Join(t.TempDir(), "peer-ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	peer := &config.PeerConfig{NodeID: "requester", Peers: map[string]config.PeerGrant{
		"owner": {URL: server.URL, OutboundToken: outbound, CAFile: caPath, RouteManagement: true},
	}}
	clients, _, err := newPeerClients(peer)
	if err != nil {
		t.Fatal(err)
	}
	defer closePeerClients(clients)
	state := &reloaderState{peer: peer, peerClients: clients, management: &config.ManagementConfig{DelegationToken: caller}}
	r := &Reloader{managementSlots: make(chan struct{}, 2)}
	r.state.Store(state)
	request := httptest.NewRequest(http.MethodGet, "https://receiver.example/management/v1/operation", nil)
	request.Header.Set("Authorization", "Bearer "+caller)
	request.Header.Set("X-Unsafe", "do-not-forward")
	request.Header.Set("Cookie", "do-not-forward")
	lookup := func(req *http.Request, s *reloaderState, owner, proposal, op string) (modelconfig.DelegatedOperation, error) {
		return r.forwardDelegatedOperation(req, s, owner, proposal, op)
	}
	record, err := lookup(request, state, "owner", fixture.input.ProposalID, fixture.input.OperationID)
	if err != nil || record.Outcome != "unknown" || record.Owner.NodeID != "owner" {
		t.Fatalf("valid peer lookup = %+v, %v", record, err)
	}
	for _, tt := range []struct {
		mode int32
		want error
	}{
		{1, errDelegationPeerResponse}, {2, errDelegationPeerResponse}, {3, errDelegationPeerResponse},
		{4, errDelegationPeerResponse}, {5, errDelegationPeerResponse}, {6, errDelegationPeerResponse},
		{7, errDelegationPeerUnavailable}, {8, errDelegationPeerUnavailable}, {9, errDelegationPeerResponse},
		{10, errDelegationPeerResponse}, {12, errDelegationPeerResponse},
	} {
		mode.Store(tt.mode)
		result, err := lookup(request, state, "owner", fixture.input.ProposalID, fixture.input.OperationID)
		if !errors.Is(err, tt.want) || result.OperationID != "" || err != nil && strings.Contains(err.Error(), "secret") {
			t.Fatalf("mode %d: %+v, %v", tt.mode, result, err)
		}
	}
	if redirects.Load() != 0 {
		t.Fatal("management request followed a redirect")
	}
	mode.Store(11)
	done := make(chan error, 1)
	go func() {
		_, err := lookup(request, state, "owner", fixture.input.ProposalID, fixture.input.OperationID)
		done <- err
	}()
	<-entered
	if r.lifecycleMu.TryLock() {
		r.lifecycleMu.Unlock()
		close(release)
		<-done
		t.Fatal("configuration reload could race an active forwarding operation")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("in-flight request failed: %v", err)
	}
	mode.Store(13)
	ctx, cancelInFlight := context.WithCancel(request.Context())
	go func() {
		_, err := lookup(request.WithContext(ctx), state, "owner", fixture.input.ProposalID, fixture.input.OperationID)
		done <- err
	}()
	<-cancelEntered
	cancelInFlight()
	if err := <-done; !errors.Is(err, errDelegationPeerUnavailable) {
		t.Fatalf("in-flight cancellation returned %v", err)
	}
	mode.Store(0)
	before := calls.Load()
	wrong := request.Clone(request.Context())
	wrong.Header.Set("Authorization", "Bearer "+outbound)
	duplicate := request.Clone(request.Context())
	duplicate.Header.Add("Authorization", "Bearer "+caller)
	plaintext := request.Clone(request.Context())
	plaintext.TLS = nil
	canceled, cancel := context.WithCancel(request.Context())
	cancel()
	for _, tt := range []struct {
		name     string
		request  *http.Request
		owner    string
		proposal string
		want     error
	}{
		{"wrong caller", wrong, "owner", fixture.input.ProposalID, errDelegationUnauthorized},
		{"duplicate caller", duplicate, "owner", fixture.input.ProposalID, errDelegationUnauthorized},
		{"plaintext", plaintext, "owner", fixture.input.ProposalID, errDelegationUnauthorized},
		{"canceled", request.WithContext(canceled), "owner", fixture.input.ProposalID, context.Canceled},
		{"unconfigured owner", request, "other", fixture.input.ProposalID, errDelegationPeerUnavailable},
		{"invalid ID", request, "owner", "../bad", modelconfig.ErrProposalInvalid},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := lookup(tt.request, state, tt.owner, tt.proposal, fixture.input.OperationID); !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	clients["owner"].routeManagement = false
	if _, err := lookup(request, state, "owner", fixture.input.ProposalID, fixture.input.OperationID); !errors.Is(err, errDelegationPeerUnavailable) {
		t.Fatalf("disabled outbound management accepted: %v", err)
	}
	clients["owner"].routeManagement = true
	r.managementSlots <- struct{}{}
	r.managementSlots <- struct{}{}
	if _, err := lookup(request, state, "owner", fixture.input.ProposalID, fixture.input.OperationID); !errors.Is(err, errDelegationBusy) {
		t.Fatalf("unbounded management requests accepted: %v", err)
	}
	<-r.managementSlots
	<-r.managementSlots
	r.state.Store(&reloaderState{})
	if _, err := lookup(request, state, "owner", fixture.input.ProposalID, fixture.input.OperationID); !errors.Is(err, errDelegationChanged) {
		t.Fatalf("stale receiving state accepted: %v", err)
	}
	if calls.Load() != before {
		t.Fatal("rejected requests reached the peer")
	}
	updated := *state
	updated.management = copyManagementConfig(state.management)
	updated.management.DelegationToken = strings.Repeat("n", 32)
	r.state.Store(&updated)
	if _, err := lookup(request, &updated, "owner", fixture.input.ProposalID, fixture.input.OperationID); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("revoked receiving credential accepted: %v", err)
	}
	rotated := request.Clone(request.Context())
	rotated.Header.Set("Authorization", "Bearer "+updated.management.DelegationToken)
	if _, err := lookup(rotated, &updated, "owner", fixture.input.ProposalID, fixture.input.OperationID); err != nil {
		t.Fatalf("rotated receiving credential rejected: %v", err)
	}
	server.Close()
	if _, err := lookup(rotated, &updated, "owner", fixture.input.ProposalID, fixture.input.OperationID); !errors.Is(err, errDelegationPeerUnavailable) {
		t.Fatalf("peer outage did not return unavailable: %v", err)
	}
}
