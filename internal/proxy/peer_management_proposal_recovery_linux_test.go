//go:build linux

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestOwnerDelegatedProposalInspectionAndExplicitCleanup(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.logger = discardLogger()
	f.r.managementSlots = make(chan struct{}, 1)
	f.state.management.PreviewToken = strings.Repeat("v", 32)
	active, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	current := active
	current.Proposal.ID = strings.Repeat("c", 32)
	current.Proposal.CreatedAt = time.Now().Add(-time.Hour)
	expired, err := modelconfig.NewDelegatedProposal(current.Owner, current.RequesterNode, current.BackendID, current.Proposal, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := modelconfig.SaveDelegatedProposal(f.proposals, expired); err != nil {
		t.Fatal(err)
	}
	// An operation-backed review remains ineligible, even after its proposal expires.
	if _, reserved, err := modelconfig.ReserveDelegatedOperation(f.operations, active, f.input.OperationID, active.Proposal.CreatedAt); err != nil || !reserved {
		t.Fatalf("reserve operation = %v, %v", reserved, err)
	}

	server := httptest.NewTLSServer(f.r.ManagementHandler())
	defer server.Close()
	token := f.state.management.BackupStore.Token
	call := func(method, credential, origin, body string) (int, []byte, http.Header) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+ownerProposalsPath, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+credential)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		response, err := server.Client().Do(req)
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

	// The expired eligible proposal is the only cleanup candidate; the response is cached safely and
	// leaks no credential, target path, or secret marker.
	assertCleanupCandidateInspection(t, call, expired, token, f.target)

	// Invalid credentials or origins are rejected; cleanup removes the expired proposal once, is
	// idempotent on repeat, and preserves the operation-backed proposal and its ledger.
	assertCleanupLifecycle(t, call, expired, token, f.target, f.proposals, f.operations, f.state.management.WriteToken, active)

	// An unregistered route must still enforce the backup credential.
	unregistered := httptest.NewRequest(http.MethodGet, "https://owner.example"+ownerProposalsPath, nil)
	unregistered.Header.Set("Authorization", "Bearer "+f.state.management.PreviewToken)
	recorder := httptest.NewRecorder()
	f.r.ManagementHandler().ServeHTTP(recorder, unregistered)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("owner proposal inspection did not enforce its backup credential: %d", recorder.Code)
	}
}

// assertCleanupCandidateInspection verifies the management listing exposes only the expired eligible
// proposal, sends no-store and nosniff headers, and leaks no credential, target path, or secret
// marker from the response body.
func assertCleanupCandidateInspection(t *testing.T, call func(method, credential, origin, body string) (int, []byte, http.Header), expired modelconfig.DelegatedProposal, token, target string) {
	t.Helper()
	status, body, headers := call(http.MethodGet, token, "", "")
	var response struct {
		Eligible []modelconfig.DelegatedProposalCleanupCandidate `json:"eligible"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response.Eligible) != 1 ||
		response.Eligible[0].ProposalID != expired.Proposal.ID {
		t.Fatalf("inspection status=%d response=%+v body=%q", status, response, body)
	}
	if headers.Get("Cache-Control") != "no-store" || headers.Get("X-Content-Type-Options") != "nosniff" ||
		bytes.Contains(body, []byte(token)) || bytes.Contains(body, []byte(target)) || bytes.Contains(body, []byte("confidential")) {
		t.Fatalf("unsafe cleanup inspection response: %s", body)
	}
}

// assertCleanupLifecycle verifies the four invalid credential/origin cases are rejected, that cleanup
// removes the expired proposal exactly once, is idempotent on a second call, and preserves the
// operation-backed proposal and its ledger.
func assertCleanupLifecycle(t *testing.T, call func(method, credential, origin, body string) (int, []byte, http.Header), expired modelconfig.DelegatedProposal, token, target, proposals, operations, writeToken string, active modelconfig.DelegatedProposal) {
	t.Helper()
	for _, tt := range []struct {
		name       string
		credential string
		origin     string
		want       int
	}{
		{"wrong credential", strings.Repeat("x", 32), "", http.StatusUnauthorized},
		{"peer credential", strings.Repeat("p", 32), "", http.StatusUnauthorized},
		{"write credential", writeToken, "", http.StatusUnauthorized},
		{"cross origin", token, "https://different.example", http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, _, _ := call(http.MethodGet, tt.credential, tt.origin, "")
			if status != tt.want {
				t.Fatalf("status=%d, want=%d", status, tt.want)
			}
		})
	}
	cleanupBody := `{"proposal_ids":["` + expired.Proposal.ID + `"]}`
	status, body, _ := call(http.MethodPost, token, "", cleanupBody)
	var cleanupResponse struct {
		Removed []string `json:"removed"`
	}
	if status != http.StatusOK || json.Unmarshal(body, &cleanupResponse) != nil || len(cleanupResponse.Removed) != 1 ||
		cleanupResponse.Removed[0] != expired.Proposal.ID {
		t.Fatalf("cleanup status=%d response=%+v body=%q", status, cleanupResponse, body)
	}
	status, body, _ = call(http.MethodPost, token, "", cleanupBody)
	if status != http.StatusOK || json.Unmarshal(body, &cleanupResponse) != nil || len(cleanupResponse.Removed) != 0 {
		t.Fatalf("repeated cleanup status=%d response=%+v body=%q", status, cleanupResponse, body)
	}
	if _, err := modelconfig.LoadDelegatedProposal(proposals, active.Proposal.ID); err != nil {
		t.Fatalf("operation-backed proposal was removed: %v", err)
	}
	if _, err := modelconfig.LoadDelegatedOperation(operations, active.Proposal.ID); err != nil {
		t.Fatalf("operation ledger was changed: %v", err)
	}
}

func TestOwnerDelegatedProposalCleanupRejectsPlaintextAndNonEligibleID(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.managementSlots = make(chan struct{}, 1)
	token := f.state.management.BackupStore.Token
	for _, tt := range []struct {
		name string
		url  string
		tls  bool
		body string
		want int
	}{
		{"plaintext", "https://owner.example" + ownerProposalsPath, false, `{"proposal_ids":["` + f.input.ProposalID + `"]}`, http.StatusForbidden},
		{"query", "https://owner.example" + ownerProposalsPath + "?x=1", true, `{"proposal_ids":["` + f.input.ProposalID + `"]}`, http.StatusNotFound},
		{"empty IDs", "https://owner.example" + ownerProposalsPath, true, `{"proposal_ids":[]}`, http.StatusBadRequest},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.url, strings.NewReader(tt.body))
			if !tt.tls {
				req.TLS = nil
			}
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			f.r.serveOwnerDelegatedProposals(recorder, req)
			if recorder.Code != tt.want {
				t.Fatalf("status=%d, want=%d, body=%s", recorder.Code, tt.want, recorder.Body.String())
			}
		})
	}
	// Existing proposal is not expired, so the owner cannot use cleanup to delete it.
	req := httptest.NewRequest(http.MethodPost, "https://owner.example"+ownerProposalsPath,
		strings.NewReader(`{"proposal_ids":["`+f.input.ProposalID+`"]}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	f.r.serveOwnerDelegatedProposals(recorder, req)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("non-expired proposal cleanup status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if _, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID); err != nil {
		t.Fatalf("non-eligible proposal was removed: %v", err)
	}
}

func TestRequesterScopedProposalRecoveryAndUnregisteredPeerHandler(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	f.r.peerSlots = make(chan struct{}, 1)
	proposal, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	input := delegatedSettingsRequest{
		Owner: proposal.Owner, Backend: proposal.BackendID, Model: proposal.Proposal.ModelID,
		Revision: proposal.Proposal.Revision, Settings: proposal.Proposal.Settings,
	}
	recovered, err := f.r.recoverDelegatedSettingsProposal(f.request, f.state, input)
	if err != nil || recovered.Proposal.ID != proposal.Proposal.ID || len(recovered.RecoveryAudit) != 1 {
		t.Fatalf("requester recovery = %+v, %v", recovered, err)
	}
	if event := recovered.RecoveryAudit[0]; event.RequesterNode != "requester" ||
		event.CredentialScope != modelconfig.DelegatedSettingsCredentialScope || event.OwnerNode != "owner" || event.Outcome != "recovered" {
		t.Fatalf("recovery audit does not identify authenticated scope: %+v", event)
	}
	f.state.peer.Peers["requester"] = config.PeerGrant{Token: strings.Repeat("p", 32), CatalogRead: true}
	if _, err := f.r.recoverDelegatedSettingsProposal(f.request, f.state, input); !errors.Is(err, errDelegationUnauthorized) {
		t.Fatalf("read-only peer recovered proposal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "https://peer.example"+peerSettingsProposalRecoveryPath, strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("p", 32))
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	f.r.PeerHandler().ServeHTTP(recorder, req)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("proposal recovery route did not enforce the read-only peer grant: %d", recorder.Code)
	}
}
