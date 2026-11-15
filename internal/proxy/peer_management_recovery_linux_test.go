//go:build linux

package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/modelconfig"
)

func TestOwnerDelegatedRecoveryHandler(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	f.r.logger = discardLogger()
	f.r.managementSlots = make(chan struct{}, 2)
	f.state.management.PreviewToken = strings.Repeat("v", 32)
	server := httptest.NewTLSServer(http.HandlerFunc(f.r.serveOwnerDelegatedOperation))
	defer server.Close()
	path := ownerOperationPath + f.input.ProposalID + "/" + f.input.OperationID
	token := f.state.management.BackupStore.Token
	call := func(method, suffix, credential, contentType, origin, body string) (int, []byte, http.Header) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path+suffix, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+credential)
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, data, resp.Header
	}
	status, body, headers := call(http.MethodGet, "", token, "", "", "")
	var record modelconfig.DelegatedOperation
	if status != http.StatusOK || json.Unmarshal(body, &record) != nil || record.Outcome != "unknown" || record.ReviewedAt != nil {
		t.Fatalf("inspection status=%d record=%+v body=%q", status, record, body)
	}
	if headers.Get("Cache-Control") != "no-store" || headers.Get("X-Content-Type-Options") != "nosniff" ||
		bytes.Contains(body, []byte(token)) || bytes.Contains(body, []byte(f.target)) || bytes.Contains(body, []byte("confidential")) {
		t.Fatalf("unsafe inspection response: %s", body)
	}
	valid := `{"acknowledge_review":true,"acknowledge_no_retry":true}`
	for _, tt := range []struct {
		name        string
		method      string
		suffix      string
		credential  string
		contentType string
		origin      string
		body        string
		want        int
	}{
		{"peer token", http.MethodPost, "/review", strings.Repeat("p", 32), "application/json", "", valid, http.StatusUnauthorized},
		{"preview token", http.MethodPost, "/review", f.state.management.PreviewToken, "application/json", "", valid, http.StatusUnauthorized},
		{"cross origin", http.MethodPost, "/review", token, "application/json", "https://different.example", valid, http.StatusForbidden},
		{"missing review", http.MethodPost, "/review", token, "application/json", "", `{"acknowledge_no_retry":true}`, http.StatusBadRequest},
		{"missing no retry", http.MethodPost, "/review", token, "application/json", "", `{"acknowledge_review":true}`, http.StatusBadRequest},
		{"duplicate fields", http.MethodPost, "/review", token, "application/json", "", `{"acknowledge_review":true,"ACKNOWLEDGE_REVIEW":true,"acknowledge_no_retry":true}`, http.StatusBadRequest},
		{"unknown fields", http.MethodPost, "/review", token, "application/json", "", `{"acknowledge_review":true,"acknowledge_no_retry":true,"extra":true}`, http.StatusBadRequest},
		{"non JSON", http.MethodPost, "/review", token, "text/plain", "", valid, http.StatusBadRequest},
		{"oversize", http.MethodPost, "/review", token, "application/json", "", strings.Repeat(" ", 8193), http.StatusBadRequest},
		{"wrong method", http.MethodGet, "/review", token, "", "", "", http.StatusMethodNotAllowed},
		{"body on inspect", http.MethodGet, "", token, "", "", "x", http.StatusBadRequest},
		{"query", http.MethodGet, "?x=1", token, "", "", "", http.StatusNotFound},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, _, _ := call(tt.method, tt.suffix, tt.credential, tt.contentType, tt.origin, tt.body)
			if status != tt.want {
				t.Fatalf("status=%d, want=%d", status, tt.want)
			}
		})
	}
	dir, err := os.Open(f.operations)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if status, _, _ := call(http.MethodPost, "/review", token, "application/json", "", valid); status != http.StatusServiceUnavailable {
		t.Fatalf("live operation reviewed: %d", status)
	}
	if err := syscall.Flock(int(dir.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	// Review does not require the write capability or an enabled peer scope.
	f.r.lifecycleMu.Lock()
	disabled := *f.state
	disabled.management = copyManagementConfig(f.state.management)
	disabled.management.WriteToken = ""
	disabled.peer = nil
	f.r.state.Store(&disabled)
	f.r.lifecycleMu.Unlock()
	status, body, _ = call(http.MethodPost, "/review", token, "application/json", "", valid)
	if status != http.StatusOK || json.Unmarshal(body, &record) != nil || record.Outcome != "unknown" || record.ReviewedAt == nil {
		t.Fatalf("review status=%d record=%+v body=%q", status, record, body)
	}
	reviewedAt := *record.ReviewedAt
	status, body, _ = call(http.MethodPost, "/review", token, "application/json", "", valid)
	if status != http.StatusOK || json.Unmarshal(body, &record) != nil || record.ReviewedAt == nil || !record.ReviewedAt.Equal(reviewedAt) {
		t.Fatalf("repeated review status=%d record=%+v", status, record)
	}
	proposal, err := modelconfig.LoadDelegatedProposal(f.proposals, f.input.ProposalID)
	if err != nil {
		t.Fatal(err)
	}
	if _, reserved, err := modelconfig.ReserveDelegatedOperation(f.operations, proposal, f.input.OperationID, reviewedAt); err != nil || reserved {
		t.Fatalf("review authorized retry: %v, %v", reserved, err)
	}
	current, err := os.ReadFile(filepath.Join(f.target, "config.yaml"))
	if err != nil || !bytes.Equal(current, f.source) {
		t.Fatalf("recovery changed target: %v", err)
	}
	unregistered := httptest.NewRequest(http.MethodGet, "https://owner.example"+path, nil)
	unregistered.Header.Set("Authorization", "Bearer "+f.state.management.PreviewToken)
	recorder := httptest.NewRecorder()
	f.r.ManagementHandler().ServeHTTP(recorder, unregistered)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("owner recovery route did not enforce backup credential scope: %d", recorder.Code)
	}
}

type delayedReviewBody struct {
	reader  *strings.Reader
	started chan struct{}
	release chan struct{}
}

func (body *delayedReviewBody) Read(p []byte) (int, error) {
	select {
	case <-body.started:
	default:
		close(body.started)
		<-body.release
	}
	return body.reader.Read(p)
}

func (*delayedReviewBody) Close() error { return nil }

func TestOwnerRecoveryRechecksAuthorizationAfterReadingBody(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	f.r.logger = discardLogger()
	f.r.managementSlots = make(chan struct{}, 1)
	body := &delayedReviewBody{
		reader:  strings.NewReader(`{"acknowledge_review":true,"acknowledge_no_retry":true}`),
		started: make(chan struct{}), release: make(chan struct{}),
	}

	path := ownerOperationPath + f.input.ProposalID + "/" + f.input.OperationID + "/review"
	req := httptest.NewRequest(http.MethodPost, "https://owner.example"+path, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.state.management.BackupStore.Token)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		f.r.serveOwnerDelegatedOperation(recorder, req)
		close(done)
	}()
	<-body.started
	updated := *f.state
	updated.management = copyManagementConfig(f.state.management)
	updated.management.BackupStore.Token = strings.Repeat("n", 32)
	f.r.lifecycleMu.Lock()
	f.r.state.Store(&updated)
	f.r.lifecycleMu.Unlock()
	close(body.release)
	<-done
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("stale authorization status=%d", recorder.Code)
	}
	record, err := modelconfig.LoadDelegatedOperation(f.operations, f.input.ProposalID)
	if err != nil || record.ReviewedAt != nil {
		t.Fatalf("revoked credential reviewed operation: %+v, %v", record, err)
	}
}

func TestOwnerRecoveryRejectsKnownAndWrongOperation(t *testing.T) {
	f := newDelegatedApplyFixture(t)
	reserveDelegatedFixture(t, f)
	f.r.logger = discardLogger()
	f.r.managementSlots = make(chan struct{}, 1)
	token := f.state.management.BackupStore.Token
	ack := `{"acknowledge_review":true,"acknowledge_no_retry":true}`
	path := ownerOperationPath + f.input.ProposalID + "/"
	send := func(operationID string, change func(*http.Request)) int {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "https://owner.example"+path+operationID+"/review", strings.NewReader(ack))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		if change != nil {
			change(req)
		}
		recorder := httptest.NewRecorder()
		f.r.serveOwnerDelegatedOperation(recorder, req)
		return recorder.Code
	}
	if status := send(strings.Repeat("f", 32), nil); status != http.StatusNotFound {
		t.Fatalf("wrong operation status=%d", status)
	}
	if status := send(f.input.OperationID, func(req *http.Request) {
		req.Header.Add("Authorization", "Bearer "+token)
	}); status != http.StatusUnauthorized {
		t.Fatalf("duplicate credential status=%d", status)
	}
	if status := send(f.input.OperationID, func(req *http.Request) { req.TLS = nil }); status != http.StatusForbidden {
		t.Fatalf("plaintext review status=%d", status)
	}
	if status := send(f.input.OperationID, func(req *http.Request) {
		req.URL.RawPath = strings.Replace(req.URL.Path, "/review", "/%72eview", 1)
	}); status != http.StatusNotFound {
		t.Fatalf("noncanonical path status=%d", status)
	}
	if status := send(f.input.OperationID, func(req *http.Request) {
		req.Header.Set("Content-Encoding", "gzip")
	}); status != http.StatusBadRequest {
		t.Fatalf("encoded body status=%d", status)
	}
	foreign := *f.state
	peer := *f.state.peer
	peer.NodeID = "another-owner"
	foreign.peer = &peer
	originalOwner := f.r.peerNodeID
	f.r.lifecycleMu.Lock()
	f.r.peerNodeID = peer.NodeID
	f.r.state.Store(&foreign)
	f.r.lifecycleMu.Unlock()
	if status := send(f.input.OperationID, nil); status != http.StatusNotFound {
		t.Fatalf("foreign owner status=%d", status)
	}
	f.r.lifecycleMu.Lock()
	f.r.peerNodeID = originalOwner
	f.r.state.Store(f.state)
	f.r.lifecycleMu.Unlock()
	f.r.managementSlots <- struct{}{}
	if status := send(f.input.OperationID, nil); status != http.StatusServiceUnavailable {
		t.Fatalf("unbounded review accepted: %d", status)
	}
	<-f.r.managementSlots
	if _, err := modelconfig.FinishDelegatedOperation(f.operations, f.input.ProposalID, f.input.OperationID,
		f.r.instanceID, modelconfig.MutationResult{}, modelconfig.ErrTargetWrite, time.Now()); err != nil {
		t.Fatal(err)
	}
	if status := send(f.input.OperationID, nil); status != http.StatusConflict {
		t.Fatalf("known operation reviewed: %d", status)
	}
	record, err := modelconfig.LoadDelegatedOperation(f.operations, f.input.ProposalID)
	if err != nil || record.Outcome != "not_written" || record.ReviewedAt != nil {
		t.Fatalf("known outcome changed: %+v, %v", record, err)
	}
}
