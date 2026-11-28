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

type delegatedOperationTransportHarness struct {
	t *testing.T
	fixture delegatedApplyFixture
	operation modelconfig.DelegatedOperation
	caller, outbound string
	server, redirect *httptest.Server
	r *Reloader
	state *reloaderState
	clients map[string]*peerClient
	request *http.Request
	mode, calls, redirects atomic.Int32
	entered, release, cancelEntered chan struct{}
}

func TestForwardDelegatedOperationUsesScopedPeerTransport(t *testing.T) {
	h := newDelegatedOperationTransportHarness(t)
	h.assertResponseValidation()
	h.assertLifecycleAndCancellation()
	h.assertPreflightAndCredentials()
}

func newDelegatedOperationTransportHarness(t *testing.T) *delegatedOperationTransportHarness {
	t.Helper()
	h := &delegatedOperationTransportHarness{
		t:t, fixture:newDelegatedApplyFixture(t), caller:strings.Repeat("c",32), outbound:strings.Repeat("t",32),
		entered:make(chan struct{}), release:make(chan struct{}), cancelEntered:make(chan struct{}),
	}
	reserveDelegatedFixture(t,h.fixture)
	var err error
	h.operation,err=modelconfig.LoadDelegatedOperation(h.fixture.operations,h.fixture.input.ProposalID)
	if err!=nil { t.Fatal(err) }
	h.redirect=httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter,*http.Request){h.redirects.Add(1)}))
	t.Cleanup(h.redirect.Close)
	h.server=httptest.NewTLSServer(http.HandlerFunc(h.serveOwnerRequest))
	t.Cleanup(h.server.Close)
	caPath:=filepath.Join(t.TempDir(),"peer-ca.pem")
	if err:=os.WriteFile(caPath,pem.EncodeToMemory(&pem.Block{Type:"CERTIFICATE",Bytes:h.server.Certificate().Raw}),0o600); err!=nil {t.Fatal(err)}
	peer:=&config.PeerConfig{NodeID:"requester",Peers:map[string]config.PeerGrant{
		"owner":{URL:h.server.URL,OutboundToken:h.outbound,CAFile:caPath,RouteManagement:true},
	}}
	h.clients,_,err=newPeerClients(peer); if err!=nil {t.Fatal(err)}
	t.Cleanup(func(){closePeerClients(h.clients)})
	h.state=&reloaderState{peer:peer,peerClients:h.clients,management:&config.ManagementConfig{DelegationToken:h.caller}}
	h.r=&Reloader{managementSlots:make(chan struct{},2)}; h.r.state.Store(h.state)
	h.request=httptest.NewRequest(http.MethodGet,"https://receiver.example/management/v1/operation",nil)
	h.request.Header.Set("Authorization","Bearer "+h.caller)
	h.request.Header.Set("X-Unsafe","do-not-forward")
	h.request.Header.Set("Cookie","do-not-forward")
	return h
}

func (h *delegatedOperationTransportHarness) lookup(req *http.Request,state *reloaderState,owner,proposal string)(modelconfig.DelegatedOperation,error){
	return h.r.forwardDelegatedOperation(req,state,owner,proposal,h.fixture.input.OperationID)
}

func (h *delegatedOperationTransportHarness) lookupOwner()(modelconfig.DelegatedOperation,error){
	return h.lookup(h.request,h.state,"owner",h.fixture.input.ProposalID)
}

func (h *delegatedOperationTransportHarness) serveOwnerRequest(w http.ResponseWriter,req *http.Request){
	h.calls.Add(1)
	if req.Method!=http.MethodGet || req.URL.Path!="/peer/v1/management/operations/"+h.fixture.input.ProposalID+"/"+h.fixture.input.OperationID ||
		req.URL.RawQuery!="" || req.Header.Get("Authorization")!="Bearer "+h.outbound || req.Header.Get("X-Unsafe")!="" || req.Header.Get("Cookie")!="" {
		h.t.Errorf("unexpected outbound request: method=%s path=%s",req.Method,req.URL.Path)
	}
	mode:=h.mode.Load()
	if h.waitForMode(w,req,mode) {return}
	writePeerOperationTestResponse(h.t,w,mode,h.operation,h.redirect.URL)
}

func (h *delegatedOperationTransportHarness) waitForMode(w http.ResponseWriter,req *http.Request,mode int32)bool{
	switch mode {
	case 7: http.Redirect(w,req,h.redirect.URL,http.StatusFound); return true
	case 8: http.Error(w,"secret remote error",http.StatusForbidden); return true
	case 11: close(h.entered); <-h.release
	case 13: close(h.cancelEntered); <-req.Context().Done(); return true
	}
	return false
}

func writePeerOperationTestResponse(t *testing.T,w http.ResponseWriter,mode int32,operation modelconfig.DelegatedOperation,redirectURL string){
	t.Helper()
	w.Header().Set("Content-Type","application/json")
	switch mode {
	case 1: operation.Owner.NodeID="different"
	case 2: operation.RequesterNode="other"
	case 3: operation.OperationID=strings.Repeat("f",32)
	case 4: _,_=io.WriteString(w,strings.Repeat("x",maxManagementRequest+1)); return
	case 5: w.Header().Set("Content-Type","text/plain")
	case 6: w.Header().Set("Content-Encoding","gzip")
	case 12: operation.CallerScope="individual"
	}
	data,err:=json.Marshal(operation); if err!=nil {t.Error(err);return}
	if mode==9 {data=append(bytes.TrimSuffix(data,[]byte("}")),[]byte(`,"outcome":"written"}`)...)}
	if mode==10 {data=append(bytes.TrimSuffix(data,[]byte("}")),[]byte(`,"unexpected":"secret"}`)...)}
	_,_=w.Write(data)
}

func (h *delegatedOperationTransportHarness) assertResponseValidation(){
	h.t.Helper()
	record,err:=h.lookupOwner()
	if err!=nil || record.Outcome!="unknown" || record.Owner.NodeID!="owner" {h.t.Fatalf("valid peer lookup = %+v, %v",record,err)}
	for _,tt:=range []struct{mode int32;want error}{
		{1,errDelegationPeerResponse},{2,errDelegationPeerResponse},{3,errDelegationPeerResponse},{4,errDelegationPeerResponse},
		{5,errDelegationPeerResponse},{6,errDelegationPeerResponse},{7,errDelegationPeerUnavailable},{8,errDelegationPeerUnavailable},
		{9,errDelegationPeerResponse},{10,errDelegationPeerResponse},{12,errDelegationPeerResponse},
	}{
		h.mode.Store(tt.mode); result,err:=h.lookupOwner()
		if !errors.Is(err,tt.want) || result.OperationID!="" || err!=nil && strings.Contains(err.Error(),"secret") {h.t.Fatalf("mode %d: %+v, %v",tt.mode,result,err)}
	}
	if h.redirects.Load()!=0 {h.t.Fatal("management request followed a redirect")}
}

func (h *delegatedOperationTransportHarness) assertLifecycleAndCancellation(){
	h.t.Helper()
	h.mode.Store(11); done:=make(chan error,1)
	go func(){_,err:=h.lookupOwner();done<-err}()
	<-h.entered
	if h.r.lifecycleMu.TryLock(){h.r.lifecycleMu.Unlock();close(h.release);<-done;h.t.Fatal("configuration reload could race an active forwarding operation")}
	close(h.release);if err:=<-done;err!=nil {h.t.Fatalf("in-flight request failed: %v",err)}
	h.mode.Store(13);ctx,cancel:=context.WithCancel(h.request.Context())
	go func(){_,err:=h.lookup(h.request.WithContext(ctx),h.state,"owner",h.fixture.input.ProposalID);done<-err}()
	<-h.cancelEntered;cancel()
	if err:=<-done;!errors.Is(err,errDelegationPeerUnavailable){h.t.Fatalf("in-flight cancellation returned %v",err)}
}

func (h *delegatedOperationTransportHarness) assertPreflightAndCredentials(){
	h.t.Helper()
	h.mode.Store(0);before:=h.calls.Load()
	wrong:=h.request.Clone(h.request.Context());wrong.Header.Set("Authorization","Bearer "+h.outbound)
	duplicate:=h.request.Clone(h.request.Context());duplicate.Header.Add("Authorization","Bearer "+h.caller)
	plaintext:=h.request.Clone(h.request.Context());plaintext.TLS=nil
	canceled,cancel:=context.WithCancel(h.request.Context());cancel()
	for _,tt:=range []struct{name string;request *http.Request;owner,proposal string;want error}{
		{"wrong caller",wrong,"owner",h.fixture.input.ProposalID,errDelegationUnauthorized},
		{"duplicate caller",duplicate,"owner",h.fixture.input.ProposalID,errDelegationUnauthorized},
		{"plaintext",plaintext,"owner",h.fixture.input.ProposalID,errDelegationUnauthorized},
		{"canceled",h.request.WithContext(canceled),"owner",h.fixture.input.ProposalID,context.Canceled},
		{"unconfigured owner",h.request,"other",h.fixture.input.ProposalID,errDelegationPeerUnavailable},
		{"invalid ID",h.request,"owner","../bad",modelconfig.ErrProposalInvalid},
	}{h.t.Run(tt.name,func(t *testing.T){if _,err:=h.lookup(tt.request,h.state,tt.owner,tt.proposal);!errors.Is(err,tt.want){t.Fatalf("got %v, want %v",err,tt.want)}})}
	h.clients["owner"].routeManagement=false
	if _,err:=h.lookupOwner();!errors.Is(err,errDelegationPeerUnavailable){h.t.Fatalf("disabled outbound management accepted: %v",err)}
	h.clients["owner"].routeManagement=true
	h.r.managementSlots<-struct{}{};h.r.managementSlots<-struct{}{}
	if _,err:=h.lookupOwner();!errors.Is(err,errDelegationBusy){h.t.Fatalf("unbounded management requests accepted: %v",err)}
	<-h.r.managementSlots; <-h.r.managementSlots
	h.r.state.Store(&reloaderState{})
	if _,err:=h.lookupOwner();!errors.Is(err,errDelegationChanged){h.t.Fatalf("stale receiving state accepted: %v",err)}
	if h.calls.Load()!=before {h.t.Fatal("rejected requests reached the peer")}
	updated:=*h.state;updated.management=copyManagementConfig(h.state.management);updated.management.DelegationToken=strings.Repeat("n",32);h.r.state.Store(&updated)
	if _,err:=h.lookup(h.request,&updated,"owner",h.fixture.input.ProposalID);!errors.Is(err,errDelegationUnauthorized){h.t.Fatalf("revoked receiving credential accepted: %v",err)}
	rotated:=h.request.Clone(h.request.Context());rotated.Header.Set("Authorization","Bearer "+updated.management.DelegationToken)
	if _,err:=h.lookup(rotated,&updated,"owner",h.fixture.input.ProposalID);err!=nil {h.t.Fatalf("rotated receiving credential rejected: %v",err)}
	h.server.Close()
	if _,err:=h.lookup(rotated,&updated,"owner",h.fixture.input.ProposalID);!errors.Is(err,errDelegationPeerUnavailable){h.t.Fatalf("peer outage did not return unavailable: %v",err)}
}
