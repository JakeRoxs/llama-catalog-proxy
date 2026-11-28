package modelconfig

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func delegatedFixture(t *testing.T) (DelegatedOwner, DelegatedProposal) {
	t.Helper()
	size := int32(4096)
	settings := CommandSettings{ContextSize: &size}
	preview, err := PreviewDocument("target", "model", []byte("models: {model: {cmd: 'llama-server -c 1024'}}"), settings)
	if err != nil {
		t.Fatal(err)
	}
	local, err := NewProposal("directory", "config.yaml", preview, settings)
	if err != nil {
		t.Fatal(err)
	}
	owner := DelegatedOwner{NodeID: "owner", ProxyInstanceID: strings.Repeat("a", 32), ConfigGeneration: 1}
	proposal, err := NewDelegatedProposal(owner, "requester", "backend", local, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return owner, proposal
}

func TestDelegatedProposalBindings(t *testing.T) {
	owner, proposal := delegatedFixture(t)
	data, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	var restored DelegatedProposal
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	now := proposal.Proposal.CreatedAt.Add(time.Minute)
	if err := ValidateDelegatedProposal(restored, owner, "requester", "backend", "target", now); err != nil {
		t.Fatalf("round-tripped proposal rejected: %v", err)
	}

	tests := []struct {
		name      string
		owner     DelegatedOwner
		requester string
		backend   string
		target    string
		now       time.Time
		want      error
	}{
		{"owner changed", DelegatedOwner{NodeID: "other", ProxyInstanceID: owner.ProxyInstanceID, ConfigGeneration: 1}, "requester", "backend", "target", now, ErrDelegatedProposalBinding},
		{"owner restarted", DelegatedOwner{NodeID: owner.NodeID, ProxyInstanceID: strings.Repeat("b", 32), ConfigGeneration: 1}, "requester", "backend", "target", now, ErrDelegatedProposalBinding},
		{"config reloaded", DelegatedOwner{NodeID: owner.NodeID, ProxyInstanceID: owner.ProxyInstanceID, ConfigGeneration: 2}, "requester", "backend", "target", now, ErrDelegatedProposalBinding},
		{"other requester", owner, "other", "backend", "target", now, ErrDelegatedProposalBinding},
		{"other backend", owner, "requester", "other", "target", now, ErrDelegatedProposalBinding},
		{"remapped target", owner, "requester", "backend", "other", now, ErrDelegatedProposalBinding},
		{"missing clock", owner, "requester", "backend", "target", time.Time{}, ErrDelegatedProposalExpired},
		{"before creation", owner, "requester", "backend", "target", proposal.Proposal.CreatedAt.Add(-time.Nanosecond), ErrDelegatedProposalExpired},
		{"at expiration", owner, "requester", "backend", "target", proposal.ExpiresAt, ErrDelegatedProposalExpired},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDelegatedProposal(restored, tt.owner, tt.requester, tt.backend, tt.target, tt.now)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	if err := ValidateDelegatedProposal(restored, owner, "requester", "backend", "target", proposal.ExpiresAt.Add(-time.Nanosecond)); err != nil {
		t.Fatalf("proposal expired prematurely: %v", err)
	}
}

func TestDelegatedProposalRejectsModifiedReview(t *testing.T) {
	owner, original := delegatedFixture(t)
	tests := []struct {
		name   string
		mutate func(*DelegatedProposal)
	}{
		{"owner", func(p *DelegatedProposal) { p.Owner.NodeID = "other" }},
		{"requester", func(p *DelegatedProposal) { p.RequesterNode = "other" }},
		{"backend", func(p *DelegatedProposal) { p.BackendID = "other" }},
		{"model", func(p *DelegatedProposal) { p.Proposal.ModelID = "other" }},
		{"target", func(p *DelegatedProposal) { p.Proposal.TargetID = "other" }},
		{"target fingerprint", func(p *DelegatedProposal) { p.Proposal.TargetFingerprint = strings.Repeat("b", 64) }},
		{"source revision", func(p *DelegatedProposal) { p.Proposal.Revision = strings.Repeat("b", 64) }},
		{"settings and changes", func(p *DelegatedProposal) {
			size := int32(8192)
			p.Proposal.Settings.ContextSize = &size
			p.Proposal.Changes = append([]SettingChange(nil), p.Proposal.Changes...)
			p.Proposal.Changes[0].After = "8192"
		}},
		{"reviewed change", func(p *DelegatedProposal) {
			p.Proposal.Changes = append([]SettingChange(nil), p.Proposal.Changes...)
			p.Proposal.Changes[0].Before = "512"
		}},
		{"expiry", func(p *DelegatedProposal) { p.ExpiresAt = p.ExpiresAt.Add(time.Minute) }},
		{"review revision", func(p *DelegatedProposal) { p.ReviewRevision = strings.Repeat("b", 64) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proposal := original
			tt.mutate(&proposal)
			err := ValidateDelegatedProposal(proposal, owner, "requester", "backend", "target", original.Proposal.CreatedAt)
			if !errors.Is(err, ErrDelegatedProposalInvalid) {
				t.Fatalf("modified review accepted: %v", err)
			}
		})
	}
}

func TestNewDelegatedProposalRejectsInvalidBindings(t *testing.T) {
	owner, existing := delegatedFixture(t)
	for _, tt := range []struct {
		name      string
		owner     DelegatedOwner
		requester string
		backend   string
		lifetime  time.Duration
	}{
		{"no owner", DelegatedOwner{}, "requester", "backend", time.Minute},
		{"invalid incarnation", DelegatedOwner{NodeID: "owner", ProxyInstanceID: strings.Repeat("A", 32), ConfigGeneration: 1}, "requester", "backend", time.Minute},
		{"self requester", owner, "owner", "backend", time.Minute},
		{"no requester", owner, "", "backend", time.Minute},
		{"no backend", owner, "requester", "", time.Minute},
		{"zero lifetime", owner, "requester", "backend", 0},
		{"excessive lifetime", owner, "requester", "backend", MaxDelegatedProposalLifetime + time.Nanosecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDelegatedProposal(tt.owner, tt.requester, tt.backend, existing.Proposal, tt.lifetime)
			if !errors.Is(err, ErrDelegatedProposalInvalid) {
				t.Fatalf("invalid binding accepted: %v", err)
			}
		})
	}
	invalid := existing.Proposal
	invalid.Revision = "invalid"
	if _, err := NewDelegatedProposal(owner, "requester", "backend", invalid, time.Minute); !errors.Is(err, ErrDelegatedProposalInvalid) {
		t.Fatalf("invalid local proposal accepted: %v", err)
	}
}
