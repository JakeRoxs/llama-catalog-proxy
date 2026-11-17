package federation

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func pairingStoreFixture(t *testing.T) (string, *PairingStore) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := OpenPairingStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return directory, store
}

func TestPairingInvitationPersistsMembershipButNotInvitationCode(t *testing.T) {
	directory, store := pairingStoreFixture(t)
	now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	invite, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(directory, storeFilename))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), invite.Code) {
		t.Fatal("raw invitation code was persisted")
	}
	info, err := os.Stat(filepath.Join(directory, storeFilename))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("pairing state mode = %v", info.Mode().Perm())
	}
	fingerprint := strings.Repeat("a", 64)
	member, err := store.ConsumeInvitation(invite.Code, "node-b", fingerprint, now)
	if err != nil || member.NodeID != "node-b" {
		t.Fatalf("consume invitation = %+v, %v", member, err)
	}
	members, err := store.ListMembers()
	if err != nil || !store.IsMember("node-b") || len(members) != 1 {
		t.Fatal("paired membership was not recorded")
	}
	if _, err := store.ConsumeInvitation(invite.Code, "node-c", fingerprint, now); err == nil {
		t.Fatal("invitation replay was accepted for a different identity")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restored, err := OpenPairingStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	if !restored.IsMember("node-b") {
		t.Fatal("membership did not persist across restart")
	}
	if replay, err := restored.ConsumeInvitation(invite.Code, "node-b", fingerprint, now.Add(time.Minute)); err != nil || replay.NodeID != member.NodeID {
		t.Fatalf("same-node retry after lost response = %+v, %v", replay, err)
	}
}

func TestPairingInvitationConcurrentConsumeIsSingleMembership(t *testing.T) {
	directory, store := pairingStoreFixture(t)
	secondStore, err := OpenPairingStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	now := time.Now().UTC()
	invite, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := strings.Repeat("b", 64)
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for index := range cap(errs) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			current := store
			if index%2 != 0 {
				current = secondStore
			}
			member, err := current.ConsumeInvitation(invite.Code, "node-b", fingerprint, now.Add(time.Second))
			if err == nil && member.NodeID != "node-b" {
				err = ErrInvitation
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent consume failed: %v", err)
		}
	}
	members, err := store.ListMembers()
	if err != nil || len(members) != 1 {
		t.Fatalf("member count = %d, err = %v", len(members), err)
	}
}

func TestPairingStoreRejectsExpiredInvitesDuplicateIdentitiesAndUnsafeDirectories(t *testing.T) {
	directory, store := pairingStoreFixture(t)
	now := time.Now().UTC()
	expired, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumeInvitation(expired.Code, "node-b", strings.Repeat("c", 64), now.Add(invitationLifetime)); err == nil {
		t.Fatal("expired invitation was accepted")
	}
	active, err := store.CreateInvitation("node-b", now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumeInvitation(active.Code, "node-b", strings.Repeat("d", 64), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AddMember("node-c", strings.Repeat("d", 64), now); err == nil {
		t.Fatal("duplicate certificate fingerprint was accepted")
	}
	if err := store.RemoveMember("node-b", now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if revoked, err := store.IsCertificateRevoked("node-b", strings.Repeat("d", 64)); err != nil || !revoked {
		t.Fatalf("revoked certificate lookup = %v, %v", revoked, err)
	}
	renewedInvite, err := store.CreateInvitation("node-b", now.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ConsumeInvitation(renewedInvite.Code, "node-b", strings.Repeat("d", 64), now.Add(3*time.Hour)); err == nil {
		t.Fatal("revoked certificate was accepted by a new invitation")
	}
	if _, err := store.ConsumeInvitation(renewedInvite.Code, "node-b", strings.Repeat("e", 64), now.Add(3*time.Hour)); err != nil {
		t.Fatalf("newly issued certificate could not be paired: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPairingStore(directory, "other.example", "node-a"); err == nil {
		t.Fatal("store was accepted for a different federation")
	}
	insecure := t.TempDir()
	if err := os.Chmod(insecure, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenPairingStore(insecure, "lab.example", "node-a"); err == nil {
		t.Fatal("group/world accessible pairing directory was accepted")
	}
}

func TestPairingStoreSynchronizesCLIAndServerProcesses(t *testing.T) {
	directory, store := pairingStoreFixture(t)
	cliStore, err := OpenPairingStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatalf("second local store could not open: %v", err)
	}
	defer cliStore.Close()
	now := time.Now().UTC()
	invite, err := store.CreateInvitation("node-b", now)
	if err != nil {
		t.Fatal(err)
	}
	invitations, err := cliStore.ListInvitations(now)
	if err != nil || len(invitations) != 1 || invitations[0].NodeID != "node-b" {
		t.Fatalf("CLI did not observe server invitation: %+v %v", invitations, err)
	}
	if _, err := cliStore.ConsumeInvitation(invite.Code, "node-b", strings.Repeat("f", 64), now); err != nil {
		t.Fatal(err)
	}
	if !store.IsMember("node-b") {
		t.Fatal("server process did not observe CLI membership update")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLostMembershipStateFailsClosedAsEmpty(t *testing.T) {
	directory, store := pairingStoreFixture(t)
	if _, err := store.AddMember("node-b", strings.Repeat("9", 64), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(directory, storeFilename)); err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenPairingStore(directory, "lab.example", "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	members, err := recovered.ListMembers()
	if err != nil || len(members) != 0 || recovered.IsMember("node-b") {
		t.Fatalf("lost state restored trust unexpectedly: %+v, %v", members, err)
	}
}
