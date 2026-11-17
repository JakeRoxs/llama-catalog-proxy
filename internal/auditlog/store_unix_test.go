//go:build linux || darwin || freebsd || openbsd || netbsd

package auditlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newAuditStoreFixture(t *testing.T) (string, *Store) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := Open(directory, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return directory, store
}

func oidcAuditEvent() Event {
	return Event{
		LocalNodeID: "node-a", PrincipalType: "oidc", UserIdentityState: "verified_local",
		IdentityIssuer: "https://identity.example", VerifiedUserSubject: "sub-operator", Role: "administer_federation",
		Action: "federation.invitation.create", TargetID: "node-b", AssertedUserSubject: "",
	}
}

func TestAuditStorePersistsVerifiedLocalAndUnknownRemoteIdentity(t *testing.T) {
	directory, store := newAuditStoreFixture(t)
	tx, err := store.Begin(oidcAuditEvent())
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Finish("succeeded"); err != nil {
		t.Fatal(err)
	}
	if err := store.Record(Event{
		LocalNodeID: "node-a", RequesterNodeID: "node-b", PrincipalType: "peer_node", UserIdentityState: "unknown",
		Action: "settings.proposal.create", TargetID: "target-a", ModelID: "repo/model-a", Phase: "event", Outcome: "succeeded",
	}); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListRecent(10)
	if err != nil || len(events) != 2 {
		t.Fatalf("audit events = %d, %v", len(events), err)
	}
	if events[0].VerifiedUserSubject != "sub-operator" || events[0].UserIdentityState != "verified_local" || events[0].Outcome != "succeeded" ||
		events[1].RequesterNodeID != "node-b" || events[1].UserIdentityState != "unknown" || events[1].VerifiedUserSubject != "" || events[1].AssertedUserSubject != "" {
		t.Fatalf("audit identity distinctions were lost: %+v", events)
	}
	data, err := os.ReadFile(filepath.Join(directory, auditFilename(time.Now())))
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"Bearer ", "invitation-code", "secret-yaml", "prompt"} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("audit log contains forbidden material %q", forbidden)
		}
	}
	info, err := os.Stat(filepath.Join(directory, auditFilename(time.Now())))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("audit event file mode = %v, err=%v", info.Mode().Perm(), err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(directory, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if persisted, err := reopened.ListRecent(10); err != nil || len(persisted) != 2 {
		t.Fatalf("audit events after restart = %d, %v", len(persisted), err)
	}
}

func TestAuditStoreRequiresStartBeforeCompletionAndBoundsRetention(t *testing.T) {
	_, store := newAuditStoreFixture(t)
	event := oidcAuditEvent()
	event.ID = strings.Repeat("a", 32)
	event.Phase = "completed"
	event.Outcome = "succeeded"
	if err := store.finish(event); err == nil {
		t.Fatal("completion without a durable start record accepted")
	}
	old := oidcAuditEvent()
	old.At = time.Now().Add(-Retention - time.Hour)
	old.Outcome = "succeeded"
	if err := store.Record(old); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListRecent(10)
	if err != nil || len(events) != 0 {
		t.Fatalf("expired audit event retained: %d, %v", len(events), err)
	}
}

func TestAuditStoreSerializesMultipleLocalProcesses(t *testing.T) {
	directory, first := newAuditStoreFixture(t)
	second, err := Open(directory, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for index := range cap(errs) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			store := first
			if index%2 != 0 {
				store = second
			}
			tx, err := store.Begin(oidcAuditEvent())
			if err == nil {
				err = tx.Finish("succeeded")
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent audit append failed: %v", err)
		}
	}
	events, err := first.ListRecent(200)
	if err != nil || len(events) != 20 {
		t.Fatalf("cross-process audit event count = %d, %v", len(events), err)
	}
}

func TestAuditStoreRejectsUnsafeDirectoryAndAssertedRemoteUsers(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(directory, "node-a"); err == nil {
		t.Fatal("group/world accessible audit directory accepted")
	}
	event := Event{
		ID: strings.Repeat("a", 32), At: time.Now(), LocalNodeID: "node-a", RequesterNodeID: "node-b",
		PrincipalType: "peer_node", UserIdentityState: "unknown", AssertedUserSubject: "unverified-user",
		Action: "settings.apply", Phase: "event", Outcome: "succeeded",
	}
	if err := ValidateEvent(event); err == nil {
		t.Fatal("asserted remote human identity accepted as verified audit evidence")
	}
}

func TestAuditStoreFailsClosedAtDailyCapacity(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	var data bytes.Buffer
	for index := 1; index < MaxEvents; index++ {
		event := Event{
			ID: fmt.Sprintf("%032x", index), At: now, LocalNodeID: "node-a", PrincipalType: "local_cli",
			UserIdentityState: "local_process", LocalAccount: "uid:1", Action: "settings.audit",
			Phase: "event", Outcome: "succeeded",
		}
		line, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if data.Len()+len(line)+1 > MaxDayFileBytes-100 {
			break
		}
		data.Write(line)
		data.WriteByte('\n')
	}
	if err := os.WriteFile(filepath.Join(directory, auditFilename(now)), data.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := Open(directory, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	err = store.Record(Event{
		LocalNodeID: "node-a", PrincipalType: "local_cli", UserIdentityState: "local_process", LocalAccount: "uid:1",
		Action: "settings.audit", Outcome: "succeeded", Phase: "event",
	})
	if err == nil || !errors.Is(err, ErrCapacity) {
		t.Fatalf("audit append at daily capacity error = %v", err)
	}
}
