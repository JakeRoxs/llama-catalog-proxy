package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JakeRoxs/llama-catalog-proxy/internal/config"
)

func confirmedCoordinatorEvidence() ComfyUIReleaseResult {
	return ComfyUIReleaseResult{
		State: "confirmed", FreeRequested: true, FreeHTTPStatus: 200,
		QueueIdleSamples: 2, MinimumQueueIdleSamples: 2,
		Devices: []ComfyUIDeviceReleaseEvidence{{DeviceKey: "cuda:0", FreeVRAMBytes: 900, MinimumFreeVRAMBytes: 800}},
	}
}

func waitCoordinatorState(t *testing.T, c *comfyUICoordinator, state string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		c.mu.Lock()
		actual, changed := c.status.State, c.changed
		c.mu.Unlock()
		if actual == state {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("coordinator state=%s, wanted %s", actual, state)
		}
	}
}

func TestComfyUICoordinatorDrainsAndCachesGenerationBoundRelease(t *testing.T) {
	var calls atomic.Int32
	var c *comfyUICoordinator
	c = newComfyUICoordinator(countingVerifier(t, &c, &calls))
	assertUnknownConstruction(t, c, &calls)
	reconcileOnce(t, c)
	leaseID, finishSubmission := acquireAndActivate(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	drainAndRelease(t, c, ctx, &calls, leaseID, finishSubmission)
	finishCompeting := reserveCompeting(t, c)
	assertCompetingBlocksNewLease(t, c)
	receipt := assertDuplicateReleaseCached(t, c, ctx, &calls, leaseID)
	assertReceiptDefensiveCopy(t, c, ctx, leaseID, receipt)
	finishCompeting()
	finishCompeting()
	nextID := assertNewLeaseAfterCompeting(t, c, leaseID)
	assertStaleLeaseRejected(t, c, ctx, &calls, leaseID, nextID)
}

func TestComfyUICoordinatorRetriesInitialCheckOnlyBeforeFree_PreFreeFailureCanRetry(t *testing.T) {
	var calls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		if calls.Add(1) == 1 {
			return ComfyUIReleaseResult{State: "unknown"}, errors.New("temporary queue probe failure")
		}
		return confirmedCoordinatorEvidence(), nil
	})
	if _, err := c.reconcileInitial(context.Background(), false); !errors.Is(err, ErrComfyUIReleaseUnknown) {
		t.Fatalf("first check error=%v, want unknown release", err)
	}
	if state := c.snapshot(); state.State != "unknown" {
		t.Fatalf("failed pre-free check changed admission state: %+v", state)
	}
	if _, err := c.reconcileInitial(context.Background(), false); err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if state := c.snapshot(); state.State != "released" || calls.Load() != 2 {
		t.Fatalf("retry state=%+v verifier calls=%d", state, calls.Load())
	}
}

func TestComfyUICoordinatorRetriesInitialCheckOnlyBeforeFree_PostFreeFailureRemainsBlocked(t *testing.T) {
	var calls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		calls.Add(1)
		return ComfyUIReleaseResult{State: "unknown", FreeRequested: true}, ErrComfyUIReleaseUnknown
	})
	if _, err := c.reconcileInitial(context.Background(), false); !errors.Is(err, ErrComfyUIReleaseUnknown) {
		t.Fatalf("first check error=%v, want unknown release", err)
	}
	if _, err := c.reconcileInitial(context.Background(), false); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatalf("post-free retry error=%v, want admission blocked", err)
	}
	if calls.Load() != 1 || c.snapshot().State != "unknown" {
		t.Fatalf("post-free verification retried: calls=%d state=%+v", calls.Load(), c.snapshot())
	}
}

func countingVerifier(t *testing.T, cPtr **comfyUICoordinator, calls *atomic.Int32) func(context.Context) (ComfyUIReleaseResult, error) {
	t.Helper()
	return func(context.Context) (ComfyUIReleaseResult, error) {
		calls.Add(1)
		if (*cPtr).snapshot().Submissions != 0 {
			t.Error("verification ran before prompt HTTP submissions drained")
		}
		return confirmedCoordinatorEvidence(), nil
	}
}

func assertUnknownConstruction(t *testing.T, c *comfyUICoordinator, calls *atomic.Int32) {
	t.Helper()
	if c.snapshot().State != "unknown" || calls.Load() != 0 {
		t.Fatal("construction inferred released state or performed I/O")
	}
	if _, err := c.beginLease(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("unknown state admitted ComfyUI")
	}
	if _, err := c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("unknown state admitted competing work")
	}
}

func reconcileOnce(t *testing.T, c *comfyUICoordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
}

func acquireAndActivate(t *testing.T, c *comfyUICoordinator) (string, func()) {
	t.Helper()
	leaseID, err := c.beginLease()
	if err != nil || len(leaseID) != 32 {
		t.Fatalf("acquire ID=%q error=%v", leaseID, err)
	}
	finishSubmission, err := c.reserveSubmission(leaseID)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(leaseID); err != nil {
		t.Fatalf("start callback waited for its own HTTP submission: %v", err)
	}
	return leaseID, finishSubmission
}

func drainAndRelease(t *testing.T, c *comfyUICoordinator, ctx context.Context, calls *atomic.Int32, leaseID string, finishSubmission func()) {
	t.Helper()
	finished := make(chan error, 1)
	go func() { _, err := c.release(ctx, leaseID); finished <- err }()
	waitCoordinatorState(t, c, "draining")
	if calls.Load() != 1 {
		t.Fatal("release verified before outstanding HTTP submission completed")
	}
	if _, err := c.reserveSubmission(leaseID); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("draining admitted another prompt")
	}
	finishSubmission()
	finishSubmission()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func reserveCompeting(t *testing.T, c *comfyUICoordinator) func() {
	t.Helper()
	finishCompeting, err := c.reserveCompeting()
	if err != nil {
		t.Fatal(err)
	}
	return finishCompeting
}

func assertCompetingBlocksNewLease(t *testing.T, c *comfyUICoordinator) {
	t.Helper()
	if _, err := c.beginLease(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("new ComfyUI lease admitted while competing HTTP work was in flight")
	}
}

func assertDuplicateReleaseCached(t *testing.T, c *comfyUICoordinator, ctx context.Context, calls *atomic.Int32, leaseID string) ComfyUIReleaseResult {
	t.Helper()
	receipt, err := c.release(ctx, leaseID)
	if err != nil || calls.Load() != 2 {
		t.Fatalf("duplicate release performed verification: calls=%d error=%v", calls.Load(), err)
	}
	return receipt
}

func assertReceiptDefensiveCopy(t *testing.T, c *comfyUICoordinator, ctx context.Context, leaseID string, receipt ComfyUIReleaseResult) {
	t.Helper()
	receipt.Devices[0].FreeVRAMBytes = 0
	stored, err := c.release(ctx, leaseID)
	if err != nil || stored.Devices[0].FreeVRAMBytes != 900 {
		t.Fatal("caller mutated stored release evidence")
	}
}

func assertNewLeaseAfterCompeting(t *testing.T, c *comfyUICoordinator, leaseID string) string {
	t.Helper()
	nextID, err := c.beginLease()
	if err != nil || nextID == leaseID {
		t.Fatalf("new acquisition reused lease ID: %q %v", nextID, err)
	}
	return nextID
}

func assertStaleLeaseRejected(t *testing.T, c *comfyUICoordinator, ctx context.Context, calls *atomic.Int32, leaseID, nextID string) {
	t.Helper()
	if _, err := c.release(ctx, leaseID); !errors.Is(err, errComfyUIStaleLease) {
		t.Fatal("old lease release was accepted for a new acquisition")
	}
	if err := c.activate(leaseID); !errors.Is(err, errComfyUIStaleLease) {
		t.Fatal("old lease start callback activated a new acquisition")
	}
	if err := c.invalidate(leaseID); !errors.Is(err, errComfyUIStaleLease) {
		t.Fatal("old lease failure invalidated a new acquisition")
	}
	if c.snapshot().State != "acquiring" || calls.Load() != 2 || c.snapshot().Submissions != 0 || c.snapshot().Competing != 0 {
		t.Fatalf("stale callback changed coordinator: %+v calls=%d", c.snapshot(), calls.Load())
	}
}

func TestComfyUICoordinatorConcurrentReleaseRunsVerifierOnce(t *testing.T) {
	var calls atomic.Int32
	entered, unblock := make(chan struct{}), make(chan struct{})
	c := newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
		if calls.Add(1) == 2 {
			close(entered)
			select {
			case <-ctx.Done():
				return ComfyUIReleaseResult{}, ctx.Err()
			case <-unblock:
			}
		}
		return confirmedCoordinatorEvidence(), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.beginLease()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(id); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 8)
	for i := 0; i < cap(results); i++ {
		go func() {
			_, err := c.release(ctx, id)
			results <- err
		}()
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("release verification did not start")
	}
	if _, err := c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("unconfirmed release admitted competing work")
	}
	followerCtx, followerCancel := context.WithCancel(ctx)
	followerDone := make(chan error, 1)
	go func() {
		_, err := c.release(followerCtx, id)
		followerDone <- err
	}()
	followerCancel()
	if err := <-followerDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled duplicate release error=%v", err)
	}
	if c.snapshot().State != "releasing" {
		t.Fatal("cancelled duplicate release cancelled the leader")
	}
	close(unblock)
	for i := 0; i < cap(results); i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("concurrent releases invoked verifier %d times including initial reconcile", calls.Load())
	}
}

func TestComfyUICoordinatorCancelledDrainStaysUnknownWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		calls.Add(1)
		return confirmedCoordinatorEvidence(), nil
	})
	if _, err := c.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	id, err := c.beginLease()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(id); err != nil {
		t.Fatal(err)
	}
	finish, err := c.reserveSubmission(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := c.release(ctx, id); done <- err }()
	waitCoordinatorState(t, c, "draining")
	cancel()
	if err := <-done; !errors.Is(err, ErrComfyUIReleaseUnknown) {
		t.Fatalf("cancelled drain error=%v", err)
	}
	if c.snapshot().State != "unknown" || calls.Load() != 1 {
		t.Fatal("cancelled drain freed memory or reopened admission")
	}
	if _, err := c.reconcile(context.Background()); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("reconciled while old submissions were still running")
	}
	finish()
	if _, err := c.release(context.Background(), id); !errors.Is(err, errComfyUIAdmissionBlocked) || calls.Load() != 1 {
		t.Fatal("ambiguous release was automatically retried")
	}
	if _, err := c.reconcile(context.Background()); err != nil || calls.Load() != 2 {
		t.Fatalf("explicit reconciliation failed: %v calls=%d", err, calls.Load())
	}
}

func TestComfyUICoordinatorInvalidationDiscardsLateVerification(t *testing.T) {
	var calls atomic.Int32
	entered, unblock := make(chan struct{}), make(chan struct{})
	c := newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
		if calls.Add(1) == 2 {
			close(entered)
			// Model a response arriving after helper failure, ignoring cancellation.
			select {
			case <-unblock:
			case <-ctx.Done():
				<-unblock
			}
		}
		return confirmedCoordinatorEvidence(), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.beginLease()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(id); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.release(ctx, id); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("release verification did not start")
	}
	if err := c.invalidate(id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.reconcile(ctx); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("a second verification overlapped a cancelled but still-running operation")
	}
	close(unblock)
	if err := <-done; !errors.Is(err, ErrComfyUIReleaseUnknown) || c.snapshot().State != "unknown" {
		t.Fatalf("late response reopened admission: state=%s err=%v", c.snapshot().State, err)
	}
	if _, err := c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("late release response admitted competing work")
	}
}

func TestComfyUICoordinatorRejectsIncompleteReleaseEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*ComfyUIReleaseResult)
	}{
		{"unknown", func(r *ComfyUIReleaseResult) { r.State = "unknown" }},
		{"no free", func(r *ComfyUIReleaseResult) { r.FreeRequested = false }},
		{"free failure", func(r *ComfyUIReleaseResult) { r.FreeHTTPStatus = 500 }},
		{"no devices", func(r *ComfyUIReleaseResult) { r.Devices = nil }},
		{"insufficient memory", func(r *ComfyUIReleaseResult) { r.Devices[0].FreeVRAMBytes = 1 }},
		{"no stable queue", func(r *ComfyUIReleaseResult) { r.QueueIdleSamples = 1 }},
		{"duplicate GPU", func(r *ComfyUIReleaseResult) { r.Devices = append(r.Devices, r.Devices[0]) }},
		{"CPU evidence", func(r *ComfyUIReleaseResult) { r.Devices[0].DeviceKey = "cpu:0" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
				result := confirmedCoordinatorEvidence()
				test.mutate(&result)
				return result, nil
			})
			if _, err := c.reconcile(context.Background()); !errors.Is(err, ErrComfyUIReleaseUnknown) || c.snapshot().State != "unknown" {
				t.Fatalf("invalid evidence confirmed release: state=%s err=%v", c.snapshot().State, err)
			}
		})
	}
}

func TestComfyUICoordinatorUsesQueueAndVRAMVerifierAgainstFakeHTTP(t *testing.T) {
	var freeCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/queue":
			_, _ = io.WriteString(w, `{"queue_running":[],"queue_pending":[]}`)
		case request.Method == http.MethodPost && request.URL.Path == "/free":
			freeCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case request.Method == http.MethodGet && request.URL.Path == "/system_stats":
			_, _ = io.WriteString(w, `{"devices":[{"name":"GPU","type":"cuda","index":0,"vram_total":1000,"vram_free":900}]}`)
		default:
			t.Errorf("unexpected fake backend operation %s %s", request.Method, request.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer backend.Close()
	c := newComfyUICoordinator(func(ctx context.Context) (ComfyUIReleaseResult, error) {
		return VerifyComfyUIRelease(ctx, backend.URL, nil, &config.ComfyUILeaseConfig{
			ReleaseTimeout: "5s", PollInterval: "100ms", MinimumFreeVRAMBytes: map[string]int64{"cuda:0": 800},
		}, backend.Client())
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := c.beginLease()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.release(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := c.release(ctx, id); err != nil || freeCalls.Load() != 2 {
		t.Fatalf("duplicate callback repeated /free: calls=%d error=%v", freeCalls.Load(), err)
	}
}

func TestComfyUICoordinatorAdmissionBoundsAndFailure(t *testing.T) {
	var fail atomic.Bool
	c := newComfyUICoordinator(func(context.Context) (ComfyUIReleaseResult, error) {
		if fail.Load() {
			return confirmedCoordinatorEvidence(), errors.New("ambiguous release failure")
		}
		return confirmedCoordinatorEvidence(), nil
	})
	if _, err := c.reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	var cleanup []func()
	for i := 0; i < 128; i++ {
		finish, err := c.reserveCompeting()
		if err != nil {
			t.Fatal(err)
		}
		cleanup = append(cleanup, finish)
	}
	if _, err := c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("competing admission exceeded bound")
	}
	for _, finish := range cleanup {
		finish()
	}
	id, err := c.beginLease()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.activate(id); err != nil {
		t.Fatal(err)
	}
	if err := c.activate(id); err != nil {
		t.Fatalf("duplicate registration for active lease failed: %v", err)
	}
	cleanup = nil
	for i := 0; i < 128; i++ {
		finish, err := c.reserveSubmission(id)
		if err != nil {
			t.Fatal(err)
		}
		cleanup = append(cleanup, finish)
	}
	if _, err := c.reserveSubmission(id); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("submission admission exceeded bound")
	}
	for _, finish := range cleanup {
		finish()
	}
	fail.Store(true)
	if _, err := c.release(context.Background(), id); !errors.Is(err, ErrComfyUIReleaseUnknown) || c.snapshot().State != "unknown" {
		t.Fatalf("release failure accepted: state=%s err=%v", c.snapshot().State, err)
	}
	if _, err := c.reserveCompeting(); !errors.Is(err, errComfyUIAdmissionBlocked) {
		t.Fatal("failed release admitted competing work")
	}
}
