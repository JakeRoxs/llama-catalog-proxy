package proxy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"
)

var (
	errComfyUIAdmissionBlocked = errors.New("ComfyUI GPU admission is blocked")
	errComfyUIStaleLease       = errors.New("ComfyUI lease identity does not match")
)

type comfyUICoordinatorStatus struct {
	State       string
	LeaseID     string
	Submissions int
	Competing   int
}

// This is an in-process primitive, not a deployed GPU lock. All load paths and
// restart reconciliation must be controlled before it can enforce host admission.
type comfyUICoordinator struct {
	mu      sync.Mutex
	status  comfyUICoordinatorStatus
	changed chan struct{}
	verify  func(context.Context) (ComfyUIReleaseResult, error)
	// startupReady is an optional read-only scheduler check, never used for
	// a handoff or after an ambiguous operation.
	startupReady func(context.Context) bool

	verifying        bool
	cancel           context.CancelFunc
	receipt          ComfyUIReleaseResult
	initialReconcile bool
	initialPrompt    bool
}

func newComfyUICoordinator(verify func(context.Context) (ComfyUIReleaseResult, error)) *comfyUICoordinator {
	return &comfyUICoordinator{
		status: comfyUICoordinatorStatus{State: "unknown"}, changed: make(chan struct{}),
		verify: verify, initialReconcile: true, initialPrompt: true,
	}
}

func (c *comfyUICoordinator) snapshot() comfyUICoordinatorStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status
}

// A transport restart or shutdown is not evidence of GPU release. Keep outstanding
// reservations so reconciliation cannot race work admitted before the failure.
func (c *comfyUICoordinator) blockAdmission() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.State, c.status.LeaseID = "unknown", ""
	c.initialReconcile = false
	c.initialPrompt = false
	c.receipt = ComfyUIReleaseResult{}
	if c.cancel != nil {
		c.cancel()
	}
	c.signalLocked()
}

// Reconciliation is explicit: constructing the coordinator never invokes /free.
// The eventual owner must establish external exclusivity before calling this.
func (c *comfyUICoordinator) reconcile(ctx context.Context) (ComfyUIReleaseResult, error) {
	return c.reconcileWithMode(ctx, false, false)
}

func (c *comfyUICoordinator) reconcileWithMode(ctx context.Context, initialOnly, allowSchedulerProbe bool) (ComfyUIReleaseResult, error) {
	if err := ctx.Err(); err != nil {
		return ComfyUIReleaseResult{State: "unknown"}, err
	}
	c.mu.Lock()
	if (initialOnly && !c.initialReconcile) || c.status.State != "unknown" ||
		c.verifying || c.status.Submissions != 0 || c.status.Competing != 0 || c.verify == nil {
		c.mu.Unlock()
		return ComfyUIReleaseResult{State: "unknown"}, errComfyUIAdmissionBlocked
	}
	var startupReady func(context.Context) bool
	if allowSchedulerProbe && initialOnly && c.initialPrompt && c.status.LeaseID == "" {
		startupReady = c.startupReady
	}
	if !initialOnly {
		c.initialPrompt = false
	}
	c.initialReconcile = false
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.cancel, c.verifying = cancel, true
	c.status.State = "reconciling"
	c.signalLocked()
	c.mu.Unlock()
	return c.finishVerificationWithStartup(operationCtx, "reconciling", startupReady)
}

// reconcileInitial permits request-triggered startup verification. Failed
// checks may be retried only when they prove /free was not requested.
func (c *comfyUICoordinator) reconcileInitial(ctx context.Context, allowSchedulerProbe bool) (ComfyUIReleaseResult, error) {
	return c.reconcileWithMode(ctx, true, allowSchedulerProbe)
}

// assumeInitialStopped records the narrow best-effort case where the first
// queue probe got ECONNREFUSED before any /free request. Scheduler dispatch can
// then start ComfyUI normally; other reconciliation failures remain unknown.
func (c *comfyUICoordinator) assumeInitialStopped() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.initialReconcile || c.status.State != "unknown" || c.verifying ||
		c.status.Submissions != 0 || c.status.Competing != 0 {
		return errComfyUIAdmissionBlocked
	}
	c.status.State = "released"
	c.receipt = ComfyUIReleaseResult{State: "stopped"}
	c.signalLocked()
	return nil
}

// assumeLeaseStopped handles an already active lease whose ComfyUI service is
// now unavailable before /free could be requested. It recovers competing model
// admission only when the verifier proved no release operation was sent.
func (c *comfyUICoordinator) assumeLeaseStopped(leaseID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if leaseID == "" || c.status.LeaseID != leaseID || c.status.State != "unknown" || c.verifying ||
		c.status.Submissions != 0 || c.status.Competing != 0 {
		return errComfyUIAdmissionBlocked
	}
	c.status.State = "released"
	c.receipt = ComfyUIReleaseResult{State: "stopped"}
	c.signalLocked()
	return nil
}

func (c *comfyUICoordinator) beginLease() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.beginLeaseLocked()
}

func (c *comfyUICoordinator) beginLeaseLocked() (string, error) {
	if c.status.State != "released" || c.verifying || c.status.Competing != 0 || c.status.Submissions != 0 {
		return "", errComfyUIAdmissionBlocked
	}
	return c.openLeaseLocked()
}

func (c *comfyUICoordinator) openLeaseLocked() (string, error) {
	var secret [16]byte
	if _, err := rand.Read(secret[:]); err != nil {
		c.status.State = "unknown"
		c.receipt = ComfyUIReleaseResult{}
		c.signalLocked()
		return "", err
	}
	c.status.LeaseID = hex.EncodeToString(secret[:])
	c.status.State = "acquiring"
	c.receipt = ComfyUIReleaseResult{}
	c.signalLocked()
	return c.status.LeaseID, nil
}

// admitPrompt creates and reserves the first lease in one critical section.
// Only the first prompt may enter while its helper is still acquiring.
func (c *comfyUICoordinator) admitPrompt() (string, func(), error) {
	id, done, _, err := c.admitSchedulerPrompt(false)
	return id, done, err
}

// An explicit scheduler request can perform the initial swap itself. This
// does not assert that the GPU is released or unlock competing admission.
func (c *comfyUICoordinator) admitSchedulerPrompt(initialAllowed bool) (string, func(), bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if initialAllowed && c.initialPrompt && c.initialReconcile && c.status.State == "unknown" &&
		!c.verifying && c.status.Submissions == 0 && c.status.Competing == 0 {
		c.initialReconcile = false
		c.initialPrompt = false
		id, err := c.openLeaseLocked()
		if err != nil {
			return "", nil, false, err
		}
		return id, c.reserveSubmissionLocked(id, true), true, nil
	}
	switch c.status.State {
	case "released":
		id, err := c.beginLeaseLocked()
		if err != nil {
			return "", nil, false, err
		}
		return id, c.reserveSubmissionLocked(id, true), true, nil
	case "active":
		if c.status.Submissions < 128 {
			id := c.status.LeaseID
			return id, c.reserveSubmissionLocked(id, false), false, nil
		}
	}
	return "", nil, false, errComfyUIAdmissionBlocked
}

func (c *comfyUICoordinator) activate(leaseID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if leaseID == "" || c.status.LeaseID != leaseID {
		return errComfyUIStaleLease
	}
	if c.status.State == "active" {
		return nil
	}
	if c.status.State != "acquiring" {
		return errComfyUIAdmissionBlocked
	}
	// Do not wait for the first submission's HTTP response: it may be waiting
	// for this very start callback from llama-swap.
	c.status.State = "active"
	c.signalLocked()
	return nil
}

func (c *comfyUICoordinator) reserveSubmission(leaseID string) (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if leaseID == "" || c.status.LeaseID != leaseID {
		return nil, errComfyUIStaleLease
	}
	if (c.status.State != "active" && c.status.State != "acquiring") || c.status.Submissions >= 128 {
		return nil, errComfyUIAdmissionBlocked
	}
	return c.reserveSubmissionLocked(leaseID, false), nil
}

func (c *comfyUICoordinator) reserveSubmissionLocked(leaseID string, first bool) func() {
	c.status.Submissions++
	c.signalLocked()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.status.Submissions--
			if first && c.status.State == "acquiring" && c.status.LeaseID == leaseID {
				// HTTP completion without activation does not prove the helper never started.
				// Permit the next request to attempt full external reconciliation; it
				// must still confirm queue and VRAM evidence before competing admission.
				c.status.State, c.status.LeaseID = "unknown", ""
				c.initialReconcile = true
				c.initialPrompt = false
			}
			c.signalLocked()
		})
	}
}

func (c *comfyUICoordinator) reserveCompeting() (func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status.State != "released" || c.verifying || c.status.Competing >= 128 {
		return nil, errComfyUIAdmissionBlocked
	}
	c.status.Competing++
	c.signalLocked()
	var once sync.Once
	return func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.status.Competing--
			c.signalLocked()
		})
	}, nil
}

// waitForDrainLocked waits for all outstanding submissions to drain while the
// coordinator is in the draining state. The caller must hold c.mu on entry; it
// returns with c.mu held. It snapshots c.changed before unlocking, waits on
// operationCtx.Done or changed, then relocks and returns whether the state is
// still draining and the context is not canceled.
func (c *comfyUICoordinator) waitForDrainLocked(operationCtx context.Context) bool {
	for c.status.Submissions > 0 && c.status.State == "draining" && operationCtx.Err() == nil {
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-operationCtx.Done():
		case <-changed:
		}
		c.mu.Lock()
	}
	return c.status.State == "draining" && operationCtx.Err() == nil
}

func (c *comfyUICoordinator) release(ctx context.Context, leaseID string) (ComfyUIReleaseResult, error) {
	c.mu.Lock()
	for {
		if err := ctx.Err(); err != nil {
			c.mu.Unlock()
			return ComfyUIReleaseResult{State: "unknown"}, err
		}
		if c.isStaleLease(leaseID) {
			c.mu.Unlock()
			return ComfyUIReleaseResult{State: "unknown"}, errComfyUIStaleLease
		}
		if c.status.State == "released" {
			// A stop callback must not free memory again after competing work starts.
			result := cloneComfyUIReleaseResult(c.receipt)
			c.mu.Unlock()
			return result, nil
		}
		if c.isVerifyingDrainOrRelease() {
			if !c.awaitReleaseSignal(ctx) {
				return ComfyUIReleaseResult{State: "unknown"}, ctx.Err()
			}
			c.mu.Lock()
			continue
		}
		if c.isAdmissionBlocked() {
			c.mu.Unlock()
			return ComfyUIReleaseResult{State: "unknown"}, errComfyUIAdmissionBlocked
		}
		break
	}
	return c.drainAndVerify(ctx)
}

// drainAndVerify transitions the coordinator to draining, waits for outstanding
// submissions to drain, then runs the release verifier. Caller must hold c.mu;
// the method unlocks in every return path.
func (c *comfyUICoordinator) drainAndVerify(ctx context.Context) (ComfyUIReleaseResult, error) {
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.cancel, c.verifying = cancel, true
	c.status.State = "draining"
	c.signalLocked()
	if !c.waitForDrainLocked(operationCtx) {
		c.status.State, c.verifying, c.cancel = "unknown", false, nil
		c.receipt = ComfyUIReleaseResult{}
		c.signalLocked()
		c.mu.Unlock()
		return ComfyUIReleaseResult{State: "unknown"}, ErrComfyUIReleaseUnknown
	}
	c.status.State = "releasing"
	c.signalLocked()
	c.mu.Unlock()
	return c.finishVerification(operationCtx, "releasing")
}

// awaitReleaseSignal waits for the changed channel or context cancellation while
// another verification drains or releases. Caller must hold c.mu; it unlocks
// during the wait and returns with c.mu unlocked. Returns false if ctx was
// cancelled.
func (c *comfyUICoordinator) awaitReleaseSignal(ctx context.Context) bool {
	changed := c.changed
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return false
	case <-changed:
	}
	return true
}

// isStaleLease reports whether leaseID is empty or does not match the held lease.
// Caller must hold c.mu.
func (c *comfyUICoordinator) isStaleLease(leaseID string) bool {
	return leaseID == "" || c.status.LeaseID != leaseID
}

// isVerifyingDrainOrRelease reports whether a verification is in flight while the
// coordinator is draining or releasing. Caller must hold c.mu.
func (c *comfyUICoordinator) isVerifyingDrainOrRelease() bool {
	return c.verifying && (c.status.State == "draining" || c.status.State == "releasing")
}

// isAdmissionBlocked reports whether release cannot proceed because the lease is
// not active, a verification is in flight, or no verifier is configured.
// Caller must hold c.mu.
func (c *comfyUICoordinator) isAdmissionBlocked() bool {
	return c.status.State != "active" || c.verifying || c.verify == nil
}

func (c *comfyUICoordinator) invalidate(leaseID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if leaseID == "" || c.status.LeaseID != leaseID {
		return errComfyUIStaleLease
	}
	c.status.State = "unknown"
	c.receipt = ComfyUIReleaseResult{}
	if c.cancel != nil {
		c.cancel()
	}
	c.signalLocked()
	return nil
}

func (c *comfyUICoordinator) finishVerification(ctx context.Context, expectedState string) (ComfyUIReleaseResult, error) {
	return c.finishVerificationWithStartup(ctx, expectedState, nil)
}

func (c *comfyUICoordinator) finishVerificationWithStartup(ctx context.Context, expectedState string, startupReady func(context.Context) bool) (ComfyUIReleaseResult, error) {
	// No lock is held across network verification or callback execution.
	var result ComfyUIReleaseResult
	var err error
	fromScheduler := startupReady != nil && startupReady(ctx)
	switch {
	case ctx.Err() != nil:
		err = ctx.Err()
	case fromScheduler:
		// This is admission evidence, not a GPU release receipt.
		result = ComfyUIReleaseResult{State: "scheduler_ready"}
	default:
		result, err = c.verify(ctx)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil && (ctx.Err() != nil || c.status.State != expectedState || (!fromScheduler && !confirmedComfyUIRelease(result))) {
		err = ErrComfyUIReleaseUnknown
	}
	c.verifying, c.cancel = false, nil
	if err != nil {
		if result.FreeRequested {
			c.initialPrompt = false
		}
		mayRetry := c.status.State == "reconciling" && expectedState == "reconciling" &&
			!result.FreeRequested && !errors.Is(err, errComfyUIServiceUnavailable)
		c.status.State = "unknown"
		c.receipt = ComfyUIReleaseResult{}
		if mayRetry {
			// The initial probe failed before /free could be sent. Allow a later
			// request to retry after a transient connection or queue failure.
			c.initialReconcile = true
		}
		c.signalLocked()
		return ComfyUIReleaseResult{State: "unknown"}, errors.Join(ErrComfyUIReleaseUnknown, err)
	}
	c.initialPrompt = false
	c.status.State = "released"
	c.receipt = cloneComfyUIReleaseResult(result)
	c.signalLocked()
	return cloneComfyUIReleaseResult(result), nil
}

func confirmedComfyUIRelease(result ComfyUIReleaseResult) bool {
	if result.State != "confirmed" || !result.FreeRequested || result.FreeHTTPStatus < 200 || result.FreeHTTPStatus >= 300 ||
		result.MinimumQueueIdleSamples < 2 || result.MinimumQueueIdleSamples > 10 || result.QueueIdleSamples < result.MinimumQueueIdleSamples ||
		len(result.Devices) == 0 || len(result.Devices) > maxComfyUIReleaseDevices {
		return false
	}
	seen := make(map[string]bool, len(result.Devices))
	thresholds := make(map[string]int64, len(result.Devices))
	for _, device := range result.Devices {
		if device.DeviceKey == "" || seen[device.DeviceKey] || device.MinimumFreeVRAMBytes <= 0 ||
			device.FreeVRAMBytes < device.MinimumFreeVRAMBytes || device.FreeVRAMBytes > maxComfyUIReleaseVRAMBytes {
			return false
		}
		seen[device.DeviceKey] = true
		thresholds[device.DeviceKey] = device.MinimumFreeVRAMBytes
	}
	return validateVRAMThresholds(thresholds) == nil
}

func cloneComfyUIReleaseResult(result ComfyUIReleaseResult) ComfyUIReleaseResult {
	result.Devices = append([]ComfyUIDeviceReleaseEvidence(nil), result.Devices...)
	return result
}

func (c *comfyUICoordinator) signalLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}
