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

	verifying        bool
	cancel           context.CancelFunc
	receipt          ComfyUIReleaseResult
	initialReconcile bool
}

func newComfyUICoordinator(verify func(context.Context) (ComfyUIReleaseResult, error)) *comfyUICoordinator {
	return &comfyUICoordinator{status: comfyUICoordinatorStatus{State: "unknown"}, changed: make(chan struct{}), verify: verify, initialReconcile: true}
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
	c.receipt = ComfyUIReleaseResult{}
	if c.cancel != nil {
		c.cancel()
	}
	c.signalLocked()
}

// Reconciliation is explicit: constructing the coordinator never invokes /free.
// The eventual owner must establish external exclusivity before calling this.
func (c *comfyUICoordinator) reconcile(ctx context.Context) (ComfyUIReleaseResult, error) {
	c.mu.Lock()
	c.initialReconcile = false
	c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ComfyUIReleaseResult{State: "unknown"}, err
	}
	c.mu.Lock()
	if c.status.State != "unknown" || c.verifying || c.status.Submissions != 0 || c.status.Competing != 0 || c.verify == nil {
		c.mu.Unlock()
		return ComfyUIReleaseResult{State: "unknown"}, errComfyUIAdmissionBlocked
	}
	operationCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.cancel, c.verifying = cancel, true
	c.status.State = "reconciling"
	c.signalLocked()
	c.mu.Unlock()
	return c.finishVerification(operationCtx, "reconciling")
}

// reconcileInitial permits request-triggered startup verification. Failed
// checks may be retried only when they prove /free was not requested.
func (c *comfyUICoordinator) reconcileInitial(ctx context.Context) (ComfyUIReleaseResult, error) {
	c.mu.Lock()
	if !c.initialReconcile {
		c.mu.Unlock()
		return ComfyUIReleaseResult{State: "unknown"}, errComfyUIAdmissionBlocked
	}
	c.initialReconcile = false
	c.mu.Unlock()
	return c.reconcile(ctx)
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
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.status.State {
	case "released":
		id, err := c.beginLeaseLocked()
		if err != nil {
			return "", nil, err
		}
		return id, c.reserveSubmissionLocked(id, true), nil
	case "active":
		if c.status.Submissions < 128 {
			id := c.status.LeaseID
			return id, c.reserveSubmissionLocked(id, false), nil
		}
	}
	return "", nil, errComfyUIAdmissionBlocked
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
				c.status.State, c.status.LeaseID = "unknown", ""
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
		if leaseID == "" || c.status.LeaseID != leaseID {
			c.mu.Unlock()
			return ComfyUIReleaseResult{State: "unknown"}, errComfyUIStaleLease
		}
		if c.status.State == "released" {
			// A stop callback must not free memory again after competing work starts.
			result := cloneComfyUIReleaseResult(c.receipt)
			c.mu.Unlock()
			return result, nil
		}
		if c.verifying && (c.status.State == "draining" || c.status.State == "releasing") {
			changed := c.changed
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return ComfyUIReleaseResult{State: "unknown"}, ctx.Err()
			case <-changed:
			}
			c.mu.Lock()
			continue
		}
		if c.status.State != "active" || c.verifying || c.verify == nil {
			c.mu.Unlock()
			return ComfyUIReleaseResult{State: "unknown"}, errComfyUIAdmissionBlocked
		}
		break
	}
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
	// No lock is held across network verification or callback execution.
	result, err := c.verify(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil && (ctx.Err() != nil || c.status.State != expectedState || !confirmedComfyUIRelease(result)) {
		err = ErrComfyUIReleaseUnknown
	}
	c.verifying, c.cancel = false, nil
	if err != nil {
		c.status.State = "unknown"
		c.receipt = ComfyUIReleaseResult{}
		if expectedState == "reconciling" && !result.FreeRequested &&
			!errors.Is(err, errComfyUIServiceUnavailable) {
			// The initial probe failed before /free could be sent. Allow a later
			// request to retry after a transient connection or queue failure.
			c.initialReconcile = true
		}
		c.signalLocked()
		return ComfyUIReleaseResult{State: "unknown"}, errors.Join(ErrComfyUIReleaseUnknown, err)
	}
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
