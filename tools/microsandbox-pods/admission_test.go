package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func admissionProvider(budget int) *provider {
	return &provider{runner: commandRunner{cfg: config{maxActiveMemoryMiB: budget}}, starts: make(chan struct{}, 2)}
}

func admissionEntry(memoryMiB int) *podEntry {
	return &podEntry{allocation: podAllocation{Version: 1, VCPUs: 1, MemoryMiB: memoryMiB}}
}

func TestMemoryAdmissionCountsDifferentGuestSizes(t *testing.T) {
	p := admissionProvider(2048)
	small, large := admissionEntry(768), admissionEntry(1280)
	for _, entry := range []*podEntry{small, large} {
		if err := p.reserveMemory(t.Context(), entry); err != nil {
			t.Fatal(err)
		}
	}
	if p.activeMemoryMiB != 2048 || !small.allocated || !large.allocated {
		t.Fatalf("did not account both actual guest sizes: %d", p.activeMemoryMiB)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	waiting := admissionEntry(768)
	if err := p.reserveMemory(ctx, waiting); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected full-budget admission to wait, got %v", err)
	}
	if waiting.allocated || p.activeMemoryMiB != 2048 {
		t.Fatal("blocked admission changed memory accounting")
	}
	p.releaseMemory(large)
	if p.activeMemoryMiB != 768 || large.allocated || !small.allocated {
		t.Fatalf("release did not subtract the larger guest: %d", p.activeMemoryMiB)
	}
}

func TestMemoryAdmissionWaiterProceedsAfterRelease(t *testing.T) {
	p := admissionProvider(2048)
	current, waiting := admissionEntry(1280), admissionEntry(1280)
	if err := p.reserveMemory(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var jobs sync.WaitGroup
	t.Cleanup(func() { cancel(); jobs.Wait() })
	result := make(chan error, 1)
	jobs.Go(func() { result <- p.reserveMemory(ctx, waiting) })
	select {
	case err := <-result:
		t.Fatalf("admitted guest before enough memory was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	p.releaseMemory(current)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiting guest did not notice released memory")
	}
	if p.activeMemoryMiB != 1280 || !waiting.allocated || current.allocated {
		t.Fatalf("wrong accounting after wake admission: %d", p.activeMemoryMiB)
	}
}

func TestStartAdmissionRejectsOversizedGuestBeforeWaitingForPermit(t *testing.T) {
	p := admissionProvider(1024)
	p.starts <- struct{}{}
	p.starts <- struct{}{}
	entry := admissionEntry(1280)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release, err := p.acquireStart(ctx, entry)
	if err == nil || !strings.Contains(err.Error(), "exceeding") || errors.Is(err, context.DeadlineExceeded) || release != nil {
		t.Fatalf("oversized guest did not fail immediately: %v", err)
	}
	if len(p.starts) != 2 || entry.allocated || p.activeMemoryMiB != 0 {
		t.Fatal("oversized admission leaked or consumed a permit or memory")
	}
}

func TestStartAdmissionRejectsInvalidAllocationWithoutPermit(t *testing.T) {
	p := admissionProvider(2048)
	entry := admissionEntry(768)
	entry.allocation.Version = 2
	release, err := p.acquireStart(t.Context(), entry)
	if err == nil || release != nil || len(p.starts) != 0 || entry.allocated || p.activeMemoryMiB != 0 {
		t.Fatalf("invalid allocation changed admission: release=%v, error=%v", release != nil, err)
	}
}

func TestStartAdmissionCancellationWhileWaitingForMemoryClaimsNeitherResource(t *testing.T) {
	p := admissionProvider(768)
	current := admissionEntry(768)
	if err := p.reserveMemory(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var jobs sync.WaitGroup
	t.Cleanup(func() { cancel(); jobs.Wait() })
	entry := admissionEntry(768)
	result := make(chan error, 1)
	jobs.Go(func() {
		release, err := p.acquireStart(ctx, entry)
		if release != nil {
			release()
		}
		result <- err
	})
	select {
	case err := <-result:
		t.Fatalf("admission did not wait for memory: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if len(p.starts) != 0 {
		t.Fatal("memory waiter consumed a start permit")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected cancellation result: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("memory admission ignored cancellation")
	}
	if len(p.starts) != 0 || entry.allocated || p.activeMemoryMiB != 768 {
		t.Fatal("canceled admission leaked memory or its start permit")
	}
}

func TestStartAdmissionMemoryWaitersDoNotBlockSmallerGuest(t *testing.T) {
	p := admissionProvider(3072)
	current := admissionEntry(1536)
	if err := p.reserveMemory(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	var jobs sync.WaitGroup
	t.Cleanup(func() { cancel(); jobs.Wait() })
	waiters := []*podEntry{admissionEntry(2048), admissionEntry(2048)}
	results := make(chan error, len(waiters))
	for _, entry := range waiters {
		jobs.Go(func() {
			release, err := p.acquireStart(ctx, entry)
			if release != nil {
				release()
			}
			results <- err
		})
	}
	select {
	case err := <-results:
		t.Fatalf("large guest did not wait for memory: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	small := admissionEntry(768)
	smallContext, smallCancel := context.WithTimeout(t.Context(), time.Second)
	defer smallCancel()
	release, err := p.acquireStart(smallContext, small)
	if err != nil {
		t.Fatalf("large memory waiters blocked a smaller guest that fits: %v", err)
	}
	release()
	if len(p.starts) != 0 || !small.allocated || p.activeMemoryMiB != 2304 {
		t.Fatalf("unexpected smaller-guest admission: permits=%d, allocated=%t, memory=%d", len(p.starts), small.allocated, p.activeMemoryMiB)
	}
	cancel()
	jobs.Wait()
	for range waiters {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("large memory waiter did not cancel: %v", err)
		}
	}
	for _, entry := range waiters {
		if entry.allocated {
			t.Fatal("canceled large memory waiter retained an allocation")
		}
	}
	if len(p.starts) != 0 || p.activeMemoryMiB != 2304 {
		t.Fatal("canceled memory waiters leaked a permit or memory")
	}
}

func TestStartAdmissionPermitWaiterDoesNotReserveMemory(t *testing.T) {
	p := admissionProvider(3072)
	p.starts <- struct{}{}
	p.starts <- struct{}{}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	entry := admissionEntry(1280)
	release, err := p.acquireStart(ctx, entry)
	if !errors.Is(err, context.DeadlineExceeded) || release != nil {
		t.Fatalf("permit waiter did not cancel: release=%t, error=%v", release != nil, err)
	}
	if entry.allocated || p.activeMemoryMiB != 0 || len(p.starts) != 2 {
		t.Fatal("permit waiter consumed memory or changed existing permits")
	}
}

func TestStartAdmissionDoesNotCountRecoveredMemoryTwice(t *testing.T) {
	p := admissionProvider(768)
	entry := admissionEntry(1280)
	entry.allocated = true
	p.activeMemoryMiB = 1280
	release, err := p.acquireStart(t.Context(), entry)
	if err != nil {
		t.Fatalf("recovered guest could not acquire its start permit: %v", err)
	}
	if p.activeMemoryMiB != 1280 || len(p.starts) != 1 {
		t.Fatal("recovered guest admission counted its memory twice")
	}
	release()
	if len(p.starts) != 0 || p.activeMemoryMiB != 1280 {
		t.Fatal("start permit release unexpectedly changed memory accounting")
	}
}

func TestMemoryAdmissionHonorsCancellationBeforeAllocating(t *testing.T) {
	p := admissionProvider(2048)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	entry := admissionEntry(1280)
	if err := p.reserveMemory(ctx, entry); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if entry.allocated || p.activeMemoryMiB != 0 {
		t.Fatal("canceled context allocated memory")
	}
}

func TestMemoryAdmissionKeepsRecoveredAllocationWhenBudgetShrinks(t *testing.T) {
	p := admissionProvider(768)
	entry := admissionEntry(1280)
	entry.allocated = true
	p.activeMemoryMiB = 1280
	if err := p.reserveMemory(t.Context(), entry); err != nil {
		t.Fatalf("already running recovered guest was rejected: %v", err)
	}
	if p.activeMemoryMiB != 1280 {
		t.Fatal("recovery admission counted the same guest twice")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.reserveMemory(ctx, admissionEntry(768)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("new guest did not wait while recovered usage exceeds budget: %v", err)
	}
	p.releaseMemory(entry)
	if p.activeMemoryMiB != 0 || entry.allocated {
		t.Fatal("could not release recovered allocation above the new budget")
	}
}

func TestMemoryAdmissionAndReleaseAreIdempotent(t *testing.T) {
	p := admissionProvider(2048)
	entry := admissionEntry(1280)
	var jobs sync.WaitGroup
	results := make(chan error, 20)
	for range 20 {
		jobs.Go(func() { results <- p.reserveMemory(t.Context(), entry) })
	}
	jobs.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if p.activeMemoryMiB != 1280 || !entry.allocated {
		t.Fatalf("duplicate admission changed accounting: %d", p.activeMemoryMiB)
	}
	for range 20 {
		jobs.Go(func() { p.releaseMemory(entry) })
	}
	jobs.Wait()
	if p.activeMemoryMiB != 0 || entry.allocated {
		t.Fatalf("duplicate release changed accounting: %d", p.activeMemoryMiB)
	}
}

func TestMemoryAdmissionNormalizesLegacyAllocation(t *testing.T) {
	p := admissionProvider(768)
	entry := &podEntry{}
	if err := p.reserveMemory(t.Context(), entry); err != nil {
		t.Fatal(err)
	}
	if entry.allocation != (podAllocation{Version: 2, VCPUs: 1, MemoryMiB: 768, RootDiskMiB: 4096}) || p.activeMemoryMiB != 768 {
		t.Fatalf("wrong legacy allocation: %+v, %d", entry.allocation, p.activeMemoryMiB)
	}
	p.releaseMemory(entry)
	if p.activeMemoryMiB != 0 || entry.allocated {
		t.Fatal("legacy release did not match its admitted size")
	}
}

func TestMemoryAdmissionDoesNotOverflowCombinedUsage(t *testing.T) {
	p := admissionProvider(math.MaxInt)
	p.activeMemoryMiB = math.MaxInt - 100
	entry := admissionEntry(768)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.reserveMemory(ctx, entry); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overflowing aggregate admission did not wait: %v", err)
	}
	if entry.allocated || p.activeMemoryMiB != math.MaxInt-100 {
		t.Fatal("aggregate admission overflowed memory accounting")
	}
}

func inactiveMemoryFixture(t *testing.T, runtimeStatus string) (*provider, *podEntry, string) {
	t.Helper()
	directory := t.TempDir()
	binary := filepath.Join(directory, "msb")
	entry := admissionEntry(1280)
	entry.pod, entry.vm, entry.state, entry.allocated = validPod(), "failed-pod", "FAILED", true
	list := `[{"name":"failed-pod"}]`
	if runtimeStatus == "absent" {
		list = "[]"
	}
	inspection := fmt.Sprintf(`{"status":%q,"config":{"labels":{%q:"memory-fixture","kubernetes.io/pod-uid":%q}}}`, runtimeStatus, ownerLabel, entry.pod.UID)
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(filepath.Join(directory, "commands")) + "\n" +
		"case \"$1\" in\nls) printf '%s\\n' " + quote(list) + ";;\n" +
		"inspect) printf '%s\\n' " + quote(inspection) + ";;\n*) exit 9;;\nesac\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	p := admissionProvider(2048)
	p.runner.cfg.state, p.runner.cfg.msb, p.runner.cfg.peerOwner = directory, binary, "memory-fixture"
	p.runner = testCommandRunner(p.runner.cfg)
	p.activeMemoryMiB = 2048 // The other 768 MiB belong to an unrelated guest.
	return p, entry, binary
}

func TestReleaseInactiveMemoryRequiresConfirmedAbsenceOrStoppedGuest(t *testing.T) {
	for _, status := range []string{"absent", "Stopped", "Running", "Paused", "Starting", ""} {
		t.Run(status, func(t *testing.T) {
			p, entry, _ := inactiveMemoryFixture(t, status)
			entry.op.Lock()
			defer entry.op.Unlock()
			if err := p.releaseInactiveMemory(t.Context(), entry); err != nil {
				t.Fatal(err)
			}
			wantAllocated, wantMemory := true, 2048
			if status == "absent" || status == "Stopped" {
				wantAllocated, wantMemory = false, 768
			}
			if entry.allocated != wantAllocated || p.activeMemoryMiB != wantMemory {
				t.Fatalf("status %q: allocated=%t memory=%d", status, entry.allocated, p.activeMemoryMiB)
			}
			if err := p.releaseInactiveMemory(t.Context(), entry); err != nil || entry.allocated != wantAllocated || p.activeMemoryMiB != wantMemory {
				t.Fatalf("repeated verification changed accounting: %v", err)
			}
			commands, err := os.ReadFile(filepath.Join(p.runner.cfg.state, "commands"))
			if err != nil {
				t.Fatal(err)
			}
			for command := range strings.SplitSeq(strings.TrimSpace(string(commands)), "\n") {
				if command != "ls --format json" && command != "inspect --format json failed-pod" {
					t.Fatalf("memory release performed a runtime mutation: %s", command)
				}
			}
		})
	}
}

func TestReleaseInactiveMemoryRetainsReservationWhenInspectionIsUncertain(t *testing.T) {
	for name, body := range map[string]string{
		"list error":               "exit 1\n",
		"invalid list":             "printf '%s\\n' 'invalid json'\n",
		"inspect error":            "if test \"$1\" = ls; then printf '%s\\n' '[{\"name\":\"failed-pod\"}]'; else exit 1; fi\n",
		"foreign owner":            "if test \"$1\" = ls; then printf '%s\\n' '[{\"name\":\"failed-pod\"}]'; else printf '%s\\n' '{\"status\":\"Stopped\",\"config\":{\"labels\":{}}}'; fi\n",
		"pending after inspection": "printf '%s\\n' run >\"$(dirname \"$0\")/native-command.pending\"\nprintf '%s\\n' '[]'\n",
	} {
		t.Run(name, func(t *testing.T) {
			p, entry, binary := inactiveMemoryFixture(t, "Stopped")
			if err := os.WriteFile(binary, []byte("#!/bin/sh\n"+body), 0700); err != nil {
				t.Fatal(err)
			}
			entry.op.Lock()
			defer entry.op.Unlock()
			if err := p.releaseInactiveMemory(t.Context(), entry); err == nil {
				t.Fatal("uncertain runtime inspection did not report an error")
			}
			if !entry.allocated || p.activeMemoryMiB != 2048 {
				t.Fatal("uncertain runtime inspection released reserved memory")
			}
		})
	}
}

func TestReleaseInactiveMemoryRejectsPendingOrInFlightNativeCommand(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			p, entry, _ := inactiveMemoryFixture(t, "absent")
			if pending {
				if err := os.WriteFile(filepath.Join(p.runner.cfg.state, "native-command.pending"), []byte("run"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				nativeMutations.Lock()
				defer nativeMutations.Unlock()
			}
			entry.op.Lock()
			defer entry.op.Unlock()
			if err := p.releaseInactiveMemory(t.Context(), entry); err == nil {
				t.Fatal("pending native command was ignored")
			}
			if !entry.allocated || p.activeMemoryMiB != 2048 {
				t.Fatal("pending native command lost its memory reservation")
			}
			if _, err := os.Stat(filepath.Join(p.runner.cfg.state, "commands")); !os.IsNotExist(err) {
				t.Fatalf("pending native command did not prevent inspection: %v", err)
			}
		})
	}
}

func TestReleaseInactiveMemorySkipsOtherStatesAndUnallocatedEntries(t *testing.T) {
	for _, state := range []string{"RUNNING", "SLEEPING", "STARTING", "RESTARTING", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			p, entry, _ := inactiveMemoryFixture(t, "absent")
			entry.state = state
			if state == "FAILED" {
				entry.allocated, p.activeMemoryMiB = false, 768
			}
			allocated, memory := entry.allocated, p.activeMemoryMiB
			entry.op.Lock()
			defer entry.op.Unlock()
			if err := p.releaseInactiveMemory(t.Context(), entry); err != nil {
				t.Fatal(err)
			}
			if entry.allocated != allocated || p.activeMemoryMiB != memory {
				t.Fatal("ineligible entry changed memory accounting")
			}
			if _, err := os.Stat(filepath.Join(p.runner.cfg.state, "commands")); !os.IsNotExist(err) {
				t.Fatalf("ineligible entry performed runtime inspection: %v", err)
			}
		})
	}
}

func TestReleaseInactiveMemoryHonorsInspectionDeadline(t *testing.T) {
	p, entry, binary := inactiveMemoryFixture(t, "Stopped")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	entry.op.Lock()
	defer entry.op.Unlock()
	if err := p.releaseInactiveMemory(ctx, entry); err == nil || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("inspection did not respect its deadline: %v", err)
	}
	if !entry.allocated || p.activeMemoryMiB != 2048 {
		t.Fatal("timed-out inspection released reserved memory")
	}
}
