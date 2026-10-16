package microsandboxruntime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

func fakeCapacity(home string, limits CapacityLimits, guests *[]capacityGuest) *capacityManager {
	return &capacityManager{home: home, limits: limits,
		inventory: func(context.Context) ([]capacityGuest, error) { return *guests, nil },
		snapshot: func(_ context.Context, selector string) (capacitySnapshot, error) {
			return capacitySnapshot{ID: selector, Digest: "immutable", MemoryMiB: 512}, nil
		}}
}

func launchArgs(name string, memory int) []string {
	return []string{"run", "-d", "--name", name, "--memory", strconv.Itoa(memory) + "M", "alpine:3.22"}
}

func prepareCapacity(t *testing.T, manager *capacityManager, args []string, source string) (string, error) {
	t.Helper()
	r, err := parse(args)
	if err != nil {
		t.Fatal(err)
	}
	return manager.prepare(t.Context(), r, args, source)
}

func TestCapacityCountsPausedGuestsAndReservationsAcrossClients(t *testing.T) {
	guests := []capacityGuest{{ID: "paused", Name: "ec2", Status: "paused", MemoryMiB: 512}, {ID: "cold", Name: "db", Status: "stopped", MemoryMiB: 2048}}
	home := t.TempDir()
	a := fakeCapacity(home, CapacityLimits{MemoryMiB: 1024, Starts: 2}, &guests)
	b := fakeCapacity(home, CapacityLimits{}, &guests)
	token, err := prepareCapacity(t, a, launchArgs("pod", 512), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCapacity(t, b, launchArgs("db-wake", 512), ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatalf("overcommit was admitted: %v", err)
	}
	if err := (&Client{capacity: a}).CancelAdmission(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCapacity(t, b, launchArgs("db-wake", 512), ""); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityRefusalLeavesRestoreSourceAndCheckpointUntouched(t *testing.T) {
	guests := []capacityGuest{{ID: "source-id", Name: "source", Status: "stopped", MemoryMiB: 512}, {ID: "running", Name: "other", Status: "running", MemoryMiB: 512}}
	m := fakeCapacity(t.TempDir(), CapacityLimits{MemoryMiB: 512}, &guests)
	args := []string{"snap", "restore", "source:sleep", "--name", "source"}
	if _, err := prepareCapacity(t, m, args, "source"); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal(err)
	}
	guests = guests[:1]
	token, err := prepareCapacity(t, m, args, "source")
	if err != nil {
		t.Fatal(err)
	}
	// Removing the old stopped guest is permitted only after capacity is held.
	guests = nil
	if err := m.dispatch(t.Context(), token, args); err != nil {
		t.Fatal(err)
	}
	if err := (&Client{capacity: m}).CancelAdmission(t.Context(), token); err == nil {
		t.Fatal("dispatched restore permit was cancelled")
	}
	guests = []capacityGuest{{ID: "restored", Name: "source", Status: "running", MemoryMiB: 512}}
	if err := m.finish(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	status, err := (&Client{capacity: m}).Capacity(t.Context())
	if err != nil || status.MemoryMiB != 512 || status.PendingStarts != 0 {
		t.Fatalf("completion accounting: %+v %v", status, err)
	}
}

func TestCapacityPermitBindsArgumentsAndSnapshotIdentity(t *testing.T) {
	guests := []capacityGuest{{ID: "source-id", Name: "source", Status: "stopped", MemoryMiB: 512}}
	m := fakeCapacity(t.TempDir(), CapacityLimits{MemoryMiB: 1024}, &guests)
	args := []string{"snap", "restore", "source:sleep", "--name", "next"}
	token, err := prepareCapacity(t, m, args, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.dispatch(t.Context(), token, launchArgs("next", 4096)); err == nil {
		t.Fatal("permit reused for a different launch")
	}
	m.snapshot = func(context.Context, string) (capacitySnapshot, error) {
		return capacitySnapshot{ID: "source:sleep", Digest: "replacement", MemoryMiB: 512}, nil
	}
	if err := m.dispatch(t.Context(), token, args); err == nil {
		t.Fatal("replacement checkpoint accepted")
	}
}

func TestCapacityUnknownDispatchSurvivesRestartAndCannotExpire(t *testing.T) {
	var guests []capacityGuest
	home := t.TempDir()
	m := fakeCapacity(home, CapacityLimits{MemoryMiB: 512, Starts: 1}, &guests)
	args := launchArgs("uncertain", 512)
	token, err := prepareCapacity(t, m, args, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.dispatch(t.Context(), token, args); err != nil {
		t.Fatal(err)
	}
	restarted := fakeCapacity(home, CapacityLimits{}, &guests)
	if _, err := prepareCapacity(t, restarted, launchArgs("replacement", 512), ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("unknown operation released after restart", err)
	}
}

func TestCapacityLoweringLimitsRetainsExistingGuestsAndRejectsUnknownGeometry(t *testing.T) {
	guests := []capacityGuest{{ID: "live", Name: "live", Status: "running", MemoryMiB: 1024}}
	m := fakeCapacity(t.TempDir(), CapacityLimits{MemoryMiB: 2048}, &guests)
	c := &Client{capacity: m}
	if err := c.SetCapacityLimits(t.Context(), CapacityLimits{MemoryMiB: 512}); err != nil {
		t.Fatal(err)
	}
	if _, err := prepareCapacity(t, m, launchArgs("small", 128), ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal(err)
	}
	guests[0].MemoryMiB = 0
	if _, err := prepareCapacity(t, m, launchArgs("small", 128), ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("unknown existing geometry bypassed capacity", err)
	}
}

func TestCapacityCompetingProcessesAdmitOnlyOneLastSlot(t *testing.T) {
	if home := os.Getenv("FLOCI_TEST_CAPACITY_HOME"); home != "" {
		var guests []capacityGuest
		m := fakeCapacity(home, CapacityLimits{MemoryMiB: 512, Starts: 1}, &guests)
		_, err := prepareCapacity(t, m, launchArgs("process-"+strconv.Itoa(os.Getpid()), 512), "")
		if errors.Is(err, ErrCapacityUnavailable) {
			os.Exit(12)
		}
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	home := t.TempDir()
	alias := filepath.Join(t.TempDir(), "same-home")
	if err := os.Symlink(home, alias); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for _, directory := range []string{home, alias} {
		wg.Go(func() {
			cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestCapacityCompetingProcessesAdmitOnlyOneLastSlot$")
			cmd.Env = append(os.Environ(), "FLOCI_TEST_CAPACITY_HOME="+directory)
			output, err := cmd.CombinedOutput()
			if err == nil {
				codes <- 0
			} else if exit, ok := errors.AsType[*exec.ExitError](err); ok && exit.ExitCode() == 12 {
				codes <- 12
			} else {
				t.Errorf("capacity child: %s %v", output, err)
				codes <- -1
			}
		})
	}
	wg.Wait()
	a, b := <-codes, <-codes
	if a+b != 12 || a == b {
		t.Fatalf("competing admission results %d, %d", a, b)
	}
}
