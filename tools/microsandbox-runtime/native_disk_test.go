//go:build native

package microsandboxruntime

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// This qualification only reads SDK and cache inventory. It neither launches
// workloads nor enables limits in the shared capacity ledger.
func TestNativeDiskInventory(t *testing.T) {
	config := Config{Executable: os.Getenv("FLOCI_NATIVE_TEST_MSB"), Home: os.Getenv("FLOCI_NATIVE_TEST_HOME")}
	image := os.Getenv("FLOCI_NATIVE_TEST_IMAGE")
	if config.Executable == "" || config.Home == "" || image == "" {
		t.Fatal("native disk inventory requires FLOCI_NATIVE_TEST_MSB, FLOCI_NATIVE_TEST_HOME and a cached FLOCI_NATIVE_TEST_IMAGE")
	}
	m, err := newCapacityManager(config)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := m.image(t.Context(), image)
	if err != nil {
		t.Fatal(err)
	}
	guests, err := m.inventory(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	status, err := m.diskStatus(t.Context(), &capacityState{}, guests)
	if err != nil {
		t.Fatal(err)
	}
	if status.ObservedBytes == 0 || status.AvailableBytes == 0 || digest == "" {
		t.Fatalf("native disk inventory is incomplete: %+v", status)
	}
	t.Logf("native disk inventory: %+v; cached image %s", status, digest)
}

func TestNativeDiskAdmissionProtectsCaptureAndRestore(t *testing.T) {
	config := Config{Executable: os.Getenv("FLOCI_NATIVE_TEST_MSB"), Home: os.Getenv("FLOCI_NATIVE_TEST_HOME")}
	image := os.Getenv("FLOCI_NATIVE_TEST_IMAGE")
	if config.Executable == "" || config.Home == "" || image == "" {
		t.Fatal("native disk lifecycle requires explicit executable, empty dedicated home and cached image")
	}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	guests, err := client.capacity.inventory(ctx)
	if err != nil || len(guests) != 0 {
		t.Fatal("native disk lifecycle requires empty VM inventory", err)
	}
	previous, err := client.Capacity(ctx)
	if err != nil || len(previous.Reservations) != 0 {
		t.Fatal("native disk lifecycle requires reconciled reservations", err)
	}
	name := "floci-disk-" + strings.ToLower(rand.Text()[:10])
	snapshot, ownedID := "", ""
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		guests, err := client.capacity.inventory(cleanup)
		if err != nil {
			t.Error("cleanup inventory", err)
		} else {
			for _, guest := range guests {
				if guest.Name != name {
					continue
				}
				if ownedID == "" || guest.ID != ownedID {
					t.Error("refusing cleanup of unconfirmed VM identity")
					continue
				}
				if _, err := client.Execute(cleanup, []string{"rm", "--force", name}, nil); err != nil {
					t.Error("cleanup owned VM", err)
				}
			}
		}
		if snapshot != "" {
			if _, err := client.Execute(cleanup, []string{"snap", "rm", "--force", snapshot}, nil); err != nil {
				t.Error("cleanup owned checkpoint", err)
			}
		}
		status, err := client.Capacity(cleanup)
		if err != nil || status.Disk == nil || status.Disk.WritableBytes != 0 || len(status.Reservations) != 0 {
			t.Errorf("disk accounting did not remain usable after capture/delete cleanup: %+v %v", status, err)
		}
		if err := client.SetCapacityLimits(cleanup, previous.Limits); err != nil {
			t.Error("restore previous capacity limits", err)
		}
	})
	execute := func(args ...string) string {
		t.Helper()
		result, err := client.Execute(ctx, args, nil)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("native %s: %v (%v), exit %d", args[0], err, errors.Unwrap(err), result.ExitCode)
		}
		return result.Stdout
	}
	limits := func(disk uint64) {
		t.Helper()
		if err := client.SetCapacityLimits(ctx, CapacityLimits{DiskMiB: disk, MinFreeDiskMiB: 64}); err != nil {
			t.Fatal(err)
		}
	}
	launch := []string{"run", "-d", "--name", name, "--cpus", "1", "--memory", "384M", "--root-disk", "flat:1G",
		"--entrypoint", "/bin/sh", image, "--", "-c", "exec sleep 2147483647"}
	limits(1)
	if _, err := client.Execute(ctx, launch, nil); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("launch bypassed disk budget", err)
	}
	limits(32768)
	execute(launch...)
	guests, err = client.capacity.inventory(ctx)
	if err != nil || len(guests) != 1 || guests[0].Name != name {
		t.Fatal("launched VM identity unavailable", err)
	}
	ownedID = guests[0].ID
	execute("exec", name, "--", "sh", "-ec", "printf retained > /dev/shm/disk-proof")
	capture := []string{"snap", "create", "budget", "--sandbox", name, "--full", "--integrity"}
	limits(1)
	if _, err := client.PrepareAdmission(ctx, AdmissionRequest{Arguments: capture}); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("capture bypassed disk budget", err)
	}
	guests, err = client.capacity.inventory(ctx)
	if err != nil || len(guests) != 1 || guests[0].Status != "running" || guests[0].ID != ownedID {
		t.Fatal("capture refusal changed the running source", err)
	}
	limits(32768)
	permit, err := client.PrepareAdmission(ctx, AdmissionRequest{Arguments: capture})
	if err != nil {
		t.Fatal(err)
	}
	execute("pause", name)
	result, err := client.ExecuteAdmitted(ctx, capture, nil, permit)
	if err != nil || result.ExitCode != 0 {
		t.Fatal("admitted capture failed", err)
	}
	snapshot = name + ":budget"
	execute("snap", "verify", snapshot)
	execute("stop", "--force", name)
	restore := []string{"snap", "restore", snapshot, "--name", name}
	limits(1)
	if _, err := client.PrepareAdmission(ctx, AdmissionRequest{Arguments: restore, SourceName: name}); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("restore bypassed disk budget", err)
	}
	execute("snap", "verify", snapshot)
	limits(32768)
	permit, err = client.PrepareAdmission(ctx, AdmissionRequest{Arguments: restore, SourceName: name})
	if err != nil {
		t.Fatal(err)
	}
	execute("rm", name)
	limits(1)
	result, err = client.ExecuteAdmitted(ctx, restore, nil, permit)
	if err != nil || result.ExitCode != 0 {
		t.Fatal("prepared restore was revoked after source removal", err)
	}
	guests, err = client.capacity.inventory(ctx)
	if err != nil || len(guests) != 1 || guests[0].Name != name {
		t.Fatal("restored VM identity unavailable", err)
	}
	ownedID = guests[0].ID
	if execute("exec", name, "--", "cat", "/dev/shm/disk-proof") != "retained" {
		t.Fatal("RAM state was not retained")
	}
	status, err := client.Capacity(ctx)
	if err != nil || len(status.Reservations) != 0 || status.Disk == nil || status.Disk.WritableBytes == 0 {
		t.Fatalf("completed disk accounting: %+v %v", status, err)
	}
}
