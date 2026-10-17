package microsandboxruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func diskReport(bytes uint64) *msb.StorageUsageReport {
	empty := msb.StorageCategoryUsage{LogicalBytes: new(uint64(0))}
	return &msb.StorageUsageReport{Images: msb.StorageCategoryUsage{LogicalBytes: new(bytes)},
		Snapshots: empty, Sandboxes: empty, Volumes: empty, BranchMemory: empty, SnapshotMemory: empty}
}

func diskCapacity(home string, limits CapacityLimits, guests *[]capacityGuest) *capacityManager {
	m := fakeCapacity(home, limits, guests)
	m.storage = func(context.Context) (diskObservation, error) {
		return diskObservation{Usage: diskReport(128 * mib), AvailableBytes: 100000 * mib}, nil
	}
	m.image = func(context.Context, string) (string, error) { return "cached-digest", nil }
	m.snapshot = func(_ context.Context, selector string) (capacitySnapshot, error) {
		return capacitySnapshot{ID: selector, Digest: "immutable", MemoryMiB: 128, RootDiskMiB: 256}, nil
	}
	return m
}

func diskLaunch(name string) []string {
	return []string{"run", "-d", "--name", name, "--memory", "128M", "--root-disk", "256M", "alpine:3.22"}
}

func captureArgs(name string) []string {
	return []string{"snap", "create", "sleep", "--sandbox", name, "--full", "--integrity"}
}

func TestDiskAdmissionCountsEverySDKCategoryAndWritablePotential(t *testing.T) {
	guests := []capacityGuest{{ID: "running", Name: "running", Status: "paused", RootDiskMiB: 256, MemoryMiB: 128},
		{ID: "cold", Status: "stopped", RootDiskMiB: 1024, MemoryMiB: 2048}}
	m := diskCapacity(t.TempDir(), CapacityLimits{DiskMiB: 4096}, &guests)
	m.storage = func(context.Context) (diskObservation, error) {
		report := diskReport(10 * mib)
		report.Volumes.LogicalBytes = new(20 * mib)
		report.Snapshots.LogicalBytes = new(30 * mib)
		report.Sandboxes.AllocatedBytes = new(40 * mib)
		report.BranchMemory.LogicalBytes = new(50 * mib)
		report.SnapshotMemory.LogicalBytes = new(60 * mib)
		return diskObservation{Usage: report, AvailableBytes: 10000 * mib}, nil
	}
	status, err := (&Client{capacity: m}).Capacity(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if status.Disk.ObservedBytes != 210*mib || status.Disk.WritableBytes != 576*mib || status.Disk.CommittedBytes != 786*mib {
		t.Fatalf("accounting: %+v", status.Disk)
	}
}

func TestDiskReservationsSerializeAcrossIndependentManagers(t *testing.T) {
	var guests []capacityGuest
	home := t.TempDir()
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, name := range []string{"pod", "database"} {
		group.Go(func() {
			m := diskCapacity(home, CapacityLimits{DiskMiB: 1280}, &guests)
			request, err := parse(diskLaunch(name))
			if err == nil {
				_, err = m.prepare(t.Context(), request, diskLaunch(name), "")
			}
			results <- err
		})
	}
	group.Wait()
	a, b := <-results, <-results
	if (a == nil) == (b == nil) || a != nil && !errors.Is(a, ErrCapacityUnavailable) || b != nil && !errors.Is(b, ErrCapacityUnavailable) {
		t.Fatalf("competing disk admissions: %v, %v", a, b)
	}
}

func TestDiskFreeFloorAndCachedImagePreflight(t *testing.T) {
	for _, reason := range []string{"free floor", "uncached image", "unknown size", "unknown root", "overflow"} {
		t.Run(reason, func(t *testing.T) {
			var guests []capacityGuest
			m := diskCapacity(t.TempDir(), CapacityLimits{MinFreeDiskMiB: 1024}, &guests)
			args := diskLaunch("pod")
			switch reason {
			case "free floor":
				m.storage = func(context.Context) (diskObservation, error) {
					return diskObservation{Usage: diskReport(0), AvailableBytes: 2048 * mib}, nil
				}
			case "uncached image":
				m.image = func(context.Context, string) (string, error) { return "", errors.New("preload required") }
			case "unknown size":
				m.storage = func(context.Context) (diskObservation, error) {
					return diskObservation{Usage: &msb.StorageUsageReport{}}, nil
				}
			case "unknown root":
				args = launchArgs("pod", 128)
			case "overflow":
				m.storage = func(context.Context) (diskObservation, error) {
					report := diskReport(^uint64(0))
					report.Volumes.LogicalBytes = new(uint64(1))
					return diskObservation{Usage: report}, nil
				}
			}
			if _, err := prepareCapacity(t, m, args, ""); !errors.Is(err, ErrCapacityUnavailable) {
				t.Fatalf("unsafe growth admitted: %v", err)
			}
		})
	}
}

func TestDiskCaptureReservesBeforePauseWithoutStartOrMemoryAdmission(t *testing.T) {
	guests := []capacityGuest{{ID: "source-id", Name: "source", Status: "running", MemoryMiB: 128, RootDiskMiB: 256}}
	m := diskCapacity(t.TempDir(), CapacityLimits{DiskMiB: 1280, MemoryMiB: 128, Starts: 1}, &guests)
	args := captureArgs("source")
	token, err := prepareCapacity(t, m, args, "")
	if err != nil {
		t.Fatal(err)
	}
	status, err := (&Client{capacity: m}).Capacity(t.Context())
	if err != nil || status.PendingStarts != 0 || status.MemoryMiB != 128 || status.Disk.ReservedBytes != 576*mib {
		t.Fatalf("capture accounting: %+v %v", status, err)
	}
	guests[0].Status = "paused"
	if err := m.dispatch(t.Context(), token, args); err != nil {
		t.Fatal(err)
	}
	if err := (&Client{capacity: m}).CancelAdmission(t.Context(), token); err == nil {
		t.Fatal("uncertain capture released its reservation")
	}
	if err := m.finish(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	status, err = (&Client{capacity: m}).Capacity(t.Context())
	if err != nil || status.Disk.ReservedBytes != 0 {
		t.Fatalf("capture reservation did not reconcile: %+v %v", status, err)
	}
}

func TestDiskCaptureRefusalAndChangedSourcePreserveWorkload(t *testing.T) {
	guests := []capacityGuest{{ID: "source-id", Name: "source", Status: "running", MemoryMiB: 128, RootDiskMiB: 256}}
	m := diskCapacity(t.TempDir(), CapacityLimits{DiskMiB: 1000}, &guests)
	if _, err := prepareCapacity(t, m, captureArgs("source"), ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("capture exceeded budget", err)
	}
	if guests[0].Status != "running" {
		t.Fatal("refusal mutated the source")
	}
	if err := (&Client{capacity: m}).SetCapacityLimits(t.Context(), CapacityLimits{DiskMiB: 4096}); err != nil {
		t.Fatal(err)
	}
	token, err := prepareCapacity(t, m, captureArgs("source"), "")
	if err != nil {
		t.Fatal(err)
	}
	guests[0].RootDiskMiB++
	if err := m.dispatch(t.Context(), token, captureArgs("source")); err == nil {
		t.Fatal("changed capture geometry used old reservation")
	}
}

func TestDiskRestoreRetainsReservationAfterSourceRemovalAndLimitReduction(t *testing.T) {
	guests := []capacityGuest{{ID: "source-id", Name: "source", Status: "stopped", MemoryMiB: 128, RootDiskMiB: 256}}
	home := t.TempDir()
	m := diskCapacity(home, CapacityLimits{DiskMiB: 1280}, &guests)
	args := []string{"snap", "restore", "source:sleep", "--name", "source"}
	token, err := prepareCapacity(t, m, args, "source")
	if err != nil {
		t.Fatal(err)
	}
	guests = nil
	restarted := diskCapacity(home, CapacityLimits{}, &guests)
	if err := (&Client{capacity: restarted}).SetCapacityLimits(t.Context(), CapacityLimits{DiskMiB: 64}); err != nil {
		t.Fatal(err)
	}
	if err := restarted.dispatch(t.Context(), token, args); err != nil {
		t.Fatal("prepared wake was revoked after source removal", err)
	}
	if _, err := prepareCapacity(t, restarted, diskLaunch("other"), ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("lower limit ignored by new work", err)
	}
}

func TestDiskAdmissionRejectsLegacyCheckpointGeometry(t *testing.T) {
	var guests []capacityGuest
	m := diskCapacity(t.TempDir(), CapacityLimits{DiskMiB: 4096}, &guests)
	m.snapshot = func(context.Context, string) (capacitySnapshot, error) {
		return capacitySnapshot{ID: "saved", Digest: "digest", MemoryMiB: 128}, nil
	}
	_, err := prepareCapacity(t, m, []string{"snap", "restore", "saved:sleep", "--name", "restored"}, "")
	if !errors.Is(err, ErrCapacityUnavailable) || !strings.Contains(err.Error(), "geometry") {
		t.Fatalf("legacy checkpoint accepted: %v", err)
	}
}

func TestDiskUnlimitedSkipsStorageAndImageInventory(t *testing.T) {
	var guests []capacityGuest
	m := diskCapacity(t.TempDir(), CapacityLimits{}, &guests)
	m.storage = func(context.Context) (diskObservation, error) {
		t.Fatal("unlimited capacity read storage")
		return diskObservation{}, nil
	}
	m.image = func(context.Context, string) (string, error) {
		t.Fatal("unlimited capacity inspected image")
		return "", nil
	}
	args := launchArgs("pod", 128)
	token, err := prepareCapacity(t, m, args, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.dispatch(t.Context(), token, args); err != nil {
		t.Fatal(err)
	}
	if status, err := (&Client{capacity: m}).Capacity(t.Context()); err != nil || status.Disk != nil {
		t.Fatalf("unlimited status: %+v %v", status, err)
	}
}

func TestDiskAdmissionDetectsChangedCachedImage(t *testing.T) {
	var guests []capacityGuest
	m := diskCapacity(t.TempDir(), CapacityLimits{DiskMiB: 4096}, &guests)
	args := diskLaunch("pod")
	token, err := prepareCapacity(t, m, args, "")
	if err != nil {
		t.Fatal(err)
	}
	m.image = func(context.Context, string) (string, error) { return "different-digest", nil }
	if err := m.dispatch(t.Context(), token, args); err == nil {
		t.Fatal("replacement image bypassed cached-image admission")
	}
}

func TestDiskFlatRootReservesUncachedBaseWithoutReflinkCredit(t *testing.T) {
	var guests []capacityGuest
	m := diskCapacity(t.TempDir(), CapacityLimits{DiskMiB: 1280}, &guests)
	args := diskLaunch("pod")
	args[7] = "flat:256M"
	if _, err := prepareCapacity(t, m, args, ""); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("flat base growth was not reserved", err)
	}
}

func TestCachedImageFilesRejectUnmeasurableAndRedirectedArtifacts(t *testing.T) {
	for _, kind := range []string{"valid", "missing", "empty", "malformed", "symlink file", "symlink directory", "symlink cache"} {
		t.Run(kind, func(t *testing.T) {
			home := t.TempDir()
			manifest, layer := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
			for directory, digest := range map[string]string{"fsmeta": manifest, "layers": layer} {
				path := filepath.Join(home, "cache", directory)
				if err := os.MkdirAll(path, 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(path, strings.ReplaceAll(digest, ":", "_")+".erofs"), make([]byte, 4096), 0600); err != nil {
					t.Fatal(err)
				}
			}
			layerPath := filepath.Join(home, "cache", "layers", strings.ReplaceAll(layer, ":", "_")+".erofs")
			switch kind {
			case "missing":
				if err := os.Remove(layerPath); err != nil {
					t.Fatal(err)
				}
			case "empty":
				if err := os.Truncate(layerPath, 0); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				layer = "sha256:../../unowned"
			case "symlink file", "symlink directory", "symlink cache":
				path := layerPath
				if kind == "symlink directory" {
					path = filepath.Dir(path)
				}
				if kind == "symlink cache" {
					path = filepath.Join(home, "cache")
				}
				moved := path + "-actual"
				if err := os.Rename(path, moved); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(moved, path); err != nil {
					t.Fatal(err)
				}
			}
			err := verifyCachedImageFiles(home, manifest, []msb.ImageLayer{{DiffID: layer, CompressedSizeBytes: new(int64(100))}})
			if (err == nil) != (kind == "valid") {
				t.Fatalf("%s artifact: %v", kind, err)
			}
		})
	}
}

func TestDiskConfigurationRejectsOverflowAndDifferentHelperLimits(t *testing.T) {
	if _, err := newCapacityManager(Config{DiskLimitMiB: maxDiskMiB + 1}); err == nil {
		t.Fatal("overflowing configuration accepted")
	}
	var guests []capacityGuest
	home := t.TempDir()
	a := diskCapacity(home, CapacityLimits{DiskMiB: 4096}, &guests)
	if _, err := (&Client{capacity: a}).Capacity(t.Context()); err != nil {
		t.Fatal(err)
	}
	b := diskCapacity(home, CapacityLimits{DiskMiB: 8192}, &guests)
	if _, err := (&Client{capacity: b}).Capacity(t.Context()); err == nil {
		t.Fatal("conflicting helpers accepted")
	}
}
