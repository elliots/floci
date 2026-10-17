//go:build native

package microsandboxruntime_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
	msb "github.com/superradcompany/microsandbox/sdk/go"
)

// This deliberately exercises the pinned SDK storage primitives before the Pod
// provider uses them. It requires an empty, dedicated home and creates real VMs.
func TestNativeNamedDiskFullRAMRestoreAndReplacement(t *testing.T) {
	config := runtime.Config{Executable: os.Getenv("FLOCI_NATIVE_TEST_MSB"), Home: os.Getenv("FLOCI_NATIVE_TEST_HOME")}
	if config.Executable == "" || config.Home == "" {
		t.Fatal("named-volume qualification requires an explicit executable and dedicated runtime home")
	}
	client, err := runtime.New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	execute := func(args ...string) string {
		t.Helper()
		result, err := client.Execute(ctx, args, nil)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("%s: %v (%v), exit=%d stderr=%s", args[0], err, errors.Unwrap(err), result.ExitCode, result.Stderr)
		}
		return strings.TrimSpace(result.Stdout)
	}
	var inventory []struct{ Name, ID string }
	if err := json.Unmarshal([]byte(execute("ls")), &inventory); err != nil || len(inventory) != 0 {
		t.Fatal("named-volume proof requires an empty dedicated VM inventory", err)
	}
	owner := strings.ToLower(rand.Text()[:12])
	vm, replacement, volume := "floci-pvc-"+owner, "floci-pvc-"+owner+"-new", "floci-pvc-"+owner+"-disk"
	labels := map[string]string{"floci.pvc-proof": owner}
	var created *runtime.NamedDiskIdentity
	attachment := runtime.NamedDiskAttachment{Owner: owner, PodUID: "first-pod", VMName: vm}
	var snapshot *msb.SnapshotArtifact
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		result, err := client.Execute(cleanup, []string{"ls"}, nil)
		if err != nil || json.Unmarshal([]byte(result.Stdout), &inventory) != nil {
			t.Error("cannot inspect volume-proof guests for cleanup", err)
			return
		}
		for _, guest := range inventory {
			if guest.Name != vm && guest.Name != replacement {
				continue
			}
			handle, err := msb.GetSandbox(cleanup, guest.Name)
			if err != nil || handle.ID() != guest.ID {
				t.Error("owned guest identity unavailable during cleanup", err)
				return
			}
			if err := handle.Destroy(cleanup, msb.WithDestroyForce()); err != nil {
				t.Error("cannot destroy owned volume-proof guest", err)
				return
			}
		}
		if snapshot != nil {
			if err := msb.Snapshot.Remove(cleanup, vm+":memory", true); err != nil {
				t.Error("cannot remove owned volume-proof checkpoint", err)
				return
			}
		}
		if created != nil {
			if err := client.ReleaseNamedDisk(cleanup, *created, attachment); err != nil {
				t.Error("cannot release owned disk attachment:", err)
				return
			}
			if err := client.RemoveNamedDisk(cleanup, *created); err != nil {
				t.Error("cannot remove owned volume-proof disk", err)
			}
		}
	})
	disk, err := client.CreateNamedDisk(ctx, runtime.NamedDiskRequest{Name: volume, CapacityMiB: 256, Labels: labels})
	if err != nil {
		t.Fatal("create ext4 volume:", err)
	}
	created = &disk
	if err := client.AttachNamedDisk(ctx, disk, attachment); err != nil {
		t.Fatal("attach owned disk:", err)
	}
	t.Logf("owned disk %s: path=%s capacity=%d inode=%d", volume, disk.Path, disk.CapacityMiB, disk.Inode)
	launch := func(name string) {
		t.Helper()
		execute("run", "-d", "--name", name,
			"--label", "floci.pvc-proof="+owner, "--cpus", "1", "--memory", "384M", "--root-disk", "flat:1G",
			"--mount-named", volume+":/persist:kind=disk,size=256M", "--entrypoint", "/bin/sh", "rancher/k3s:v1.35.0-k3s1", "--", "-ec", "exec sleep 2147483647")
	}
	launch(vm)
	identity := execute("exec", vm, "--", "sh", "-ec", "awk '$2 == \"/persist\" && $3 == \"ext4\" {found=1} END {exit !found}' /proc/mounts; "+
		"printf persistent > /persist/marker; printf ram-only > /dev/shm/pvc-marker; sync; cat /proc/sys/kernel/random/boot_id; awk '{print $22}' /proc/1/stat")
	execute("pause", vm)
	execute("snap", "create", "memory", "--sandbox", vm, "--full", "--integrity")
	snapshot, err = msb.Snapshot.Open(ctx, vm+":memory")
	if err != nil {
		t.Fatal("open named disk RAM checkpoint:", err)
	}
	execute("snap", "verify", vm+":memory")
	execute("stop", "--force", vm)
	if err := client.ReleaseNamedDisk(ctx, disk, attachment); err == nil {
		t.Fatal("sleeping VM released its persistent disk")
	}
	execute("rm", vm)
	if err := client.ReleaseNamedDisk(ctx, disk, attachment); err == nil {
		t.Fatal("restorable checkpoint released its persistent disk")
	}
	execute("snap", "restore", vm+":memory", "--name", vm, "--mount-named", volume+":/persist:kind=disk,size=256M")
	currentDisk, err := client.InspectNamedDisk(ctx, volume)
	if err != nil || !disk.Matches(currentDisk) {
		t.Fatal("named disk identity changed across full-RAM restore", err)
	}
	after := execute("exec", vm, "--", "sh", "-ec", "test \"$(cat /persist/marker)\" = persistent; test \"$(cat /dev/shm/pvc-marker)\" = ram-only; "+
		"printf changed-after-restore > /persist/marker; sync; cat /proc/sys/kernel/random/boot_id; awk '{print $22}' /proc/1/stat")
	if after != identity {
		t.Fatalf("RAM restore changed process identity: before=%q after=%q", identity, after)
	}
	t.Log("full-RAM restore preserved ext4 data, boot ID, PID1 start time and tmpfs marker")
	execute("stop", "--force", vm)
	execute("rm", vm)
	if err := msb.Snapshot.Remove(ctx, vm+":memory", true); err != nil {
		t.Fatal("remove restorable old Pod state:", err)
	}
	snapshot = nil
	if err := client.ReleaseNamedDisk(ctx, disk, attachment); err != nil {
		t.Fatal(err)
	}
	attachment = runtime.NamedDiskAttachment{Owner: owner, PodUID: "replacement-pod", VMName: replacement}
	if err := client.AttachNamedDisk(ctx, disk, attachment); err != nil {
		t.Fatal(err)
	}
	launch(replacement)
	replaced := execute("exec", replacement, "--", "sh", "-ec", "test \"$(cat /persist/marker)\" = changed-after-restore; "+
		"test ! -e /dev/shm/pvc-marker; cat /proc/sys/kernel/random/boot_id; awk '{print $22}' /proc/1/stat")
	if replaced == identity {
		t.Fatal("replacement unexpectedly reused the old VM process identity")
	}
	t.Log("replacement VM mounted the same durable disk after old VM and checkpoint deletion")
}
