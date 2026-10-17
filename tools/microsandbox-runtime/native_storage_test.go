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

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func TestNativeStorageCleanupPreservesRAMAndImageDependencies(t *testing.T) {
	config := Config{Executable: os.Getenv("FLOCI_NATIVE_TEST_MSB"), Home: os.Getenv("FLOCI_NATIVE_TEST_HOME")}
	image := os.Getenv("FLOCI_NATIVE_TEST_IMAGE")
	if config.Executable == "" || config.Home == "" || image == "" {
		t.Fatal("requires explicit executable, dedicated native home and cached image")
	}
	client, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	guests, err := client.capacity.inventory(ctx)
	if err != nil || len(guests) != 0 {
		t.Fatal("requires empty dedicated VM inventory", err)
	}
	images, err := client.storage.images(ctx)
	if err != nil {
		t.Fatal(err)
	}
	target, err := msb.Image.Get(ctx, image)
	if err != nil {
		t.Fatal(err)
	}
	request := StoragePruneRequest{Images: true}
	for _, cached := range images {
		if cached.reference != target.Reference() {
			request.RetainImages = append(request.RetainImages, cached.reference)
		}
	}
	name := "floci-storage-" + strings.ToLower(rand.Text()[:10])
	ownedID, checkpointID := "", ""
	clean := func(cleanup context.Context) error {
		guests, err := client.capacity.inventory(cleanup)
		if err != nil {
			return err
		}
		for _, guest := range guests {
			if guest.Name != name {
				continue
			}
			if ownedID == "" || guest.ID != ownedID {
				return errors.New("cannot prove VM ownership for cleanup")
			}
			if _, err := client.Execute(cleanup, []string{"rm", "--force", name}, nil); err != nil {
				return err
			}
		}
		if checkpointID != "" {
			if _, err := client.Execute(cleanup, []string{"snap", "rm", "--force", checkpointID}, nil); err != nil {
				return err
			}
			checkpointID = ""
		}
		return nil
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := clean(cleanup); err != nil {
			t.Error("native storage cleanup", err)
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
	identify := func() {
		t.Helper()
		guests, err := client.capacity.inventory(ctx)
		if err != nil || len(guests) != 1 || guests[0].Name != name {
			t.Fatal("owned VM identity unavailable", err)
		}
		ownedID = guests[0].ID
	}
	execute("run", "-d", "--name", name, "--cpus", "1", "--memory", "384M", "--root-disk", "flat:1G", "--entrypoint", "/bin/sh", image, "--", "-c", "exec sleep 2147483647")
	identify()
	execute("exec", name, "--", "sh", "-ec", "printf preserved >/dev/shm/storage-proof")
	execute("pause", name)
	execute("snap", "create", "sleep", "--sandbox", name, "--full", "--integrity")
	checkpoint, err := msb.Snapshot.Open(ctx, name+":sleep")
	if err != nil {
		t.Fatal(err)
	}
	checkpointID = checkpoint.ID()
	execute("stop", "--force", name)
	for _, preview := range []bool{true, false} {
		request.DryRun = preview
		report, err := client.PruneStorage(ctx, request)
		if err != nil || report.Memory == nil {
			t.Fatalf("storage cleanup: %+v %v", report, err)
		}
		for _, outcome := range report.Images {
			if outcome.Reference == target.Reference() && outcome.State != "candidate" && outcome.State != "in-use" {
				t.Fatalf("checkpoint image was not protected: %+v", outcome)
			}
		}
		execute("snap", "verify", checkpointID)
	}
	execute("rm", name)
	execute("snap", "restore", checkpointID, "--name", name)
	identify()
	if execute("exec", name, "--", "cat", "/dev/shm/storage-proof") != "preserved" {
		t.Fatal("storage cleanup lost checkpoint RAM")
	}
	if err := clean(ctx); err != nil {
		t.Fatal(err)
	}
	report, err := client.PruneStorage(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	removed := false
	for _, outcome := range report.Images {
		removed = removed || outcome.Reference == target.Reference() && outcome.State == "removed"
	}
	if !removed {
		t.Fatalf("unused fixture image was not removed after deleting its owners: %+v", report.Images)
	}
	status, err := client.Capacity(ctx)
	if err != nil || status.MemoryMiB != 0 || len(status.Reservations) != 0 {
		t.Fatalf("capacity leaked after cleanup: %+v %v", status, err)
	}
	t.Log("preview and applied cleanup retained stopped VM/checkpoint image dependencies; RAM restored; unused fixture image reclaimed after owned cleanup")
}
