package microsandboxruntime

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

type storageFixture struct {
	items       []cachedStorageImage
	removed     []string
	options     *msb.StoragePruneOptions
	removeError map[string]error
	usageReport StorageReport
}

func (s *storageFixture) usage(context.Context) (StorageReport, error)         { return s.usageReport, nil }
func (s *storageFixture) images(context.Context) ([]cachedStorageImage, error) { return s.items, nil }
func (s *storageFixture) removeImage(_ context.Context, image cachedStorageImage) error {
	s.removed = append(s.removed, image.reference)
	return s.removeError[image.reference]
}
func (s *storageFixture) pruneMemory(_ context.Context, options msb.StoragePruneOptions) (*msb.StoragePruneReport, error) {
	s.options = &options
	return &msb.StoragePruneReport{DryRun: options.DryRun}, nil
}

func TestStorageMaintenanceDefaultsToPreviewAndStrictInput(t *testing.T) {
	request, err := DecodeStoragePruneRequest(strings.NewReader(`{}`))
	if err != nil || !request.DryRun || request.OlderThanSeconds != 86400 || request.Images {
		t.Fatalf("unsafe defaults: %+v %v", request, err)
	}
	for _, body := range []string{`null`, `[]`, `{} {}`, `{"force":true}`, `{"olderThanSeconds":-1}`, `{"images":"true"}`} {
		if _, err := DecodeStoragePruneRequest(strings.NewReader(body)); err == nil {
			t.Errorf("accepted %s", body)
		}
	}
	request, err = DecodeStoragePruneRequest(strings.NewReader(`{"dryRun":false,"olderThanSeconds":0,"images":true}`))
	if err != nil || request.DryRun || request.OlderThanSeconds != 0 || !request.Images {
		t.Fatalf("explicit application lost: %+v %v", request, err)
	}
}

func TestStorageCleanupPreservesAgePinsAndRuntimeOwnership(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour)
	store := &storageFixture{removeError: map[string]error{"busy": &msb.Error{Kind: msb.ErrImageInUse}, "changed": errImageChanged, "failed": errors.New("private host path")}}
	for _, name := range []string{"old", "busy", "changed", "failed", "pinned", "unknown", "recent"} {
		store.items = append(store.items, cachedStorageImage{reference: name, digest: "sha256:owned", created: old, used: old})
	}
	store.items[5].created, store.items[5].used = time.Time{}, time.Time{}
	store.items[6].used = time.Now()
	client := &Client{storage: store, capacity: &capacityManager{home: t.TempDir()}}
	request := StoragePruneRequest{DryRun: true, Images: true, OlderThanSeconds: 86400, RetainImages: []string{"pinned"}}
	report, err := client.PruneStorage(t.Context(), request)
	if err != nil || len(store.removed) != 0 || store.options == nil || !store.options.DryRun {
		t.Fatalf("preview mutated cache: %+v %v", report, err)
	}
	request.DryRun = false
	report, err = client.PruneStorage(t.Context(), request)
	if err != nil || !slices.Equal(store.removed, []string{"busy", "changed", "failed", "old"}) || store.options.DryRun || store.options.OlderThanSeconds != 86400 {
		t.Fatalf("unsafe removal set: %+v %+v %v", store.removed, report, err)
	}
	want := map[string]string{"old": "removed", "busy": "in-use", "changed": "changed", "failed": "failed", "pinned": "retained", "unknown": "unknown", "recent": "recent"}
	for _, image := range report.Images {
		if image.State != want[image.Reference] {
			t.Errorf("wrong outcome: %+v", image)
		}
	}
}

func TestImageCleanupCannotPassPreparedCapacityReservation(t *testing.T) {
	store := &storageFixture{}
	manager := &capacityManager{home: t.TempDir()}
	client := &Client{storage: store, capacity: manager}
	if err := manager.transaction(t.Context(), func(state *capacityState) error {
		state.Permits["prepared-wake"] = capacityPermit{Operation: "run", ImageRef: "private:image"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PruneStorage(t.Context(), StoragePruneRequest{Images: true}); err == nil || store.options != nil || len(store.removed) != 0 {
		t.Fatal("cleanup bypassed prepared admission")
	}
	// Runtime memory pruning uses SDK ownership locks and may proceed without
	// evicting the preload required by a waiting launch.
	if _, err := client.PruneStorage(t.Context(), StoragePruneRequest{}); err != nil || store.options == nil {
		t.Fatalf("safe memory cleanup blocked: %v", err)
	}
}

func TestStorageInventoryPreservesUnknownInsteadOfZero(t *testing.T) {
	client := &Client{storage: &storageFixture{usageReport: StorageReport{Usage: &msb.StorageUsageReport{}, AvailableBytes: 1234}}}
	report, err := client.Storage(t.Context())
	if err != nil || report.Usage.Snapshots.LogicalBytes != nil || report.AvailableBytes != 1234 {
		t.Fatalf("unknown inventory changed: %+v %v", report, err)
	}
	client.storage = &storageFixture{}
	if _, err := client.Storage(t.Context()); err == nil {
		t.Fatal("missing inventory returned as empty")
	}
}
