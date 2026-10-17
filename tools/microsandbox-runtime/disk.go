package microsandboxruntime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	msb "github.com/superradcompany/microsandbox/sdk/go"
	"golang.org/x/sys/unix"
)

const (
	mib        = uint64(1 << 20)
	maxDiskMiB = ^uint64(0) / mib
)

// DiskCapacityStatus reports conservative admission accounting, not an exclusive
// physical allocation or a filesystem quota. Shared/reflink bytes receive no credit.
type DiskCapacityStatus struct {
	ObservedBytes  uint64 `json:"observedBytes"`
	WritableBytes  uint64 `json:"writableBytes"`
	ReservedBytes  uint64 `json:"reservedBytes"`
	CommittedBytes uint64 `json:"committedBytes"`
	AvailableBytes uint64 `json:"availableBytes"`
}

type diskObservation struct {
	Usage          *msb.StorageUsageReport
	AvailableBytes uint64
}

func diskBudgetEnabled(limits CapacityLimits) bool {
	return limits.DiskMiB != 0 || limits.MinFreeDiskMiB != 0
}

func observeNativeDisk(ctx context.Context, home string) (diskObservation, error) {
	report, err := msb.StorageUsage(ctx)
	if err != nil {
		return diskObservation{}, err
	}
	var stat unix.Statfs_t
	if err := unix.Statfs(home, &stat); err != nil {
		return diskObservation{}, err
	}
	if stat.Bsize <= 0 || uint64(stat.Bavail) > ^uint64(0)/uint64(stat.Bsize) {
		return diskObservation{}, errors.New("native filesystem space is unavailable")
	}
	return diskObservation{Usage: report, AvailableBytes: uint64(stat.Bavail) * uint64(stat.Bsize)}, nil
}

func cachedImageGeometry(ctx context.Context, home, reference string) (string, error) {
	image, err := msb.Image.Inspect(ctx, reference)
	if err != nil {
		return "", errors.Join(errors.New("disk admission requires a preloaded image"), err)
	}
	if image == nil || image.ImageHandle == nil || image.ManifestDigest() == "" || len(image.Layers) == 0 {
		return "", errors.New("disk admission requires a cached image with known layers")
	}
	if err := verifyCachedImageFiles(home, image.ManifestDigest(), image.Layers); err != nil {
		return "", errors.Join(errors.New("disk admission requires preloaded, converted image files"), err)
	}
	return image.ManifestDigest(), nil
}

// SDK 0.7.8 stores nil erofs_size_bytes even for materialized image layers.
// Its pinned GlobalCache uses cache/layers/<DiffID-safe>.erofs and
// cache/fsmeta/<manifest-safe>.erofs. Stat only those immutable SDK identities;
// never follow symlinks, enumerate unrelated files, pull or convert an image.
func verifyCachedImageFiles(home, manifest string, layers []msb.ImageLayer) error {
	homeFD, err := unix.Open(home, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(homeFD)
	cacheFD, err := unix.Openat(homeFD, "cache", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(cacheFD)
	check := func(directory, digest string) error {
		hexDigest, ok := strings.CutPrefix(digest, "sha256:")
		if !ok || len(hexDigest) != 64 || strings.ToLower(hexDigest) != hexDigest {
			return errors.New("cached image digest is invalid")
		}
		if _, err := hex.DecodeString(hexDigest); err != nil {
			return errors.New("cached image digest is invalid")
		}
		dirFD, err := unix.Openat(cacheFD, directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return err
		}
		defer unix.Close(dirFD)
		var stat unix.Stat_t
		if err := unix.Fstatat(dirFD, "sha256_"+hexDigest+".erofs", &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size <= 0 {
			return errors.New("cached image artifact is not a nonempty regular file")
		}
		return nil
	}
	if err := check("fsmeta", manifest); err != nil {
		return err
	}
	for _, layer := range layers {
		if layer.CompressedSizeBytes == nil || *layer.CompressedSizeBytes < 0 {
			return errors.New("cached layer size is unknown")
		}
		if err := check("layers", layer.DiffID); err != nil {
			return err
		}
	}
	return nil
}

func rootDiskGeometry(config msb.SandboxConfig) uint64 {
	if config.RootDisk != nil {
		if config.RootDisk.Kind() == msb.RootDiskKindManaged || config.RootDisk.Kind() == msb.RootDiskKindFlat {
			return uint64(config.RootDisk.SizeMiB)
		}
		return 0
	}
	return uint64(config.OCIUpperSizeMiB)
}

func addDisk(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if value > ^uint64(0)-total {
			return 0, errors.New("native disk accounting overflow")
		}
		total += value
	}
	return total, nil
}

// Charge the complete writable geometry, two RAM backings and fixed capture
// metadata headroom. The second RAM backing covers copy/materialization work.
func diskGrowth(root, memory uint64) (uint64, error) {
	if root == 0 || memory == 0 || root > maxDiskMiB || memory > maxDiskMiB {
		return 0, errors.New("native writable geometry is unknown; recreate the checkpoint with disk accounting enabled")
	}
	return addDisk(root*mib, memory*mib, memory*mib, 64*mib)
}

func (m *capacityManager) diskStatus(ctx context.Context, state *capacityState, guests []capacityGuest) (*DiskCapacityStatus, error) {
	observed, err := m.storage(ctx)
	if err != nil {
		return nil, err
	}
	if observed.Usage == nil {
		return nil, errors.New("native storage inventory unavailable")
	}
	status := &DiskCapacityStatus{AvailableBytes: observed.AvailableBytes}
	for _, category := range []msb.StorageCategoryUsage{observed.Usage.Images, observed.Usage.Snapshots,
		observed.Usage.Sandboxes, observed.Usage.Volumes, observed.Usage.BranchMemory, observed.Usage.SnapshotMemory} {
		if category.LogicalBytes == nil {
			return nil, errors.New("native storage inventory contains unknown sizes")
		}
		size := *category.LogicalBytes
		if category.AllocatedBytes != nil {
			size = max(size, *category.AllocatedBytes)
		}
		status.ObservedBytes, err = addDisk(status.ObservedBytes, size)
		if err != nil {
			return nil, err
		}
	}
	for _, guest := range guests {
		if guest.Status == "stopped" {
			continue
		}
		growth, err := diskGrowth(guest.RootDiskMiB, guest.MemoryMiB)
		if err != nil {
			return nil, err
		}
		status.WritableBytes, err = addDisk(status.WritableBytes, growth)
		if err != nil {
			return nil, err
		}
	}
	for _, permit := range state.Permits {
		if permit.DiskBytes == 0 {
			return nil, errors.New("existing native reservation has no disk geometry; reconcile it before enabling disk admission")
		}
		status.ReservedBytes, err = addDisk(status.ReservedBytes, permit.DiskBytes)
		if err != nil {
			return nil, err
		}
	}
	status.CommittedBytes, err = addDisk(status.ObservedBytes, status.WritableBytes, status.ReservedBytes)
	return status, err
}

func (m *capacityManager) reserveDisk(ctx context.Context, state *capacityState, guests []capacityGuest, request command, permit *capacityPermit) error {
	if !diskBudgetEnabled(state.Limits) {
		return nil
	}
	growth, err := diskGrowth(permit.RootDiskMiB, permit.MemoryMiB)
	if err != nil {
		return err
	}
	permit.DiskBytes = growth
	if request.op != "snap create" {
		// Charge the operation's private files and its future writable allowance.
		permit.DiskBytes, err = addDisk(growth, growth)
		if err != nil {
			return err
		}
	}
	if request.op == "run" {
		permit.ImageRef = request.image
		permit.ImageDigest, err = m.image(ctx, request.image)
		if err != nil {
			return err
		}
		if request.config.RootDisk != nil && request.config.RootDisk.Kind() == msb.RootDiskKindFlat {
			// The SDK may create a shared flat base before cloning the private disk.
			permit.DiskBytes, err = addDisk(permit.DiskBytes, permit.RootDiskMiB*mib)
			if err != nil {
				return err
			}
		}
	}
	status, err := m.diskStatus(ctx, state, guests)
	if err != nil {
		return err
	}
	committed, err := addDisk(status.CommittedBytes, permit.DiskBytes)
	if err != nil {
		return err
	}
	if state.Limits.DiskMiB != 0 && committed > state.Limits.DiskMiB*mib {
		return errors.New("native disk admission budget reached")
	}
	needed, err := addDisk(state.Limits.MinFreeDiskMiB*mib, status.WritableBytes, status.ReservedBytes, permit.DiskBytes)
	if err != nil {
		return err
	}
	if needed > status.AvailableBytes {
		return errors.New("native disk admission would consume reserved filesystem free space")
	}
	return nil
}

func (m *capacityManager) revalidateDisk(ctx context.Context, state *capacityState, _ []capacityGuest, permit capacityPermit) error {
	if !diskBudgetEnabled(state.Limits) {
		return nil
	}
	if permit.DiskBytes == 0 {
		return errors.New("native permit predates disk admission")
	}
	if permit.ImageRef != "" {
		digest, err := m.image(ctx, permit.ImageRef)
		if err != nil || digest != permit.ImageDigest {
			return errors.Join(errors.New("native cached image changed after admission"), err)
		}
	}
	// Existing reservations survive lowering limits. Rechecking available space
	// here would revoke a wake after its stopped source has already been removed.
	return nil
}

func (m *capacityManager) prepareCapture(ctx context.Context, request command, args []string) (string, error) {
	token := rand.Text()
	err := m.transaction(ctx, func(state *capacityState) error {
		guests, err := m.inventory(ctx)
		if err != nil {
			return err
		}
		permit := capacityPermit{Hash: argumentsHash(args), Name: request.source + ":" + request.selector,
			Operation: request.op, SourceName: request.source, Full: request.full}
		for _, guest := range guests {
			if guest.Name == request.source {
				if request.full && guest.Status != "running" && guest.Status != "paused" {
					return errors.New("full capture requires a running or paused source")
				}
				permit.SourceID, permit.MemoryMiB, permit.RootDiskMiB = guest.ID, guest.MemoryMiB, guest.RootDiskMiB
			}
		}
		if permit.SourceID == "" {
			return errors.New("native capture source identity unavailable")
		}
		for _, pending := range state.Permits {
			if pending.Name == permit.Name || pending.Name == permit.SourceName || pending.SourceID == permit.SourceID {
				return errors.New("native capture source has an unresolved reservation")
			}
		}
		if len(state.Permits) >= 10000 {
			return errors.New("native reservation ledger requires reconciliation")
		}
		if err := m.reserveDisk(ctx, state, guests, request, &permit); err != nil {
			return fmt.Errorf("checkpoint admission: %w", err)
		}
		state.Permits[token] = permit
		return nil
	})
	if err != nil {
		return "", errors.Join(ErrCapacityUnavailable, err)
	}
	return token, nil
}
