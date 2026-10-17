package microsandboxruntime

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
	"golang.org/x/sys/unix"
)

var (
	ErrNamedDiskNotFound  = errors.New("named disk does not exist")
	ErrNamedDiskUncertain = errors.New("named disk operation has an uncertain outcome; reconcile its owned storage before retrying")
	ErrNamedDiskAttached  = errors.New("named disk is attached to another Pod; sleeping Pods retain their attachment until their VM and checkpoints are deleted")
	namedDiskName         = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
)

type NamedDiskRequest struct {
	Name        string            `json:"name"`
	CapacityMiB uint32            `json:"capacityMiB"`
	Labels      map[string]string `json:"labels"`
}

// Local SDK volumes have no immutable SDK ID. This identity pins the never-reused
// name, owner labels, creation metadata and actual raw disk inode instead.
type NamedDiskIdentity struct {
	Name        string            `json:"name"`
	Path        string            `json:"path"`
	CapacityMiB uint32            `json:"capacityMiB"`
	Labels      map[string]string `json:"labels"`
	CreatedAt   time.Time         `json:"createdAt"`
	Device      uint64            `json:"device"`
	Inode       uint64            `json:"inode"`
}

func (i NamedDiskIdentity) Matches(other NamedDiskIdentity) bool {
	return i.Name == other.Name && i.Path == other.Path && i.CapacityMiB == other.CapacityMiB &&
		i.CreatedAt.Equal(other.CreatedAt) && i.Device == other.Device && i.Inode == other.Inode && maps.Equal(i.Labels, other.Labels)
}

type namedDiskRecord struct {
	Version    int                  `json:"version"`
	Request    NamedDiskRequest     `json:"request"`
	Phase      string               `json:"phase"`
	Permit     string               `json:"permit,omitempty"`
	Identity   *NamedDiskIdentity   `json:"identity,omitempty"`
	Attachment *NamedDiskAttachment `json:"attachment,omitempty"`
}

type NamedDiskAttachment struct {
	Owner  string `json:"owner"`
	PodUID string `json:"podUID"`
	VMName string `json:"vmName"`
}

type namedDiskBackend interface {
	inspect(context.Context, string) (NamedDiskIdentity, error)
	create(context.Context, NamedDiskRequest) error
	remove(context.Context, string) error
	referenced(context.Context, string) (bool, error)
}

func validNamedDiskRequest(request NamedDiskRequest) error {
	if !namedDiskName.MatchString(request.Name) || request.CapacityMiB < 256 || len(request.Labels) == 0 || len(request.Labels) > 32 {
		return invalid("named disk requires a unique DNS label, at least 256MiB and owner labels")
	}
	for key, value := range request.Labels {
		if key == "" || value == "" || len(key)+len(value) > 1024 {
			return invalid("named disk owner label is invalid")
		}
	}
	return nil
}

func namedDiskRecordPath(home, name string) string {
	digest := sha256.Sum256([]byte(name))
	return filepath.Join(home, "floci-named-disks", hex.EncodeToString(digest[:])+".json")
}

func sameNamedDiskRequest(a, b NamedDiskRequest) bool {
	return a.Name == b.Name && a.CapacityMiB == b.CapacityMiB && maps.Equal(a.Labels, b.Labels)
}

func (c *Client) readNamedDisk(name string) (namedDiskRecord, error) {
	var record namedDiskRecord
	err := readCapacityRecord(namedDiskRecordPath(c.config.Home, name), &record)
	if err == nil && (record.Version != 1 || record.Request.Name != name || validNamedDiskRequest(record.Request) != nil) {
		return record, errors.New("named disk ownership record is invalid")
	}
	if err == nil {
		switch record.Phase {
		case "creating":
			if record.Permit == "" || record.Identity != nil {
				return record, errors.New("named disk creation intent is invalid")
			}
		case "ready", "deleting", "deleted":
			if record.Identity == nil || record.Identity.Name != name {
				return record, errors.New("named disk identity record is incomplete")
			}
		default:
			return record, errors.New("named disk ownership state is invalid")
		}
	}
	return record, err
}

// CreateNamedDisk is idempotent only for its recorded owner and exact geometry.
// A dispatched failure retains the disk reservation and ownership intent. A
// retry can reconcile a fully indexed disk, but never deletes partial artifacts
// or retries an absent/ambiguous disk after an uncertain SDK mutation.
func (c *Client) CreateNamedDisk(ctx context.Context, request NamedDiskRequest) (NamedDiskIdentity, error) {
	if err := validNamedDiskRequest(request); err != nil {
		return NamedDiskIdentity{}, err
	}
	if err := c.core.initialize(); err != nil {
		return NamedDiskIdentity{}, err
	}
	request.Labels = maps.Clone(request.Labels)
	var result NamedDiskIdentity
	var token string
	create := false
	err := c.capacity.transaction(ctx, func(state *capacityState) error {
		record, err := c.readNamedDisk(request.Name)
		if err == nil {
			if !sameNamedDiskRequest(record.Request, request) || record.Phase == "deleted" || record.Phase == "deleting" {
				return errors.New("named disk belongs to a different or retired allocation")
			}
			result, err = c.reconcileNamedDisk(ctx, state, &record)
			return err
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, err := c.disks.inspect(ctx, request.Name); !errors.Is(err, ErrNamedDiskNotFound) {
			return errors.Join(errors.New("refusing to adopt an unrecorded named disk"), err)
		}
		guests, err := c.capacity.inventory(ctx)
		if err != nil {
			return errors.Join(ErrCapacityUnavailable, err)
		}
		bytes := (uint64(request.CapacityMiB) + 16) * mib
		if err := c.capacity.reserveDiskBytes(ctx, state, guests, bytes); err != nil {
			return errors.Join(ErrCapacityUnavailable, err)
		}
		token = rand.Text()
		state.Permits[token] = capacityPermit{Operation: "volume create", Name: "volume:" + request.Name, DiskBytes: bytes}
		record = namedDiskRecord{Version: 1, Request: request, Phase: "creating", Permit: token}
		if err := writeNetworkRecord(namedDiskRecordPath(c.config.Home, request.Name), record); err != nil {
			return err
		}
		create = true
		return nil
	})
	if err != nil || !create {
		return result, err
	}
	if err := c.capacity.transaction(ctx, func(state *capacityState) error {
		permit, exists := state.Permits[token]
		if !exists || permit.Dispatched || permit.Name != "volume:"+request.Name {
			return ErrNamedDiskUncertain
		}
		permit.Dispatched = true
		state.Permits[token] = permit
		return nil
	}); err != nil {
		cleanupErr := c.capacity.transaction(context.WithoutCancel(ctx), func(state *capacityState) error {
			permit, exists := state.Permits[token]
			if exists && permit.Dispatched {
				return ErrNamedDiskUncertain
			}
			if err := os.Remove(namedDiskRecordPath(c.config.Home, request.Name)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			delete(state.Permits, token)
			return nil
		})
		if cleanupErr != nil {
			return NamedDiskIdentity{}, errors.Join(err, cleanupErr)
		}
		return NamedDiskIdentity{}, errors.Join(ErrCapacityUnavailable, err)
	}
	// An accepted native mutation must finish even when the Kubernetes API
	// request disconnects. Its durable intent covers process crashes as well.
	ctx = context.WithoutCancel(ctx)
	if err := c.disks.create(ctx, request); err != nil {
		return NamedDiskIdentity{}, errors.Join(ErrNamedDiskUncertain, err)
	}
	err = c.capacity.transaction(ctx, func(state *capacityState) error {
		record, err := c.readNamedDisk(request.Name)
		if err != nil {
			return err
		}
		result, err = c.reconcileNamedDisk(ctx, state, &record)
		return err
	})
	return result, err
}

func (c *Client) reconcileNamedDisk(ctx context.Context, state *capacityState, record *namedDiskRecord) (NamedDiskIdentity, error) {
	actual, err := c.disks.inspect(ctx, record.Request.Name)
	if err != nil {
		return actual, errors.Join(ErrNamedDiskUncertain, err)
	}
	if actual.CapacityMiB != record.Request.CapacityMiB || !maps.Equal(actual.Labels, record.Request.Labels) ||
		record.Identity != nil && !record.Identity.Matches(actual) {
		return NamedDiskIdentity{}, errors.New("named disk identity differs from its recorded owner or geometry")
	}
	if record.Permit != "" {
		permit, exists := state.Permits[record.Permit]
		if !exists && record.Phase != "ready" || exists && (!permit.Dispatched || permit.Name != "volume:"+record.Request.Name) {
			return NamedDiskIdentity{}, ErrNamedDiskUncertain
		}
		if exists && diskBudgetEnabled(state.Limits) {
			observed, err := c.capacity.storage(ctx)
			if err != nil || observed.Usage == nil || observed.Usage.Volumes.LogicalBytes == nil ||
				*observed.Usage.Volumes.LogicalBytes < uint64(actual.CapacityMiB)*mib {
				return NamedDiskIdentity{}, errors.Join(errors.New("new named disk is not accounted in native storage usage"), err)
			}
		}
		delete(state.Permits, record.Permit)
	}
	record.Identity, record.Phase = &actual, "ready"
	if err := writeNetworkRecord(namedDiskRecordPath(c.config.Home, record.Request.Name), record); err != nil {
		return NamedDiskIdentity{}, err
	}
	return actual, nil
}

func (c *Client) InspectNamedDisk(ctx context.Context, name string) (NamedDiskIdentity, error) {
	if !namedDiskName.MatchString(name) {
		return NamedDiskIdentity{}, invalid("invalid named disk name")
	}
	if err := c.core.initialize(); err != nil {
		return NamedDiskIdentity{}, err
	}
	return c.disks.inspect(ctx, name)
}

// RemoveNamedDisk requires the caller to release all guest/checkpoint attachment
// leases first. SDK reference checks add another fence. The tombstone prevents
// recreating the name and silently attaching an old Kubernetes allocation to it.
func (c *Client) RemoveNamedDisk(ctx context.Context, expected NamedDiskIdentity) error {
	if !namedDiskName.MatchString(expected.Name) {
		return invalid("invalid named disk name")
	}
	if err := c.core.initialize(); err != nil {
		return err
	}
	alreadyDeleted := false
	err := c.capacity.transaction(ctx, func(_ *capacityState) error {
		record, err := c.readNamedDisk(expected.Name)
		if err != nil || record.Identity == nil || !record.Identity.Matches(expected) {
			return errors.Join(errors.New("named disk removal identity differs"), err)
		}
		if record.Phase == "deleted" {
			alreadyDeleted = true
			return nil
		}
		if record.Attachment != nil {
			return ErrNamedDiskAttached
		}
		actual, err := c.disks.inspect(ctx, expected.Name)
		if errors.Is(err, ErrNamedDiskNotFound) && record.Phase == "deleting" {
			record.Phase = "deleted"
			alreadyDeleted = true
			return writeNetworkRecord(namedDiskRecordPath(c.config.Home, expected.Name), record)
		}
		if err != nil || !actual.Matches(expected) {
			return errors.Join(ErrNamedDiskUncertain, err)
		}
		record.Phase = "deleting"
		return writeNetworkRecord(namedDiskRecordPath(c.config.Home, expected.Name), record)
	})
	if err != nil || alreadyDeleted {
		return err
	}
	ctx = context.WithoutCancel(ctx)
	if err := c.disks.remove(ctx, expected.Name); err != nil {
		return errors.Join(ErrNamedDiskUncertain, err)
	}
	return c.capacity.transaction(ctx, func(_ *capacityState) error {
		if _, err := c.disks.inspect(ctx, expected.Name); !errors.Is(err, ErrNamedDiskNotFound) {
			return ErrNamedDiskUncertain
		}
		record, err := c.readNamedDisk(expected.Name)
		if err != nil {
			return err
		}
		record.Phase = "deleted"
		return writeNetworkRecord(namedDiskRecordPath(c.config.Home, expected.Name), record)
	})
}

func (c *Client) AttachNamedDisk(ctx context.Context, expected NamedDiskIdentity, attachment NamedDiskAttachment) error {
	if attachment.Owner == "" || attachment.PodUID == "" || !namedDiskName.MatchString(attachment.VMName) {
		return invalid("named disk attachment requires controller, Pod and VM identities")
	}
	return c.namedDiskAttachment(ctx, expected, attachment, false)
}

func (c *Client) ReleaseNamedDisk(ctx context.Context, expected NamedDiskIdentity, attachment NamedDiskAttachment) error {
	return c.namedDiskAttachment(ctx, expected, attachment, true)
}

func (c *Client) namedDiskAttachment(ctx context.Context, expected NamedDiskIdentity, attachment NamedDiskAttachment, release bool) error {
	if !namedDiskName.MatchString(expected.Name) {
		return invalid("invalid named disk name")
	}
	if err := c.core.initialize(); err != nil {
		return err
	}
	return c.capacity.transaction(ctx, func(state *capacityState) error {
		record, err := c.readNamedDisk(expected.Name)
		if err != nil || record.Identity == nil || !record.Identity.Matches(expected) {
			return errors.Join(errors.New("named disk attachment identity differs"), err)
		}
		// A Pod can journal its intent immediately before another controller
		// reclaims an unattached disk. Its later cleanup has no lease to release.
		if release && record.Attachment == nil && (record.Phase == "deleting" || record.Phase == "deleted") {
			return nil
		}
		if record.Phase != "ready" {
			return errors.New("named disk attachment is not ready")
		}
		actual, err := c.disks.inspect(ctx, expected.Name)
		if err != nil || !actual.Matches(expected) {
			return errors.Join(errors.New("named disk backing identity differs"), err)
		}
		if record.Attachment != nil && *record.Attachment != attachment {
			if release {
				// A rejected attachment still has a Pod-side intent to clean up.
				// Its cleanup must preserve the other Pod's exact live lease.
				return nil
			}
			return ErrNamedDiskAttached
		}
		if release {
			if record.Attachment == nil {
				return nil
			}
			for _, permit := range state.Permits {
				if permit.Name == attachment.VMName || permit.SourceName == attachment.VMName {
					return ErrNamedDiskUncertain
				}
			}
			if used, err := c.disks.referenced(ctx, attachment.VMName); err != nil || used {
				return errors.Join(errors.New("delete the attached VM and all its checkpoints before releasing its disk"), err)
			}
			record.Attachment = nil
		} else {
			record.Attachment = &attachment
		}
		return writeNetworkRecord(namedDiskRecordPath(c.config.Home, expected.Name), record)
	})
}

type sdkNamedDisks struct{ home string }

func (b sdkNamedDisks) create(ctx context.Context, request NamedDiskRequest) error {
	_, err := msb.CreateVolume(ctx, request.Name, msb.WithVolumeKind(msb.VolumeKindDisk),
		msb.WithVolumeSize(request.CapacityMiB), msb.WithVolumeLabels(request.Labels))
	return err
}

func (b sdkNamedDisks) remove(ctx context.Context, name string) error {
	return msb.RemoveVolume(ctx, name)
}

func (b sdkNamedDisks) referenced(ctx context.Context, vm string) (bool, error) {
	if _, err := msb.GetSandbox(ctx, vm); err == nil {
		return true, nil
	} else if !msb.IsKind(err, msb.ErrSandboxNotFound) {
		return false, err
	}
	snapshots, err := msb.Snapshot.List(ctx)
	if err != nil {
		return false, err
	}
	for _, snapshot := range snapshots {
		if group := snapshot.Group(); group != nil && *group == vm {
			return true, nil
		}
	}
	return false, nil
}

func (b sdkNamedDisks) inspect(ctx context.Context, name string) (NamedDiskIdentity, error) {
	volume, err := msb.GetVolume(ctx, name)
	if msb.IsKind(err, msb.ErrVolumeNotFound) {
		if _, statErr := os.Lstat(filepath.Join(b.home, "volumes", name)); !errors.Is(statErr, os.ErrNotExist) {
			return NamedDiskIdentity{}, errors.Join(ErrNamedDiskUncertain, statErr)
		}
		return NamedDiskIdentity{}, ErrNamedDiskNotFound
	}
	if err != nil {
		return NamedDiskIdentity{}, err
	}
	if volume.Kind() != msb.VolumeKindDisk || volume.CapacityBytes() == nil || *volume.CapacityBytes() == 0 ||
		*volume.CapacityBytes()%mib != 0 || *volume.CapacityBytes()/mib > math.MaxUint32 || volume.CreatedAt().IsZero() ||
		volume.Path() != filepath.Join(b.home, "volumes", name) || volume.DiskFstype() == nil || *volume.DiskFstype() != "ext4" {
		return NamedDiskIdentity{}, errors.New("native named disk geometry is invalid")
	}
	directory, err := unix.Open(volume.Path(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return NamedDiskIdentity{}, err
	}
	defer unix.Close(directory)
	var stat unix.Stat_t
	if err := unix.Fstatat(directory, "disk.raw", &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return NamedDiskIdentity{}, err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Size != int64(*volume.CapacityBytes()) || stat.Ino == 0 {
		return NamedDiskIdentity{}, errors.New("native named disk backing file is invalid")
	}
	return NamedDiskIdentity{Name: name, Path: volume.Path(), CapacityMiB: uint32(*volume.CapacityBytes() / mib),
		Labels: maps.Clone(volume.Labels()), CreatedAt: volume.CreatedAt(), Device: uint64(stat.Dev), Inode: stat.Ino}, nil
}
