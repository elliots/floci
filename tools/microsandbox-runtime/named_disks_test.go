package microsandboxruntime

import (
	"context"
	"errors"
	"io"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeNamedDisks struct {
	mu                           sync.Mutex
	items                        map[string]NamedDiskIdentity
	creates, removes             int
	createFailure, removeFailure error
	leaveCreated                 bool
	references                   bool
	onCreate                     func(context.Context)
}

func (f *fakeNamedDisks) inspect(_ context.Context, name string) (NamedDiskIdentity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if value, exists := f.items[name]; exists {
		value.Labels = maps.Clone(value.Labels)
		return value, nil
	}
	return NamedDiskIdentity{}, ErrNamedDiskNotFound
}

func (f *fakeNamedDisks) create(ctx context.Context, request NamedDiskRequest) error {
	if f.onCreate != nil {
		f.onCreate(ctx)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates++
	if f.createFailure == nil || f.leaveCreated {
		f.items[request.Name] = NamedDiskIdentity{Name: request.Name, Path: "/owned/" + request.Name,
			CapacityMiB: request.CapacityMiB, Labels: maps.Clone(request.Labels), CreatedAt: time.Unix(123, 0), Device: 5, Inode: 8}
	}
	return f.createFailure
}

func (f *fakeNamedDisks) remove(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removes++
	delete(f.items, name)
	return f.removeFailure
}

func (f *fakeNamedDisks) referenced(context.Context, string) (bool, error) { return f.references, nil }

func namedDiskClient(t *testing.T, limits CapacityLimits) (*Client, *fakeNamedDisks) {
	t.Helper()
	home := t.TempDir()
	disks := &fakeNamedDisks{items: map[string]NamedDiskIdentity{}}
	var guests []capacityGuest
	manager := diskCapacity(home, limits, &guests)
	manager.storage = func(context.Context) (diskObservation, error) {
		disks.mu.Lock()
		defer disks.mu.Unlock()
		report := diskReport(128 * mib)
		report.Volumes.LogicalBytes = new(uint64)
		for _, disk := range disks.items {
			*report.Volumes.LogicalBytes += uint64(disk.CapacityMiB) * mib
		}
		return diskObservation{Usage: report, AvailableBytes: 10000 * mib}, nil
	}
	return &Client{config: Config{Home: home}, capacity: manager, disks: disks,
		core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) { return 0, nil })}, disks
}

func diskRequest() NamedDiskRequest {
	return NamedDiskRequest{Name: "pvc-unique-uid", CapacityMiB: 256, Labels: map[string]string{"cluster": "cluster-uid", "pvc": "claim-uid"}}
}

func TestNamedDiskReleaseAfterUnattachedReclaimIsIdempotent(t *testing.T) {
	client, _ := namedDiskClient(t, CapacityLimits{})
	disk, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveNamedDisk(t.Context(), disk); err != nil {
		t.Fatal(err)
	}
	attachment := NamedDiskAttachment{Owner: "cluster/node", PodUID: "pod-uid", VMName: "fvp-owned"}
	if err := client.ReleaseNamedDisk(t.Context(), disk, attachment); err != nil {
		t.Fatal("reclaimed unattached intent blocked Pod cleanup", err)
	}
	if err := client.AttachNamedDisk(t.Context(), disk, attachment); err == nil {
		t.Fatal("reattached reclaimed disk")
	}
	disk.Inode++
	if err := client.ReleaseNamedDisk(t.Context(), disk, attachment); err == nil {
		t.Fatal("wrong identity accepted for deleted disk")
	}
}

func TestNamedDiskAdmissionRejectsMissingMountBeforeReservationOrDispatch(t *testing.T) {
	client, _ := namedDiskClient(t, CapacityLimits{MemoryMiB: 1024})
	args := []string{"run", "--detach", "--name", "fvp-owned", "--memory", "512M", "--mount-named", "pvc-missing:/data:kind=disk,size=256M", "busybox:1.37.0"}
	if _, err := client.Execute(t.Context(), args, nil); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("mount preflight was not a known nondispatch", err)
	}
	status, err := client.Capacity(t.Context())
	if err != nil || len(status.Reservations) != 0 {
		t.Fatal("rejected mount retained reservation", status, err)
	}
	client.capacity.snapshot = func(context.Context, string) (capacitySnapshot, error) {
		t.Fatal("snapshot admission preceded missing-disk check")
		return capacitySnapshot{}, nil
	}
	restore := []string{"snap", "restore", "owned:checkpoint", "--name", "fvp-owned", "--mount-named", "pvc-missing:/data:kind=disk,size=256M"}
	if _, err := client.PrepareAdmission(t.Context(), AdmissionRequest{Arguments: restore, SourceName: "fvp-owned"}); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("restore was prepared with a missing disk", err)
	}
}

func TestNamedDiskPreparedMountIsRevalidatedBeforeDispatchAndCancelsKnownFailure(t *testing.T) {
	client, disks := namedDiskClient(t, CapacityLimits{MemoryMiB: 1024})
	disk, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil {
		t.Fatal(err)
	}
	attachment := NamedDiskAttachment{Owner: "cluster/node", PodUID: "pod-uid", VMName: "fvp-owned"}
	if err := client.AttachNamedDisk(t.Context(), disk, attachment); err != nil {
		t.Fatal(err)
	}
	args := []string{"run", "--detach", "--name", attachment.VMName, "--memory", "512M", "--mount-named", disk.Name + ":/data:kind=disk,size=256M", "busybox:1.37.0"}
	token, err := client.PrepareAdmission(t.Context(), AdmissionRequest{Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseNamedDisk(t.Context(), disk, attachment); !errors.Is(err, ErrNamedDiskUncertain) {
		t.Fatal("lease changed between prepare and dispatch", err)
	}
	changed := disk
	changed.Inode++
	disks.items[disk.Name] = changed
	if _, err := client.ExecuteAdmitted(t.Context(), args, nil, token); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal("changed mount reached native dispatch", err)
	}
	status, err := client.Capacity(t.Context())
	if err != nil || len(status.Reservations) != 0 {
		t.Fatal("known nondispatch leaked prepared permit", status, err)
	}
	disks.items[disk.Name] = disk
	if err := client.ReleaseNamedDisk(t.Context(), disk, attachment); err != nil {
		t.Fatal("known nondispatch blocked cleanup", err)
	}
}

func TestNamedDiskCreationReservesBeforeDispatchAndCountsNoGuestRAM(t *testing.T) {
	client, disks := namedDiskClient(t, CapacityLimits{MemoryMiB: 1, Starts: 1, DiskMiB: 512})
	disks.onCreate = func(ctx context.Context) {
		if ctx.Done() != nil {
			t.Error("accepted disk creation can be abandoned by request cancellation")
		}
		status, err := client.Capacity(t.Context())
		if err != nil || status.PendingStarts != 0 || status.MemoryMiB != 0 || len(status.Reservations) != 1 || status.Disk.ReservedBytes != 272*mib {
			t.Errorf("disk creation was not durably reserved before SDK dispatch: %+v %v", status, err)
		}
	}
	identity, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil {
		t.Fatal(err)
	}
	status, err := client.Capacity(t.Context())
	if err != nil || len(status.Reservations) != 0 || status.Disk.ObservedBytes != 384*mib {
		t.Fatalf("disk completion was not accounted: %+v %v", status, err)
	}
	again, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil || !identity.Matches(again) || disks.creates != 1 {
		t.Fatalf("same PVC allocation recreated its disk: %+v %v", again, err)
	}
	changed := diskRequest()
	changed.CapacityMiB = 512
	if _, err := client.CreateNamedDisk(t.Context(), changed); err == nil {
		t.Fatal("accepted in-place expansion")
	}
	changed = diskRequest()
	changed.Labels["pvc"] = "replacement-uid"
	if _, err := client.CreateNamedDisk(t.Context(), changed); err == nil {
		t.Fatal("adopted disk with a different PVC UID")
	}
}

func TestNamedDiskCapacityRefusalCreatesNoIntentAndCanRetry(t *testing.T) {
	client, disks := namedDiskClient(t, CapacityLimits{DiskMiB: 256})
	if _, err := client.CreateNamedDisk(t.Context(), diskRequest()); !errors.Is(err, ErrCapacityUnavailable) {
		t.Fatal(err)
	}
	if disks.creates != 0 {
		t.Fatal("SDK creation ran without disk capacity")
	}
	if _, err := os.Stat(namedDiskRecordPath(client.config.Home, diskRequest().Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("refusal persisted a dispatched intent", err)
	}
	if err := client.SetCapacityLimits(t.Context(), CapacityLimits{DiskMiB: 512}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateNamedDisk(t.Context(), diskRequest()); err != nil {
		t.Fatal(err)
	}
}

func TestNamedDiskUnknownCreateDoesNotReplayOrForgetItsReservation(t *testing.T) {
	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent", true: "indexed"}[indexed], func(t *testing.T) {
			client, disks := namedDiskClient(t, CapacityLimits{DiskMiB: 512})
			disks.createFailure, disks.leaveCreated = errors.New("lost completion"), indexed
			if _, err := client.CreateNamedDisk(t.Context(), diskRequest()); !errors.Is(err, ErrNamedDiskUncertain) {
				t.Fatal(err)
			}
			status, err := client.Capacity(t.Context())
			if err != nil || len(status.Reservations) != 1 {
				t.Fatalf("uncertain allocation lost reservation: %+v %v", status, err)
			}
			_, err = client.CreateNamedDisk(t.Context(), diskRequest())
			if indexed && err != nil || !indexed && !errors.Is(err, ErrNamedDiskUncertain) {
				t.Fatal(err)
			}
			if disks.creates != 1 {
				t.Fatal("uncertain creation was replayed")
			}
		})
	}
}

func TestNamedDiskAttachmentSurvivesSleepAndRejectsReplacementUntilCleanup(t *testing.T) {
	client, disks := namedDiskClient(t, CapacityLimits{})
	identity, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil {
		t.Fatal(err)
	}
	old := NamedDiskAttachment{Owner: "node-controller", PodUID: "pod-one", VMName: "fvp-pod-one"}
	if err := client.AttachNamedDisk(t.Context(), identity, old); err != nil {
		t.Fatal(err)
	}
	rejected := NamedDiskAttachment{Owner: old.Owner, PodUID: "pod-rejected", VMName: "fvp-rejected"}
	if err := client.AttachNamedDisk(t.Context(), identity, rejected); !errors.Is(err, ErrNamedDiskAttached) {
		t.Fatal(err)
	}
	if err := client.ReleaseNamedDisk(t.Context(), identity, rejected); err != nil {
		t.Fatal("failed attachment cleanup blocked deletion", err)
	}
	record, err := client.readNamedDisk(identity.Name)
	if err != nil || record.Attachment == nil || *record.Attachment != old {
		t.Fatal("failed contender cleanup changed original lease", err)
	}
	disks.references = true // A stopped VM or its full-RAM checkpoint retains the attachment.
	if err := client.ReleaseNamedDisk(t.Context(), identity, old); err == nil {
		t.Fatal("released a restorable Pod's disk")
	}
	if err := client.RemoveNamedDisk(t.Context(), identity); !errors.Is(err, ErrNamedDiskAttached) {
		t.Fatal(err)
	}
	next := NamedDiskAttachment{Owner: "node-controller", PodUID: "pod-two", VMName: "fvp-pod-two"}
	if err := client.AttachNamedDisk(t.Context(), identity, next); !errors.Is(err, ErrNamedDiskAttached) {
		t.Fatal(err)
	}
	disks.references = false
	if err := client.capacity.transaction(t.Context(), func(state *capacityState) error {
		state.Permits["unknown-restore"] = capacityPermit{Name: old.VMName, Operation: "snap restore", Dispatched: true}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseNamedDisk(t.Context(), identity, old); !errors.Is(err, ErrNamedDiskUncertain) {
		t.Fatal(err)
	}
	if err := client.capacity.transaction(t.Context(), func(state *capacityState) error { delete(state.Permits, "unknown-restore"); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseNamedDisk(t.Context(), identity, old); err != nil {
		t.Fatal(err)
	}
	if err := client.AttachNamedDisk(t.Context(), identity, next); err != nil {
		t.Fatal(err)
	}
	if err := client.ReleaseNamedDisk(t.Context(), identity, next); err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveNamedDisk(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	if err := client.RemoveNamedDisk(t.Context(), identity); err != nil || disks.removes != 1 {
		t.Fatal("deletion not idempotent", err)
	}
	if _, err := client.CreateNamedDisk(t.Context(), diskRequest()); err == nil {
		t.Fatal("reused a retired disk name")
	}
}

func TestNamedDiskReplacementInodeCannotBeAttachedOrDeleted(t *testing.T) {
	client, disks := namedDiskClient(t, CapacityLimits{})
	identity, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil {
		t.Fatal(err)
	}
	replaced := disks.items[identity.Name]
	replaced.Inode++
	disks.items[identity.Name] = replaced
	if err := client.AttachNamedDisk(t.Context(), identity, NamedDiskAttachment{Owner: "node", PodUID: "pod", VMName: "vm"}); err == nil {
		t.Fatal("accepted replaced disk")
	}
	if err := client.RemoveNamedDisk(t.Context(), identity); err == nil || disks.removes != 0 {
		t.Fatal("deleted replaced disk", err)
	}
}

func TestNamedDiskMountsRequireExactAttachmentAndExplicitGeometry(t *testing.T) {
	client, _ := namedDiskClient(t, CapacityLimits{})
	identity, err := client.CreateNamedDisk(t.Context(), diskRequest())
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"snap", "restore", "vm:sleep", "--name", "vm", "--mount-named", identity.Name + ":/persist:kind=disk,size=256M"}
	request, err := parse(args)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.validateNamedDiskMounts(t.Context(), request); err == nil {
		t.Fatal("unattached disk mounted")
	}
	if err := client.AttachNamedDisk(t.Context(), identity, NamedDiskAttachment{Owner: "node", PodUID: "pod", VMName: "vm"}); err != nil {
		t.Fatal(err)
	}
	if err := client.validateNamedDiskMounts(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	request.name = "other"
	if err := client.validateNamedDiskMounts(t.Context(), request); err == nil {
		t.Fatal("other VM mounted a leased disk")
	}
	for _, mount := range []string{"../disk:/persist:kind=disk,size=256M", "disk:/persist:kind=dir,size=256M", "disk:/persist:kind=disk", "disk:/:kind=disk,size=256M", "disk:/persist:kind=disk,size=1M", "disk:/persist:kind=disk,size=256M,create"} {
		args[len(args)-1] = mount
		if _, err := parse(args); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("accepted unsafe mount %q: %v", mount, err)
		}
	}
}

func TestNamedDiskUnrecordedAllocationIsNeverAdopted(t *testing.T) {
	client, disks := namedDiskClient(t, CapacityLimits{})
	if err := disks.create(t.Context(), diskRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateNamedDisk(t.Context(), diskRequest()); err == nil {
		t.Fatal("adopted unrecorded storage")
	}
	files, err := filepath.Glob(filepath.Join(client.config.Home, "floci-named-disks", "*.json"))
	if err != nil || len(files) != 0 {
		t.Fatal("unowned disk gained an ownership record", err)
	}
}
