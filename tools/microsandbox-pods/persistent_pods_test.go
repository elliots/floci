package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	native "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

type podDiskFixture struct {
	nativeRuntime
	claims     map[string]*corev1.PersistentVolumeClaim
	volumes    map[string]*corev1.PersistentVolume
	disks      map[string]native.NamedDiskIdentity
	resolveErr error
	attach     func(native.NamedDiskIdentity, native.NamedDiskAttachment) error
	release    func(native.NamedDiskIdentity, native.NamedDiskAttachment) error
}

func TestRecoveryKeepsColdPVCContenderRetryableWithoutStealingLease(t *testing.T) {
	_, e, f := persistentPodFixture(t)
	e.pod.UID = types.UID("11111111-2222-3333-4444-555555555555")
	pv := f.volumes["floci-pvc-owned"]
	e.persistentVolumes = []podPersistentVolume{{Name: "data", ClaimName: "database", ClaimUID: "claim-uid", PVUID: "pv-uid", GuestPath: pv.Spec.Local.Path, Disk: f.disks[pv.Name]}}
	for _, state := range []string{"COLD", "WAITINGVOLUMES", "FAILED"} {
		t.Run(state, func(t *testing.T) {
			p, _ := recoveryStatusFixture(t, journalRecord{Pod: e.pod.DeepCopy(), PersistentVolumes: e.persistentVolumes, View: lifecycleView{State: state}}, "")
			f.nativeRuntime = p.runner.runtime
			p.runner.runtime = f
			p.storageOwner = "cluster/node"
			f.attach = func(native.NamedDiskIdentity, native.NamedDiskAttachment) error { return native.ErrNamedDiskAttached }
			// Keep the normal waiter blocked before admission; recovery itself
			// must not require a lease held by another sleeping Pod.
			p.storage = nil
			if err := p.recover(t.Context()); err != nil {
				t.Fatal("cold contender blocked controller recovery", err)
			}
			entry := p.pods[e.pod.Namespace+"/"+e.pod.Name]
			entry.op.Lock()
			if entry.state != state || entry.created || entry.allocated || len(entry.persistentVolumes) != 1 {
				t.Fatal("cold contender lost its retryable intent")
			}
			entry.op.Unlock()
			entry.cancel()
			<-entry.done
		})
	}
}

func (f *podDiskFixture) Resolve(_ context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, native.NamedDiskIdentity, error) {
	if f.resolveErr != nil {
		return nil, nil, native.NamedDiskIdentity{}, f.resolveErr
	}
	claim := f.claims[namespace+"/"+name]
	if claim == nil {
		return nil, nil, native.NamedDiskIdentity{}, errors.New("PVC is not Bound")
	}
	return claim.DeepCopy(), f.volumes[claim.Spec.VolumeName].DeepCopy(), f.disks[claim.Spec.VolumeName], nil
}
func (*podDiskFixture) CreateNamedDisk(context.Context, native.NamedDiskRequest) (native.NamedDiskIdentity, error) {
	return native.NamedDiskIdentity{}, errors.New("Pod must not provision disks")
}
func (f *podDiskFixture) InspectNamedDisk(_ context.Context, name string) (native.NamedDiskIdentity, error) {
	return f.disks[name], nil
}
func (*podDiskFixture) RemoveNamedDisk(context.Context, native.NamedDiskIdentity) error {
	return errors.New("Pod must not reclaim disks")
}
func (f *podDiskFixture) AttachNamedDisk(_ context.Context, disk native.NamedDiskIdentity, attachment native.NamedDiskAttachment) error {
	if f.attach != nil {
		return f.attach(disk, attachment)
	}
	return nil
}
func (f *podDiskFixture) ReleaseNamedDisk(_ context.Context, disk native.NamedDiskIdentity, attachment native.NamedDiskAttachment) error {
	if f.release != nil {
		return f.release(disk, attachment)
	}
	return nil
}

func persistentPodFixture(t *testing.T) (*provider, *podEntry, *podDiskFixture) {
	t.Helper()
	p, e := probeFixture(t)
	e.vm = "fvp-owned-volume"
	e.state = "COLD"
	e.pod.Namespace = "apps"
	e.pod.Name = "database"
	e.pod.UID = "pod-uid"
	e.pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "database"}}}}
	e.pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{{Name: "data", MountPath: "/data"}}
	p.storageOwner = "cluster/node"
	f := &podDiskFixture{nativeRuntime: p.runner.runtime, claims: map[string]*corev1.PersistentVolumeClaim{}, volumes: map[string]*corev1.PersistentVolume{}, disks: map[string]native.NamedDiskIdentity{}}
	disk := native.NamedDiskIdentity{Name: "floci-pvc-owned", Path: "/private/native/volumes/floci-pvc-owned", CapacityMiB: 256, Labels: map[string]string{"cluster": "cluster"}, CreatedAt: time.Unix(123, 0), Device: 1, Inode: 2}
	f.disks[disk.Name] = disk
	f.claims["apps/database"] = &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "database", Namespace: "apps", UID: "claim-uid"}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: disk.Name}}
	f.volumes[disk.Name] = &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: disk.Name, UID: "pv-uid"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{Local: &corev1.LocalVolumeSource{Path: "/var/lib/floci/persistent-volumes/" + disk.Name}}}}
	p.storage = f
	p.runner.runtime = f
	return p, e, f
}

func TestPersistentPodWaitsForBoundClaimWithoutAllocatingGuest(t *testing.T) {
	p, e, f := persistentPodFixture(t)
	f.resolveErr = errors.New("PVC is not Bound on this node")
	f.attach = func(native.NamedDiskIdentity, native.NamedDiskAttachment) error {
		t.Fatal("attached unresolved PVC")
		return nil
	}
	if _, err := p.readVolumeSources(t.Context(), e); !errors.Is(err, errPersistentVolumePending) {
		t.Fatal(err)
	}
	if err := p.preparePersistentVolumes(t.Context(), e); !errors.Is(err, errPersistentVolumePending) {
		t.Fatal(err)
	}
	if e.created || e.allocated || len(e.persistentVolumes) != 0 {
		t.Fatal("unbound claim allocated runtime resources")
	}
	if _, err := os.Stat(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("unresolved disk was journaled", err)
	}
}

func TestPersistentPodJournalsAllIntendedAttachmentsBeforeTakingLease(t *testing.T) {
	p, e, f := persistentPodFixture(t)
	other := e.pod.Spec.Volumes[0].DeepCopy()
	other.Name = "alias"
	e.pod.Spec.Volumes = append(e.pod.Spec.Volumes, *other)
	e.pod.Spec.Containers[0].VolumeMounts = append(e.pod.Spec.Containers[0].VolumeMounts, corev1.VolumeMount{Name: "alias", MountPath: "/alias"})
	calls := 0
	f.attach = func(disk native.NamedDiskIdentity, attachment native.NamedDiskAttachment) error {
		calls++
		data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
		if err != nil {
			t.Fatal("lease preceded durable Pod intent", err)
		}
		var journal journalRecord
		if err := json.Unmarshal(data, &journal); err != nil || len(journal.PersistentVolumes) != 2 {
			t.Fatal("partial attachment intent", err)
		}
		if attachment.Owner != p.storageOwner || attachment.PodUID != string(e.pod.UID) || attachment.VMName != e.vm || disk.Name != "floci-pvc-owned" {
			t.Fatal("attachment lost immutable ownership")
		}
		if calls == 2 {
			return native.ErrNamedDiskAttached
		}
		return nil
	}
	if err := p.preparePersistentVolumes(t.Context(), e); !errors.Is(err, native.ErrNamedDiskAttached) || !errors.Is(err, errPersistentVolumePending) {
		t.Fatal(err)
	}
	if calls != 2 || e.created || e.allocated {
		t.Fatal("partial attachment started a VM")
	}
	args := persistentMountArgs(e)
	if !slices.Equal(args, []string{"--mount-named", "floci-pvc-owned:/var/lib/floci/persistent-volumes/floci-pvc-owned:kind=disk,size=256M"}) {
		t.Fatalf("one PVC alias generated multiple native mounts: %v", args)
	}
	var released []string
	f.release = func(disk native.NamedDiskIdentity, _ native.NamedDiskAttachment) error {
		released = append(released, disk.Name)
		return nil
	}
	if err := p.releasePersistentVolumes(t.Context(), e); err != nil || len(released) != 2 {
		t.Fatal("partial attachment cleanup was incomplete", err)
	}
}

func TestSleepingPodRejectsReplacementClaimPVAndBackingDiskBeforeWake(t *testing.T) {
	for _, changed := range []string{"claim", "pv", "inode", "path", "geometry"} {
		t.Run(changed, func(t *testing.T) {
			p, e, f := persistentPodFixture(t)
			if err := p.preparePersistentVolumes(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			e.state, e.created, e.snapshots = "SLEEPING", true, []string{"owned:snapshot"}
			switch changed {
			case "claim":
				f.claims["apps/database"].UID = types.UID("replaced")
			case "pv":
				f.volumes["floci-pvc-owned"].UID = types.UID("replaced")
			default:
				disk := f.disks["floci-pvc-owned"]
				switch changed {
				case "inode":
					disk.Inode++
				case "path":
					disk.Path += "-moved"
				case "geometry":
					disk.CapacityMiB++
				}
				f.disks[disk.Name] = disk
			}
			f.attach = func(native.NamedDiskIdentity, native.NamedDiskAttachment) error {
				t.Fatal("replacement identity took attachment")
				return nil
			}
			if err := p.preparePersistentVolumes(t.Context(), e); !errors.Is(err, errPersistentVolumePending) {
				t.Fatal(err)
			}
			if e.state != "SLEEPING" || !e.created || len(e.snapshots) != 1 {
				t.Fatal("validation changed recoverable sleeping state")
			}
		})
	}
}

func TestPersistentVolumeJournalRejectsMissingDuplicateAndMismatchedBindings(t *testing.T) {
	for _, change := range []string{"missing", "duplicate", "wrong-name", "wrong-claim", "empty-inode", "guest-path"} {
		t.Run(change, func(t *testing.T) {
			p, e, _ := persistentPodFixture(t)
			if err := p.preparePersistentVolumes(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			e.created = true
			switch change {
			case "missing":
				e.persistentVolumes = nil
			case "duplicate":
				e.persistentVolumes = append(e.persistentVolumes, e.persistentVolumes[0])
			case "wrong-name":
				e.persistentVolumes[0].Name = "other"
			case "wrong-claim":
				e.persistentVolumes[0].ClaimName = "other"
			case "empty-inode":
				e.persistentVolumes[0].Disk.Inode = 0
			case "guest-path":
				e.persistentVolumes[0].GuestPath = "/other"
			}
			if err := validatePersistentVolumeJournal(e); err == nil {
				t.Fatal("accepted invalid persistent attachment journal")
			}
		})
	}
}

func TestPersistentVolumesUseBoundFilesystemAndHonorReadOnlyAndFSGroup(t *testing.T) {
	p, e, _ := persistentPodFixture(t)
	e.pod.Spec.SecurityContext = &corev1.PodSecurityContext{FSGroup: new(int64(4321)), FSGroupChangePolicy: new(corev1.FSGroupChangeOnRootMismatch)}
	if err := p.preparePersistentVolumes(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	script, err := persistentVolumeSetup(e, e.pod.Spec.Volumes[0])
	if err != nil || !strings.Contains(script, "mount --bind") || !strings.Contains(script, "4321") || !strings.Contains(script, "--on-root-mismatch") || strings.Contains(script, "cp ") {
		t.Fatalf("persistent filesystem was not mounted with fsGroup: %s %v", script, err)
	}
	e.pod.Spec.Volumes[0].PersistentVolumeClaim.ReadOnly = true
	script, err = persistentVolumeSetup(e, e.pod.Spec.Volumes[0])
	if err != nil || !strings.Contains(script, "mount -o remount,bind,ro") {
		t.Fatalf("read-only source writable in guest: %s %v", script, err)
	}
	if args := volumeMountArgs(e.pod); !strings.HasSuffix(args[1], ":ro") {
		t.Fatalf("read-only PVC writable inside application: %v", args)
	}
	if err := validateVolumes(e.pod); err != nil {
		t.Fatal(err)
	}
	e.pod.Spec.Volumes[0].EmptyDir = &corev1.EmptyDirVolumeSource{}
	if err := validateVolumes(e.pod); err == nil {
		t.Fatal("accepted two volume sources")
	}
}

func TestPersistentPodJournalFencesLegacyOrphanCleanup(t *testing.T) {
	p, e, _ := persistentPodFixture(t)
	if err := p.journal(e); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Version != 7 {
		t.Fatal("older controller could accept PVC journal", record.Version)
	}
	if _, err := record.guestAllocation(); err != nil {
		t.Fatal("current controller rejected PVC allocation", err)
	}
	record.Version = 6
	if _, err := record.guestAllocation(); err == nil {
		t.Fatal("legacy PVC journal admitted destructive orphan recovery")
	}
	record.Pod = validPod()
	record.Version = 7
	if _, err := record.guestAllocation(); err == nil {
		t.Fatal("PVC journal accepted without persistent storage specification")
	}
	if podJournalVersion(record.Pod) != 6 {
		t.Fatal("ordinary journal compatibility changed")
	}
}

func TestOrphanRecoveryRejectsLegacyPVCJournalBeforeNativeCleanup(t *testing.T) {
	_, e, _ := persistentPodFixture(t)
	e.pod.UID = types.UID("11111111-2222-3333-4444-555555555555")
	p, path := recoveryStatusFixture(t, journalRecord{Pod: e.pod, View: lifecycleView{State: "RUNNING"}}, "Running")
	if err := p.client.CoreV1().Pods(e.pod.Namespace).Delete(t.Context(), e.pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	record.Version = 6
	data, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	p.runner.runtime = stubNativeRuntime{execute: func(context.Context, []string, []byte) (native.Result, error) {
		t.Fatal("legacy orphan journal reached native cleanup")
		return native.Result{}, nil
	}}
	if err := p.recover(t.Context()); err == nil || !strings.Contains(err.Error(), "journal version 7") {
		t.Fatal("legacy orphan accepted", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("orphan ownership journal lost", err)
	}
}
