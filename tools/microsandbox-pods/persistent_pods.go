package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
)

var errPersistentVolumePending = errors.New("persistent volume is not ready for this Pod")

type persistentDiskRuntime interface {
	CreateNamedDisk(context.Context, msbruntime.NamedDiskRequest) (msbruntime.NamedDiskIdentity, error)
	InspectNamedDisk(context.Context, string) (msbruntime.NamedDiskIdentity, error)
	RemoveNamedDisk(context.Context, msbruntime.NamedDiskIdentity) error
	AttachNamedDisk(context.Context, msbruntime.NamedDiskIdentity, msbruntime.NamedDiskAttachment) error
	ReleaseNamedDisk(context.Context, msbruntime.NamedDiskIdentity, msbruntime.NamedDiskAttachment) error
}

type persistentVolumeResolver interface {
	Resolve(context.Context, string, string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, msbruntime.NamedDiskIdentity, error)
}

type podPersistentVolume struct {
	Name      string                       `json:"name"`
	ClaimName string                       `json:"claimName"`
	ClaimUID  string                       `json:"claimUID"`
	PVUID     string                       `json:"pvUID"`
	GuestPath string                       `json:"guestPath"`
	Disk      msbruntime.NamedDiskIdentity `json:"disk"`
}

func hasPersistentVolumes(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, volume := range mountedVolumes(pod) {
		if volume.PersistentVolumeClaim != nil {
			return true
		}
	}
	return false
}

func (p *provider) configurePersistentStorage() error {
	disks, ok := p.runner.runtime.(persistentDiskRuntime)
	if !ok {
		return errors.New("native SDK named-disk support is unavailable")
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, "owner"))
	if err != nil {
		return err
	}
	var identity stateIdentity
	if err := json.Unmarshal(data, &identity); err != nil || identity.ClusterUID == "" || identity.Node != p.runner.cfg.nodeName {
		return errors.New("persistent storage requires the exact controller cluster and node identity")
	}
	p.storageOwner = identity.ClusterUID + "/" + identity.Node
	provisioner, err := newPersistentProvisioner(p.client, disks, p.runner.cfg.state, identity.Node, identity.ClusterUID)
	if err != nil {
		return err
	}
	p.storage = provisioner
	p.storageRun = provisioner.Run
	return nil
}

func (p *provider) diskAttachment(e *podEntry) msbruntime.NamedDiskAttachment {
	return msbruntime.NamedDiskAttachment{Owner: p.storageOwner, PodUID: string(e.pod.UID), VMName: e.vm}
}

func (p *provider) persistentVolumeUnavailable(e *podEntry, err error) {
	p.unavailable(e, "FailedMount")
	e.mu.Lock()
	e.pod.Status.Message = err.Error()
	e.mu.Unlock()
	p.publish(e)
}

func (p *provider) resolvePersistentVolumes(ctx context.Context, e *podEntry) ([]podPersistentVolume, error) {
	if err := validatePersistentVolumeJournal(e); err != nil {
		return nil, err
	}
	var resolved []podPersistentVolume
	for _, volume := range mountedVolumes(e.pod) {
		if volume.PersistentVolumeClaim == nil {
			continue
		}
		if p.storage == nil {
			return nil, errors.New("persistent storage provisioner is unavailable")
		}
		claim, pv, disk, err := p.storage.Resolve(ctx, e.pod.Namespace, volume.PersistentVolumeClaim.ClaimName)
		if err != nil {
			return nil, fmt.Errorf("PVC %s: %w", volume.PersistentVolumeClaim.ClaimName, err)
		}
		if claim == nil || pv == nil || pv.Spec.Local == nil || claim.UID == "" || pv.UID == "" ||
			pv.Spec.Local.Path != "/var/lib/floci/persistent-volumes/"+disk.Name {
			return nil, errors.New("persistent volume has no verified guest mount identity")
		}
		current := podPersistentVolume{Name: volume.Name, ClaimName: claim.Name, ClaimUID: string(claim.UID), PVUID: string(pv.UID), GuestPath: pv.Spec.Local.Path, Disk: disk}
		for _, stored := range e.persistentVolumes {
			if stored.Name == volume.Name && (stored.ClaimName != current.ClaimName || stored.ClaimUID != current.ClaimUID ||
				stored.PVUID != current.PVUID || stored.GuestPath != current.GuestPath || !stored.Disk.Matches(current.Disk)) {
				return nil, fmt.Errorf("PVC %s was replaced or its backing disk changed; refusing to resume the old Pod", claim.Name)
			}
		}
		resolved = append(resolved, current)
	}
	if len(e.persistentVolumes) > 0 && len(resolved) != len(e.persistentVolumes) {
		return nil, errors.New("persisted Pod volume bindings differ from its immutable specification")
	}
	return resolved, nil
}

func validatePersistentVolumeJournal(e *podEntry) error {
	expected := map[string]string{}
	for _, volume := range mountedVolumes(e.pod) {
		if volume.PersistentVolumeClaim != nil {
			expected[volume.Name] = volume.PersistentVolumeClaim.ClaimName
		}
	}
	if len(e.persistentVolumes) == 0 {
		if len(expected) > 0 && (e.created || len(e.snapshots) > 0) {
			return errors.New("existing native Pod has no persistent disk attachment journal")
		}
		return nil
	}
	if len(e.persistentVolumes) != len(expected) {
		return errors.New("persisted Pod volume bindings differ from its immutable specification")
	}
	seen := map[string]bool{}
	for _, volume := range e.persistentVolumes {
		if seen[volume.Name] || expected[volume.Name] != volume.ClaimName || volume.ClaimUID == "" || volume.PVUID == "" ||
			volume.Disk.Name == "" || strings.ContainsAny(volume.Disk.Name, "/\\:") || volume.Disk.CapacityMiB < 256 ||
			volume.Disk.Path == "" || volume.Disk.CreatedAt.IsZero() || volume.Disk.Inode == 0 || len(volume.Disk.Labels) == 0 ||
			volume.GuestPath != "/var/lib/floci/persistent-volumes/"+volume.Disk.Name {
			return errors.New("persistent disk attachment journal is invalid")
		}
		seen[volume.Name] = true
	}
	return nil
}

func (p *provider) verifyPersistentAttachments(ctx context.Context, e *podEntry) error {
	if err := validatePersistentVolumeJournal(e); err != nil {
		return err
	}
	if len(e.persistentVolumes) == 0 {
		return nil
	}
	// Intents precede attachment. A rejected cold contender must not stop
	// controller recovery while another Pod legitimately owns the disk.
	if !e.created && len(e.snapshots) == 0 {
		return nil
	}
	disks, ok := p.runner.runtime.(persistentDiskRuntime)
	if !ok {
		return errors.New("native SDK named-disk support is unavailable")
	}
	for _, volume := range e.persistentVolumes {
		if err := disks.AttachNamedDisk(ctx, volume.Disk, p.diskAttachment(e)); err != nil {
			return err
		}
	}
	return nil
}

// The caller holds the Pod operation lock. Save every intended attachment before
// taking any lease, so interrupted startup/deletion can release partial work.
func (p *provider) preparePersistentVolumes(ctx context.Context, e *podEntry) error {
	resolved, err := p.resolvePersistentVolumes(ctx, e)
	if err != nil {
		return errors.Join(errPersistentVolumePending, err)
	}
	if len(resolved) == 0 {
		return nil
	}
	disks, ok := p.runner.runtime.(persistentDiskRuntime)
	if !ok {
		return errors.New("native SDK named-disk support is unavailable")
	}
	if len(e.persistentVolumes) == 0 {
		if e.created || len(e.snapshots) != 0 {
			return errors.New("existing native Pod has no persistent disk attachment journal")
		}
		e.persistentVolumes = resolved
		if err := p.journal(e); err != nil {
			return err
		}
	}
	for _, volume := range e.persistentVolumes {
		if err := disks.AttachNamedDisk(ctx, volume.Disk, p.diskAttachment(e)); err != nil {
			return errors.Join(errPersistentVolumePending, err)
		}
	}
	return nil
}

func (p *provider) releasePersistentVolumes(ctx context.Context, e *podEntry) error {
	if len(e.persistentVolumes) == 0 {
		return nil
	}
	disks, ok := p.runner.runtime.(persistentDiskRuntime)
	if !ok {
		return errors.New("native SDK named-disk support is unavailable")
	}
	var failures []error
	for _, volume := range e.persistentVolumes {
		if err := disks.ReleaseNamedDisk(ctx, volume.Disk, p.diskAttachment(e)); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func persistentMountArgs(e *podEntry) []string {
	var args []string
	seen := map[string]bool{}
	for _, volume := range e.persistentVolumes {
		if !seen[volume.Disk.Name] {
			args = append(args, "--mount-named", volume.Disk.Name+":"+volume.GuestPath+":kind=disk,size="+strconv.FormatUint(uint64(volume.Disk.CapacityMiB), 10)+"M")
			seen[volume.Disk.Name] = true
		}
	}
	return args
}

func persistentVolumeSetup(e *podEntry, volume corev1.Volume) (string, error) {
	for _, stored := range e.persistentVolumes {
		if stored.Name != volume.Name {
			continue
		}
		alias := guestVolumePath(volume.Name)
		script := "test -d " + quote(stored.GuestPath) + "; test \"$(readlink -f " + quote(stored.GuestPath) + ")\" = " + quote(stored.GuestPath) + "; mountpoint -q " + quote(stored.GuestPath) + "; mkdir -p /var/lib/floci-volumes; chmod 0700 /var/lib/floci-volumes; mkdir -p " + quote(alias) + "; test \"$(readlink -f " + quote(alias) + ")\" = " + quote(alias) + "; " +
			"if ! mountpoint -q " + quote(alias) + "; then mount --bind " + quote(stored.GuestPath) + " " + quote(alias) + "; " +
			volumeOwnershipPolicy(e.pod, alias, volume.PersistentVolumeClaim.ReadOnly, false, true)
		if volume.PersistentVolumeClaim.ReadOnly {
			script += "mount -o remount,bind,ro " + quote(alias) + "; "
		}
		return script + "fi", nil
	}
	return "", errors.New("persistent volume has no journaled disk attachment")
}
