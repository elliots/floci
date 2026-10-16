package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"maps"
	"path"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

const volumeRefreshInterval = 30 * time.Second

func (p *provider) startStrictPod(e *podEntry) error {
	if err := p.waitForVolumeSources(e.lifetime, e); err != nil {
		return err
	}
	ctx, cancel := boundedContext(e.lifetime)
	defer cancel()
	e.op.Lock()
	defer e.op.Unlock()
	defer func() { e.initialVolumes = nil }()
	if err := p.checkCurrentPod(ctx, e); err != nil {
		if e.lifetime.Err() == nil {
			p.fail(e, err)
		}
		return err
	}
	release, err := p.acquireStart(ctx, e)
	if err == nil {
		defer release()
		e.state = "STARTING"
		err = p.journal(e)
		if err == nil {
			err = p.startAdmitted(ctx, e)
		}
	}
	if err != nil && e.lifetime.Err() == nil {
		p.fail(e, err)
	}
	return err
}

func mountedVolumes(pod *corev1.Pod) []corev1.Volume {
	mounted := make(map[string]bool)
	for _, container := range slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers) {
		for _, mount := range container.VolumeMounts {
			mounted[mount.Name] = true
		}
	}
	var volumes []corev1.Volume
	for _, volume := range pod.Spec.Volumes {
		if mounted[volume.Name] {
			volumes = append(volumes, volume)
		}
	}
	return volumes
}

// Callers hold e.op, so projections cannot race snapshots or container restarts.
func (p *provider) prepareVolumes(ctx context.Context, e *podEntry) error {
	for _, volume := range mountedVolumes(e.pod) {
		allocation, err := validateStoredAllocation(e.allocation)
		if err != nil {
			return err
		}
		if _, err := p.runner.guest(ctx, e.vm, volumeSetupScript(volume, allocation.MemoryMiB)); err != nil {
			return fmt.Errorf("initialize volume %s: %w", volume.Name, err)
		}
	}
	if e.started.IsZero() && e.initialVolumes != nil {
		// Use the snapshot fetched before admission. A source/API outage during
		// image pull must not turn a successful mount preflight into Pod failure.
		for name, files := range e.initialVolumes {
			if err := p.writeProjection(ctx, e, name, files); err != nil {
				return fmt.Errorf("populate volume %s: %w", name, err)
			}
		}
		e.initialVolumes = nil
		e.volumesChecked = time.Now()
		return nil
	}
	return p.refreshVolumes(ctx, e, volumeSourcesRequired(e))
}

// Every mounted volume is prepared before the first init starts. Its journaled
// start time proves existing projections after recovery, when digest caches are
// empty. Synthetic StartError statuses have no start time and prove nothing.
func volumeSourcesRequired(e *podEntry) bool {
	if !e.started.IsZero() {
		return false
	}
	for _, status := range e.pod.Status.InitContainerStatuses {
		if running := status.State.Running; running != nil && !running.StartedAt.IsZero() {
			return false
		}
		for _, terminated := range []*corev1.ContainerStateTerminated{status.State.Terminated, status.LastTerminationState.Terminated} {
			if terminated != nil && !terminated.StartedAt.IsZero() {
				return false
			}
		}
	}
	return true
}

func (p *provider) refreshVolumes(ctx context.Context, e *podEntry, required bool) error {
	e.volumesChecked = time.Now()
	for _, volume := range mountedVolumes(e.pod) {
		if volume.ConfigMap == nil && volume.Secret == nil {
			continue
		}
		files, err := resolveProjectedVolume(ctx, p.client, e.pod.Namespace, volume)
		if err == nil {
			err = p.writeProjection(ctx, e, volume.Name, files)
		}
		if err != nil {
			_, alreadyProjected := e.volumeDigests[volume.Name]
			if required && !alreadyProjected {
				return fmt.Errorf("populate volume %s: %w", volume.Name, err)
			}
			// Kubernetes retains the last good mounted data if a source cannot be
			// read. Recheck while awake; this must never wake a sleeping pod.
			slog.Warn("cannot refresh mounted volume; retaining previous projection", "pod", e.pod.Namespace+"/"+e.pod.Name, "volume", volume.Name, "error", err)
		}
	}
	return nil
}

func (p *provider) readVolumeSources(ctx context.Context, e *podEntry) (map[string]map[string]volumeFile, error) {
	projections := make(map[string]map[string]volumeFile)
	for _, volume := range mountedVolumes(e.pod) {
		if volume.ConfigMap != nil || volume.Secret != nil {
			files, err := resolveProjectedVolume(ctx, p.client, e.pod.Namespace, volume)
			if err != nil {
				return nil, fmt.Errorf("resolve volume %s: %w", volume.Name, err)
			}
			projections[volume.Name] = files
		}
	}
	if err := validateProjectedSubpaths(e.pod, projections); err != nil {
		return nil, err
	}
	return projections, nil
}

// Missing required sources leave a strict pod Pending without allocating a VM.
func (p *provider) waitForVolumeSources(ctx context.Context, e *podEntry) error {
	for {
		check, cancel := context.WithTimeout(ctx, 10*time.Second)
		projections, err := p.readVolumeSources(check, e)
		cancel()
		if err == nil {
			e.op.Lock()
			if err := ctx.Err(); err != nil {
				e.op.Unlock()
				return err
			}
			e.initialVolumes = projections
			e.mu.Lock()
			if e.pod.Status.Reason == "FailedMount" {
				e.pod.Status.Reason, e.pod.Status.Message = "NativeVMStarting", ""
			}
			e.mu.Unlock()
			e.op.Unlock()
			return nil
		}
		e.op.Lock()
		if err := ctx.Err(); err != nil {
			e.op.Unlock()
			return err
		}
		e.mu.Lock()
		e.pod.Status.Reason, e.pod.Status.Message = "FailedMount", err.Error()
		e.mu.Unlock()
		p.publish(e)
		e.op.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func volumeSetupScript(volume corev1.Volume, memoryMiB int) string {
	root := quote(guestVolumePath(volume.Name))
	script := "umask 022; mkdir -p /var/lib/floci-volumes; chmod 0700 /var/lib/floci-volumes; "
	if volume.EmptyDir != nil && volume.EmptyDir.Medium == corev1.StorageMediumDefault {
		// Only set the initial mode: apps may change permissions between restarts.
		return script + "if ! test -d " + root + "; then mkdir " + root + "; chmod 0777 " + root + "; fi"
	}
	script += "mkdir -p " + root + "; "
	if volume.Secret != nil || volume.EmptyDir != nil {
		limit, mode := int64(memoryMiB)*bytesPerMiB, "0755"
		if volume.EmptyDir != nil {
			mode = "0777"
			if volume.EmptyDir.SizeLimit != nil {
				limit = volume.EmptyDir.SizeLimit.Value()
			}
		}
		script += fmt.Sprintf("if ! mountpoint -q %s; then mount -t tmpfs -o size=%d,mode=%s,nosuid,nodev tmpfs %s; fi", root, limit, mode, root)
	}
	return script
}

func volumeMountArgs(pod *corev1.Pod) []string {
	return volumeMountArgsForContainer(pod, pod.Spec.Containers[0])
}

func volumeMountArgsForContainer(pod *corev1.Pod, container corev1.Container) []string {
	volumes := make(map[string]corev1.Volume, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	var args []string
	for index, mount := range container.VolumeMounts {
		volume := volumes[mount.Name]
		mode := "rw"
		if mount.ReadOnly || volume.ConfigMap != nil || volume.Secret != nil {
			mode = "ro"
		}
		source := guestVolumePath(mount.Name)
		if mount.SubPath != "" || mount.SubPathExpr != "" {
			source = guestSubpathPath(container.Name, index)
		}
		args = append(args, "--mount", "type=bind,src="+source+",dst="+mount.MountPath+",options=rbind:rprivate:"+mode)
	}
	return args
}

func (p *provider) writeProjection(ctx context.Context, e *podEntry, name string, files map[string]volumeFile) error {
	archive, err := projectionArchive(files)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(archive)
	if previous, ok := e.volumeDigests[name]; ok && previous == digest {
		return nil
	}
	script := projectionScript(guestVolumePath(name), "..floci-"+rand.Text(), files)
	if err := p.runner.guestInput(ctx, e.vm, script, archive); err != nil {
		return err
	}
	if e.volumeDigests == nil {
		e.volumeDigests = make(map[string][sha256.Size]byte)
	}
	e.volumeDigests[name] = digest
	return nil
}

// No source bytes are included in argv, diagnostics, or the lifecycle journal.
// The tar stream preserves arbitrary binary data and per-key POSIX modes.
func projectionArchive(files map[string]volumeFile) ([]byte, error) {
	var buffer bytes.Buffer
	archive := tar.NewWriter(&buffer)
	directories := make(map[string]bool)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		if err := validateProjectionPath(name); err != nil {
			return nil, err
		}
		if files[name].Mode < 0 || files[name].Mode > 0o777 {
			return nil, fmt.Errorf("invalid projected file mode for %q", name)
		}
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			directories[parent] = true
		}
	}
	for _, directory := range slices.Sorted(maps.Keys(directories)) {
		if err := archive.WriteHeader(&tar.Header{Name: directory + "/", Typeflag: tar.TypeDir, Mode: 0755}); err != nil {
			return nil, err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(files)) {
		file := files[name]
		if err := archive.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: int64(file.Mode), Size: int64(len(file.Data))}); err != nil {
			return nil, err
		}
		if _, err := archive.Write(file.Data); err != nil {
			return nil, err
		}
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Like kubelet's atomic writer, stable top-level links follow a single ..data
// link. Replacing it switches all existing keys without rewriting open files.
func projectionScript(root, generation string, files map[string]volumeFile) string {
	tops := make(map[string]bool)
	for name := range files {
		top, _, _ := strings.Cut(name, "/")
		tops[top] = true
	}
	script := "cd " + quote(root) + "; umask 022; generation=" + quote(generation) + "; " +
		"mkdir \"$generation\"; committed=0; " +
		"trap 'if test \"$committed\" = 0; then rm -rf -- \"$generation\"; fi; rm -f ..data_tmp' EXIT; " +
		"tar -xf - -C \"$generation\"; chmod 0755 \"$generation\"; " +
		"ln -s \"$generation\" ..data_tmp; mv -Tf ..data_tmp ..data; committed=1; "
	for _, top := range slices.Sorted(maps.Keys(tops)) {
		script += "if ! test -L " + quote(top) + "; then ln -s -- " + quote("..data/"+top) + " " + quote(top) + "; fi; "
	}
	script += "for entry in * .[!.]* ..?*; do " +
		"case \"$entry\" in ..data|..data_tmp|..floci-*) continue;; esac; " +
		"if test -L \"$entry\" && ! test -e \"$generation/$entry\"; then rm -- \"$entry\"; fi; done; " +
		projectionGenerationCleanupScript(root, guestSubpathsRoot)
	return script
}
