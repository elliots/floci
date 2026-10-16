package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const guestFS = "/var/lib/floci-tools/guestfs"

func validatePodGroups(sc *corev1.PodSecurityContext) error {
	groups := slices.Clone(sc.SupplementalGroups)
	if sc.FSGroup != nil {
		groups = append(groups, *sc.FSGroup)
	}
	for _, group := range groups {
		if problems := validation.IsValidGroupID(group); len(problems) != 0 {
			return fmt.Errorf("pod group: %s", strings.Join(problems, "; "))
		}
	}
	if sc.FSGroupChangePolicy != nil && *sc.FSGroupChangePolicy != corev1.FSGroupChangeAlways && *sc.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		return errors.New("fsGroupChangePolicy must be Always or OnRootMismatch")
	}
	if sc.SupplementalGroupsPolicy != nil && *sc.SupplementalGroupsPolicy != corev1.SupplementalGroupsPolicyMerge && *sc.SupplementalGroupsPolicy != corev1.SupplementalGroupsPolicyStrict {
		return errors.New("supplementalGroupsPolicy must be Merge or Strict")
	}
	return nil
}

func requiresGuestFS(pod *corev1.Pod) bool {
	sc := pod.Spec.SecurityContext
	return sc != nil && (sc.FSGroup != nil || len(sc.SupplementalGroups) != 0 || sc.SupplementalGroupsPolicy != nil && *sc.SupplementalGroupsPolicy == corev1.SupplementalGroupsPolicyStrict)
}

func readGuestFS(name string) ([]byte, error) {
	if name == "" {
		return nil, errors.New("fsGroup and supplemental groups require --guest-fs-binary")
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 16<<20+1))
	if err != nil {
		return nil, err
	}
	if len(data) < 64 || len(data) > 16<<20 || !bytes.Equal(data[:4], []byte{127, 'E', 'L', 'F'}) || data[4] != 2 || data[5] != 1 {
		return nil, errors.New("guestfs must be a Linux ELF64 binary of at most 16 MiB")
	}
	machine := binary.LittleEndian.Uint16(data[18:20])
	if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" || runtime.GOARCH == "arm64" && machine != 183 || runtime.GOARCH == "amd64" && machine != 62 {
		return nil, errors.New("guestfs architecture differs from the host")
	}
	return data, nil
}

func (p *provider) installGuestFS(ctx context.Context, e *podEntry) error {
	data, err := readGuestFS(p.runner.cfg.guestFSBinary)
	if err != nil {
		return err
	}
	want := fmt.Sprintf("%x", sha256.Sum256(data))
	current, err := p.runner.guest(ctx, e.vm, "if test -x "+guestFS+"; then sha256sum "+guestFS+" | cut -d' ' -f1; fi")
	if err != nil || current == want {
		return err
	}
	return p.runner.guestInput(ctx, e.vm, "umask 077; mkdir -p /var/lib/floci-tools; chmod 0700 /var/lib/floci-tools; cat > "+guestFS+".pending; chmod 0700 "+guestFS+".pending; mv "+guestFS+".pending "+guestFS, data)
}

func containerGroupWrapper(pod *corev1.Pod, state string) string {
	sc := pod.Spec.SecurityContext
	if sc == nil {
		sc = &corev1.PodSecurityContext{}
	}
	groups := slices.Clone(sc.SupplementalGroups)
	if sc.FSGroup != nil {
		groups = append(groups, *sc.FSGroup)
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	ids := make([]string, len(groups))
	for i, id := range groups {
		ids[i] = strconv.FormatInt(id, 10)
	}
	policy := "Merge"
	if sc.SupplementalGroupsPolicy != nil {
		policy = string(*sc.SupplementalGroupsPolicy)
	}
	return "#!/bin/sh\nexec " + guestFS + " runc --groups " + quote(strings.Join(ids, ",")) + " --policy " + quote(policy) + " --state " + quote(state) + " -- \"$@\"\n"
}

func (p *provider) prepareContainerGroups(ctx context.Context, e *podEntry, task string) ([]string, error) {
	if !requiresGuestFS(e.pod) && p.runner.cfg.guestFSBinary == "" {
		return nil, nil
	}
	if err := p.installGuestFS(ctx, e); err != nil {
		return nil, err
	}
	// A runtime binary is stored per container by containerd, so exec and probes
	// invoke the same fixed group policy as create, including after RAM restore.
	name := groupRuntimePath(task)
	if err := p.runner.guestInput(ctx, e.vm, "umask 077; rm -f "+quote(name+".groups.json")+"; cat > "+quote(name+".pending")+"; chmod 0700 "+quote(name+".pending")+"; mv "+quote(name+".pending")+" "+quote(name), []byte(containerGroupWrapper(e.pod, name+".groups.json"))); err != nil {
		return nil, err
	}
	return []string{"--runc-binary", name}, nil
}

func groupRuntimePath(task string) string {
	return guestFS + "-runc-" + fmt.Sprintf("%x", sha256.Sum256([]byte(task)))[:16]
}

func (p *provider) observeContainerGroups(ctx context.Context, e *podEntry, task string, status *corev1.ContainerStatus) error {
	if !requiresGuestFS(e.pod) && p.runner.cfg.guestFSBinary == "" || status.User != nil {
		return nil
	}
	output, err := p.runner.guest(ctx, e.vm, "if test -f "+quote(groupRuntimePath(task)+".groups.json")+"; then cat "+quote(groupRuntimePath(task)+".groups.json")+"; fi")
	if err != nil || output == "" {
		return err
	}
	var identity struct {
		UID    int64   `json:"uid"`
		GID    int64   `json:"gid"`
		Groups []int64 `json:"additionalGids"`
	}
	if err := json.Unmarshal([]byte(output), &identity); err != nil {
		return fmt.Errorf("invalid container group identity: %w", err)
	}
	e.mu.Lock()
	status.User = &corev1.ContainerUser{Linux: &corev1.LinuxContainerUser{UID: identity.UID, GID: identity.GID, SupplementalGroups: identity.Groups}}
	e.mu.Unlock()
	return nil
}

func volumeOwnership(pod *corev1.Pod, root string, readonly, rootOnly bool) string {
	if pod == nil || pod.Spec.SecurityContext == nil || pod.Spec.SecurityContext.FSGroup == nil {
		return ""
	}
	args := []string{guestFS, "ownership", "--group", strconv.FormatInt(*pod.Spec.SecurityContext.FSGroup, 10)}
	if readonly {
		args = append(args, "--read-only")
	}
	if rootOnly {
		args = append(args, "--root-only")
	}
	return joinCommand(append(args, "--", root)) + "; "
}
