package main

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Linux capabilities through CAP_CHECKPOINT_RESTORE, supported by the pinned
// containerd/runc guest. ctr does not accept the Kubernetes ALL shorthand.
var linuxCapabilities = []string{
	"CHOWN", "DAC_OVERRIDE", "DAC_READ_SEARCH", "FOWNER", "FSETID", "KILL",
	"SETGID", "SETUID", "SETPCAP", "LINUX_IMMUTABLE", "NET_BIND_SERVICE",
	"NET_BROADCAST", "NET_ADMIN", "NET_RAW", "IPC_LOCK", "IPC_OWNER",
	"SYS_MODULE", "SYS_RAWIO", "SYS_CHROOT", "SYS_PTRACE", "SYS_PACCT",
	"SYS_ADMIN", "SYS_BOOT", "SYS_NICE", "SYS_RESOURCE", "SYS_TIME",
	"SYS_TTY_CONFIG", "MKNOD", "LEASE", "AUDIT_WRITE", "AUDIT_CONTROL",
	"SETFCAP", "MAC_OVERRIDE", "MAC_ADMIN", "SYSLOG", "WAKE_ALARM",
	"BLOCK_SUSPEND", "AUDIT_READ", "PERFMON", "BPF", "CHECKPOINT_RESTORE",
}

func validateSecurityContexts(pod *corev1.Pod) error {
	if sc := pod.Spec.SecurityContext; sc != nil {
		if err := validateSecurityIDs(sc.RunAsUser, sc.RunAsGroup); err != nil {
			return fmt.Errorf("pod securityContext: %w", err)
		}
		if err := validateSeccompProfile(sc.SeccompProfile); err != nil {
			return fmt.Errorf("pod securityContext: %w", err)
		}
		if err := validatePodGroups(sc); err != nil {
			return fmt.Errorf("pod securityContext: %w", err)
		}
		remaining := *sc
		remaining.RunAsUser, remaining.RunAsGroup, remaining.RunAsNonRoot = nil, nil, nil
		remaining.SeccompProfile = nil
		remaining.FSGroup, remaining.FSGroupChangePolicy = nil, nil
		remaining.SupplementalGroups, remaining.SupplementalGroupsPolicy = nil, nil
		if len(remaining.Sysctls) == 0 {
			remaining.Sysctls = nil
		}
		if !reflect.DeepEqual(remaining, corev1.PodSecurityContext{}) {
			return fmt.Errorf("pod securityContext supports runAsUser, runAsGroup, runAsNonRoot, fsGroup, fsGroupChangePolicy, supplementalGroups, supplementalGroupsPolicy and RuntimeDefault/Unconfined seccomp; SELinux, AppArmor, sysctls and Windows options are not qualified")
		}
	}
	for container := range podContainers(pod) {
		if err := validateContainerSecurity(container.SecurityContext); err != nil {
			return fmt.Errorf("container %s securityContext: %w", container.Name, err)
		}
		uid, _, nonRoot := effectiveSecurityIdentity(pod, container)
		if nonRoot != nil && *nonRoot && uid != nil && *uid == 0 {
			return fmt.Errorf("container %s runAsUser 0 conflicts with runAsNonRoot", container.Name)
		}
	}
	return nil
}

func validateSecurityIDs(uid, gid *int64) error {
	if uid != nil {
		if problems := validation.IsValidUserID(*uid); len(problems) != 0 {
			return fmt.Errorf("runAsUser: %s", strings.Join(problems, "; "))
		}
	}
	if gid != nil {
		if problems := validation.IsValidGroupID(*gid); len(problems) != 0 {
			return fmt.Errorf("runAsGroup: %s", strings.Join(problems, "; "))
		}
	}
	return nil
}

func validateContainerSecurity(sc *corev1.SecurityContext) error {
	if sc == nil {
		return nil
	}
	if err := validateSecurityIDs(sc.RunAsUser, sc.RunAsGroup); err != nil {
		return err
	}
	if sc.Privileged != nil && *sc.Privileged {
		return fmt.Errorf("privileged containers are not qualified")
	}
	if sc.ProcMount != nil && *sc.ProcMount != corev1.DefaultProcMount {
		return fmt.Errorf("only the default proc mount is qualified")
	}
	if _, err := securityCapabilityArgs(sc.Capabilities); err != nil {
		return err
	}
	if err := validateSeccompProfile(sc.SeccompProfile); err != nil {
		return err
	}
	remaining := *sc
	remaining.RunAsUser, remaining.RunAsGroup, remaining.RunAsNonRoot = nil, nil, nil
	remaining.ReadOnlyRootFilesystem, remaining.AllowPrivilegeEscalation = nil, nil
	remaining.Privileged, remaining.ProcMount, remaining.Capabilities = nil, nil, nil
	remaining.SeccompProfile = nil
	if !reflect.DeepEqual(remaining, corev1.SecurityContext{}) {
		return fmt.Errorf("SELinux, AppArmor and Windows security options are not qualified")
	}
	return nil
}

func validateSeccompProfile(profile *corev1.SeccompProfile) error {
	if profile == nil {
		return nil
	}
	if profile.LocalhostProfile != nil {
		return fmt.Errorf("seccomp localhost profile paths are not qualified")
	}
	switch profile.Type {
	case corev1.SeccompProfileTypeRuntimeDefault, corev1.SeccompProfileTypeUnconfined:
		return nil
	default:
		return fmt.Errorf("seccomp profile %q is not qualified; use RuntimeDefault or Unconfined", profile.Type)
	}
}

func effectiveSecurityIdentity(pod *corev1.Pod, container corev1.Container) (uid, gid *int64, nonRoot *bool) {
	if sc := pod.Spec.SecurityContext; sc != nil {
		uid, gid, nonRoot = sc.RunAsUser, sc.RunAsGroup, sc.RunAsNonRoot
	}
	if sc := container.SecurityContext; sc != nil {
		if sc.RunAsUser != nil {
			uid = sc.RunAsUser
		}
		if sc.RunAsGroup != nil {
			gid = sc.RunAsGroup
		}
		if sc.RunAsNonRoot != nil {
			nonRoot = sc.RunAsNonRoot
		}
	}
	return uid, gid, nonRoot
}

func securityArgs(pod *corev1.Pod, container corev1.Container, imageUser string) ([]string, error) {
	if err := validateSecurityContexts(pod); err != nil {
		return nil, err
	}
	uid, gid, nonRoot := effectiveSecurityIdentity(pod, container)
	user, _, _ := strings.Cut(imageUser, ":")
	if uid != nil {
		user = strconv.FormatInt(*uid, 10)
	}
	if user == "" {
		user = "0"
	}
	if nonRoot != nil && *nonRoot {
		value, err := strconv.ParseInt(user, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("container %s runAsNonRoot cannot verify non-numeric image user %q", container.Name, user)
		}
		if value <= 0 || value > math.MaxInt32 {
			return nil, fmt.Errorf("container %s runAsNonRoot requires a user ID between 1 and %d", container.Name, math.MaxInt32)
		}
	}
	var args []string
	if uid != nil || gid != nil {
		if gid != nil {
			user += ":" + strconv.FormatInt(*gid, 10)
		}
		args = append(args, "--user", user)
	}
	sc := container.SecurityContext
	var seccompProfile *corev1.SeccompProfile
	if pod.Spec.SecurityContext != nil {
		seccompProfile = pod.Spec.SecurityContext.SeccompProfile
	}
	if sc != nil && sc.SeccompProfile != nil {
		seccompProfile = sc.SeccompProfile
	}
	if seccompProfile != nil && seccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault {
		// Both ctr and containerd CRI use seccomp.WithDefaultProfile(). Omitted
		// and Unconfined profiles preserve the normal non-seccompDefault case.
		args = append(args, "--seccomp")
	}
	// Kubernetes defaults allowPrivilegeEscalation to true, while ctr defaults
	// NoNewPrivileges to true. Never pass --allow-new-privs=false: ctr tests IsSet.
	if sc == nil || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		args = append(args, "--allow-new-privs")
	}
	if sc != nil {
		if sc.ReadOnlyRootFilesystem != nil && *sc.ReadOnlyRootFilesystem {
			args = append(args, "--read-only")
		}
		capArgs, err := securityCapabilityArgs(sc.Capabilities)
		if err != nil {
			return nil, err
		}
		args = append(args, capArgs...)
	}
	return args, nil
}

func securityCapabilityArgs(capabilities *corev1.Capabilities) ([]string, error) {
	if capabilities == nil {
		return nil, nil
	}
	add, drop := make(map[string]bool), make(map[string]bool)
	for _, group := range []struct {
		values []corev1.Capability
		names  map[string]bool
	}{{capabilities.Add, add}, {capabilities.Drop, drop}} {
		for _, value := range group.values {
			name := strings.ToUpper(string(value))
			if name != "ALL" && !slices.Contains(linuxCapabilities, name) {
				return nil, fmt.Errorf("unsupported Linux capability %q; use Kubernetes names without CAP_", value)
			}
			group.names[name] = true
		}
	}
	var args []string
	for _, name := range linuxCapabilities {
		// CRI applies ALL first, then named additions, then named drops. ctr
		// applies additions before drops, so exclude added names from drop ALL.
		if add[name] || add["ALL"] && !drop["ALL"] {
			args = append(args, "--cap-add", "CAP_"+name)
		}
		if drop[name] || drop["ALL"] && !add[name] {
			args = append(args, "--cap-drop", "CAP_"+name)
		}
	}
	return args, nil
}
