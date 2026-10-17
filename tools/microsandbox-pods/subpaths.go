package main

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const guestSubpathsRoot = "/var/lib/floci-subpaths"

func guestSubpathPath(containerName string, mountIndex int) string {
	return guestSubpathsRoot + "/" + containerName + "/" + strconv.Itoa(mountIndex)
}

func resolveMountSubpath(mount corev1.VolumeMount, container corev1.Container) (string, error) {
	if mount.SubPath == "" && mount.SubPathExpr == "" {
		return "", nil
	}
	if len(validation.IsDNS1123Label(container.Name)) != 0 {
		return "", fmt.Errorf("subPath requires a valid container name")
	}
	if mount.SubPath != "" && mount.SubPathExpr != "" {
		return "", fmt.Errorf("subPath and subPathExpr are mutually exclusive")
	}
	value := mount.SubPath
	if mount.SubPathExpr != "" {
		var err error
		value, err = expandSubpathExpression(mount.SubPathExpr, container.Env)
		if err != nil {
			return "", err
		}
	}
	if value == "" || value == "." || path.IsAbs(value) || path.Clean(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("subPath must expand to a nonempty clean relative path without control characters")
	}
	for component := range strings.SplitSeq(value, "/") {
		if component == ".." {
			return "", fmt.Errorf("subPath cannot contain parent directory traversal")
		}
	}
	return value, nil
}

// Unlike command expansion, Kubernetes rejects missing or empty subPathExpr
// variables. Escaped dollars stay literal and expanded values are not rescanned.
func expandSubpathExpression(expression string, environment []corev1.EnvVar) (string, error) {
	values := make(map[string]string, len(environment))
	for _, variable := range environment {
		values[variable.Name] = variable.Value
	}
	var result strings.Builder
	for index := 0; index < len(expression); index++ {
		if expression[index] == '$' && index+1 < len(expression) {
			switch expression[index+1] {
			case '$':
				result.WriteByte('$')
				index++
				continue
			case '(':
				if end := strings.IndexByte(expression[index+2:], ')'); end >= 0 {
					end += index + 2
					name := expression[index+2 : end]
					if values[name] == "" {
						return "", fmt.Errorf("subPathExpr requires a nonempty value for environment variable %q", name)
					}
					result.WriteString(values[name])
					index = end
					continue
				}
			}
		}
		result.WriteByte(expression[index])
	}
	return result.String(), nil
}

// Required file and directory subpaths are checked with their projections before
// admission, so a missing key keeps a strict Pod Pending without creating a VM.
func validateProjectedSubpaths(pod *corev1.Pod, projections map[string]map[string]volumeFile) error {
	for _, container := range slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers) {
		for _, mount := range container.VolumeMounts {
			files, projected := projections[mount.Name]
			if !projected {
				continue
			}
			subpath, err := resolveMountSubpath(mount, container)
			if err != nil {
				return err
			}
			if subpath == "" {
				continue
			}
			found := false
			for name := range files {
				if name == subpath || strings.HasPrefix(name, subpath+"/") {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("container %s: volume %s is missing the requested subPath", container.Name, mount.Name)
			}
		}
	}
	return nil
}

// The caller holds e.op and starts this only after the container's previous task
// has stopped. Pod containers must not mutate the volume during path preparation.
func (p *provider) prepareContainerSubpaths(ctx context.Context, e *podEntry, container corev1.Container) error {
	volumes := make(map[string]corev1.Volume, len(e.pod.Spec.Volumes))
	for _, volume := range e.pod.Spec.Volumes {
		volumes[volume.Name] = volume
	}
	for index, mount := range container.VolumeMounts {
		subpath, err := resolveMountSubpath(mount, container)
		if err != nil {
			return fmt.Errorf("prepare subPath for container %s volume %s: %w", container.Name, mount.Name, err)
		}
		if subpath == "" {
			continue
		}
		volume, found := volumes[mount.Name]
		if !found {
			return fmt.Errorf("subPath references undefined volume %q", mount.Name)
		}
		createDirectories := volume.EmptyDir != nil || volume.PersistentVolumeClaim != nil && !volume.PersistentVolumeClaim.ReadOnly
		script := subpathSetupScript(guestVolumePath(volume.Name), guestSubpathPath(container.Name, index), subpath, createDirectories)
		if _, err := p.runner.guest(ctx, e.vm, script); err != nil {
			return fmt.Errorf("prepare subPath for container %s volume %s: %w", container.Name, mount.Name, err)
		}
	}
	return nil
}

func (p *provider) releaseContainerSubpaths(ctx context.Context, e *podEntry, container corev1.Container) error {
	for index, mount := range container.VolumeMounts {
		if mount.SubPath == "" && mount.SubPathExpr == "" {
			continue
		}
		script := subpathCleanupScript(guestSubpathPath(container.Name, index))
		if _, err := p.runner.guest(ctx, e.vm, script); err != nil {
			return fmt.Errorf("release subPath for container %s volume %s: %w", container.Name, mount.Name, err)
		}
	}
	return nil
}

func subpathCleanupScript(target string) string {
	return "target=" + quote(target) + "; " + subpathUnmountScript("/proc/self/mountinfo") +
		"rm -rf -- \"$target\"; rm -f -- \"$target.generation\""
}

// BusyBox mountpoint compares directory metadata and misses file bind mounts.
// Our owned target contains only DNS labels and an index, so mountinfo's fifth
// field has no escaped whitespace. Read errors abort before any target removal.
func subpathUnmountScript(mountinfo string) string {
	return "mounted=$(awk -v target=\"$target\" '$5 == target {print \"mounted\"; exit}' " + quote(mountinfo) + "); " +
		"if test \"$mounted\" = mounted; then /bin/busybox umount \"$target\"; fi; "
}

// The pinned inode is mounted through /proc/<pid>/fd, never through a path that
// a container can replace. BusyBox mount passes this source unchanged to mount(2).
// Only directories and regular files are accepted; FIFOs and devices are rejected
// before opening them. This executes between tasks, not alongside running apps.
func subpathSetupScript(root, target, subpath string, createDirectories bool) string {
	script := "umask 022; root=" + quote(root) + "; target=" + quote(target) + "; " +
		"test \"$(readlink -f \"$root\")\" = \"$root\"; " +
		"mkdir -p " + quote(path.Dir(target)) + "; chmod 0700 " + quote(path.Dir(path.Dir(target))) + "; " +
		subpathUnmountScript("/proc/self/mountinfo") +
		"rm -rf -- \"$target\"; rm -f -- \"$target.generation\"; exec 3<\"$root\"; "
	for component := range strings.SplitSeq(subpath, "/") {
		script += "next=\"/proc/$$/fd/3/\"" + quote(component) + "; "
		if createDirectories {
			script += "if ! test -e \"$next\"; then test ! -L \"$next\"; mkdir -m 0777 -- \"$next\"; fi; "
		}
		script += "if ! test -d \"$next\" && ! test -f \"$next\"; then echo 'subPath source must be an existing directory or regular file' >&2; exit 1; fi; " +
			"exec 4<\"$next\"; resolved=$(readlink \"/proc/$$/fd/4\"); " +
			"case \"$resolved\" in \"$root\"|\"$root\"/*) ;; *) echo 'subPath escapes its volume' >&2; exit 1;; esac; " +
			"exec 3<&4; exec 4<&-; "
	}
	script += "if test -d \"/proc/$$/fd/3\"; then mkdir \"$target\"; else : >\"$target\"; fi; " +
		"/bin/busybox mount -o bind \"/proc/$$/fd/3\" \"$target\"; "
	if !createDirectories {
		// A bind of a projected directory must retain its entire generation. A
		// bind alone keeps the directory inode but not files deleted below it.
		script += "relative=${resolved#\"$root\"/}; generation=${relative%%/*}; " +
			"case \"$generation\" in ..floci-*) ln -s -- \"$root/$generation\" \"$target.generation\";; " +
			"*) /bin/busybox umount \"$target\"; echo 'subPath projection is outside its data generation' >&2; exit 1;; esac; "
	}
	return script + "exec 3<&-"
}

func projectionGenerationCleanupScript(root, pinsRoot string) string {
	return "for old in ..floci-*; do if test \"$old\" != \"$generation\"; then keep=0; " +
		"for pin in " + quote(pinsRoot) + "/*/*.generation; do " +
		"if test -L \"$pin\" && test \"$(readlink \"$pin\")\" = " + quote(root) + "/\"$old\"; then keep=1; break; fi; done; " +
		"if test \"$keep\" = 0; then rm -rf -- \"$old\"; fi; fi; done"
}
