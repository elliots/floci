package main

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"reflect"
	"slices"
	"strings"
	"unicode"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

type volumeFile struct {
	Data []byte
	Mode int32
}

func guestVolumePath(volumeName string) string {
	return "/var/lib/floci-volumes/" + volumeName
}

func validateVolumes(pod *corev1.Pod) error {
	return validateVolumesWithCA(pod, "")
}

func validateVolumesWithCA(pod *corev1.Pod, caPath string) error {
	allocation, err := derivePodAllocation(pod)
	if err != nil {
		return err
	}
	volumes := make(map[string]corev1.Volume, len(pod.Spec.Volumes))
	for _, volume := range pod.Spec.Volumes {
		if _, found := volumes[volume.Name]; found {
			return fmt.Errorf("duplicate volume %q", volume.Name)
		}
		if err := validateVolumeWithCA(volume, allocation.MemoryMiB, caPath); err != nil {
			return fmt.Errorf("volume %q: %w", volume.Name, err)
		}
		volumes[volume.Name] = volume
	}
	for _, container := range slices.Concat(pod.Spec.InitContainers, pod.Spec.Containers) {
		if len(container.VolumeDevices) != 0 {
			return fmt.Errorf("volume devices are not qualified")
		}
		mounts := make([]string, 0, len(container.VolumeMounts))
		for _, mount := range container.VolumeMounts {
			if _, found := volumes[mount.Name]; !found {
				return fmt.Errorf("mount references undefined volume %q", mount.Name)
			}
			if volumes[mount.Name].HostPath != nil && (!mount.ReadOnly || mount.MountPath != reservedCAPath || mount.SubPath != "" || mount.SubPathExpr != "") {
				return fmt.Errorf("volume %q: the public CA requires its reserved read-only mount path without subPath", mount.Name)
			}
			if len(mount.BindMountOptions) != 0 ||
				mount.MountPropagation != nil && *mount.MountPropagation != corev1.MountPropagationNone ||
				mount.RecursiveReadOnly != nil && *mount.RecursiveReadOnly != corev1.RecursiveReadOnlyDisabled {
				return fmt.Errorf("volume %q: mount propagation, recursive read-only and bind mount options are not qualified", mount.Name)
			}
			if _, err := resolveMountSubpath(mount, container); err != nil {
				return fmt.Errorf("volume %q: %w", mount.Name, err)
			}
			if !path.IsAbs(mount.MountPath) || path.Clean(mount.MountPath) != mount.MountPath || mount.MountPath == "/" || unsafeMountCharacters(mount.MountPath) {
				return fmt.Errorf("volume %q: mountPath must be a clean absolute path without control characters, commas or double quotes", mount.Name)
			}
			for _, reserved := range []string{"/proc", "/sys", "/dev", "/etc/resolv.conf"} {
				if pathsOverlap(mount.MountPath, reserved) {
					return fmt.Errorf("volume %q: mountPath conflicts with runtime path %s", mount.Name, reserved)
				}
			}
			for _, other := range mounts {
				if pathsOverlap(mount.MountPath, other) {
					return fmt.Errorf("volume %q: overlapping mount paths are not qualified", mount.Name)
				}
			}
			mounts = append(mounts, mount.MountPath)
		}
	}
	return nil
}

func validateVolume(volume corev1.Volume, memoryMiB int) error {
	return validateVolumeWithCA(volume, memoryMiB, "")
}

func validateVolumeWithCA(volume corev1.Volume, memoryMiB int, caPath string) error {
	if len(validation.IsDNS1123Label(volume.Name)) != 0 {
		return fmt.Errorf("name must be a DNS label")
	}
	if volume.HostPath != nil {
		source := volume.HostPath
		remaining := volume.VolumeSource
		remaining.HostPath = nil
		if caPath != reservedCAPath || source.Path != caPath || source.Type == nil || *source.Type != corev1.HostPathFile ||
			!reflect.DeepEqual(remaining, corev1.VolumeSource{}) {
			return fmt.Errorf("hostPath is limited to the configured public network CA file")
		}
		return nil
	}
	if volume.Projected != nil {
		remaining := volume.VolumeSource
		remaining.Projected = nil
		if !reflect.DeepEqual(remaining, corev1.VolumeSource{}) {
			return fmt.Errorf("exactly one supported volume source is required")
		}
		return validateCombinedProjection(volume)
	}
	remaining := volume.VolumeSource
	remaining.EmptyDir, remaining.ConfigMap, remaining.Secret = nil, nil, nil
	if !reflect.DeepEqual(remaining, corev1.VolumeSource{}) {
		return fmt.Errorf("only emptyDir, ConfigMap and Secret volumes are qualified")
	}
	count := 0
	if volume.EmptyDir != nil {
		count++
	}
	if volume.ConfigMap != nil {
		count++
	}
	if volume.Secret != nil {
		count++
	}
	if count != 1 {
		return fmt.Errorf("exactly one supported volume source is required")
	}
	if volume.EmptyDir != nil {
		source := volume.EmptyDir
		if source.Medium != corev1.StorageMediumDefault && source.Medium != corev1.StorageMediumMemory {
			return fmt.Errorf("only disk and Memory emptyDir media are qualified")
		}
		if source.Mode != nil {
			return fmt.Errorf("emptyDir mode overrides are not qualified")
		}
		if source.SizeLimit != nil {
			if source.Medium != corev1.StorageMediumMemory {
				return fmt.Errorf("disk emptyDir sizeLimit requires storage quotas, which are not qualified")
			}
			if source.SizeLimit.Sign() <= 0 || source.SizeLimit.Cmp(*resource.NewQuantity(int64(memoryMiB)*bytesPerMiB, resource.BinarySI)) > 0 {
				return fmt.Errorf("Memory emptyDir sizeLimit must be positive and at most the guest's %d MiB allocation", memoryMiB)
			}
		}
		return nil
	}
	if volume.ConfigMap != nil && volume.ConfigMap.DefaultUser != nil || volume.Secret != nil && volume.Secret.DefaultUser != nil {
		return fmt.Errorf("projected volume ownership overrides are not qualified")
	}
	name, items, mode, _ := projectionSettings(volume)
	if len(validation.IsDNS1123Subdomain(name)) != 0 {
		return fmt.Errorf("source name must be a DNS subdomain")
	}
	if mode < 0 || mode > 0o777 {
		return fmt.Errorf("defaultMode must be between 0000 and 0777")
	}
	paths := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item.User != nil {
			return fmt.Errorf("projected item ownership overrides are not qualified")
		}
		if len(validation.IsConfigMapKey(item.Key)) != 0 {
			return fmt.Errorf("invalid projection key %q", item.Key)
		}
		if err := validateProjectionPath(item.Path); err != nil {
			return err
		}
		if item.Mode != nil && (*item.Mode < 0 || *item.Mode > 0o777) {
			return fmt.Errorf("item mode must be between 0000 and 0777")
		}
		if _, found := paths[item.Path]; found {
			return fmt.Errorf("duplicate projection path %q", item.Path)
		}
		paths[item.Path] = struct{}{}
	}
	for name := range paths {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, found := paths[parent]; found {
				return fmt.Errorf("projection paths %q and %q conflict", parent, name)
			}
		}
	}
	return nil
}

func projectionSettings(volume corev1.Volume) (name string, items []corev1.KeyToPath, mode int32, optional bool) {
	var defaultMode *int32
	var optionalSource *bool
	if volume.ConfigMap != nil {
		source := volume.ConfigMap
		name, items, defaultMode, optionalSource = source.Name, source.Items, source.DefaultMode, source.Optional
	} else {
		source := volume.Secret
		name, items, defaultMode, optionalSource = source.SecretName, source.Items, source.DefaultMode, source.Optional
	}
	mode = 0o644
	if defaultMode != nil {
		mode = *defaultMode
	}
	optional = optionalSource != nil && *optionalSource
	return
}

func resolveProjectedVolume(ctx context.Context, client kubernetes.Interface, namespace string, volume corev1.Volume) (map[string]volumeFile, error) {
	if err := validateVolume(volume, maxGuestMemoryMiB); err != nil {
		return nil, err
	}
	if volume.ConfigMap == nil && volume.Secret == nil {
		return nil, fmt.Errorf("volume %q is not a ConfigMap or Secret", volume.Name)
	}
	if client == nil {
		return nil, fmt.Errorf("volume %q requires a Kubernetes client", volume.Name)
	}
	name, items, mode, optional := projectionSettings(volume)
	data := make(map[string][]byte)
	if volume.ConfigMap != nil {
		source, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if optional && apierrors.IsNotFound(err) {
				return map[string]volumeFile{}, nil
			}
			return nil, fmt.Errorf("read ConfigMap %s/%s for volume %s: %w", namespace, name, volume.Name, err)
		}
		for key, value := range source.Data {
			data[key] = []byte(value)
		}
		for key, value := range source.BinaryData {
			if _, found := data[key]; found {
				return nil, fmt.Errorf("ConfigMap %s/%s has duplicate text and binary key %q", namespace, name, key)
			}
			data[key] = value
		}
	} else {
		source, err := client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if optional && apierrors.IsNotFound(err) {
				return map[string]volumeFile{}, nil
			}
			return nil, fmt.Errorf("read Secret %s/%s for volume %s: %w", namespace, name, volume.Name, err)
		}
		data = source.Data
	}
	files := make(map[string]volumeFile)
	if len(items) == 0 {
		for key, value := range data {
			if len(validation.IsConfigMapKey(key)) != 0 {
				return nil, fmt.Errorf("volume %q: invalid source key %q", volume.Name, key)
			}
			if err := validateProjectionPath(key); err != nil {
				return nil, err
			}
			files[key] = volumeFile{Data: bytes.Clone(value), Mode: mode}
		}
		return files, nil
	}
	for _, item := range items {
		value, found := data[item.Key]
		if !found {
			if optional {
				continue
			}
			return nil, fmt.Errorf("volume %q: required key %q is missing from source %q", volume.Name, item.Key, name)
		}
		fileMode := mode
		if item.Mode != nil {
			fileMode = *item.Mode
		}
		files[item.Path] = volumeFile{Data: bytes.Clone(value), Mode: fileMode}
	}
	return files, nil
}

func validateProjectionPath(value string) error {
	if value == "" || value == "." || path.IsAbs(value) || path.Clean(value) != value || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("projection path %q must be a clean relative path", value)
	}
	for part := range strings.SplitSeq(value, "/") {
		if strings.HasPrefix(part, "..") {
			return fmt.Errorf("projection path %q contains a reserved component", value)
		}
	}
	return nil
}

func unsafeMountCharacters(value string) bool {
	return strings.ContainsAny(value, ",\"") || strings.IndexFunc(value, unicode.IsControl) >= 0
}

func pathsOverlap(first, second string) bool {
	return first == second || strings.HasPrefix(first, second+"/") || strings.HasPrefix(second, first+"/")
}
