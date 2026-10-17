package main

import (
	"context"
	"fmt"
	"maps"
	"math/big"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"
)

func projectedData(volume corev1.Volume) bool {
	return volume.ConfigMap != nil || volume.Secret != nil || volume.Projected != nil || volume.DownwardAPI != nil
}

func projectedToken(volume corev1.Volume) bool {
	if volume.Projected != nil {
		for _, source := range volume.Projected.Sources {
			if source.ServiceAccountToken != nil {
				return true
			}
		}
	}
	return false
}

func projectedMode(source *corev1.ProjectedVolumeSource) int32 {
	if source.DefaultMode != nil {
		return *source.DefaultMode
	}
	return 0o644
}

func projectionSource(volume corev1.Volume, source corev1.VolumeProjection) corev1.Volume {
	result := corev1.Volume{Name: volume.Name}
	mode := projectedMode(volume.Projected)
	if source.ConfigMap != nil {
		value := source.ConfigMap
		result.ConfigMap = &corev1.ConfigMapVolumeSource{LocalObjectReference: value.LocalObjectReference,
			Items: value.Items, Optional: value.Optional, DefaultMode: new(mode)}
	}
	if source.Secret != nil {
		value := source.Secret
		result.Secret = &corev1.SecretVolumeSource{SecretName: value.Name, Items: value.Items,
			Optional: value.Optional, DefaultMode: new(mode)}
	}
	return result
}

func validateCombinedProjection(volume corev1.Volume) error {
	volume = normalizeDownwardVolume(volume)
	source := volume.Projected
	if projectedMode(source) < 0 || projectedMode(source) > 0o777 || source.DefaultUser != nil {
		return fmt.Errorf("projected defaultMode must be between 0000 and 0777; ownership overrides are not qualified")
	}
	if len(source.Sources) == 0 {
		return fmt.Errorf("projected volume requires at least one source")
	}
	for _, item := range source.Sources {
		remaining := item
		remaining.ConfigMap, remaining.Secret, remaining.DownwardAPI, remaining.ServiceAccountToken = nil, nil, nil, nil
		if !reflect.DeepEqual(remaining, corev1.VolumeProjection{}) {
			return fmt.Errorf("projected sources support ConfigMap, Secret, DownwardAPI metadata and serviceAccountToken")
		}
		count := 0
		for _, present := range []bool{item.ConfigMap != nil, item.Secret != nil, item.DownwardAPI != nil, item.ServiceAccountToken != nil} {
			if present {
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("each projection requires exactly one source")
		}
		if item.ConfigMap != nil || item.Secret != nil {
			if err := validateVolume(projectionSource(volume, item), maxGuestMemoryMiB); err != nil {
				return err
			}
		}
		if item.ServiceAccountToken != nil {
			if err := validateTokenProjection(item.ServiceAccountToken); err != nil {
				return err
			}
		}
		if item.DownwardAPI != nil {
			for _, file := range item.DownwardAPI.Items {
				if err := validateProjectionPath(file.Path); err != nil {
					return err
				}
				if file.User != nil || (file.ResourceFieldRef == nil) == (file.FieldRef == nil) {
					return fmt.Errorf("DownwardAPI projections require exactly one fieldRef or resourceFieldRef without ownership overrides")
				}
				if file.Mode != nil && (*file.Mode < 0 || *file.Mode > 0o777) {
					return fmt.Errorf("DownwardAPI item mode must be between 0000 and 0777")
				}
				if file.FieldRef != nil {
					if _, err := downwardMetadata(&corev1.Pod{}, file.FieldRef); err != nil {
						return err
					}
				} else if err := validateDownwardResource(file.ResourceFieldRef); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func downwardMetadata(pod *corev1.Pod, selector *corev1.ObjectFieldSelector) (string, error) {
	if selector.APIVersion != "" && selector.APIVersion != "v1" {
		return "", fmt.Errorf("DownwardAPI fieldRef requires apiVersion v1")
	}
	field, key, indexed := strings.Cut(selector.FieldPath, "['")
	var values map[string]string
	switch field {
	case "metadata.name":
		if !indexed {
			return pod.Name, nil
		}
	case "metadata.namespace":
		if !indexed {
			return pod.Namespace, nil
		}
	case "metadata.uid":
		if !indexed {
			return string(pod.UID), nil
		}
	case "metadata.labels":
		values = pod.Labels
	case "metadata.annotations":
		values = pod.Annotations
	default:
		return "", fmt.Errorf("DownwardAPI fieldRef supports Pod metadata only")
	}
	if field != "metadata.labels" && field != "metadata.annotations" {
		return "", fmt.Errorf("DownwardAPI metadata field does not support a key selector")
	}
	if indexed {
		var closed bool
		key, closed = strings.CutSuffix(key, "']")
		validated := key
		if field == "metadata.annotations" {
			validated = strings.ToLower(key)
		}
		if !closed || len(validation.IsQualifiedName(validated)) != 0 {
			return "", fmt.Errorf("DownwardAPI metadata key selector is invalid")
		}
		return values[key], nil
	}
	var lines []string
	for _, key := range slices.Sorted(maps.Keys(values)) {
		lines = append(lines, key+"="+strconv.Quote(values[key]))
	}
	return strings.Join(lines, "\n"), nil
}

func (p *provider) resolvePodProjection(ctx context.Context, e *podEntry, volume corev1.Volume) (map[string]volumeFile, error) {
	volume = normalizeDownwardVolume(volume)
	if volume.Projected == nil {
		return resolveProjectedVolume(ctx, p.client, e.pod.Namespace, volume)
	}
	if err := validateCombinedProjection(volume); err != nil {
		return nil, err
	}
	e.mu.Lock()
	pod := e.pod.DeepCopy()
	e.mu.Unlock()
	mode := projectedMode(volume.Projected)
	files := make(map[string]volumeFile)
	for _, source := range volume.Projected.Sources {
		if source.ConfigMap != nil || source.Secret != nil {
			values, err := resolveProjectedVolume(ctx, p.client, pod.Namespace, projectionSource(volume, source))
			if err != nil {
				return nil, err
			}
			maps.Copy(files, values)
		}
		if source.DownwardAPI != nil {
			for _, item := range source.DownwardAPI.Items {
				var value string
				var err error
				if item.FieldRef != nil {
					value, err = downwardMetadata(pod, item.FieldRef)
				} else {
					var capacity corev1.ResourceList
					capacity, err = p.runner.cfg.nodeCapacity()
					if err == nil {
						value, err = downwardResource(pod, item.ResourceFieldRef, capacity)
					}
				}
				if err != nil {
					return nil, err
				}
				fileMode := mode
				if item.Mode != nil {
					fileMode = *item.Mode
				}
				files[item.Path] = volumeFile{Data: []byte(value), Mode: fileMode}
			}
		}
		if source.ServiceAccountToken != nil {
			token, err := e.tokens.token(ctx, p.client, pod, source.ServiceAccountToken)
			if err != nil {
				return nil, err
			}
			files[source.ServiceAccountToken.Path] = volumeFile{Data: []byte(token), Mode: mode}
		}
	}
	for name := range files {
		for parent := path.Dir(name); parent != "."; parent = path.Dir(parent) {
			if _, conflict := files[parent]; conflict {
				return nil, fmt.Errorf("projected volume has overlapping file and directory paths")
			}
		}
	}
	return files, nil
}

func (p *provider) preflightWakeTokens(ctx context.Context, e *podEntry) error {
	e.tokens.clear()
	for _, volume := range mountedVolumes(e.pod) {
		if projectedToken(volume) {
			if _, err := p.resolvePodProjection(ctx, e, volume); err != nil {
				return fmt.Errorf("refresh Pod identity before wake: %w", err)
			}
		}
	}
	return nil
}

func (p *provider) refreshRecoveredTokens(ctx context.Context, e *podEntry) error {
	if e.state != "RUNNING" && e.state != "READY" && e.state != "INITIALIZING" {
		return nil
	}
	for _, volume := range mountedVolumes(e.pod) {
		if projectedToken(volume) {
			return p.refreshVolumeProjections(ctx, e, false, true)
		}
	}
	return nil
}

func normalizeDownwardVolume(volume corev1.Volume) corev1.Volume {
	if source := volume.DownwardAPI; source != nil {
		volume.Projected = &corev1.ProjectedVolumeSource{DefaultMode: source.DefaultMode,
			Sources: []corev1.VolumeProjection{{DownwardAPI: &corev1.DownwardAPIProjection{Items: source.Items}}}}
		volume.DownwardAPI = nil
	}
	return volume
}

func validateDownwardResource(selector *corev1.ResourceFieldSelector) error {
	if selector.ContainerName == "" {
		return fmt.Errorf("DownwardAPI volume resourceFieldRef requires containerName")
	}
	switch selector.Resource {
	case "limits.cpu", "requests.cpu", "limits.memory", "requests.memory", "limits.ephemeral-storage", "requests.ephemeral-storage":
	default:
		return fmt.Errorf("unsupported DownwardAPI resource %q", selector.Resource)
	}
	if selector.Divisor.Sign() < 0 {
		return fmt.Errorf("DownwardAPI divisor must be positive")
	}
	return nil
}

func downwardResource(pod *corev1.Pod, selector *corev1.ResourceFieldSelector, capacity corev1.ResourceList) (string, error) {
	if err := validateDownwardResource(selector); err != nil {
		return "", err
	}
	var selected *corev1.Container
	for container := range podContainers(pod) {
		if container.Name == selector.ContainerName {
			selected = &container
			break
		}
	}
	if selected == nil {
		return "", fmt.Errorf("DownwardAPI container %q does not exist", selector.ContainerName)
	}
	kind, name, _ := strings.Cut(selector.Resource, ".")
	resourceName := corev1.ResourceName(name)
	quantity := selected.Resources.Requests[resourceName]
	if kind == "limits" {
		quantity = selected.Resources.Limits[resourceName]
		if quantity.IsZero() && (resourceName == corev1.ResourceCPU || resourceName == corev1.ResourceMemory) {
			quantity = capacity[resourceName]
		}
	}
	divisor := selector.Divisor
	if divisor.IsZero() {
		divisor = resource.MustParse("1")
	}
	value, unit := quantity.Value(), divisor.Value()
	if resourceName == corev1.ResourceCPU {
		value, unit = quantity.MilliValue(), divisor.MilliValue()
	}
	if unit <= 0 {
		return "", fmt.Errorf("invalid resource divisor")
	}
	whole, remainder := new(big.Int).QuoRem(big.NewInt(value), big.NewInt(unit), new(big.Int))
	if remainder.Sign() > 0 {
		whole.Add(whole, big.NewInt(1))
	}
	return whole.String(), nil
}
