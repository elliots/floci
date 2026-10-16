package main

import (
	"fmt"
	"math"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	// This local profile reserves memory for the guest kernel, agent and
	// containerd separately from the application's declared resources.
	guestMemoryHeadroomMiB = 256
	maxGuestVCPUs          = 255
	maxGuestMemoryMiB      = int(math.MaxUint32 - 1)
	bytesPerMiB            = int64(1024 * 1024)
)

type podAllocation struct {
	Version   int `json:"version"`
	VCPUs     int `json:"vcpus"`
	MemoryMiB int `json:"memoryMiB"`
}

func derivePodAllocation(pod *corev1.Pod) (podAllocation, error) {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return podAllocation{}, fmt.Errorf("resource allocation requires at least one regular container")
	}
	if pod.Spec.Resources != nil || len(pod.Spec.ResourceClaims) != 0 || len(pod.Spec.Overhead) != 0 {
		return podAllocation{}, fmt.Errorf("pod-level resources, resource claims and RuntimeClass overhead are not qualified")
	}
	var cpuMilli, memoryBytes int64
	for container := range podContainers(pod) {
		if err := validateContainerResources(container); err != nil {
			return podAllocation{}, fmt.Errorf("container %s: %w", container.Name, err)
		}
	}
	// Each sequential init stage overlaps previously started sidecars. Regular
	// applications overlap all sidecars after initialization completes.
	for _, container := range pod.Spec.Containers {
		cpuMilli += max(container.Resources.Requests.Cpu().MilliValue(), container.Resources.Limits.Cpu().MilliValue())
		memoryBytes += max(container.Resources.Requests.Memory().Value(), container.Resources.Limits.Memory().Value())
		if cpuMilli > maxGuestVCPUs*1000 || memoryBytes > int64(maxGuestMemoryMiB-guestMemoryHeadroomMiB)*bytesPerMiB {
			return podAllocation{}, fmt.Errorf("concurrent application resources exceed native VM capacity")
		}
	}
	applicationCPU, applicationMemory := cpuMilli, memoryBytes
	var sidecarCPU, sidecarMemory int64
	for _, container := range pod.Spec.InitContainers {
		cpu := max(container.Resources.Requests.Cpu().MilliValue(), container.Resources.Limits.Cpu().MilliValue())
		memory := max(container.Resources.Requests.Memory().Value(), container.Resources.Limits.Memory().Value())
		cpuMilli, memoryBytes = max(cpuMilli, sidecarCPU+cpu), max(memoryBytes, sidecarMemory+memory)
		if restartableInit(container) {
			sidecarCPU, sidecarMemory = sidecarCPU+cpu, sidecarMemory+memory
			if sidecarCPU > maxGuestVCPUs*1000 || sidecarMemory > int64(maxGuestMemoryMiB-guestMemoryHeadroomMiB)*bytesPerMiB {
				return podAllocation{}, fmt.Errorf("concurrent sidecar resources exceed native VM capacity")
			}
		}
	}
	cpuMilli, memoryBytes = max(cpuMilli, applicationCPU+sidecarCPU), max(memoryBytes, applicationMemory+sidecarMemory)
	memoryMiB := int64(0)
	if memoryBytes > 0 {
		memoryMiB = (memoryBytes-1)/bytesPerMiB + 1
	}
	guestMemoryMiB := max(vmMemoryMiB, int(memoryMiB)+guestMemoryHeadroomMiB)
	// The pinned runtime's guest-memory topology requires 2 MiB alignment.
	guestMemoryMiB += guestMemoryMiB % 2
	allocation := podAllocation{Version: 1, VCPUs: max(1, int((cpuMilli+999)/1000)), MemoryMiB: guestMemoryMiB}
	return validateStoredAllocation(allocation)
}

func validateStoredAllocation(allocation podAllocation) (podAllocation, error) {
	if allocation == (podAllocation{}) {
		// Existing journals always describe this fixed guest. Recomputing from
		// the Pod or a newer sizing policy would misaccount a restored snapshot.
		return podAllocation{Version: 1, VCPUs: 1, MemoryMiB: vmMemoryMiB}, nil
	}
	if allocation.Version != 1 || allocation.VCPUs < 1 || allocation.VCPUs > maxGuestVCPUs ||
		allocation.MemoryMiB < vmMemoryMiB || allocation.MemoryMiB > maxGuestMemoryMiB || allocation.MemoryMiB%2 != 0 {
		return podAllocation{}, fmt.Errorf("invalid or unsupported persisted native pod allocation")
	}
	return allocation, nil
}

func validateContainerResources(container corev1.Container) error {
	if len(container.Resources.Claims) != 0 {
		return fmt.Errorf("container resource claims are not qualified")
	}
	for _, list := range []corev1.ResourceList{container.Resources.Requests, container.Resources.Limits} {
		for name, quantity := range list {
			if quantity.Sign() < 0 {
				return fmt.Errorf("container %s resource must not be negative", name)
			}
			switch name {
			case corev1.ResourceCPU:
				if quantity.CmpInt64(maxGuestVCPUs) > 0 {
					return fmt.Errorf("container CPU resources exceed the native maximum of %d vCPUs", maxGuestVCPUs)
				}
				if quantity.Cmp(*resource.NewMilliQuantity(quantity.MilliValue(), resource.DecimalSI)) != 0 {
					return fmt.Errorf("container CPU resources must use whole millicores")
				}
			case corev1.ResourceMemory:
				maximum := int64(maxGuestMemoryMiB-guestMemoryHeadroomMiB) * bytesPerMiB
				if quantity.CmpInt64(maximum) > 0 {
					return fmt.Errorf("container memory resources exceed native memory capacity after guest headroom")
				}
			default:
				return fmt.Errorf("container resource %q is not qualified; only CPU and memory are supported", name)
			}
		}
	}
	for name, request := range container.Resources.Requests {
		if limit, exists := container.Resources.Limits[name]; exists && request.Cmp(limit) > 0 {
			return fmt.Errorf("container %s request exceeds its limit", name)
		}
	}
	return nil
}

func containerResourceArgs(container corev1.Container) ([]string, error) {
	if err := validateContainerResources(container); err != nil {
		return nil, err
	}
	cpuRequest, exists := container.Resources.Requests[corev1.ResourceCPU]
	if !exists {
		cpuRequest = container.Resources.Limits[corev1.ResourceCPU]
	}
	// Match kubelet's default CFS policy: requests determine shares; only an
	// explicit nonzero limit sets a quota or a memory ceiling.
	shares := min(int64(262144), max(int64(2), cpuRequest.MilliValue()*1024/1000))
	args := []string{"--cpu-shares", strconv.FormatInt(shares, 10)}
	if cpuLimit := container.Resources.Limits.Cpu().MilliValue(); cpuLimit > 0 {
		quota := max(int64(1000), cpuLimit*100)
		args = append(args, "--cpu-period", "100000", "--cpu-quota", strconv.FormatInt(quota, 10))
	}
	if memoryLimit := container.Resources.Limits.Memory().Value(); memoryLimit > 0 {
		args = append(args, "--memory-limit", strconv.FormatInt(memoryLimit, 10))
	}
	return args, nil
}
