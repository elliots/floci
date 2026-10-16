package main

import (
	"math"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func resourcePod(requests, limits corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app",
		Resources: corev1.ResourceRequirements{Requests: requests, Limits: limits}}}}}
}

func TestDerivePodAllocationUsesDeclarationsAndGuestHeadroom(t *testing.T) {
	for _, test := range []struct {
		name             string
		requests, limits corev1.ResourceList
		vcpus, memoryMiB int
	}{
		{name: "no resources", vcpus: 1, memoryMiB: 768},
		{name: "zero resources", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0"), corev1.ResourceMemory: resource.MustParse("0")}, vcpus: 1, memoryMiB: 768},
		{name: "small request", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("256Mi")}, vcpus: 1, memoryMiB: 768},
		{name: "large request", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}, vcpus: 2, memoryMiB: 1280},
		{name: "limits reserve room to burst", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
			limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3"), corev1.ResourceMemory: resource.MustParse("2Gi")}, vcpus: 3, memoryMiB: 2304},
		{name: "limits only", limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}, vcpus: 3, memoryMiB: 1280},
		{name: "round to aligned MiB", requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("536870913")}, vcpus: 1, memoryMiB: 770},
		{name: "fractional bytes round up", requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("536870912.1")}, vcpus: 1, memoryMiB: 770},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := resourcePod(test.requests, test.limits)
			before := pod.DeepCopy()
			allocation, err := derivePodAllocation(pod)
			if err != nil || allocation != (podAllocation{Version: 1, VCPUs: test.vcpus, MemoryMiB: test.memoryMiB}) {
				t.Fatalf("unexpected allocation: %+v, %v", allocation, err)
			}
			if !reflect.DeepEqual(before, pod) {
				t.Fatal("allocating guest resources changed the Kubernetes request or limit")
			}
		})
	}
}

func TestDerivePodAllocationRejectsUnsupportedOrInvalidResources(t *testing.T) {
	for _, test := range []struct {
		name     corev1.ResourceName
		quantity string
	}{
		{corev1.ResourceCPU, "-1"}, {corev1.ResourceCPU, "0.1m"}, {corev1.ResourceCPU, "255001m"},
		{corev1.ResourceCPU, "1e30"}, {corev1.ResourceMemory, "-1"}, {corev1.ResourceMemory, "1e30"},
		{corev1.ResourceMemory, "8Ei"}, {corev1.ResourceEphemeralStorage, "1Gi"},
		{"hugepages-2Mi", "2Mi"}, {"example.com/device", "1"},
	} {
		for _, asLimit := range []bool{false, true} {
			pod := resourcePod(nil, nil)
			resources := corev1.ResourceList{test.name: resource.MustParse(test.quantity)}
			if asLimit {
				pod.Spec.Containers[0].Resources.Limits = resources
			} else {
				pod.Spec.Containers[0].Resources.Requests = resources
			}
			if _, err := derivePodAllocation(pod); err == nil {
				t.Errorf("accepted invalid resource %s=%s (limit=%t)", test.name, test.quantity, asLimit)
			}
			if _, err := containerResourceArgs(pod.Spec.Containers[0]); err == nil {
				t.Errorf("built ctr arguments for invalid resource %s=%s (limit=%t)", test.name, test.quantity, asLimit)
			}
		}
	}
}

func TestDerivePodAllocationRejectsPodResourceFeaturesAndClaims(t *testing.T) {
	for _, mutate := range []func(*corev1.Pod){
		func(pod *corev1.Pod) { pod.Spec.Resources = &corev1.ResourceRequirements{} },
		func(pod *corev1.Pod) { pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "device"}} },
		func(pod *corev1.Pod) {
			pod.Spec.Overhead = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")}
		},
		func(pod *corev1.Pod) {
			pod.Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "device"}}
		},
	} {
		pod := resourcePod(nil, nil)
		mutate(pod)
		if _, err := derivePodAllocation(pod); err == nil {
			t.Fatal("accepted an unsupported resource feature")
		}
	}
}

func TestDerivePodAllocationRejectsRequestsExceedingLimits(t *testing.T) {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		for _, limit := range []string{"0", "1"} {
			pod := resourcePod(corev1.ResourceList{name: resource.MustParse("2")}, corev1.ResourceList{name: resource.MustParse(limit)})
			if _, err := derivePodAllocation(pod); err == nil || !strings.Contains(err.Error(), "request exceeds its limit") {
				t.Fatalf("expected request/limit validation for %s, got %v", name, err)
			}
		}
	}
}

func TestDerivePodAllocationChecksExactNativeLimits(t *testing.T) {
	maximum := int64(maxGuestMemoryMiB-guestMemoryHeadroomMiB) * bytesPerMiB
	pod := resourcePod(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("255"),
		corev1.ResourceMemory: *resource.NewQuantity(maximum, resource.BinarySI)}, nil)
	allocation, err := derivePodAllocation(pod)
	if err != nil || allocation != (podAllocation{Version: 1, VCPUs: 255, MemoryMiB: maxGuestMemoryMiB}) {
		t.Fatalf("native bounds did not round-trip: %+v, %v", allocation, err)
	}
	pod.Spec.Containers[0].Resources.Requests[corev1.ResourceMemory] = *resource.NewQuantity(maximum+1, resource.BinarySI)
	if _, err := derivePodAllocation(pod); err == nil {
		t.Fatal("accepted memory that overflows native MiB capacity after guest headroom")
	}
}

func TestValidateStoredAllocationPreservesLegacyAndRecordedGuestSize(t *testing.T) {
	for _, test := range []struct{ input, expected podAllocation }{
		{podAllocation{}, podAllocation{Version: 1, VCPUs: 1, MemoryMiB: 768}},
		{podAllocation{Version: 1, VCPUs: 3, MemoryMiB: 2304}, podAllocation{Version: 1, VCPUs: 3, MemoryMiB: 2304}},
	} {
		actual, err := validateStoredAllocation(test.input)
		if err != nil || actual != test.expected {
			t.Fatalf("stored allocation changed: %+v, %v", actual, err)
		}
	}
	for _, allocation := range []podAllocation{
		{Version: 2, VCPUs: 1, MemoryMiB: 768}, {Version: 0, VCPUs: 1, MemoryMiB: 768},
		{Version: 1, VCPUs: 0, MemoryMiB: 768}, {Version: 1, VCPUs: 256, MemoryMiB: 768},
		{Version: 1, VCPUs: 1, MemoryMiB: 769}, {Version: 1, VCPUs: 1, MemoryMiB: math.MaxUint32},
		{Version: 1, VCPUs: 1, MemoryMiB: 767}, {Version: 1, VCPUs: 1, MemoryMiB: math.MaxUint32 + 1},
	} {
		if _, err := validateStoredAllocation(allocation); err == nil {
			t.Errorf("accepted invalid recorded allocation: %+v", allocation)
		}
	}
}

func TestContainerResourceArgsPreserveRequestAndLimitSemantics(t *testing.T) {
	for _, test := range []struct {
		name             string
		requests, limits corev1.ResourceList
		args             []string
	}{
		{name: "best effort", args: []string{"--cpu-shares", "2"}},
		{name: "request does not limit", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
			args: []string{"--cpu-shares", "256"}},
		{name: "declared limits", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
			limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m"), corev1.ResourceMemory: resource.MustParse("1Gi")},
			args:   []string{"--cpu-shares", "512", "--cpu-period", "100000", "--cpu-quota", "150000", "--memory-limit", "1073741824"}},
		{name: "limit defaults missing request", limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			args: []string{"--cpu-shares", "2048", "--cpu-period", "100000", "--cpu-quota", "200000"}},
		{name: "explicit zero request", requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}, limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
			args: []string{"--cpu-shares", "2", "--cpu-period", "100000", "--cpu-quota", "100000"}},
		{name: "minimum kernel quota", limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1m")},
			args: []string{"--cpu-shares", "2", "--cpu-period", "100000", "--cpu-quota", "1000"}},
		{name: "explicit zero limits", limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0"), corev1.ResourceMemory: resource.MustParse("0")},
			args: []string{"--cpu-shares", "2"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			container := resourcePod(test.requests, test.limits).Spec.Containers[0]
			args, err := containerResourceArgs(container)
			if err != nil || !reflect.DeepEqual(args, test.args) {
				t.Fatalf("unexpected runtime resource arguments: %v, %v", args, err)
			}
		})
	}
}
