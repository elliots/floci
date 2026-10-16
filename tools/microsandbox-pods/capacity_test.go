package main

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodeCapacityDefaultsMatchExistingScheduling(t *testing.T) {
	cfg := config{maxPods: 512, maxActiveMemoryMiB: 4096}
	capacity, err := cfg.nodeCapacity()
	if err != nil {
		t.Fatal(err)
	}
	if capacity.Cpu().Cmp(resource.MustParse("4")) != 0 ||
		capacity.Memory().Cmp(resource.MustParse("4Gi")) != 0 || capacity.Pods().Value() != 512 {
		t.Fatalf("unexpected default scheduling capacity: %v", capacity)
	}
}

func TestNodeCapacitySeparatesColdSchedulingFromActiveMemory(t *testing.T) {
	cfg := config{maxPods: 512, maxActiveMemoryMiB: 4096, schedulingCPU: "500", schedulingMemory: "384Gi"}
	capacity, err := cfg.nodeCapacity()
	if err != nil {
		t.Fatal(err)
	}
	if capacity.Cpu().Cmp(resource.MustParse("500")) != 0 ||
		capacity.Memory().Cmp(resource.MustParse("384Gi")) != 0 || capacity.Pods().Value() != 512 {
		t.Fatalf("unexpected explicit scheduling capacity: %v", capacity)
	}
	if cfg.maxActiveMemoryMiB != 4096 {
		t.Fatalf("scheduling capacity changed active RAM admission: %d", cfg.maxActiveMemoryMiB)
	}
}

func TestNodeCapacityAllowsFractionalCPUAndIndependentOverrides(t *testing.T) {
	for _, cfg := range []config{
		{maxPods: 3, maxActiveMemoryMiB: 4096, schedulingCPU: "250m"},
		{maxPods: 3, maxActiveMemoryMiB: 4096, schedulingMemory: "128Mi"},
	} {
		capacity, err := cfg.nodeCapacity()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.schedulingCPU != "" && (capacity.Cpu().MilliValue() != 250 || capacity.Memory().Value() != 4*1024*1024*1024) {
			t.Fatalf("CPU override altered another capacity: %v", capacity)
		}
		if cfg.schedulingMemory != "" && (capacity.Memory().Value() != 128*1024*1024 || capacity.Cpu().Value() != 4) {
			t.Fatalf("memory override altered another capacity: %v", capacity)
		}
	}
}

func TestNodeCapacityRejectsInvalidSchedulingQuantities(t *testing.T) {
	for _, value := range []string{"0", "-1", "-1Gi", "NaN", "Inf", "1e100", "1e2147483648", "1e-2147483648", "8Ei", "80Ei", "9223372036854775808"} {
		for _, flag := range []string{"scheduling-cpu", "scheduling-memory"} {
			t.Run(flag+"/"+value, func(t *testing.T) {
				cfg := config{maxPods: 512, maxActiveMemoryMiB: 4096}
				if flag == "scheduling-cpu" {
					cfg.schedulingCPU = value
				} else {
					cfg.schedulingMemory = value
				}
				if _, err := cfg.nodeCapacity(); err == nil || !strings.Contains(err.Error(), flag) {
					t.Fatalf("expected %s validation failure for %q, got %v", flag, value, err)
				}
			})
		}
	}
}

func TestNodeCapacityAcceptsDecimalExponentQuantities(t *testing.T) {
	cfg := config{maxPods: 512, maxActiveMemoryMiB: 4096, schedulingCPU: "5e2", schedulingMemory: "384e6"}
	capacity, err := cfg.nodeCapacity()
	if err != nil || capacity.Cpu().Value() != 500 || capacity.Memory().Value() != 384_000_000 {
		t.Fatalf("decimal exponent quantities did not round-trip: %v, %v", capacity, err)
	}
}

func TestNodeCapacityChecksCPUAtMillicorePrecision(t *testing.T) {
	cfg := config{maxPods: 1, maxActiveMemoryMiB: 4096, schedulingCPU: "9223372036854775.808"}
	if _, err := cfg.nodeCapacity(); err == nil {
		t.Fatal("expected CPU milli-value overflow to fail")
	}
	cfg.schedulingCPU = "9223372036854775.807"
	capacity, err := cfg.nodeCapacity()
	if err != nil || capacity.Cpu().MilliValue() != math.MaxInt64 {
		t.Fatalf("maximum scheduler CPU quantity did not round-trip: %v, %v", capacity, err)
	}
}

func TestNodeCapacityAllowsMaximumMemoryWithoutSaturation(t *testing.T) {
	cfg := config{maxPods: 1, maxActiveMemoryMiB: 4096, schedulingMemory: strconv.FormatInt(math.MaxInt64, 10)}
	capacity, err := cfg.nodeCapacity()
	if err != nil || capacity.Memory().Value() != math.MaxInt64 {
		t.Fatalf("maximum scheduler memory quantity did not round-trip: %v, %v", capacity, err)
	}
	cfg.schedulingMemory = "7.999999999999999999132638262011596452794037759304046630859375Ei"
	capacity, err = cfg.nodeCapacity()
	if err != nil || capacity.Memory().Value() != math.MaxInt64 {
		t.Fatalf("maximum binary scheduler memory quantity did not round-trip: %v, %v", capacity, err)
	}
}

func TestNodeCapacityRejectsActiveMemoryOverflowEvenWithSchedulingOverride(t *testing.T) {
	for _, active := range []int{0, -1, math.MaxInt64/(1024*1024) + 1} {
		cfg := config{maxPods: 512, maxActiveMemoryMiB: active, schedulingMemory: "384Gi"}
		if _, err := cfg.nodeCapacity(); err == nil || !strings.Contains(err.Error(), "max-active-memory-mib") {
			t.Fatalf("expected active RAM byte-count validation failure for %d, got %v", active, err)
		}
	}
}
