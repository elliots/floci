package main

import (
	"cmp"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// Scheduling reserves declared requests even while their guests are asleep.
// This capacity is independent of the admission budget for running guest RAM.
func (c config) nodeCapacity() (corev1.ResourceList, error) {
	if c.maxActiveMemoryMiB <= 0 || int64(c.maxActiveMemoryMiB) > math.MaxInt64/(1024*1024) {
		return nil, fmt.Errorf("max-active-memory-mib must be positive and fit an int64 byte count")
	}
	cpu, err := schedulingQuantity("scheduling-cpu", cmp.Or(c.schedulingCPU, "4"),
		*resource.NewMilliQuantity(math.MaxInt64, resource.DecimalSI))
	if err != nil {
		return nil, err
	}
	memory, err := schedulingQuantity("scheduling-memory",
		cmp.Or(c.schedulingMemory, fmt.Sprintf("%dMi", c.maxActiveMemoryMiB)),
		*resource.NewQuantity(math.MaxInt64, resource.BinarySI))
	if err != nil {
		return nil, err
	}
	return corev1.ResourceList{
		corev1.ResourceCPU:    cpu,
		corev1.ResourceMemory: memory,
		corev1.ResourcePods:   *resource.NewQuantity(int64(c.maxPods), resource.DecimalSI),
	}, nil
}

func schedulingQuantity(name, value string, maximum resource.Quantity) (resource.Quantity, error) {
	if exponent := strings.LastIndexAny(value, "eE"); exponent >= 0 && exponent < len(value)-1 && !strings.HasSuffix(value, "Ei") {
		// Bound decimal exponents before Quantity parsing, which can wrap their
		// int32 scale or allocate excessively while rounding a huge negative scale.
		number, err := strconv.ParseFloat(value, 64)
		if err != nil || number <= 0 || math.IsInf(number, 0) || math.IsNaN(number) {
			return resource.Quantity{}, fmt.Errorf("%s must be a finite positive Kubernetes quantity", name)
		}
	}
	quantity, err := resource.ParseQuantity(value)
	if err != nil {
		return resource.Quantity{}, fmt.Errorf("%s must be a positive Kubernetes quantity: %w", name, err)
	}
	if quantity.Sign() <= 0 || quantity.Cmp(maximum) > 0 || binaryQuantityOverflow(value, quantity) {
		return resource.Quantity{}, fmt.Errorf("%s must be positive and fit Kubernetes scheduler accounting", name)
	}
	return quantity, nil
}

func binaryQuantityOverflow(value string, quantity resource.Quantity) bool {
	if quantity.Format != resource.BinarySI || quantity.CmpInt64(math.MaxInt64) < 0 {
		return false
	}
	// ParseQuantity caps overflowing binary quantities at MaxInt64. Check the
	// original magnitude so a typo such as 80Ei cannot become a valid capacity.
	number, ok := new(big.Rat).SetString(value[:len(value)-2])
	if !ok {
		return true
	}
	shift := (strings.IndexByte("KMGTPE", value[len(value)-2]) + 1) * 10
	factor := new(big.Int).Lsh(big.NewInt(1), uint(shift))
	number.Mul(number, new(big.Rat).SetInt(factor))
	return number.Cmp(new(big.Rat).SetInt64(math.MaxInt64)) > 0
}
