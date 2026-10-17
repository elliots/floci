package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestNetworkPolicyJournalFencesOlderControllersAndUnknownHookVersions(t *testing.T) {
	record := journalRecord{Version: 9, NetworkPolicyVersion: 1, Pod: validPod(), Allocation: podAllocation{Version: 2, VCPUs: 1, MemoryMiB: 768, RootDiskMiB: 4096}}
	if _, err := record.guestAllocation(); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []struct{ journal, policy int }{{8, 1}, {9, 0}, {9, 2}, {9, -1}, {10, 1}} {
		record.Version, record.NetworkPolicyVersion = invalid.journal, invalid.policy
		if _, err := record.guestAllocation(); err == nil {
			t.Fatalf("accepted incompatible policy journal %+v", invalid)
		}
	}
	record.Version, record.NetworkPolicyVersion = 8, 0
	if _, err := record.guestAllocation(); err != nil {
		t.Fatalf("legacy unselected lifecycle journal rejected: %v", err)
	}
}

func TestNodeWidePolicyAdmissionProtectsUnselectedPods(t *testing.T) {
	p := &provider{runner: commandRunner{cfg: config{networkPolicy: true}}, client: fake.NewClientset()}
	pod := validPod()
	if err := p.checkNetworkPolicy(t.Context(), pod); err == nil {
		t.Fatal("raw-capability Pod admitted despite having no selecting policy")
	}
	pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	if err := p.checkNetworkPolicy(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	p.runner.cfg.networkPolicy = false
	pod.Spec.Containers[0].SecurityContext = nil
	if err := p.checkNetworkPolicy(t.Context(), pod); err != nil {
		t.Fatalf("ordinary node capabilities changed without opting in: %v", err)
	}
}

func TestNodeWidePolicyRecoveryCannotChangeCapabilityMode(t *testing.T) {
	p := &provider{runner: commandRunner{cfg: config{networkPolicy: true}}}
	record := journalRecord{Pod: validPod()}
	if err := p.validatePolicyRecovery(record); err == nil {
		t.Fatal("legacy guest adopted without the node-wide socket protections")
	}
	record.NetworkPolicyVersion = 1
	if err := p.validatePolicyRecovery(record); err == nil {
		t.Fatal("raw-capability guest adopted under secure mode")
	}
	record.Pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	if err := p.validatePolicyRecovery(record); err != nil {
		t.Fatal(err)
	}
	p.runner.cfg.networkPolicy = false
	if err := p.validatePolicyRecovery(record); err == nil {
		t.Fatal("secure journal silently downgraded to ordinary mode")
	}
}
