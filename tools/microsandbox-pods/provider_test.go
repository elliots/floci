package main

import (
	"net/netip"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func validPod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default", UID: types.UID("550e8400-e29b-41d4-a716-446655440000")},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, DNSPolicy: corev1.DNSClusterFirst,
			Containers: []corev1.Container{{Name: "app", Image: "busybox:1.37.0", Command: []string{"sleep", "300"}}}}}
}

func TestUnsupportedFeaturesAreRejectedBeforeVMCreation(t *testing.T) {
	cases := map[string]func(*corev1.Pod){
		"multiple containers": func(p *corev1.Pod) { p.Spec.Containers = append(p.Spec.Containers, p.Spec.Containers[0]) },

		"projected volume": func(p *corev1.Pod) { p.Spec.Volumes = []corev1.Volume{{Name: "token"}} },
		"host network":     func(p *corev1.Pod) { p.Spec.HostNetwork = true },
		"security context": func(p *corev1.Pod) {
			p.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Privileged: new(true)}
		},
		"http readiness": func(p *corev1.Pod) {
			p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/ready"}}}
		},
		"liveness": func(p *corev1.Pod) { p.Spec.Containers[0].LivenessProbe = &corev1.Probe{} },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			pod := validPod()
			change(pod)
			p := &provider{}
			if err := p.CreatePod(t.Context(), pod); err == nil {
				t.Fatal("unsupported pod accepted")
			}
		})
	}
	if err := validatePod(validPod()); err != nil {
		t.Fatal(err)
	}
}

func TestDifferentPodUIDCannotReplaceOrDeleteOwnedGeneration(t *testing.T) {
	pod := validPod()
	p := &provider{pods: map[string]*podEntry{"default/app": {pod: pod}}}
	changed := pod.DeepCopy()
	changed.UID = "660e8400-e29b-41d4-a716-446655440000"
	if err := p.CreatePod(t.Context(), changed); err == nil {
		t.Fatal("new generation replaced live owner")
	}
	if err := p.DeletePod(t.Context(), changed); err != nil {
		t.Fatal(err)
	}
	if len(p.pods) != 1 {
		t.Fatal("stale delete removed live generation")
	}
}

func TestStatusCopiesCannotMutateProviderState(t *testing.T) {
	pod := validPod()
	pod.Status.Phase = corev1.PodPending
	p := &provider{pods: map[string]*podEntry{"default/app": {pod: pod}}}
	copy, err := p.GetPod(t.Context(), "default", "app")
	if err != nil {
		t.Fatal(err)
	}
	copy.Status.Phase = corev1.PodRunning
	status, err := p.GetPodStatus(t.Context(), "default", "app")
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != corev1.PodPending {
		t.Fatal("caller mutated provider state")
	}
}

func TestNetworkAddressesAreUniqueAndRemainWithinOwnedCIDR(t *testing.T) {
	prefix := netip.MustParsePrefix("10.42.200.0/22")
	n := &network{prefix: prefix}
	seen := map[string]bool{}
	for slot := range 500 {
		address := n.address(slot)
		if seen[address] || !prefix.Contains(netip.MustParseAddr(address)) {
			t.Fatalf("invalid address %s", address)
		}
		seen[address] = true
	}
}

func TestShellArgumentsRemainLiteral(t *testing.T) {
	value := "literal ' $(touch /tmp/should-not-exist) `echo secret`\nnext"
	r := commandRunner{}
	output, err := r.run(t.Context(), "sh", "-c", "printf '%s' "+quote(value))
	if err != nil {
		t.Fatal(err)
	}
	if output != value {
		t.Fatalf("argument changed: %q", output)
	}
	if !strings.Contains(joinCommand([]string{"printf", "%s", value}), "'\"'\"'") {
		t.Fatal("apostrophe was not escaped")
	}
}
