package main

import (
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func policyPod(namespace, name, ip string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, UID: types.UID(namespace + "-" + name), Labels: map[string]string{"app": name}},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "app", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"NET_RAW"}}}, Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}}}},
		Status: corev1.PodStatus{PodIP: ip}}
}

func policyFor(pod *corev1.Pod) *networkingv1.NetworkPolicy {
	return &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: "policy"}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{MatchLabels: pod.Labels}}}
}

func TestNetworkPolicyDefaultDenyIsDirectionalAndAdditive(t *testing.T) {
	pod := policyPod("apps", "api", "10.0.0.1")
	deny := policyFor(pod)
	plan := compilePodPolicy(pod, policyInventory{policies: []*networkingv1.NetworkPolicy{deny}})
	if !plan.ingressIsolated || plan.egressIsolated || len(plan.ingress) != 0 || plan.unsupported != "" {
		t.Fatalf("default ingress deny: %#v", plan)
	}
	allow := policyFor(pod)
	allow.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{Ports: []networkingv1.NetworkPolicyPort{{Port: new(intstr.FromInt32(8080))}}}}
	allow.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
	plan = compilePodPolicy(pod, policyInventory{policies: []*networkingv1.NetworkPolicy{deny, allow, allow}})
	if !plan.ingressIsolated || !plan.egressIsolated || !reflect.DeepEqual(plan.ingress, []policyRule{{CIDR: "0.0.0.0/0", Protocol: "tcp", Port: "8080"}}) || !reflect.DeepEqual(plan.egress, []policyRule{{CIDR: "0.0.0.0/0"}}) {
		t.Fatalf("additive allow: %#v", plan)
	}
}

func TestNetworkPolicySelectorIntersectionAndPeerUnion(t *testing.T) {
	target := policyPod("apps", "api", "10.0.0.1")
	local := policyPod("apps", "client", "10.0.0.2")
	remote := policyPod("trusted", "client", "10.0.0.3")
	wrongApp := policyPod("trusted", "other", "10.0.0.4")
	wrongNamespace := policyPod("untrusted", "client", "10.0.0.5")
	inventory := policyInventory{pods: []*corev1.Pod{target, local, remote, wrongApp, wrongNamespace},
		namespaces: map[string]labels.Set{"apps": {"team": "apps"}, "trusted": {"team": "trusted"}, "untrusted": {"team": "other"}},
		addresses:  map[string][]string{string(remote.UID): {"10.99.0.3"}}}
	policy := policyFor(target)
	policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{From: []networkingv1.NetworkPolicyPeer{
		{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "client"}}},
		{PodSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "client"}}, NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "trusted"}}},
	}}}
	inventory.policies = []*networkingv1.NetworkPolicy{policy}
	plan := compilePodPolicy(target, inventory)
	want := []policyRule{{CIDR: "10.0.0.2/32"}, {CIDR: "10.0.0.3/32"}, {CIDR: "10.99.0.3/32"}}
	if !reflect.DeepEqual(plan.ingress, want) || plan.unsupported != "" {
		t.Fatalf("selector rules = %#v, error=%s", plan.ingress, plan.unsupported)
	}
	remote.Labels["app"] = "changed"
	plan = compilePodPolicy(target, inventory)
	if !reflect.DeepEqual(plan.ingress, want[:1]) {
		t.Fatalf("label update did not revoke both aliases: %#v", plan.ingress)
	}
}

func TestNetworkPolicyNamedPortsResolveOnDestination(t *testing.T) {
	target := policyPod("apps", "api", "10.0.0.1")
	peer := policyPod("apps", "client", "10.0.0.2")
	peer.Spec.Containers[0].Ports[0].ContainerPort = 9090
	policy := policyFor(target)
	ports := []networkingv1.NetworkPolicyPort{{Port: new(intstr.FromString("http"))}}
	peers := []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{MatchLabels: peer.Labels}}}
	policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{From: peers, Ports: ports}}
	policy.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{To: peers, Ports: ports}}
	plan := compilePodPolicy(target, policyInventory{pods: []*corev1.Pod{peer}, policies: []*networkingv1.NetworkPolicy{policy}})
	if !reflect.DeepEqual(plan.ingress, []policyRule{{CIDR: "10.0.0.2/32", Protocol: "tcp", Port: "8080"}}) || !reflect.DeepEqual(plan.egress, []policyRule{{CIDR: "10.0.0.2/32", Protocol: "tcp", Port: "9090"}}) {
		t.Fatalf("named ports = %#v", plan)
	}
	policy.Spec.Egress[0].To = nil
	plan = compilePodPolicy(target, policyInventory{policies: []*networkingv1.NetworkPolicy{policy}})
	if !strings.Contains(plan.unsupported, "named egress") || len(plan.ingress) != 0 || len(plan.egress) != 0 {
		t.Fatalf("unsupported named egress did not deny both directions: %#v", plan)
	}
}

func TestNetworkPolicyIPBlockSubtractsOverlappingExceptions(t *testing.T) {
	got, err := policyIPBlock(&networkingv1.IPBlock{CIDR: "10.0.0.0/24", Except: []string{"10.0.0.64/26", "10.0.0.96/27", "10.0.0.192/26"}})
	if err != nil {
		t.Fatal(err)
	}
	for last := range 256 {
		ip := netip.AddrFrom4([4]byte{10, 0, 0, byte(last)})
		allowed := slices.ContainsFunc(got, func(raw string) bool { return netip.MustParsePrefix(raw).Contains(ip) })
		if allowed != (last < 64 || last >= 128 && last < 192) {
			t.Fatalf("IP %s allowed=%v, subnets=%v", ip, allowed, got)
		}
	}
	for _, block := range []*networkingv1.IPBlock{{CIDR: "::/0"}, {CIDR: "10.0.0.1/24"}, {CIDR: "10.0.0.0/24", Except: []string{"10.0.1.0/24"}}} {
		if _, err := policyIPBlock(block); err == nil {
			t.Fatalf("invalid block accepted: %#v", block)
		}
	}
}

func TestNetworkPolicyPortsPreserveProtocolAndRanges(t *testing.T) {
	pod := policyPod("apps", "api", "10.0.0.1")
	policy := policyFor(pod)
	policy.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{{Ports: []networkingv1.NetworkPolicyPort{
		{Protocol: new(corev1.ProtocolUDP), Port: new(intstr.FromInt32(53))},
		{Port: new(intstr.FromInt32(8000)), EndPort: new(int32(8099))},
	}}}
	plan := compilePodPolicy(pod, policyInventory{policies: []*networkingv1.NetworkPolicy{policy}})
	want := []policyRule{{CIDR: "0.0.0.0/0", Protocol: "tcp", Port: "8000:8099"}, {CIDR: "0.0.0.0/0", Protocol: "udp", Port: "53"}}
	if !reflect.DeepEqual(plan.ingress, want) {
		t.Fatalf("ports=%#v", plan)
	}
	policy.Spec.Ingress[0].Ports = []networkingv1.NetworkPolicyPort{{Protocol: new(corev1.ProtocolSCTP)}}
	if plan := compilePodPolicy(pod, policyInventory{policies: []*networkingv1.NetworkPolicy{policy}}); plan.unsupported == "" {
		t.Fatal("unqualified SCTP policy silently accepted")
	}
}

func TestNetworkPolicyCapabilityAdmissionDoesNotChangeUnselectedPods(t *testing.T) {
	pod := policyPod("apps", "api", "10.0.0.1")
	pod.Spec.Containers[0].SecurityContext = nil
	if plan := compilePodPolicy(pod, policyInventory{}); plan.unsupported != "" {
		t.Fatalf("unselected Pod rejected: %#v", plan)
	}
	inventory := policyInventory{policies: []*networkingv1.NetworkPolicy{policyFor(pod)}}
	if plan := compilePodPolicy(pod, inventory); !strings.Contains(plan.unsupported, "drop NET_RAW") {
		t.Fatalf("default raw capability accepted: %#v", plan)
	}
	for _, capability := range []corev1.Capability{"NET_RAW", "NET_ADMIN", "SYS_ADMIN", "BPF", "SYS_PTRACE"} {
		pod.Spec.Containers[0].SecurityContext = &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}, Add: []corev1.Capability{capability}}}
		if plan := compilePodPolicy(pod, inventory); plan.unsupported == "" {
			t.Fatalf("unsafe capability %s accepted", capability)
		}
	}
	pod.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"NET_BIND_SERVICE"}
	if plan := compilePodPolicy(pod, inventory); plan.unsupported != "" {
		t.Fatalf("ordinary capability rejected: %#v", plan)
	}
}
