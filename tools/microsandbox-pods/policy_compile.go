package main

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type policyInventory struct {
	observed, available bool
	revision            uint64
	pods                []*corev1.Pod
	namespaces          map[string]labels.Set
	policies            []*networkingv1.NetworkPolicy
	addresses           map[string][]string
}

type policyRule struct {
	CIDR, Protocol, Port string
}

type podPolicyPlan struct {
	ingress, egress                 []policyRule
	ingressIsolated, egressIsolated bool
	unsupported                     string
	stale                           bool
}

func compilePodPolicy(pod *corev1.Pod, inventory policyInventory) podPolicyPlan {
	var plan podPolicyPlan
	for _, policy := range inventory.policies {
		if policy.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			plan.unsupported = "NetworkPolicy has an invalid Pod selector"
			plan.ingressIsolated, plan.egressIsolated = true, true
			continue
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		types := policy.Spec.PolicyTypes
		if len(types) == 0 {
			types = []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}
			if len(policy.Spec.Egress) > 0 {
				types = append(types, networkingv1.PolicyTypeEgress)
			}
		}
		for _, kind := range types {
			switch kind {
			case networkingv1.PolicyTypeIngress:
				plan.ingressIsolated = true
				for _, rule := range policy.Spec.Ingress {
					rules, err := compilePolicyRule(pod, policy.Namespace, rule.From, rule.Ports, false, inventory)
					if err != nil {
						plan.unsupported = fmt.Sprintf("NetworkPolicy %s ingress: %v", policy.Name, err)
					}
					plan.ingress = append(plan.ingress, rules...)
				}
			case networkingv1.PolicyTypeEgress:
				plan.egressIsolated = true
				for _, rule := range policy.Spec.Egress {
					rules, err := compilePolicyRule(pod, policy.Namespace, rule.To, rule.Ports, true, inventory)
					if err != nil {
						plan.unsupported = fmt.Sprintf("NetworkPolicy %s egress: %v", policy.Name, err)
					}
					plan.egress = append(plan.egress, rules...)
				}
			default:
				plan.unsupported = "NetworkPolicy has an unsupported policy type"
			}
		}
	}
	if plan.ingressIsolated || plan.egressIsolated {
		if err := policyCapabilities(pod); err != nil {
			plan.unsupported = err.Error()
		}
	}
	if plan.unsupported != "" {
		// A malformed or unsupported selected rule must never turn into allow-all.
		plan.ingress, plan.egress = nil, nil
		plan.ingressIsolated, plan.egressIsolated = true, true
	}
	plan.ingress = uniquePolicyRules(plan.ingress)
	plan.egress = uniquePolicyRules(plan.egress)
	return plan
}

func policyCapabilities(pod *corev1.Pod) error {
	for container := range podContainers(pod) {
		sc := container.SecurityContext
		if sc == nil || sc.Capabilities == nil || !slices.Contains(sc.Capabilities.Drop, "NET_RAW") && !slices.Contains(sc.Capabilities.Drop, "ALL") {
			return fmt.Errorf("NetworkPolicy requires container %s to explicitly drop NET_RAW or ALL", container.Name)
		}
		for _, capability := range sc.Capabilities.Add {
			if slices.Contains([]corev1.Capability{"ALL", "NET_RAW", "NET_ADMIN", "SYS_ADMIN", "SYS_MODULE", "BPF", "SYS_RAWIO", "SYS_PTRACE", "PERFMON"}, capability) {
				return fmt.Errorf("NetworkPolicy cannot protect container %s with capability %s", container.Name, capability)
			}
		}
	}
	return nil
}

func uniquePolicyRules(rules []policyRule) []policyRule {
	slices.SortFunc(rules, func(a, b policyRule) int {
		return strings.Compare(a.CIDR+"/"+a.Protocol+"/"+a.Port, b.CIDR+"/"+b.Protocol+"/"+b.Port)
	})
	return slices.Compact(rules)
}

type policyPeer struct {
	cidrs []string
	pod   *corev1.Pod
}

func compilePolicyRule(target *corev1.Pod, namespace string, peers []networkingv1.NetworkPolicyPeer, ports []networkingv1.NetworkPolicyPort, egress bool, inventory policyInventory) ([]policyRule, error) {
	var selected []policyPeer
	if len(peers) == 0 {
		selected = append(selected, policyPeer{cidrs: []string{"0.0.0.0/0"}})
	}
	for _, peer := range peers {
		if peer.IPBlock != nil {
			if peer.PodSelector != nil || peer.NamespaceSelector != nil {
				return nil, errors.New("IPBlock cannot be combined with selectors")
			}
			prefixes, err := policyIPBlock(peer.IPBlock)
			if err != nil {
				return nil, err
			}
			selected = append(selected, policyPeer{cidrs: prefixes})
			continue
		}
		if peer.PodSelector == nil && peer.NamespaceSelector == nil {
			selected = append(selected, policyPeer{cidrs: []string{"0.0.0.0/0"}})
			continue
		}
		podSelector, namespaceSelector := labels.Everything(), labels.Everything()
		var err error
		if peer.PodSelector != nil {
			podSelector, err = metav1.LabelSelectorAsSelector(peer.PodSelector)
			if err != nil {
				return nil, err
			}
		}
		if peer.NamespaceSelector != nil {
			namespaceSelector, err = metav1.LabelSelectorAsSelector(peer.NamespaceSelector)
			if err != nil {
				return nil, err
			}
		}
		for _, pod := range inventory.pods {
			if pod.Spec.HostNetwork || !podSelector.Matches(labels.Set(pod.Labels)) {
				continue
			}
			if peer.NamespaceSelector == nil && pod.Namespace != namespace || peer.NamespaceSelector != nil && !namespaceSelector.Matches(inventory.namespaces[pod.Namespace]) {
				continue
			}
			selected = append(selected, policyPeer{cidrs: policyPodAddresses(pod, inventory), pod: pod})
		}
	}
	var rules []policyRule
	for _, peer := range selected {
		if len(ports) == 0 {
			for _, cidr := range peer.cidrs {
				rules = append(rules, policyRule{CIDR: cidr})
			}
			continue
		}
		for _, port := range ports {
			protocol := corev1.ProtocolTCP
			if port.Protocol != nil {
				protocol = *port.Protocol
			}
			if !slices.Contains([]corev1.Protocol{corev1.ProtocolTCP, corev1.ProtocolUDP}, protocol) {
				return nil, errors.New("only TCP and UDP policy ports are qualified")
			}
			if port.Port != nil && port.Port.Type == intstr.String {
				if port.EndPort != nil || port.Port.StrVal == "" {
					return nil, errors.New("invalid named port")
				}
				destinations := []*corev1.Pod{target}
				if egress {
					if peer.pod == nil {
						return nil, errors.New("named egress ports require Pod or namespace selectors")
					}
					destinations = []*corev1.Pod{peer.pod}
				}
				for _, destination := range destinations {
					for _, number := range policyNamedPorts(destination, port.Port.StrVal, protocol) {
						for _, cidr := range peer.cidrs {
							rules = append(rules, policyRule{CIDR: cidr, Protocol: strings.ToLower(string(protocol)), Port: strconv.Itoa(int(number))})
						}
					}
				}
				continue
			}
			number := ""
			if port.Port != nil {
				if port.Port.IntVal < 1 || port.Port.IntVal > 65535 {
					return nil, errors.New("port must be 1..65535")
				}
				number = strconv.Itoa(int(port.Port.IntVal))
				if port.EndPort != nil {
					if *port.EndPort < port.Port.IntVal || *port.EndPort > 65535 {
						return nil, errors.New("invalid endPort")
					}
					number += ":" + strconv.Itoa(int(*port.EndPort))
				}
			} else if port.EndPort != nil {
				return nil, errors.New("endPort requires a numeric port")
			}
			for _, cidr := range peer.cidrs {
				rules = append(rules, policyRule{CIDR: cidr, Protocol: strings.ToLower(string(protocol)), Port: number})
			}
		}
	}
	return rules, nil
}

func policyPodAddresses(pod *corev1.Pod, inventory policyInventory) []string {
	addresses := slices.Clone(inventory.addresses[string(pod.UID)])
	addresses = append(addresses, pod.Status.PodIP)
	for _, ip := range pod.Status.PodIPs {
		addresses = append(addresses, ip.IP)
	}
	var result []string
	for _, raw := range addresses {
		if address, err := netip.ParseAddr(raw); err == nil && address.Is4() && !address.IsUnspecified() {
			result = append(result, address.String()+"/32")
		}
	}
	slices.Sort(result)
	return slices.Compact(result)
}

func policyNamedPorts(pod *corev1.Pod, name string, protocol corev1.Protocol) []int32 {
	var ports []int32
	for _, port := range applicationPorts(pod) {
		if port.Name == name && (port.Protocol == protocol || port.Protocol == "" && protocol == corev1.ProtocolTCP) {
			ports = append(ports, port.ContainerPort)
		}
	}
	slices.Sort(ports)
	return slices.Compact(ports)
}

func policyIPBlock(block *networkingv1.IPBlock) ([]string, error) {
	prefix, err := netip.ParsePrefix(block.CIDR)
	if err != nil || !prefix.Addr().Is4() || prefix != prefix.Masked() {
		return nil, errors.New("IPBlock requires a canonical IPv4 CIDR")
	}
	prefixes := []netip.Prefix{prefix}
	for _, raw := range block.Except {
		exception, err := netip.ParsePrefix(raw)
		if err != nil || !exception.Addr().Is4() || exception != exception.Masked() || exception.Bits() <= prefix.Bits() || !prefix.Contains(exception.Addr()) {
			return nil, errors.New("IPBlock exception must be a smaller IPv4 subnet")
		}
		var remaining []netip.Prefix
		for _, candidate := range prefixes {
			remaining = append(remaining, subtractPolicyPrefix(candidate, exception)...)
		}
		prefixes = remaining
	}
	result := make([]string, len(prefixes))
	for i, value := range prefixes {
		result[i] = value.String()
	}
	return result, nil
}

func subtractPolicyPrefix(prefix, exception netip.Prefix) []netip.Prefix {
	if !prefix.Overlaps(exception) {
		return []netip.Prefix{prefix}
	}
	if exception.Bits() <= prefix.Bits() {
		return nil
	}
	bits := prefix.Bits() + 1
	left := netip.PrefixFrom(prefix.Addr(), bits)
	bytes := prefix.Addr().As4()
	bytes[(bits-1)/8] |= 1 << (7 - (bits-1)%8)
	right := netip.PrefixFrom(netip.AddrFrom4(bytes), bits)
	return append(subtractPolicyPrefix(left, exception), subtractPolicyPrefix(right, exception)...)
}
