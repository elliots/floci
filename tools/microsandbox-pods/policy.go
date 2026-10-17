package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (p *provider) checkNetworkPolicy(ctx context.Context, pod *corev1.Pod) error {
	if p.runner.cfg.networkPolicy {
		if err := policyCapabilities(pod); err != nil {
			return fmt.Errorf("node-wide NetworkPolicy mode: %w", err)
		}
	}
	if p.policies != nil {
		plan := p.policies.plan(pod, p.policies.inventory())
		if plan.unsupported != "" {
			return fmt.Errorf("NetworkPolicy: %s", plan.unsupported)
		}
		if plan.stale {
			return fmt.Errorf("NetworkPolicy inventory watch is unavailable")
		}
		return nil
	}
	policies, err := p.client.NetworkingV1().NetworkPolicies(pod.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, policy := range policies.Items {
		selector, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			return err
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			return fmt.Errorf("NetworkPolicy %s selects this pod; native network policy enforcement is not supported", policy.Name)
		}
	}
	return nil
}

func (p *provider) checkCurrentPod(ctx context.Context, e *podEntry) error {
	if p.client == nil {
		return nil
	}
	current, err := p.client.CoreV1().Pods(e.pod.Namespace).Get(ctx, e.pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if current.UID != e.pod.UID || current.Spec.NodeName != p.runner.cfg.nodeName || current.DeletionTimestamp != nil {
		return fmt.Errorf("Pod identity or assignment changed before wake")
	}
	if p.policies != nil && e.networkPolicyVersion != 1 {
		plan := p.policies.plan(current, p.policies.inventory())
		if plan.ingressIsolated || plan.egressIsolated {
			return fmt.Errorf("NetworkPolicy requires a new Pod UID created by this controller; preserve PVCs and recreate the legacy Pod")
		}
	}
	return p.checkNetworkPolicy(ctx, current)
}

func (p *provider) validatePolicyRecovery(record journalRecord) error {
	if p.runner.cfg.networkPolicy != (record.NetworkPolicyVersion == 1) {
		return fmt.Errorf("Pod %s was created under a different node-wide NetworkPolicy mode; use its original controller mode to stop and delete it, preserve PVCs, then recreate it", record.View.UID)
	}
	if p.runner.cfg.networkPolicy {
		if err := policyCapabilities(record.Pod); err != nil {
			return fmt.Errorf("unsafe NetworkPolicy recovery journal: %w", err)
		}
	}
	return nil
}

func containerNetworkArgs(policyVersion int) []string {
	if policyVersion == 0 {
		return []string{"--net-host"}
	}
	// ctr --net-host appends the node resolv.conf after user mounts, overriding
	// ClusterFirst. Join the same network namespace without replacing Pod DNS.
	return []string{"--with-ns", "network:/proc/1/ns/net", "--mount", "type=bind,src=/etc/hosts,dst=/etc/hosts,options=rbind:ro"}
}
