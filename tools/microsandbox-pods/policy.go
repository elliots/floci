package main

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func (p *provider) checkNetworkPolicy(ctx context.Context, pod *corev1.Pod) error {
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
	return p.checkNetworkPolicy(ctx, current)
}
