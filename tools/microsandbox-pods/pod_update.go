package main

import (
	"cmp"
	"context"
	"maps"
	"reflect"
	"slices"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (p *provider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	entry, err := p.entry(pod.Namespace, pod.Name)
	if err != nil {
		return err
	}
	entry.op.Lock()
	defer entry.op.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if entry.pod.UID != pod.UID {
		return errdefs.InvalidInput("in-place pod spec updates are not qualified")
	}
	if p.client == nil {
		if !resolvedSpecsEqual(entry.pod.Spec, pod.Spec) {
			return errdefs.InvalidInput("in-place pod spec updates are not qualified")
		}
	} else {
		declared, err := p.readDeclaredPod(ctx, pod)
		if err != nil {
			return err
		}
		if err := reconcileDeclaredSpec(entry, declared); err != nil {
			return err
		}
	}
	for _, key := range []string{"microsandbox.dev/availability", "microsandbox.dev/on-demand",
		"microsandbox.dev/idle-owner", "microsandbox.dev/idle-timeout-seconds", "microsandbox.dev/proxy-protocol"} {
		if entry.pod.Annotations[key] != pod.Annotations[key] {
			return errdefs.InvalidInput("runtime annotation changes require Pod recreation")
		}
	}
	entry.mu.Lock()
	previous := entry.pod.ObjectMeta.DeepCopy()
	entry.pod.Labels = maps.Clone(pod.Labels)
	entry.pod.Annotations = maps.Clone(pod.Annotations)
	entry.pod.ResourceVersion = pod.ResourceVersion
	entry.pod.Generation = pod.Generation
	entry.mu.Unlock()
	if err := p.journal(entry); err != nil {
		entry.mu.Lock()
		entry.pod.ObjectMeta = *previous
		entry.mu.Unlock()
		return err
	}
	p.publish(entry)
	return nil
}

func (p *provider) readDeclaredPod(ctx context.Context, resolved *corev1.Pod) (*corev1.Pod, error) {
	if p.client == nil {
		return resolved.DeepCopy(), nil
	}
	declared, err := p.client.CoreV1().Pods(resolved.Namespace).Get(ctx, resolved.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if declared.UID != resolved.UID || declared.Spec.NodeName != p.runner.cfg.nodeName || declared.DeletionTimestamp != nil {
		return nil, errdefs.InvalidInput("Pod identity or assignment changed")
	}
	if !specsEqualOutsideResolvedEnv(resolved.Spec, declared.Spec) {
		return nil, errdefs.InvalidInput("resolved Pod does not match its declared spec")
	}
	return declared, nil
}

func reconcileDeclaredSpec(entry *podEntry, declared *corev1.Pod) error {
	if declared.UID != entry.pod.UID {
		return errdefs.InvalidInput("Pod identity changed")
	}
	if entry.declaredSpec == nil {
		// Older journals retained only VK's resolved environment. Kubernetes keeps
		// declared environment immutable for a Pod UID, so migrate that field only.
		if !specsEqualOutsideResolvedEnv(entry.pod.Spec, declared.Spec) {
			return errdefs.InvalidInput("legacy Pod journal no longer matches its declared spec")
		}
		entry.declaredSpec = declared.Spec.DeepCopy()
		return nil
	}
	if !reflect.DeepEqual(entry.declaredSpec, &declared.Spec) {
		return errdefs.InvalidInput("in-place pod spec updates are not qualified")
	}
	return nil
}

func specsEqualOutsideResolvedEnv(first, second corev1.PodSpec) bool {
	left, right := first.DeepCopy(), second.DeepCopy()
	for _, spec := range []*corev1.PodSpec{left, right} {
		for i := range spec.Containers {
			spec.Containers[i].Env, spec.Containers[i].EnvFrom = nil, nil
		}
		for i := range spec.InitContainers {
			spec.InitContainers[i].Env, spec.InitContainers[i].EnvFrom = nil, nil
		}
		for i := range spec.EphemeralContainers {
			spec.EphemeralContainers[i].Env, spec.EphemeralContainers[i].EnvFrom = nil, nil
		}
	}
	return reflect.DeepEqual(left, right)
}

func resolvedSpecsEqual(first, second corev1.PodSpec) bool {
	left, right := first.DeepCopy(), second.DeepCopy()
	for _, spec := range []*corev1.PodSpec{left, right} {
		for i := range spec.Containers {
			if !normalizeResolvedEnv(spec.Containers[i].Env) {
				return false
			}
		}
		for i := range spec.InitContainers {
			if !normalizeResolvedEnv(spec.InitContainers[i].Env) {
				return false
			}
		}
		for i := range spec.EphemeralContainers {
			if !normalizeResolvedEnv(spec.EphemeralContainers[i].Env) {
				return false
			}
		}
	}
	return reflect.DeepEqual(left, right)
}

func normalizeResolvedEnv(env []corev1.EnvVar) bool {
	// Virtual Kubelet resolves environment values through maps on each update.
	// Only their ordering is irrelevant; changed values still require a new Pod.
	slices.SortFunc(env, func(a, b corev1.EnvVar) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Value, b.Value))
	})
	for i := 1; i < len(env); i++ {
		if env[i-1].Name == env[i].Name {
			return false
		}
	}
	return true
}
