package main

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

type deploymentSnapshotError struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Error     string `json:"error"`
}

func (p *provider) deploymentSnapshot(w http.ResponseWriter, r *http.Request) {
	rows, err := p.readDeploymentSnapshot(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "deployment snapshot could not be read"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deployments": rows})
}

func (p *provider) readDeploymentSnapshot(ctx context.Context) ([]any, error) {
	deployments, err := p.client.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list deployments: %w", err)
	}
	replicaSets, err := p.client.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list replica sets: %w", err)
	}
	pods, err := p.client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list pods: %w", err)
	}
	views := make(map[types.UID]lifecycleView)
	p.mu.Lock()
	for _, entry := range p.pods {
		// Lifecycle I/O holds entry.op. Published immutable views let discovery
		// of unrelated workloads continue while a VM is starting or restoring.
		if entry.lifetime.Err() == nil {
			if current := entry.publishedView.Load(); current != nil {
				views[entry.pod.UID] = *current
			}
		}
	}
	p.mu.Unlock()
	return buildDeploymentSnapshot(deployments.Items, replicaSets.Items, pods.Items, views,
		p.runner.cfg.nodeName, p.runner.cfg.clusterName), nil
}

func buildDeploymentSnapshot(deployments []appsv1.Deployment, replicaSets []appsv1.ReplicaSet,
	pods []corev1.Pod, views map[types.UID]lifecycleView, nodeName, clusterName string) []any {
	sets := make(map[types.UID]*appsv1.ReplicaSet, len(replicaSets))
	for i := range replicaSets {
		sets[replicaSets[i].UID] = &replicaSets[i]
	}
	byUID := make(map[types.UID]*appsv1.Deployment, len(deployments))
	for i := range deployments {
		byUID[deployments[i].UID] = &deployments[i]
	}
	owned := make(map[types.UID][]corev1.Pod)
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		setOwner := controllerOwner(pod.OwnerReferences, "ReplicaSet")
		if setOwner == nil {
			continue
		}
		set := sets[setOwner.UID]
		if set == nil || set.Namespace != pod.Namespace || set.Name != setOwner.Name {
			continue
		}
		deployment := controllerOwner(set.OwnerReferences, "Deployment")
		if deployment != nil {
			actual := byUID[deployment.UID]
			if actual != nil && actual.Name == deployment.Name && actual.Namespace == set.Namespace && actual.Namespace == pod.Namespace {
				owned[deployment.UID] = append(owned[deployment.UID], pod)
			}
		}
	}
	ordered := slices.Clone(deployments)
	slices.SortFunc(ordered, func(a, b appsv1.Deployment) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	rows := make([]any, 0, len(ordered))
	for _, deployment := range ordered {
		result, err := deploymentSnapshotRow(deployment, owned[deployment.UID], views, nodeName, clusterName)
		if err != nil {
			rows = append(rows, deploymentSnapshotError{Namespace: deployment.Namespace, Name: deployment.Name, Error: err.Error()})
		} else {
			rows = append(rows, result)
		}
	}
	return rows
}

func deploymentSnapshotRow(deployment appsv1.Deployment, pods []corev1.Pod,
	views map[types.UID]lifecycleView, nodeName, clusterName string) (deploymentView, error) {
	result := deploymentView{Namespace: deployment.Namespace, Name: deployment.Name, UID: string(deployment.UID),
		Generation: deployment.Generation, ClusterName: clusterName, Pods: []lifecycleView{}}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < 1 {
		return result, fmt.Errorf("deployment must retain at least one replica")
	}
	result.Replicas = *deployment.Spec.Replicas
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return result, fmt.Errorf("deployment selector is invalid")
	}
	seen := make(map[string]bool)
	for _, pod := range pods {
		if pod.Namespace != deployment.Namespace || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		current, exists := views[pod.UID]
		if !exists || pod.Spec.NodeName != nodeName || current.Namespace != pod.Namespace ||
			current.Name != pod.Name || current.UID != string(pod.UID) || seen[current.UID] {
			return result, fmt.Errorf("deployment has a pod outside this controller's active ownership")
		}
		seen[current.UID] = true
		result.Pods = append(result.Pods, current)
	}
	if len(result.Pods) != int(result.Replicas) {
		return result, fmt.Errorf("deployment membership is incomplete or changing")
	}
	slices.SortFunc(result.Pods, func(a, b lifecycleView) int { return cmp.Compare(a.Name, b.Name) })
	return result, nil
}

func controllerOwner(owners []metav1.OwnerReference, kind string) *metav1.OwnerReference {
	for i := range owners {
		owner := &owners[i]
		if owner.Kind == kind && owner.Controller != nil && *owner.Controller {
			return owner
		}
	}
	return nil
}
