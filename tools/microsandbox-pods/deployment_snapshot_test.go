package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func snapshotFixture(name string) (appsv1.Deployment, appsv1.ReplicaSet, corev1.Pod, lifecycleView) {
	deployment := appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: name,
		UID: types.UID("deployment-" + name), Generation: 3}, Spec: appsv1.DeploymentSpec{
		Replicas: new(int32(1)), Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}}}}
	set := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "replicas-" + name,
		UID: types.UID("replicas-" + name), OwnerReferences: []metav1.OwnerReference{{
			Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: new(true)}}}}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "apps", Name: "pod-" + name,
		UID: types.UID("pod-" + name), Labels: map[string]string{"app": name}, OwnerReferences: []metav1.OwnerReference{{
			Kind: "ReplicaSet", Name: set.Name, UID: set.UID, Controller: new(true)}}}, Spec: corev1.PodSpec{NodeName: "native"}}
	view := lifecycleView{UID: string(pod.UID), Namespace: pod.Namespace, Name: pod.Name, State: "COLD", Generation: 0}
	return deployment, set, pod, view
}

func TestDeploymentSnapshotIsolatesForeignPodsFromOtherDeployments(t *testing.T) {
	good, goodSet, goodPod, goodView := snapshotFixture("good")
	bad, badSet, badPod, _ := snapshotFixture("foreign")
	badPod.Spec.NodeName = "other-node"
	rows := buildDeploymentSnapshot([]appsv1.Deployment{good, bad}, []appsv1.ReplicaSet{goodSet, badSet},
		[]corev1.Pod{goodPod, badPod}, map[types.UID]lifecycleView{goodPod.UID: goodView}, "native", "dev")
	if len(rows) != 2 {
		t.Fatalf("got %d rows", len(rows))
	}
	failure, ok := rows[0].(deploymentSnapshotError)
	if !ok || failure.Name != "foreign" || failure.Error == "" {
		t.Fatalf("foreign deployment did not fail closed: %#v", rows[0])
	}
	result, ok := rows[1].(deploymentView)
	if !ok || result.Name != "good" || result.ClusterName != "dev" || len(result.Pods) != 1 || result.Pods[0].State != "COLD" {
		t.Fatalf("unrelated deployment was lost: %#v", rows[1])
	}
}

func TestDeploymentSnapshotValidatesUidChainAndRolloutMembership(t *testing.T) {
	cases := map[string]func(*appsv1.Deployment, *appsv1.ReplicaSet, *[]corev1.Pod){
		"replaced replica set": func(_ *appsv1.Deployment, set *appsv1.ReplicaSet, _ *[]corev1.Pod) {
			set.UID = "replacement-replica-set"
		},
		"foreign surge pod": func(_ *appsv1.Deployment, _ *appsv1.ReplicaSet, pods *[]corev1.Pod) {
			extra := (*pods)[0].DeepCopy()
			extra.UID, extra.Name = "surge-pod", "surge-pod"
			*pods = append(*pods, *extra)
		},
		"selector mismatch": func(_ *appsv1.Deployment, _ *appsv1.ReplicaSet, pods *[]corev1.Pod) {
			(*pods)[0].Labels["app"] = "different-app"
		},
		"wrong node": func(_ *appsv1.Deployment, _ *appsv1.ReplicaSet, pods *[]corev1.Pod) {
			(*pods)[0].Spec.NodeName = "other-node"
		},
		"replica count changed": func(deployment *appsv1.Deployment, _ *appsv1.ReplicaSet, _ *[]corev1.Pod) {
			deployment.Spec.Replicas = new(int32(2))
		},
		"zero replicas": func(deployment *appsv1.Deployment, _ *appsv1.ReplicaSet, _ *[]corev1.Pod) {
			deployment.Spec.Replicas = new(int32(0))
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			deployment, set, pod, view := snapshotFixture("app")
			pods := []corev1.Pod{pod}
			change(&deployment, &set, &pods)
			rows := buildDeploymentSnapshot([]appsv1.Deployment{deployment}, []appsv1.ReplicaSet{set}, pods,
				map[types.UID]lifecycleView{pod.UID: view}, "native", "dev")
			if _, ok := rows[0].(deploymentSnapshotError); !ok {
				t.Fatalf("unsafe membership was accepted: %#v", rows[0])
			}
		})
	}
}

func TestDeploymentSnapshotIgnoresTerminatingOldPods(t *testing.T) {
	deployment, set, pod, view := snapshotFixture("app")
	old := pod.DeepCopy()
	old.UID, old.Name, old.DeletionTimestamp = "old-pod", "old-pod", new(metav1.Now())
	rows := buildDeploymentSnapshot([]appsv1.Deployment{deployment}, []appsv1.ReplicaSet{set}, []corev1.Pod{pod, *old},
		map[types.UID]lifecycleView{pod.UID: view}, "native", "dev")
	result, ok := rows[0].(deploymentView)
	if !ok || len(result.Pods) != 1 || result.Pods[0].UID != string(pod.UID) {
		t.Fatalf("terminating predecessor changed membership: %#v", rows[0])
	}
}

func TestDeploymentSnapshotReadsFiveHundredWorkloadsWithThreeListsWithoutLifecycleLocks(t *testing.T) {
	objects := make([]runtime.Object, 0, 1500)
	entries := make(map[string]*podEntry)
	for number := range 500 {
		deployment, set, pod, view := snapshotFixture(fmt.Sprintf("app-%03d", number))
		objects = append(objects, &deployment, &set, &pod)
		entry := &podEntry{pod: &pod, lifetime: t.Context()}
		entry.publishedView.Store(&view)
		entries[pod.Namespace+"/"+pod.Name] = entry
	}
	client := fake.NewClientset(objects...)
	provider := &provider{client: client, pods: entries, runner: commandRunner{cfg: config{nodeName: "native", clusterName: "dev"}}}
	busy := entries["apps/pod-app-000"]
	busy.op.Lock()
	defer busy.op.Unlock()
	response := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		provider.deploymentSnapshot(response, httptest.NewRequest(http.MethodGet, "/v1/deployments", nil))
		close(finished)
	}()
	awaitSignal(t, finished)
	if response.Code != http.StatusOK {
		t.Fatalf("batch returned %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Deployments []deploymentView `json:"deployments"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || len(body.Deployments) != 500 {
		t.Fatalf("invalid batch, count=%d error=%v", len(body.Deployments), err)
	}
	actions := client.Actions()
	if len(actions) != 3 {
		t.Fatalf("batch used %d API calls instead of three", len(actions))
	}
	for _, action := range actions {
		if action.GetVerb() != "list" || action.GetNamespace() != "" {
			t.Fatalf("unexpected API call: %#v", action)
		}
	}
}
