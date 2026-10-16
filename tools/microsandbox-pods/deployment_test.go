package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

func TestDeploymentReadsFiveHundredReplicasWithThreeListsWithoutLifecycleLocks(t *testing.T) {
	deployment, set, template, _ := snapshotFixture("large")
	deployment.Spec.Replicas = new(int32(500))
	objects := []runtime.Object{&deployment, &set}
	entries := make(map[string]*podEntry, 500)
	for number := range 500 {
		pod := template.DeepCopy()
		pod.Name = fmt.Sprintf("large-%03d", number)
		pod.UID = types.UID(pod.Name)
		view := lifecycleView{UID: string(pod.UID), Namespace: pod.Namespace, Name: pod.Name, State: "COLD"}
		entry := &podEntry{pod: pod, lifetime: t.Context()}
		entry.publishedView.Store(&view)
		objects = append(objects, pod)
		entries[pod.Namespace+"/"+pod.Name] = entry
	}
	client := fake.NewClientset(objects...)
	p := &provider{client: client, pods: entries, runner: commandRunner{cfg: config{nodeName: "native", clusterName: "dev"}}}
	busy := entries["apps/large-000"]
	busy.op.Lock()
	defer busy.op.Unlock()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/deployments/{namespace}/{name}", p.deployment)
	response := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/v1/deployments/apps/large", nil))
		close(finished)
	}()
	awaitSignal(t, finished)
	if response.Code != http.StatusOK {
		t.Fatalf("discovery returned %d: %s", response.Code, response.Body.String())
	}
	var body deploymentView
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.UID != string(deployment.UID) || body.Replicas != 500 || len(body.Pods) != 500 || body.ClusterName != "dev" {
		t.Fatalf("incomplete discovery: uid=%q replicas=%d pods=%d cluster=%q", body.UID, body.Replicas, len(body.Pods), body.ClusterName)
	}
	for number, view := range body.Pods {
		if view.UID != fmt.Sprintf("large-%03d", number) || view.State != "COLD" {
			t.Fatalf("unexpected pod view at %d: %#v", number, view)
		}
	}
	actions := client.Actions()
	if len(actions) != 3 {
		t.Fatalf("single-deployment discovery used %d API calls instead of three", len(actions))
	}
	resources := make(map[string]bool)
	for _, action := range actions {
		if action.GetVerb() != "list" || action.GetNamespace() != "" {
			t.Fatalf("unexpected API call: %#v", action)
		}
		resources[action.GetResource().Resource] = true
	}
	if !resources["deployments"] || !resources["replicasets"] || !resources["pods"] {
		t.Fatalf("unexpected discovery resources: %#v", resources)
	}
}

func TestDeploymentErrorsRemainStructuredAndFailClosed(t *testing.T) {
	for _, name := range []string{"missing", "foreign pod", "list failure"} {
		t.Run(name, func(t *testing.T) {
			deployment, set, pod, view := snapshotFixture("app")
			client := fake.NewClientset(&deployment, &set, &pod)
			entry := &podEntry{pod: &pod, lifetime: t.Context()}
			entry.publishedView.Store(&view)
			p := &provider{client: client, pods: map[string]*podEntry{"apps/pod-app": entry},
				runner: commandRunner{cfg: config{nodeName: "native", clusterName: "dev"}}}
			path, status := "/v1/deployments/apps/app", http.StatusConflict
			switch name {
			case "missing":
				path, status = "/v1/deployments/apps/missing", http.StatusNotFound
			case "foreign pod":
				p.pods = nil
			case "list failure":
				status = http.StatusServiceUnavailable
				client.PrependReactor("list", "replicasets", func(clienttesting.Action) (bool, runtime.Object, error) {
					return true, nil, errors.New("private upstream failure details")
				})
			}
			mux := http.NewServeMux()
			mux.HandleFunc("GET /v1/deployments/{namespace}/{name}", p.deployment)
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			var body map[string]string
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if response.Code != status || body["error"] == "" || len(body) != 1 {
				t.Fatalf("unexpected error response %d: %#v", response.Code, body)
			}
			if name == "list failure" && body["error"] != "deployment snapshot could not be read" {
				t.Fatalf("upstream error leaked: %q", body["error"])
			}
		})
	}
}

func TestDeploymentSnapshotRejectsMismatchedDeploymentOwnerIdentity(t *testing.T) {
	for _, name := range []string{"wrong owner name", "different deployment namespace"} {
		t.Run(name, func(t *testing.T) {
			deployment, set, pod, view := snapshotFixture("app")
			switch name {
			case "wrong owner name":
				set.OwnerReferences[0].Name = "unrelated-deployment"
			case "different deployment namespace":
				deployment.Namespace = "unrelated-namespace"
			}
			rows := buildDeploymentSnapshot([]appsv1.Deployment{deployment}, []appsv1.ReplicaSet{set}, []corev1.Pod{pod},
				map[types.UID]lifecycleView{pod.UID: view}, "native", "dev")
			if len(rows) != 1 {
				t.Fatalf("got %d deployment rows", len(rows))
			}
			failure, ok := rows[0].(deploymentSnapshotError)
			if !ok || failure.Error == "" || failure.Namespace != deployment.Namespace || failure.Name != deployment.Name {
				t.Fatalf("invalid owner chain was accepted: %#v", rows[0])
			}
		})
	}
}
