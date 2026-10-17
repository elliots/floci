package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func applicationsFixture(t *testing.T) (*provider, *podEntry, *[]string) {
	t.Helper()
	p, e, _ := initFixture(t)
	e.pod.Spec.InitContainers, e.pod.Status.InitContainerStatuses = nil, nil
	e.pod.Spec.Containers = append(e.pod.Spec.Containers, corev1.Container{Name: "worker", Image: "busybox:1.37.0", Command: []string{"sleep", "300"}})
	e.pod.Status.ContainerStatuses = nil
	for _, container := range e.pod.Spec.Containers {
		e.pod.Status.ContainerStatuses = append(e.pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: container.Name, Image: container.Image,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}})
	}
	e.state, e.volumesChecked = "RUNNING", time.Now()
	base := p.runner.runtime
	var submissions []string
	p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
		if len(input) != 0 {
			submissions = append(submissions, string(input))
			return msbruntime.Result{Stdout: `{"state":"queued"}`}, nil
		}
		joined := strings.Join(args, " ")
		if strings.Contains(joined, "if test -f") && strings.Contains(joined, "application.exit") {
			for _, id := range []string{"app-0", "app-1"} {
				if strings.Contains(joined, "/floci-tasks/"+id+"/") {
					data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, id+".observation"))
					return msbruntime.Result{Stdout: string(data)}, err
				}
			}
		}
		return base.Execute(ctx, args, input)
	}}
	return p, e, &submissions
}

func applicationObservation(t *testing.T, p *provider, id, output string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(p.runner.cfg.state, id+".observation"), []byte(output), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestMultipleApplicationsValidateAndReserveConcurrentResources(t *testing.T) {
	pod := validPod()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("600m"), corev1.ResourceMemory: resource.MustParse("600Mi")}
	second := pod.Spec.Containers[0].DeepCopy()
	second.Name = "worker"
	pod.Spec.Containers = append(pod.Spec.Containers, *second)
	pod.Spec.InitContainers = []corev1.Container{{Name: "prepare", Image: "busybox:1.37.0", Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3"), corev1.ResourceMemory: resource.MustParse("700Mi")}}}}
	if err := validatePod(pod); err != nil {
		t.Fatal(err)
	}
	allocation, err := derivePodAllocation(pod)
	if err != nil || allocation.VCPUs != 3 || allocation.MemoryMiB != 1456 {
		t.Fatalf("concurrent applications and sequential init have incorrect budget: %+v %v", allocation, err)
	}
	pod.Spec.Containers[1].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("255")
	if _, err := derivePodAllocation(pod); err == nil {
		t.Fatal("accepted a concurrent CPU total over the native limit")
	}
}

func TestMultipleApplicationsStartIndependentlyAndRequireAllReadiness(t *testing.T) {
	p, e, submissions := applicationsFixture(t)
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if len(*submissions) != 2 || !strings.Contains((*submissions)[0], "'app-0'") || !strings.Contains((*submissions)[1], "'app-1'") {
		t.Fatalf("applications did not get independent requests: %d", len(*submissions))
	}
	applicationObservation(t, p, "app-0", "app-0 10 RUNNING\napp-1 11 RUNNING")
	applicationObservation(t, p, "app-1", "app-0 10 RUNNING")
	if err := p.monitorApplications(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if e.actualReady || !e.pod.Status.ContainerStatuses[0].Ready || e.pod.Status.ContainerStatuses[1].Ready {
		t.Fatal("one running container made the entire Pod ready")
	}
	applicationObservation(t, p, "app-1", "app-0 10 RUNNING\napp-1 11 RUNNING")
	if err := p.monitorApplications(t.Context(), e); err != nil || !e.actualReady || e.state != "READY" {
		t.Fatalf("all containers did not become ready: %s %v", e.state, err)
	}
	if task, err := executingContainerTask(e, "worker"); err != nil || task != "app-1" {
		t.Fatalf("named exec did not resolve second container: %s %v", task, err)
	}
}

func TestFailedApplicationBackoffDoesNotBlockHealthyContainerOrReplayIt(t *testing.T) {
	p, e, submissions := applicationsFixture(t)
	e.pod.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	applicationObservation(t, p, "app-0", "EXIT 7")
	applicationObservation(t, p, "app-1", "app-1 11 RUNNING")
	if err := p.monitorApplications(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if e.pod.Status.ContainerStatuses[0].State.Waiting.Reason != "CrashLoopBackOff" || !e.pod.Status.ContainerStatuses[1].Ready {
		t.Fatal("failed app blocked the healthy app's readiness update")
	}
	if err := p.monitorApplications(t.Context(), e); err != nil || len(*submissions) != 2 {
		t.Fatal("backoff restarted a container early", err)
	}
	e.progress(0).nextRestart = time.Now().Add(-time.Second)
	if err := p.monitorApplication(t.Context(), e, 0); err != nil {
		t.Fatal(err)
	}
	if len(*submissions) != 3 || !strings.Contains((*submissions)[2], "'app-0'") || e.pod.Status.ContainerStatuses[0].RestartCount != 1 || e.pod.Status.ContainerStatuses[1].RestartCount != 0 {
		t.Fatal("restart replayed the healthy container or lost restart count")
	}
}

func TestPodCompletionWaitsForEveryRegularContainer(t *testing.T) {
	p, e, _ := applicationsFixture(t)
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	applicationObservation(t, p, "app-0", "EXIT 0")
	applicationObservation(t, p, "app-1", "app-1 11 RUNNING")
	if err := p.monitorApplications(t.Context(), e); err != nil || e.state == "TERMINATED" {
		t.Fatal("first completed container terminated the Pod", err)
	}
	applicationObservation(t, p, "app-1", "EXIT 5\nOOMKilled")
	if err := p.monitorApplications(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if e.state != "TERMINATED" || e.pod.Status.Phase != corev1.PodFailed || e.pod.Status.ContainerStatuses[1].State.Terminated.Reason != "OOMKilled" || e.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 {
		t.Fatal("Pod completion lost a container's exit or OOM attribution")
	}
}

func TestConcurrentProxyReadinessDoesNotOverwriteActualApplicationReadiness(t *testing.T) {
	p, e, _ := applicationsFixture(t)
	e.proxyAvailability, e.proxyRoutesReady = true, true
	e.pod.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	applicationObservation(t, p, "app-0", "EXIT 7")
	applicationObservation(t, p, "app-1", "app-1 11 RUNNING")
	for range 2 {
		if err := p.monitorApplications(t.Context(), e); err != nil {
			t.Fatal(err)
		}
		if e.actualReady || e.state == "READY" || !e.pod.Status.ContainerStatuses[0].Ready {
			t.Fatal("proxy availability leaked into actual all-container readiness")
		}
	}
}

func TestApplicationRecoveryPreservesIndependentBackoffAndCompletedStates(t *testing.T) {
	p, e, _ := applicationsFixture(t)
	e.pod.Spec.RestartPolicy = corev1.RestartPolicyOnFailure
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if err := p.applicationExited(e, 0, 7, "Error", ""); err != nil {
		t.Fatal(err)
	}
	if err := p.applicationExited(e, 1, 0, "Completed", ""); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	recovered := &podEntry{pod: record.Pod.DeepCopy(), state: "RUNNING"}
	if err := restoreApplicationRecords(recovered, record); err != nil || record.Version != 6 || recovered.progress(0).nextRestart.IsZero() || !recovered.progress(1).nextRestart.IsZero() {
		t.Fatalf("independent progress did not round-trip: %v", err)
	}
	p.unavailable(recovered, "ControllerRecovering")
	if recovered.pod.Status.ContainerStatuses[1].State.Terminated == nil || recovered.pod.Status.ContainerStatuses[0].State.Waiting.Reason != "CrashLoopBackOff" {
		t.Fatal("recovery destroyed completed/backoff status and could replay work")
	}
	delete(record.Applications, "worker")
	if err := restoreApplicationRecords(recovered, record); err == nil {
		t.Fatal("incomplete concurrent journal accepted")
	}
}

func TestNamedProbeGraceDoesNotBlockOtherApplications(t *testing.T) {
	p, e, _ := applicationsFixture(t)
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	e.pod.Spec.TerminationGracePeriodSeconds = new(int64(30))
	applicationObservation(t, p, "app-0", "app-0 10 RUNNING")
	applicationObservation(t, p, "app-1", "app-1 11 RUNNING")
	before := time.Now()
	if err := p.stopNamedUnhealthy(t.Context(), e, 0, nil, "LivenessProbeFailed"); err != nil {
		t.Fatal(err)
	}
	if time.Since(before) > 5*time.Second || e.progress(0).nextKill.IsZero() {
		t.Fatal("grace period blocked the Pod monitor")
	}
	if err := p.hibernate(t.Context(), e); err == nil || !strings.Contains(err.Error(), "probe-triggered") {
		t.Fatal("pending container termination was eligible for RAM hibernation")
	}
	if err := p.monitorApplications(t.Context(), e); err != nil || !e.progress(1).ready || e.progress(0).ready {
		t.Fatal("grace period hid another container's readiness", err)
	}
}

func TestMultiApplicationRecoveryPreservesPreviouslyMountedSources(t *testing.T) {
	_, e, _ := applicationsFixture(t)
	if !volumeSourcesRequired(e) {
		t.Fatal("never-started containers proved a volume projection")
	}
	e.progress(1).started = time.Now()
	if volumeSourcesRequired(e) {
		t.Fatal("recovered application required sources despite its existing mounted projection")
	}
}

func TestMultiApplicationSubmissionFailureRetainsStartFence(t *testing.T) {
	p, e, _ := applicationsFixture(t)
	base := p.runner.runtime
	p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
		if len(input) != 0 {
			return msbruntime.Result{}, errors.New("lost completion")
		}
		return base.Execute(ctx, args, input)
	}}
	if err := p.startApplication(t.Context(), e); err == nil || e.state != "APPSTARTING" {
		t.Fatal("ambiguous start became a retryable known failure")
	}
	if recoverAction(e.state, "Running", true, false) != "FAILED" {
		t.Fatal("recovery would replay uncertain application submission")
	}
}

func TestNamedApplicationProbeUsesItsOwnContainerAndTask(t *testing.T) {
	p, e, _ := applicationsFixture(t)
	var called string
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		called = strings.Join(args, " ")
		return msbruntime.Result{}, nil
	}}
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"health", "worker"}}}}
	if healthy, err := p.executeContainerProbe(t.Context(), e, e.pod.Spec.Containers[1], "app-1", probe); err != nil || !healthy || !strings.Contains(called, "'app-1' 'health' 'worker'") {
		t.Fatalf("probe targeted wrong task: %s %v", called, err)
	}
	e.pod.Status.ContainerStatuses[1].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}
	if task, err := executingContainerTask(e, "worker"); err != nil || task != "app-1" {
		t.Fatalf("exec targeted wrong task: %s %v", task, err)
	}
}
