package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
)

func TestLifecycleValidationMatchesContainerKinds(t *testing.T) {
	pod := validPod()
	pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}, PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 0}}}
	if err := validatePod(pod); err != nil {
		t.Fatal(err)
	}
	container := pod.Spec.Containers[0].DeepCopy()
	container.Lifecycle.PostStart = &corev1.LifecycleHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8080)}}
	if err := validateLifecycle(*container); err != nil {
		t.Fatal(err)
	}
	if !networkProbes(*container) {
		t.Fatal("HTTP lifecycle hook did not require network transport")
	}
	container.Lifecycle.PostStart.Sleep = &corev1.SleepAction{Seconds: 1}
	if validateLifecycle(*container) == nil {
		t.Fatal("ambiguous/postStart sleep accepted")
	}
	pod.Spec.InitContainers = []corev1.Container{*pod.Spec.Containers[0].DeepCopy()}
	pod.Spec.InitContainers[0].Name = "init"
	if validatePod(pod) == nil {
		t.Fatal("ordinary init hooks accepted")
	}
	pod.Spec.InitContainers[0].RestartPolicy = new(corev1.ContainerRestartPolicyAlways)
	if err := validatePod(pod); err != nil {
		t.Fatal(err)
	}
}

func TestPostStartRunsBeforeStartupAndDoesNotReplayAfterRestore(t *testing.T) {
	p, e := probeFixture(t)
	e.pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"hook-command"}}}}
	calls := 0
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		calls++
		if !strings.Contains(strings.Join(args, " "), "hook-command") {
			t.Fatalf("unexpected command %v", args)
		}
		data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var journal journalRecord
		if err := json.Unmarshal(data, &journal); err != nil {
			t.Fatal(err)
		}
		if journal.Hooks.PostStart != "pending" || journal.StartupComplete {
			t.Fatal("hook dispatch was not fenced before startup")
		}
		return msbruntime.Result{}, nil
	}}
	if _, reason, err := p.healthChecks(t.Context(), e); err != nil || reason != "" {
		t.Fatalf("hook failed: %s %v", reason, err)
	}
	resetProbeProgress(e)
	if _, _, err := p.healthChecks(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || e.hooks.PostStart != "done" || !e.startupComplete {
		t.Fatal("postStart repeated or startup was not admitted")
	}
	e.hooks.PostStart = "pending"
	if _, reason, err := p.healthChecks(t.Context(), e); err != nil || reason != "FailedPostStartHook" || calls != 1 {
		t.Fatal("ambiguous recovered hook was replayed")
	}
}

func TestPreStopConsumesGraceAndEmitsFailureWithoutReplay(t *testing.T) {
	p, e := probeFixture(t)
	events := record.NewFakeRecorder(10)
	p.events = events
	e.pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 60}}}
	start := time.Now()
	if err := p.preStop(t.Context(), e, managedContainer{}, start.Add(20*time.Millisecond)); err == nil {
		t.Fatal("hook ignored grace")
	}
	if time.Since(start) > time.Second || e.hooks.PreStop != "failed" {
		t.Fatal("grace was exceeded or completion lost")
	}
	select {
	case message := <-events.Events:
		if !strings.Contains(message, "FailedPreStopHook") {
			t.Fatal(message)
		}
	default:
		t.Fatal("missing hook failure Event")
	}
	start = time.Now()
	if err := p.preStop(t.Context(), e, managedContainer{}, start.Add(time.Minute)); err == nil || time.Since(start) > time.Second {
		t.Fatal("failed hook replayed")
	}
}

func TestSleepingContainerRetainsLogicalExecutionForGracefulDeletion(t *testing.T) {
	p, e := probeFixture(t)
	e.state = "HIBERNATING"
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, Ready: true, Started: new(true)}}
	p.unavailable(e, "Hibernating")
	e.state = "SLEEPING"
	p.unavailable(e, "Hibernated")
	if status := e.pod.Status.ContainerStatuses[0]; status.Ready || status.State.Running == nil || status.Started == nil || !*status.Started {
		t.Fatal("sleeping live container appeared exited to Kubernetes deletion")
	}
	e.pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 0}}}
	if !needsTerminationWake(e, e.pod) {
		t.Fatal("sleeping preStop did not request bounded wake")
	}
	e.pod.DeletionGracePeriodSeconds = new(int64(0))
	if needsTerminationWake(e, e.pod) {
		t.Fatal("forced deletion woke the VM")
	}
	e.state = "COLD"
	e.pod.DeletionGracePeriodSeconds = nil
	if needsTerminationWake(e, e.pod) {
		t.Fatal("cold deletion booted the workload")
	}
}

func TestLifecycleJournalFencesOlderCleanup(t *testing.T) {
	pod := validPod()
	pod.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Sleep: &corev1.SleepAction{Seconds: 0}}}
	allocation, err := derivePodAllocation(pod)
	if err != nil {
		t.Fatal(err)
	}
	if podJournalVersion(pod) != 8 {
		t.Fatal("hook ownership lacks downgrade fence")
	}
	record := journalRecord{Pod: pod, Allocation: allocation, Version: 6}
	if _, err := record.guestAllocation(); err == nil {
		t.Fatal("legacy journal allowed ambiguous hook recovery")
	}
	record.Version = 8
	if _, err := record.guestAllocation(); err != nil {
		t.Fatal(err)
	}
}
