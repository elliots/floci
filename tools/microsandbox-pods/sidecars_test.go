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
	"k8s.io/apimachinery/pkg/api/resource"
)

type sidecarFixture struct {
	p            *provider
	e            *podEntry
	observations map[string]string
	requests     []string
	signals      []string
	startup      bool
	ready        bool
}

func newSidecarFixture(t *testing.T) *sidecarFixture {
	t.Helper()
	p, e, _ := initFixture(t)
	e.pod.Spec.RestartPolicy = corev1.RestartPolicyNever
	e.pod.Spec.InitContainers[0].Name = "proxy"
	e.pod.Spec.InitContainers[0].RestartPolicy = new(corev1.ContainerRestartPolicyAlways)
	e.pod.Spec.InitContainers[0].StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"startup"}}}, FailureThreshold: 10}
	e.pod.Spec.InitContainers[0].ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"ready"}}}, FailureThreshold: 1}
	e.pod.Status.InitContainerStatuses = nil
	initializeInitStatuses(e.pod)
	e.pod.Status.ContainerStatuses[0].Image = e.pod.Spec.Containers[0].Image
	e.volumesChecked = time.Now()
	f := &sidecarFixture{p: p, e: e, observations: map[string]string{}}
	base := p.runner.runtime
	p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
		joined := strings.Join(args, " ")
		if len(input) > 0 {
			f.requests = append(f.requests, string(input))
			return msbruntime.Result{Stdout: `{"state":"queued"}`}, nil
		}
		if strings.Contains(joined, "if test -f") && strings.Contains(joined, "application.exit") {
			for id, output := range f.observations {
				if strings.Contains(joined, "/floci-tasks/"+id+"/") || strings.Contains(joined, "/floci-"+id+"/") {
					return msbruntime.Result{Stdout: output}, nil
				}
			}
		}
		if strings.Contains(joined, "tasks kill") {
			f.signals = append(f.signals, joined)
			return msbruntime.Result{}, nil
		}
		if strings.Contains(joined, "tasks exec --exec-id probe-") {
			pass := f.startup
			if strings.HasSuffix(joined, "'ready'") {
				pass = f.ready
			}
			if !pass {
				return msbruntime.Result{ExitCode: 1}, nil
			}
			return msbruntime.Result{}, nil
		}
		return base.Execute(ctx, args, input)
	}}
	return f
}

func (f *sidecarFixture) initialize(t *testing.T) {
	t.Helper()
	f.startup, f.ready = true, true
	if err := f.p.advanceInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "init-0 10 RUNNING"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-1"] = "EXIT 0"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["app-0"] = "app-0 20 RUNNING"
	if err := f.p.monitorApplications(t.Context(), f.e); err != nil || !f.e.actualReady {
		t.Fatalf("initialized sidecar Pod did not become Ready: %v", err)
	}
}

func TestSidecarStartupGatesLaterInitButReadinessDoesNot(t *testing.T) {
	f := newSidecarFixture(t)
	if err := validatePod(f.e.pod); err != nil {
		t.Fatal(err)
	}
	if err := f.p.advanceInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "init-0 10 RUNNING"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if nextInitForEntry(f.e) != 0 || f.e.pod.Status.InitContainerStatuses[1].State.Waiting == nil {
		t.Fatal("later init started before sidecar startupProbe passed")
	}
	f.startup = true
	progress := (managedContainer{index: 0, init: true}).progress(f.e)
	progress.startup.at = time.Time{}
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if !progress.admitted || f.e.pod.Status.InitContainerStatuses[1].State.Running == nil || f.e.pod.Status.InitContainerStatuses[0].Ready {
		t.Fatal("startup/readiness gates were conflated")
	}
}

func TestSidecarRestartsIndependentlyUnderNeverWithoutReplayingInit(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	regularStarted := f.e.progress(0).started
	managed := managedContainer{index: 0, init: true}
	if err := f.p.managedContainerExited(f.e, managed, 0, "", ""); err != nil {
		t.Fatal(err)
	}
	if managed.progress(f.e).nextRestart.IsZero() || nextInitForEntry(f.e) != -1 {
		t.Fatal("sidecar followed Pod Never or forgot initial startup admission")
	}
	managed.progress(f.e).nextRestart = time.Now().Add(-time.Second)
	if err := f.p.monitorApplications(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if managed.status(f.e).RestartCount != 1 || !f.e.progress(0).started.Equal(regularStarted) || f.e.pod.Status.InitContainerStatuses[1].State.Terminated == nil || len(f.requests) != 3 {
		t.Fatal("sidecar restart replayed regular applications or completed init")
	}
	if f.e.actualReady || f.e.pod.Status.Conditions[0].Status != corev1.ConditionTrue {
		t.Fatal("sidecar restart retained readiness or lost initialized status")
	}
}

func TestSidecarReadinessControlsActualPodReadiness(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	f.ready = false
	(managedContainer{index: 0, init: true}).progress(f.e).readiness.at = time.Time{}
	if err := f.p.monitorApplications(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if f.e.actualReady || !f.e.pod.Status.ContainerStatuses[0].Ready || f.e.pod.Status.InitContainerStatuses[0].Ready {
		t.Fatal("sidecar readiness did not affect the aggregate independently")
	}
}

func TestSidecarResourcePeakIncludesPreviouslyStartedSidecars(t *testing.T) {
	pod := validPod()
	container := func(name, cpu, memory string, sidecar bool) corev1.Container {
		value := corev1.Container{Name: name, Image: "busybox:1.37.0", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory),
		}}}
		if sidecar {
			value.RestartPolicy = new(corev1.ContainerRestartPolicyAlways)
		}
		return value
	}
	pod.Spec.Containers = []corev1.Container{container("app", "1500m", "600Mi", false)}
	pod.Spec.InitContainers = []corev1.Container{container("proxy", "200m", "64Mi", true), container("prepare", "2", "800Mi", false), container("logs", "300m", "32Mi", true)}
	allocation, err := derivePodAllocation(pod)
	if err != nil || allocation.VCPUs != 3 || allocation.MemoryMiB != 1120 {
		t.Fatalf("sidecar stage peak incorrectly allocated: %+v %v", allocation, err)
	}
}

func TestSidecarJournalPreservesAdmissionAndRejectsPrematureLaterStages(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	managed := managedContainer{index: 0, init: true}
	if err := f.p.managedContainerExited(f.e, managed, 7, "", ""); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.p.runner.cfg.state, string(f.e.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil || record.Version != 6 {
		t.Fatalf("sidecar journal version: %v", err)
	}
	if err := validateInitJournal(record); err != nil {
		t.Fatal(err)
	}
	recovered := &podEntry{pod: record.Pod.DeepCopy(), state: record.View.State}
	if err := restoreApplicationRecords(recovered, record); err != nil || nextInitForEntry(recovered) != -1 {
		t.Fatalf("sidecar recovery lost admission: %v", err)
	}
	stored := record.Applications["proxy"]
	stored.Admitted = false
	record.Applications["proxy"] = stored
	if err := validateInitJournal(record); err == nil {
		t.Fatal("later init completion accepted without preceding sidecar startup")
	}
}

func TestSidecarJobWaitsForReverseShutdownAndReleasesMemory(t *testing.T) {
	f := newSidecarFixture(t)
	f.e.pod.Spec.InitContainers[1].RestartPolicy = new(corev1.ContainerRestartPolicyAlways)
	f.e.pod.Spec.InitContainers[0].StartupProbe, f.e.pod.Spec.InitContainers[0].ReadinessProbe = nil, nil
	for index := range f.e.pod.Spec.InitContainers {
		managed := managedContainer{index: index, init: true}
		if err := f.p.startManagedContainer(t.Context(), f.e, managed); err != nil {
			t.Fatal(err)
		}
		f.observations[managed.task(f.e)] = managed.task(f.e) + " 10 RUNNING"
		if err := f.p.monitorManagedContainer(t.Context(), f.e, managed); err != nil {
			t.Fatal(err)
		}
	}
	f.e.state = "RUNNING"
	if err := f.p.startApplications(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if err := f.p.applicationExited(f.e, 0, 0, "", ""); err != nil {
		t.Fatal(err)
	}
	if err := f.p.publishApplications(t.Context(), f.e); err != nil || f.e.state != "DRAINING" {
		t.Fatalf("completed Job did not begin sidecar shutdown: %v", err)
	}
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if len(f.signals) != 1 || !strings.Contains(f.signals[0], "SIGTERM 'init-1'") {
		t.Fatalf("sidecars did not stop in reverse order: %v", f.signals)
	}
	f.observations["init-1"] = "EXIT 143"
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if len(f.signals) != 2 || !strings.Contains(f.signals[1], "SIGTERM 'init-0'") {
		t.Fatalf("first sidecar stopped before last sidecar exited: %v", f.signals)
	}
	f.observations["init-0"] = "EXIT 143"
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil || f.e.state != "TERMINATED" || f.e.pod.Status.Phase != corev1.PodSucceeded {
		t.Fatalf("normal sidecar termination failed the Job or kept it alive: %v", err)
	}
}

func TestSidecarDeletionStopsApplicationsBeforeSidecars(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	base := f.p.runner.runtime
	f.p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
		result, err := base.Execute(ctx, args, input)
		joined := strings.Join(args, " ")
		if strings.Contains(joined, "tasks kill --signal SIGTERM") {
			for _, task := range []string{"app-0", "init-0"} {
				if strings.Contains(joined, "SIGTERM '"+task+"'") {
					f.observations[task] = "EXIT 143"
				}
			}
		}
		return result, err
	}}
	if err := f.p.drainBeforeDeletion(t.Context(), f.e, f.e.pod); err != nil {
		t.Fatal(err)
	}
	if len(f.signals) != 2 || !strings.Contains(f.signals[0], "SIGTERM 'app-0'") || !strings.Contains(f.signals[1], "SIGTERM 'init-0'") || f.e.state != "TERMINATED" {
		t.Fatalf("Pod deletion did not stop regular applications before sidecars: %v", f.signals)
	}
}

func TestSidecarShutdownRecoveryKeepsDeadlineAndEscalates(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	if err := f.p.beginSidecarShutdown(f.e, corev1.PodSucceeded); err != nil {
		t.Fatal(err)
	}
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	managed := managedContainer{index: 0, init: true}
	deadline := time.Now().Add(-time.Second)
	f.e.shutdownDeadline, managed.progress(f.e).nextKill = deadline, deadline
	if err := f.p.journal(f.e); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.p.runner.cfg.state, string(f.e.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var record journalRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if err := validateInitJournal(record); err != nil {
		t.Fatal(err)
	}
	f.e.pod = record.Pod.DeepCopy()
	f.e.applications = nil
	f.e.state = recoverAction(record.View.State, "Running", true, false)
	f.e.completionPhase, f.e.shutdownDeadline = record.CompletionPhase, record.ShutdownDeadline
	if err := restoreApplicationRecords(f.e, record); err != nil {
		t.Fatal(err)
	}
	if err := f.p.beginSidecarShutdown(f.e, corev1.PodFailed); err != nil {
		t.Fatal(err)
	}
	if !f.e.shutdownDeadline.Equal(deadline) || f.e.completionPhase != corev1.PodSucceeded {
		t.Fatal("recovery reset shutdown grace or changed Job outcome")
	}
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if len(f.signals) != 1 || f.e.state != "TERMINATED" || f.e.pod.Status.Phase != corev1.PodSucceeded || managed.status(f.e).State.Terminated.Reason != "ContainerStatusUnknown" {
		t.Fatalf("recovered sidecar did not use the original termination deadline: %v", f.signals)
	}
}

func TestSidecarFailedStartupNeverAdmitsLaterInit(t *testing.T) {
	f := newSidecarFixture(t)
	f.e.pod.Spec.InitContainers[0].StartupProbe.FailureThreshold = 1
	if err := f.p.advanceInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "init-0 10 RUNNING"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "EXIT 143"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	managed := managedContainer{index: 0, init: true}
	if managed.progress(f.e).admitted || managed.progress(f.e).nextRestart.IsZero() || f.e.pod.Status.InitContainerStatuses[1].State.Waiting == nil || nextInitForEntry(f.e) != 0 {
		t.Fatal("failed startup admitted a later init or failed to restart the sidecar")
	}
}

func TestSidecarStopsAfterOrdinaryInitFailureWithoutStartingApplications(t *testing.T) {
	f := newSidecarFixture(t)
	f.startup = true
	if err := f.p.advanceInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "init-0 10 RUNNING"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-1"] = "EXIT 7"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil || f.e.state != "DRAINING" {
		t.Fatalf("init failure did not begin sidecar shutdown: %v", err)
	}
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "EXIT 143"
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if f.e.pod.Status.Phase != corev1.PodFailed || f.e.pod.Status.ContainerStatuses[0].State.Waiting == nil || len(f.requests) != 1 {
		t.Fatal("failed init launched applications or lost failure outcome")
	}
}

func TestSidecarHibernationWithdrawsOnlyRunningInitReadiness(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	f.p.unavailable(f.e, "Hibernated")
	if f.e.pod.Status.InitContainerStatuses[0].Ready || !f.e.pod.Status.InitContainerStatuses[1].Ready {
		t.Fatal("hibernation kept a sidecar Ready or erased completed ordinary init readiness")
	}
}

func TestSidecarForceDeletionDoesNotGrantNewGrace(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	requested := f.e.pod.DeepCopy()
	requested.DeletionGracePeriodSeconds = new(int64(0))
	before := time.Now()
	if err := f.p.drainBeforeDeletion(t.Context(), f.e, requested); err != nil {
		t.Fatal(err)
	}
	if time.Since(before) >= time.Second || len(f.signals) != 0 || f.e.shutdownDeadline.After(time.Now()) {
		t.Fatal("force deletion waited for a fresh graceful shutdown")
	}
}

func TestSidecarStatusPublishesWhileLaterOrdinaryInitRuns(t *testing.T) {
	f := newSidecarFixture(t)
	f.startup, f.ready = true, true
	if err := f.p.advanceInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-0"] = "init-0 10 RUNNING"
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	f.observations["init-1"] = "init-1 11 RUNNING"
	f.observations["init-0"] = "EXIT 7"
	var published *corev1.Pod
	f.p.notify = func(pod *corev1.Pod) { published = pod }
	if err := f.p.monitorInit(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	if published == nil || published.Status.InitContainerStatuses[0].State.Waiting == nil || published.Status.InitContainerStatuses[0].State.Waiting.Reason != "CrashLoopBackOff" || published.Status.InitContainerStatuses[1].State.Running == nil {
		t.Fatal("sidecar status became invisible while an ordinary init remained active")
	}
}

func TestSidecarDeletionWaitsForQueuedApplicationsBeforeSignaling(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	f.e.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}
	f.observations["app-0"] = ""
	observed := 0
	base := f.p.runner.runtime
	f.p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
		joined := strings.Join(args, " ")
		if strings.Contains(joined, "if test -f") && strings.Contains(joined, "/app-0/") {
			observed++
			if observed == 2 {
				f.observations["app-0"] = "app-0 20 RUNNING"
			}
		}
		if strings.Contains(joined, "tasks kill --signal SIGTERM 'app-0'") && observed < 2 {
			t.Error("application signaled before its queued task became observable")
			return msbruntime.Result{ExitCode: 1}, nil
		}
		result, err := base.Execute(ctx, args, input)
		for _, task := range []string{"app-0", "init-0"} {
			if strings.Contains(joined, "tasks kill --signal SIGTERM '"+task+"'") {
				f.observations[task] = "EXIT 143"
			}
		}
		return result, err
	}}
	if err := f.p.drainBeforeDeletion(t.Context(), f.e, f.e.pod); err != nil {
		t.Fatal(err)
	}
	if len(f.signals) != 2 || f.e.state != "TERMINATED" {
		t.Fatal("queued application prevented ordered sidecar shutdown")
	}
}

func TestSidecarMissingExitCannotExtendJobShutdownDeadline(t *testing.T) {
	f := newSidecarFixture(t)
	f.initialize(t)
	if err := f.p.beginSidecarShutdown(f.e, corev1.PodSucceeded); err != nil {
		t.Fatal(err)
	}
	f.e.shutdownDeadline = time.Now().Add(-time.Second)
	f.observations["init-0"] = ""
	if err := f.p.drainSidecars(t.Context(), f.e); err != nil {
		t.Fatal(err)
	}
	status := f.e.pod.Status.InitContainerStatuses[0]
	if f.e.state != "TERMINATED" || f.e.pod.Status.Phase != corev1.PodSucceeded || status.State.Terminated == nil || status.State.Terminated.Reason != "ContainerStatusUnknown" {
		t.Fatal("missing sidecar completion status failed or prolonged a completed Job")
	}
}
