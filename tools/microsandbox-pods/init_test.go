package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func initFixture(t *testing.T) (*provider, *podEntry, string) {
	t.Helper()
	p, e := probeFixture(t)
	e.vm, e.lifetime, e.started, e.state = "init-fixture", t.Context(), time.Time{}, "INITIALIZING"
	e.pod.Spec.InitContainers = []corev1.Container{
		{Name: "prepare", Image: "busybox:1.37.0", Command: []string{"sh", "-c", "echo prepared"}},
		{Name: "finish", Image: "busybox:1.37.0", Args: []string{"sh", "-c", "echo finished"}},
	}
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}}}}
	initializeInitStatuses(e.pod)
	p.pods = map[string]*podEntry{"default/app": e}
	log := filepath.Join(p.runner.cfg.state, "calls")
	manifestDigest, configDigest := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	manifest := `{"schemaVersion":2,"config":{"digest":"` + configDigest + `"}}`
	config := `{"os":"linux","architecture":"` + runtime.GOARCH + `","config":{"Cmd":["sh"],"User":"0"}}`
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(log) + "\ncase \"$*\" in\n" +
		"*'images check'*) echo docker.io/library/busybox:1.37.0;;\n" +
		"*'images list'*) echo 'docker.io/library/busybox:1.37.0 application/vnd.oci.image.manifest.v1+json " + manifestDigest + "';;\n" +
		"*'content get'*" + manifestDigest + "*) printf '%s\\n' " + quote(manifest) + ";;\n" +
		"*'content get'*" + configDigest + "*) printf '%s\\n' " + quote(config) + ";;\n" +
		"*'if test -f'*'application.exit'*) cat " + quote(filepath.Join(p.runner.cfg.state, "observation")) + ";;\n" +
		"*'tail -c'*) printf 'prepare log\\nsecond line\\n';;\n" +
		"ls*) echo '[]';;\n" +
		"*) exit 0;;\nesac\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return p, e, log
}

func observeInit(t *testing.T, p *provider, e *podEntry, output string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(p.runner.cfg.state, "observation"), []byte(output), 0600); err != nil {
		t.Fatal(err)
	}
	if err := p.monitorInit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
}

func TestInitContainersCompleteSequentiallyAndAppRestartsDoNotReplayThem(t *testing.T) {
	p, e, log := initFixture(t)
	if err := p.advanceInit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if e.state != "INITIALIZING" || e.pod.Status.InitContainerStatuses[0].State.Running == nil || e.pod.Status.InitContainerStatuses[1].State.Waiting == nil {
		t.Fatal("init containers did not start sequentially")
	}
	if conditions := podConditionsFor(e.pod, false); conditions[0].Status != corev1.ConditionFalse || e.pod.Status.Phase != corev1.PodPending {
		t.Fatal("initializing strict pod did not remain Pending and uninitialized")
	}
	observeInit(t, p, e, "EXIT 0")
	if nextInitContainer(e.pod) != 1 || e.pod.Status.InitContainerStatuses[0].State.Terminated.Reason != "Completed" {
		t.Fatal("first successful init did not advance")
	}
	if err := p.advanceInit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	observeInit(t, p, e, "EXIT 0")
	if nextInitContainer(e.pod) != -1 || podConditionsFor(e.pod, false)[0].Status != corev1.ConditionTrue {
		t.Fatal("successful init sequence did not initialize the pod")
	}
	if err := p.advanceInit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if e.state != "RUNNING" || e.started.IsZero() {
		t.Fatal("app was not started after initialization")
	}
	if err := p.startApplication(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if starts := strings.Count(string(data), "mv /tmp/application.pending /tmp/application.request"); starts != 4 {
		t.Fatalf("expected two init launches and two app launches, got %d", starts)
	}
	for _, status := range e.pod.Status.InitContainerStatuses {
		if status.RestartCount != 0 || status.State.Terminated == nil || status.State.Terminated.ExitCode != 0 {
			t.Fatal("application restart changed completed init status")
		}
	}
	var record journalRecord
	data, err = os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
	if err != nil || json.Unmarshal(data, &record) != nil || record.Version != 3 || validateInitJournal(record) != nil {
		t.Fatal("completed init state was not durably recoverable")
	}
	logs, err := p.GetContainerLogs(t.Context(), "default", "app", "prepare", api.ContainerLogOpts{Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer logs.Close()
	data, err = io.ReadAll(logs)
	if err != nil || string(data) != "second line\n" {
		t.Fatalf("completed init logs unavailable: %q %v", data, err)
	}
}

func TestInitFailureRetriesOnlyTheFailedStageWithBackoff(t *testing.T) {
	for _, policy := range []corev1.RestartPolicy{corev1.RestartPolicyAlways, corev1.RestartPolicyOnFailure} {
		t.Run(string(policy), func(t *testing.T) {
			p, e, _ := initFixture(t)
			e.pod.Spec.RestartPolicy = policy
			if err := p.advanceInit(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			observeInit(t, p, e, "EXIT 7")
			status := &e.pod.Status.InitContainerStatuses[0]
			if status.State.Waiting == nil || status.State.Waiting.Reason != "CrashLoopBackOff" || status.LastTerminationState.Terminated.ExitCode != 7 || status.RestartCount != 0 {
				t.Fatalf("init failed to preserve retry diagnostics: %+v", status)
			}
			if err := p.advanceInit(t.Context(), e); err != nil || status.State.Waiting == nil {
				t.Fatal("init ignored restart backoff")
			}
			status.LastTerminationState.Terminated.FinishedAt = metav1.NewTime(time.Now().Add(-time.Minute))
			if err := p.advanceInit(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			if status.State.Running == nil || status.RestartCount != 1 || status.LastTerminationState.Terminated.ExitCode != 7 || e.pod.Status.InitContainerStatuses[1].State.Waiting == nil {
				t.Fatal("init retry advanced the wrong container or lost its exit state")
			}
			observeInit(t, p, e, "EXIT 0")
			if nextInitContainer(e.pod) != 1 {
				t.Fatal("successful retry did not advance")
			}
		})
	}
}

func TestInitNeverFailureStopsPodPreservesLogsAndNeverStartsApp(t *testing.T) {
	p, e, _ := initFixture(t)
	if err := p.advanceInit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	observeInit(t, p, e, "EXIT 137\nOOMKilled")
	status := e.pod.Status.InitContainerStatuses[0]
	if e.state != "TERMINATED" || e.pod.Status.Phase != corev1.PodFailed || e.pod.Status.Reason != "InitContainerFailed" || !e.started.IsZero() ||
		status.State.Terminated.ExitCode != 137 || status.State.Terminated.Reason != "OOMKilled" || status.Ready {
		t.Fatalf("Never init failure was not terminal: %+v", e.pod.Status)
	}
	logs, err := p.GetContainerLogs(t.Context(), "default", "app", "prepare", api.ContainerLogOpts{})
	if err != nil {
		t.Fatal(err)
	}
	logs.Close()
	p.monitor(t.Context(), e)
}

func TestProxyReadyDoesNotClaimInitContainersCompleted(t *testing.T) {
	p, e, _ := initFixture(t)
	e.state, e.proxyAvailability, e.proxyRoutesReady = "COLD", true, true
	p.projectProxyAvailability(e)
	if !e.pod.Status.ContainerStatuses[0].Ready || e.pod.Status.Conditions[0].Status != corev1.ConditionFalse || nextInitContainer(e.pod) != 0 {
		t.Fatal("cold proxy falsely claimed initialization had completed")
	}
}

func TestInitAllocationUsesMaximumOfSequentialStagesAndValidatesSecurity(t *testing.T) {
	pod := validPod()
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("768Mi")}
	pod.Spec.InitContainers = []corev1.Container{{Name: "prepare", Image: "busybox:1.37.0", Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("1025Mi")}}}}
	allocation, err := derivePodAllocation(pod)
	if err != nil || allocation.VCPUs != 2 || allocation.MemoryMiB != 1282 {
		t.Fatalf("init allocation should use maximum, not sum: %+v %v", allocation, err)
	}
	if err := validatePod(pod); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*corev1.Container){
		"unsupported init restart override": func(c *corev1.Container) { c.RestartPolicy = new(corev1.ContainerRestartPolicy("Never")) },
		"probe":                             func(c *corev1.Container) { c.ReadinessProbe = &corev1.Probe{} },
		"privileged":                        func(c *corev1.Container) { c.SecurityContext = &corev1.SecurityContext{Privileged: new(true)} },
		"unresolved env": func(c *corev1.Container) {
			c.Env = []corev1.EnvVar{{Name: "SECRET", ValueFrom: &corev1.EnvVarSource{}}}
		},
		"extra resource": func(c *corev1.Container) {
			c.Resources.Requests = corev1.ResourceList{corev1.ResourceEphemeralStorage: resource.MustParse("1Gi")}
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := pod.DeepCopy()
			mutate(&copy.Spec.InitContainers[0])
			if err := validatePod(copy); err == nil {
				t.Fatal("unsupported init behavior accepted")
			}
		})
	}
}

func TestInitRecoveryRequiresConfirmedSequentialJournal(t *testing.T) {
	_, e, _ := initFixture(t)
	if recoverAction("INITSTARTING", "Running", true, false) != "FAILED" || recoverAction("INITIALIZING", "Running", true, false) != "INITIALIZING" || recoverAction("INITIALIZING", "Stopped", true, false) != "FAILED" {
		t.Fatal("init recovery confused guest liveness with committed execution")
	}
	record := journalRecord{Version: 3, Pod: e.pod, View: lifecycleView{State: "INITIALIZING"}}
	if err := validateInitJournal(record); err != nil {
		t.Fatal(err)
	}
	record.Version = 2
	if validateInitJournal(record) == nil {
		t.Fatal("legacy journal silently adopted init behavior")
	}
	record.Version, record.View.State = 3, "SLEEPING"
	if validateInitJournal(record) == nil {
		t.Fatal("incomplete initialization accepted an application snapshot")
	}
	record.View.State = "INITIALIZING"
	record.Pod.Status.InitContainerStatuses[1].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}
	if validateInitJournal(record) == nil {
		t.Fatal("out-of-order init completion accepted")
	}
}

func TestInitMissingTaskFailsRatherThanClaimingAppReadiness(t *testing.T) {
	p, e, _ := initFixture(t)
	if err := p.advanceInit(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	e.applicationMissingSince = time.Now().Add(-time.Minute)
	if err := os.WriteFile(filepath.Join(p.runner.cfg.state, "observation"), []byte("application 42 RUNNING"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := p.monitorInit(t.Context(), e); err == nil || !strings.Contains(err.Error(), "without a published exit status") {
		t.Fatalf("missing init task accepted: %v", err)
	}
}

func TestInitStartFailureFollowsPodRestartPolicy(t *testing.T) {
	for _, policy := range []corev1.RestartPolicy{corev1.RestartPolicyNever, corev1.RestartPolicyAlways} {
		t.Run(string(policy), func(t *testing.T) {
			p, e, _ := initFixture(t)
			e.pod.Spec.RestartPolicy = policy
			script := "#!/bin/sh\ncase \"$*\" in\n*'images pull'*) exit 9;;\nls*) echo '[]';;\n*) exit 0;;\nesac\n"
			if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			if err := p.advanceInit(t.Context(), e); err != nil {
				t.Fatal(err)
			}
			status := e.pod.Status.InitContainerStatuses[0]
			if policy == corev1.RestartPolicyNever {
				if e.state != "TERMINATED" || status.State.Terminated == nil || status.State.Terminated.Reason != "StartError" {
					t.Fatal("Never start failure was not terminal")
				}
			} else if e.state != "INITIALIZING" || status.State.Waiting == nil || status.LastTerminationState.Terminated.Reason != "StartError" {
				t.Fatal("retryable start failure did not retain an initializing pod")
			}
			if !e.started.IsZero() || nextInitContainer(e.pod) != 0 {
				t.Fatal("start failure bypassed the first init container")
			}
		})
	}
}

func TestInitExecTargetsOnlyTheCurrentRunningInit(t *testing.T) {
	_, e, _ := initFixture(t)
	e.pod.Status.InitContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}
	if task, err := executingContainerTask(e, "prepare"); err != nil || task != "init-0" {
		t.Fatalf("running init exec target: %q %v", task, err)
	}
	for _, name := range []string{"finish", "app", "missing"} {
		if task, err := executingContainerTask(e, name); err == nil {
			t.Fatalf("nonexecuting container %s resolved to %s", name, task)
		}
	}
}

func TestCapturedInitLogsPreserveWhitespaceAndUsePrivateFiles(t *testing.T) {
	p, e, _ := initFixture(t)
	wanted := "\n  indented output\nlast line\n\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte("#!/bin/sh\nprintf '%s' "+quote(wanted)+"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.captureInitLogs(t.Context(), e, 0); err != nil {
		t.Fatal(err)
	}
	data, err := p.readInitLogs(t.Context(), e, 0, api.ContainerLogOpts{})
	if err != nil || string(data) != wanted {
		t.Fatalf("init logs changed bytes: %q %v", data, err)
	}
	info, err := os.Stat(p.initLogPath(e, 0))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("init log permissions: %v %v", info, err)
	}
}

func TestRecoveryMonitorsRunningInitWithoutLaunchingItAgain(t *testing.T) {
	_, original, _ := initFixture(t)
	started := metav1.NewTime(time.Now().Add(-time.Minute).Truncate(time.Second))
	original.pod.Status.InitContainerStatuses[0].State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: started}}
	original.pod.Status.InitContainerStatuses[0].Started = new(true)
	original.pod.Status.Phase = corev1.PodPending
	record := journalRecord{Pod: original.pod, View: lifecycleView{State: "INITIALIZING"}}
	p, _ := recoveryStatusFixture(t, record, "Running")
	script, err := os.ReadFile(p.runner.cfg.msb)
	if err != nil {
		t.Fatal(err)
	}
	script = []byte(strings.Replace(string(script), "*) exit 9", "exec) printf 'init-0 42 RUNNING\\n';;\n*) exit 9", 1))
	if err := os.WriteFile(p.runner.cfg.msb, script, 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, e := range p.pods {
			e.cancel()
		}
		p.jobs.Wait()
	})
	e := p.pods["default/app"]
	if e == nil {
		t.Fatal("recover omitted initializing pod")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, "commands"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "tasks list") {
			if strings.Contains(string(data), "application.request") || strings.Contains(string(data), "images pull") {
				t.Fatal("recovery replayed init launch instead of monitoring the existing task")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("recovery did not resume init monitoring")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.op.Lock()
	defer e.op.Unlock()
	status := e.pod.Status.InitContainerStatuses[0]
	if e.state != "INITIALIZING" || status.RestartCount != 0 || status.State.Running == nil || !status.State.Running.StartedAt.Equal(&started) || e.pod.Status.Phase != corev1.PodPending {
		t.Fatalf("recovery changed running init execution: %+v", e.pod.Status)
	}
}
