package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestProbeValidationAndNamedPorts(t *testing.T) {
	container := validPod().Spec.Containers[0]
	container.Ports = []corev1.ContainerPort{{Name: "health", ContainerPort: 9090, Protocol: corev1.ProtocolTCP}}
	container.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("health"), Path: "/ready"}}}
	container.StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(9091)}}}
	container.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}}
	if err := validateProbes(container); err != nil {
		t.Fatal(err)
	}
	if port, err := probePort(intstr.FromString("health"), container); err != nil || port != 9090 {
		t.Fatalf("named port: %d %v", port, err)
	}
	for name, change := range map[string]func(*corev1.Container){
		"unknown named port":        func(c *corev1.Container) { c.ReadinessProbe.HTTPGet.Port = intstr.FromString("missing") },
		"UDP named port":            func(c *corev1.Container) { c.Ports[0].Protocol = corev1.ProtocolUDP },
		"HTTP host override":        func(c *corev1.Container) { c.ReadinessProbe.HTTPGet.Host = "external.invalid" },
		"TCP host override":         func(c *corev1.Container) { c.StartupProbe.TCPSocket.Host = "127.0.0.1" },
		"invalid startup threshold": func(c *corev1.Container) { c.StartupProbe.SuccessThreshold = 2 },
		"multiple handlers":         func(c *corev1.Container) { c.ReadinessProbe.Exec = &corev1.ExecAction{Command: []string{"true"}} },
		"invalid gRPC port": func(c *corev1.Container) {
			c.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{GRPC: &corev1.GRPCAction{Port: 65536}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			copy := container.DeepCopy()
			change(copy)
			if validateProbes(*copy) == nil {
				t.Fatal("invalid probe accepted")
			}
		})
	}
}

func TestProbeThresholdsRequireConsecutiveResults(t *testing.T) {
	probe := &corev1.Probe{SuccessThreshold: 2, FailureThreshold: 3}
	progress := probeProgress{}
	for _, result := range []bool{true, false, true} {
		progress.observe(result, probe)
	}
	if progress.passed {
		t.Fatal("nonconsecutive successes became ready")
	}
	progress.observe(true, probe)
	if !progress.passed {
		t.Fatal("success threshold did not become ready")
	}
	for range 2 {
		progress.observe(false, probe)
	}
	if !progress.passed {
		t.Fatal("failure threshold applied too early")
	}
	progress.observe(false, probe)
	if progress.passed {
		t.Fatal("failure threshold did not remove readiness")
	}
}

func probeFixture(t *testing.T) (*provider, *podEntry) {
	t.Helper()
	directory := t.TempDir()
	script := filepath.Join(directory, "msb")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	e := &podEntry{pod: validPod(), state: "RUNNING", applicationProgress: applicationProgress{started: time.Now().Add(-time.Hour)}}
	p := &provider{runner: testCommandRunner(config{state: directory, msb: script})}
	return p, e
}

func TestStartupGatesLivenessAndSurvivesJournalAndResume(t *testing.T) {
	p, e := probeFixture(t)
	e.pod.Spec.Containers[0].StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}, InitialDelaySeconds: 7200}
	e.pod.Spec.Containers[0].LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"false"}}}}
	if failed, _, err := p.healthChecks(t.Context(), e); err != nil || failed != nil || e.startupComplete || !e.liveness.at.IsZero() {
		t.Fatal("liveness ran during startup")
	}
	e.pod.Spec.Containers[0].StartupProbe.InitialDelaySeconds = 0
	if _, _, err := p.healthChecks(t.Context(), e); err != nil || !e.startupComplete {
		t.Fatalf("startup did not complete: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var journal journalRecord
	if err := json.Unmarshal(data, &journal); err != nil || !journal.StartupComplete {
		t.Fatalf("startup success was not persisted: %v", err)
	}
	resetProbeProgress(e)
	if !e.startupComplete || e.readiness.passed {
		t.Fatal("resume reset startup success or reused readiness")
	}
	e.startup.at = time.Time{}
	if _, _, err := p.healthChecks(t.Context(), e); err != nil || !e.startup.at.IsZero() {
		t.Fatal("startup probe reran after successful startup")
	}
}

func TestColdAndSleepingPodsDoNotRunReadinessProbes(t *testing.T) {
	p, e := probeFixture(t)
	for _, state := range []string{"COLD", "SLEEPING", "CHECKPOINTED", "FAILED"} {
		e.state = state
		if p.probe(t.Context(), e) {
			t.Fatalf("%s app reported actual readiness", state)
		}
	}
}

func TestStartupAndLivenessFailOnlyAtTheirOwnThreshold(t *testing.T) {
	for _, startup := range []bool{true, false} {
		p, e := probeFixture(t)
		if err := os.WriteFile(p.runner.cfg.msb, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
			t.Fatal(err)
		}
		probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"false"}}}, FailureThreshold: 2}
		progress := &e.liveness
		want := "LivenessProbeFailed"
		if startup {
			e.pod.Spec.Containers[0].StartupProbe = probe
			progress = &e.startup
			want = "StartupProbeFailed"
		} else {
			e.startupComplete = true
			e.pod.Spec.Containers[0].LivenessProbe = probe
		}
		if failed, _, err := p.healthChecks(t.Context(), e); err != nil || failed != nil {
			t.Fatalf("early failure: %v", err)
		}
		progress.at = time.Time{}
		if failed, reason, err := p.healthChecks(t.Context(), e); err != nil || failed != probe || reason != want {
			t.Fatalf("threshold: %s %v", reason, err)
		}
	}
}

func TestFailedProbeJournalsReasonBeforeTerminationAndEscalates(t *testing.T) {
	p, e := probeFixture(t)
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app"}}
	e.pod.Spec.TerminationGracePeriodSeconds = new(int64(0))
	log := filepath.Join(p.runner.cfg.state, "signals")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(log) + "\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := p.stopUnhealthy(t.Context(), e, nil, "LivenessProbeFailed"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "SIGTERM") || !strings.Contains(string(data), "SIGKILL") {
		t.Fatalf("missing termination escalation: %s", data)
	}
	data, err = os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var journal journalRecord
	if err := json.Unmarshal(data, &journal); err != nil || journal.ProbeFailure != "LivenessProbeFailed" {
		t.Fatalf("lost failure reason: %v", err)
	}
	if err := p.hibernate(t.Context(), e); err == nil {
		t.Fatal("pending unhealthy termination was checkpointed")
	}
}
