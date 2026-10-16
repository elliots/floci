package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func TestApplicationPublicationNeverPublishesFailedWritesOrRenames(t *testing.T) {
	for _, publication := range []struct {
		name, script, final, pending string
	}{
		{"request", applicationRequestScript("exec /application"), "application.request", "application.pending"},
		{"exit", applicationProcessScript([]string{"true"}, ":"), "application.exit", "application.exit.pending"},
	} {
		for _, failure := range []struct{ name, setup string }{
			{"partial write", "printf() { command printf partial; return 1; }; "},
			{"rename", "mv() { return 1; }; "},
		} {
			t.Run(publication.name+"/"+failure.name, func(t *testing.T) {
				directory := t.TempDir()
				if err := os.WriteFile(filepath.Join(directory, publication.pending), []byte("stale pending data"), 0600); err != nil {
					t.Fatal(err)
				}
				command := exec.CommandContext(t.Context(), "sh", "-c", failure.setup+strings.ReplaceAll(publication.script, "/tmp/application", "./application"))
				command.Dir = directory
				if output, err := command.CombinedOutput(); err == nil {
					t.Fatalf("failed publication returned success: %s", output)
				}
				if _, err := os.Stat(filepath.Join(directory, publication.final)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("failed publication exposed a final marker: %v", err)
				}
				pending, err := os.ReadFile(filepath.Join(directory, publication.pending))
				if err != nil || string(pending) == "stale pending data" || failure.name == "partial write" && string(pending) != "partial" {
					t.Fatalf("failure fixture did not reach the intended write: %q, %v", pending, err)
				}
			})
		}
	}
}

func TestApplicationRequestPreservesLiteralScript(t *testing.T) {
	directory := t.TempDir()
	script := "printf '%s\\n' '$unexpanded and `literal`'\nexit 7\n"
	command := exec.CommandContext(t.Context(), "sh", "-c", strings.ReplaceAll(applicationRequestScript(script), "/tmp/application", "./application"))
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("publish request: %v: %s", err, output)
	}
	data, err := os.ReadFile(filepath.Join(directory, "application.request"))
	if err != nil || string(data) != script {
		t.Fatalf("request content changed: %q, %v", data, err)
	}
}

func TestApplicationExitPreservesCodeAndAccountingFailure(t *testing.T) {
	for _, accounting := range []string{":", "false"} {
		t.Run(accounting, func(t *testing.T) {
			directory := t.TempDir()
			script := applicationProcessScript([]string{"sh", "-c", "exit 7"}, accounting)
			command := exec.CommandContext(t.Context(), "sh", "-c", strings.ReplaceAll(script, "/tmp/application", "./application"))
			command.Dir = directory
			output, err := command.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || exit.ExitCode() != 7 {
				t.Fatalf("application exit changed: %v: %s", err, output)
			}
			data, err := os.ReadFile(filepath.Join(directory, "application.exit"))
			if err != nil || string(data) != "7\n" {
				t.Fatalf("exit marker changed: %q, %v", data, err)
			}
			_, err = os.Stat(filepath.Join(directory, "application.oom-error"))
			if (accounting == "false" && err != nil) || (accounting == ":" && !errors.Is(err, os.ErrNotExist)) {
				t.Fatalf("accounting failure marker: %v", err)
			}
		})
	}
}

func TestApplicationMissingGraceRequiresExactRunningTask(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entry := &podEntry{}
	for _, observation := range []struct {
		after   time.Duration
		output  string
		expired bool
	}{
		{0, "TASK PID STATUS", false},
		{29 * time.Second, "TASK PID STATUS\nother 42 RUNNING", false},
		{30 * time.Second, "application 42 STOPPED", true},
	} {
		running, expired := entry.observeApplicationTask(observation.output, start.Add(observation.after))
		if running || expired != observation.expired || !entry.applicationMissingSince.Equal(start) {
			t.Fatalf("missing observation after %s: running=%t expired=%t since=%s", observation.after, running, expired, entry.applicationMissingSince)
		}
	}
	if running, expired := entry.observeApplicationTask("TASK    PID    STATUS\napplication    42    RUNNING", start.Add(time.Minute)); !running || expired || !entry.applicationMissingSince.IsZero() {
		t.Fatal("running application did not clear missing-task grace")
	}
	if running, expired := entry.observeApplicationTask("", start.Add(2*time.Minute)); running || expired || !entry.applicationMissingSince.Equal(start.Add(2*time.Minute)) {
		t.Fatal("a later disappearance reused an expired grace period")
	}
}

func TestApplicationMonitorWithdrawsMissingTasksAndBoundsStatusHandoff(t *testing.T) {
	for _, test := range []struct {
		name, output, wantState string
		missingSince            time.Time
		wantReady               bool
	}{
		{"first disappearance", "TASK PID STATUS\nother 42 RUNNING", "RUNNING", time.Time{}, false},
		{"lost exit marker", "TASK PID STATUS", "FAILED", time.Now().Add(-time.Minute), false},
		{"running task returns", "TASK PID STATUS\napplication 42 RUNNING", "READY", time.Now().Add(-time.Minute), true},
		{"exit marker arrives", "EXIT 0", "TERMINATED", time.Now().Add(-time.Minute), false},
	} {
		for _, proxy := range []bool{false, true} {
			name := test.name + "/strict"
			if proxy {
				name = test.name + "/proxy"
			}
			t.Run(name, func(t *testing.T) {
				p, entry := probeFixture(t)
				entry.lifetime, entry.state, entry.actualReady = t.Context(), "READY", true
				entry.proxyAvailability, entry.proxyRoutesReady = proxy, true
				entry.applicationMissingSince, entry.volumesChecked = test.missingSince, time.Now()
				entry.pod.Spec.RestartPolicy = corev1.RestartPolicyNever
				entry.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", Ready: true}}
				script := "#!/bin/sh\ncase \"$1\" in\nexec) printf '%s\\n' " + quote(test.output) + ";;\nls) echo '[]';;\n*) exit 9;;\nesac\n"
				if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var notified *corev1.Pod
				p.notify = func(pod *corev1.Pod) {
					notified = pod
					cancel()
				}
				p.monitor(ctx, entry)
				if notified == nil || entry.state != test.wantState || notified.Status.ContainerStatuses[0].Ready != test.wantReady || entry.actualReady != test.wantReady {
					t.Fatalf("unexpected monitor result: state=%s ready=%t published=%+v", entry.state, entry.actualReady, notified)
				}
				if test.wantState == "FAILED" && !strings.Contains(notified.Status.Message, "without a published exit status") {
					t.Fatalf("missing exit failure is not diagnosable: %s", notified.Status.Message)
				}
				if test.wantState == "READY" && !entry.applicationMissingSince.IsZero() {
					t.Fatal("running task retained missing-task grace")
				}
			})
		}
	}
}

func TestApplicationFailureReconcilesOnlyConfirmedInactiveMemory(t *testing.T) {
	for _, status := range []string{"absent", "Running"} {
		t.Run(status, func(t *testing.T) {
			p, entry, _ := inactiveMemoryFixture(t, status)
			entry.state, entry.actualReady = "READY", true
			entry.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", Ready: true}}
			entry.op.Lock()
			p.fail(entry, errors.New("fixture runtime failure"))
			entry.op.Unlock()
			wantMemory := 768
			if status == "Running" {
				wantMemory = 2048
			}
			if entry.state != "FAILED" || entry.actualReady || entry.pod.Status.ContainerStatuses[0].Ready || p.activeMemoryMiB != wantMemory {
				t.Fatalf("failure state or memory changed incorrectly: state=%s ready=%t memory=%d", entry.state, entry.actualReady, p.activeMemoryMiB)
			}
		})
	}
}
