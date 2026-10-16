package guesttask

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSupervisorRunsContainersConcurrentlyAndPreservesPrivateArguments(t *testing.T) {
	root := t.TempDir()
	startSupervisor(t, root)
	gate := filepath.Join(root, "release")
	first, err := planAt(root, Request{Action: "start", ID: "first", Command: []string{"sh", "-c", `while ! test -f "$1"; do sleep 0.02; done; exit 19`, "wait", gate}}, "")
	if err != nil {
		t.Fatal(err)
	}
	run(t, first)
	secret := "private '$() `touch bad`\nvalue"
	second, err := planAt(root, Request{Action: "start", ID: "second", Command: []string{"sh", "-c", `printf '%s' "$VALUE"; exit 7`}, Environment: map[string]string{"VALUE": secret}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(second.Args, " "), "private") {
		t.Fatal("credential reached host command arguments")
	}
	run(t, second)
	status := awaitExit(t, root, "second")
	if status.ExitCode == nil || *status.ExitCode != 7 || status.StartedAt == "" || status.FinishedAt == "" {
		t.Fatalf("unexpected child status: %+v", status)
	}
	log, err := os.ReadFile(filepath.Join(root, "second", "application.log"))
	if err != nil || string(log) != secret {
		t.Fatalf("literal secret was changed: log=%q error=%v", log, err)
	}
	if _, err := os.Stat(filepath.Join(root, "first", "application.exit")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first container did not remain running: %v", err)
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if status := awaitExit(t, root, "first"); status.ExitCode == nil || *status.ExitCode != 19 {
		t.Fatalf("first exit lost: %+v", status)
	}
}

func TestDuplicateSubmissionCannotReplaceAnActiveCommand(t *testing.T) {
	root := t.TempDir()
	request, err := planAt(root, Request{Action: "start", ID: "task", Command: []string{"printf", "original"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	run(t, request)
	replacement, err := planAt(root, Request{Action: "start", ID: "task", Command: []string{"printf", "replacement"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), replacement.Args[0], replacement.Args[1:]...)
	cmd.Stdin = strings.NewReader(string(replacement.Stdin))
	if err := cmd.Run(); err == nil {
		t.Fatal("duplicate submission succeeded")
	}
	queued, err := os.ReadFile(filepath.Join(root, "task", "request"))
	if err != nil || string(queued) != string(request.Stdin) {
		t.Fatalf("queued command changed: %v", err)
	}
	startSupervisor(t, root)
	awaitExit(t, root, "task")
}

func TestRestartRetainsPreviousLogsAndCompletionFailure(t *testing.T) {
	root := t.TempDir()
	startSupervisor(t, root)
	first, err := planAt(root, Request{Action: "start", ID: "task", Command: []string{"printf", "first"}}, "exit 1")
	if err != nil {
		t.Fatal(err)
	}
	run(t, first)
	awaitExit(t, root, "task")
	if _, err := os.Stat(filepath.Join(root, "task", "application.oom-error")); err != nil {
		t.Fatal("completion failure was not published", err)
	}
	second, err := planAt(root, Request{Action: "start", ID: "task", Command: []string{"printf", "second"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	run(t, second)
	awaitExit(t, root, "task")
	previous, err := os.ReadFile(filepath.Join(root, "task", "application.previous.log"))
	if err != nil || string(previous) != "first" {
		t.Fatalf("previous logs lost: %q %v", previous, err)
	}
	if _, err := os.Stat(filepath.Join(root, "task", "application.oom-error")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("previous accounting failure leaked into next attempt: %v", err)
	}
}

func TestInvalidRequestsDoNotLeakValues(t *testing.T) {
	for _, request := range []Request{
		{Action: "start", ID: "../escape", Command: []string{"private"}},
		{Action: "start", ID: "task", Environment: map[string]string{"INVALID;private": "private"}, Command: []string{"echo"}},
		{Action: "start", ID: "task", Command: []string{"private\x00"}},
		{Action: "status", ID: "task", Command: []string{"private"}},
		{Action: "stop", ID: "task", Signal: "private;kill"},
		{Action: "logs", ID: "task", Tail: -1},
	} {
		_, err := Plan(request)
		if !errors.Is(err, ErrInvalidArgument) || strings.Contains(err.Error(), "private") {
			t.Fatalf("invalid request has unsafe error: %v", err)
		}
	}
}

func startSupervisor(t *testing.T, root string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "/bin/sh", "-ec", supervisorAt(root))
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
}

func run(t *testing.T, plan Command) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), plan.Args[0], plan.Args[1:]...)
	cmd.Stdin = strings.NewReader(string(plan.Stdin))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("guest task failed: %s: %v", output, err)
	}
}

func awaitExit(t *testing.T, root, id string) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(filepath.Join(root, id, "status.json"))
		var status Status
		if err == nil && json.Unmarshal(data, &status) == nil && status.State == "exited" {
			if _, err := os.Stat(filepath.Join(root, id, "active")); errors.Is(err, os.ErrNotExist) {
				return status
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("task did not publish a complete exit")
	return Status{}
}
