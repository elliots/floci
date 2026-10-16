package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecoveryNeverReplaysAnAbsentOrUncommittedSource(t *testing.T) {
	cases := []struct {
		journal, runtime string
		exists, verified bool
		want             string
	}{
		{"COLD", "", false, false, "COLD"},
		{"COLD", "Running", true, false, "FAILED"},
		{"SLEEPING", "", false, true, "FAILED"},
		{"SLEEPING", "Stopped", true, true, "SLEEPING"},
		{"SLEEPING", "Stopped", true, false, "FAILED"},
		{"SLEEPING", "Running", true, true, "FAILED"},
		{"CHECKPOINTED", "Paused", true, true, "FINISH_SLEEP"},
		{"CHECKPOINTED", "Stopped", true, true, "FINISH_SLEEP"},
		{"RESUMING", "", false, true, "FAILED"},
		{"RESUMING", "Stopped", true, true, "FAILED"},
		{"HIBERNATING", "Paused", true, false, "ROLLBACK"},
		{"FAILED", "Running", true, true, "FAILED"},
		{"TERMINATED", "Running", true, true, "TERMINATED"},
		{"READY", "", false, true, "FAILED"},
		{"READY", "Running", true, false, "RUNNING"},
	}
	for _, c := range cases {
		t.Run(c.journal+"/"+c.runtime, func(t *testing.T) {
			if got := recoverAction(c.journal, c.runtime, c.exists, c.verified); got != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
		})
	}
}

func TestFiveHundredColdPodsReserveUniqueFutureHostPorts(t *testing.T) {
	used := map[int]bool{}
	for range 500 {
		if _, err := uniquePort(used, availableUDPPort); err != nil {
			t.Fatal(err)
		}
		if _, err := uniquePort(used, availableTCPPort); err != nil {
			t.Fatal(err)
		}
	}
	if len(used) != 1000 {
		t.Fatalf("only %d unique future bindings", len(used))
	}
}

func TestPendingNativeCommandBlocksEveryNewMutation(t *testing.T) {
	directory := t.TempDir()
	r := commandRunner{cfg: config{state: directory, msb: "must-not-execute"}}
	if err := os.WriteFile(filepath.Join(directory, "native-command.pending"), []byte("restore"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := r.vm(t.Context(), "rm", "owned"); err == nil {
		t.Fatal("unsettled command was ignored")
	}
	if err := r.checkPending(); err == nil {
		t.Fatal("restart accepted unsettled native command")
	}
}

func TestReadOnlyNativeCommandsDoNotCreateMutationMarkers(t *testing.T) {
	for _, args := range [][]string{{"ls"}, {"inspect"}, {"exec"}, {"snap", "ls"}, {"snap", "verify"}} {
		if nativeMutation(args) {
			t.Fatalf("read-only operation classified as mutation: %v", args)
		}
	}
}
