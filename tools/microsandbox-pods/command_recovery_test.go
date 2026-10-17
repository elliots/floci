package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type commandRecoveryFixture struct {
	runner                                          commandRunner
	journal                                         journalRecord
	vmID, status, parent, owner, snapshotID, digest string
	mutations                                       int
	readError                                       bool
}

func newCommandRecoveryFixture(t *testing.T) *commandRecoveryFixture {
	t.Helper()
	pod := validPod()
	vm := "fvp-" + stringsWithoutHyphens(string(pod.UID))[:20]
	f := &commandRecoveryFixture{journal: journalRecord{Pod: pod, View: lifecycleView{UID: string(pod.UID), VM: vm, Generation: 9}, SnapshotIDs: map[string]string{}},
		vmID: "immutable-vm", status: "Running", owner: "controller"}
	f.runner = commandRunner{cfg: config{state: t.TempDir(), peerOwner: f.owner}}
	f.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (runtime.Result, error) {
		if f.readError {
			return runtime.Result{}, errors.New("inspection unavailable")
		}
		var value any
		switch {
		case args[0] == "ls":
			value = []map[string]string{}
			if f.vmID != "" {
				value = []map[string]string{{"name": vm, "id": f.vmID}}
			}
		case args[0] == "inspect":
			value = map[string]any{"status": f.status, "config": map[string]any{"snapshot_parent": f.parent,
				"labels": map[string]string{ownerLabel: f.owner, "kubernetes.io/pod-uid": string(pod.UID)}}}
		case len(args) > 1 && args[0] == "snap" && args[1] == "ls":
			value = []map[string]string{}
			if f.snapshotID != "" {
				value = []map[string]string{{"group": vm, "name": "sleep", "snapshot_id": f.snapshotID, "digest": f.digest}}
			}
		case len(args) > 1 && args[0] == "snap" && args[1] == "verify":
			return runtime.Result{}, nil
		default:
			f.mutations++
			return runtime.Result{}, errors.New("recovery attempted a native mutation")
		}
		encoded, err := json.Marshal(value)
		return runtime.Result{Stdout: string(encoded)}, err
	}}
	if err := os.WriteFile(filepath.Join(f.runner.cfg.state, "owner"), []byte(`{"Version":1,"ClusterUID":"cluster","Node":"node","PeerOwner":"controller"}`), 0600); err != nil {
		t.Fatal(err)
	}
	f.writeJournal(t)
	return f
}

func (f *commandRecoveryFixture) writeJournal(t *testing.T) {
	t.Helper()
	data, err := json.Marshal(f.journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.runner.cfg.state, f.journal.View.UID+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func (f *commandRecoveryFixture) intent(t *testing.T, args ...string) nativeCommandRecord {
	t.Helper()
	record := f.runner.commandIntent(t.Context(), args)
	if record.Before == nil {
		t.Fatalf("fixture has no bound pre-operation proof: %+v", record)
	}
	if err := f.runner.writeCommandRecord(record, true); err != nil {
		t.Fatal(err)
	}
	return record
}

func (f *commandRecoveryFixture) terminal(t *testing.T, record *nativeCommandRecord, notStarted bool) {
	t.Helper()
	if err := f.runner.completeCommand(t.Context(), record, notStarted); err != nil {
		t.Fatal(err)
	}
	if record.After == nil || record.Phase == "intent" {
		t.Fatalf("fixture did not reach durable terminal proof: %+v", record)
	}
}

func TestTerminalCommandRecoveryValidatesLifecycleWithoutReplayingMutation(t *testing.T) {
	for operation, status := range map[string]string{"pause": "Paused", "stop": "Stopped", "resume": "Running", "start": "Running", "rm": ""} {
		t.Run(operation, func(t *testing.T) {
			f := newCommandRecoveryFixture(t)
			if operation == "resume" {
				f.status = "Paused"
			}
			if operation == "start" {
				f.status = "Stopped"
			}
			record := f.intent(t, operation, f.journal.View.VM)
			f.status = status
			if operation == "rm" {
				f.vmID = ""
			}
			f.terminal(t, &record, false)
			if err := f.runner.recoverCommand(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := f.runner.checkPending(); err != nil || f.mutations != 0 {
				t.Fatal("terminal recovery replayed or retained completed mutation", err)
			}
		})
	}
}

func TestTerminalCommandRecoveryRejectsChangedOwnershipOrOutcome(t *testing.T) {
	for _, change := range []string{"vm ID", "owner", "state", "controller", "generation", "journal UID", "read failure", "intent only", "missing proof", "legacy"} {
		t.Run(change, func(t *testing.T) {
			f := newCommandRecoveryFixture(t)
			record := f.intent(t, "stop", f.journal.View.VM)
			f.status = "Stopped"
			f.terminal(t, &record, false)
			switch change {
			case "vm ID":
				f.vmID = "replacement-vm"
			case "owner":
				f.owner = "different-controller"
			case "state":
				f.status = "Running"
			case "controller":
				if err := os.WriteFile(filepath.Join(f.runner.cfg.state, "owner"), []byte("different cluster"), 0600); err != nil {
					t.Fatal(err)
				}
			case "generation":
				f.journal.View.Generation++
				f.writeJournal(t)
			case "journal UID":
				f.journal.Pod.UID = "replacement"
				f.writeJournal(t)
			case "read failure":
				f.readError = true
			case "intent only":
				record.Phase = "intent"
				if err := f.runner.writeCommandRecord(record, false); err != nil {
					t.Fatal(err)
				}
			case "missing proof":
				record.After = nil
				if err := f.runner.writeCommandRecord(record, false); err != nil {
					t.Fatal(err)
				}
			case "legacy":
				if err := os.WriteFile(filepath.Join(f.runner.cfg.state, "native-command.pending"), []byte("stop owned\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.runner.recoverCommand(t.Context()); err == nil {
				t.Fatal("unsafe terminal state accepted")
			}
			if _, err := os.Stat(filepath.Join(f.runner.cfg.state, "native-command.pending")); err != nil || f.mutations != 0 {
				t.Fatal("uncertain outcome fence was removed or mutation replayed", err)
			}
		})
	}
}

func TestTerminalSnapshotRecoveryBindsDigestAndRestoredLineage(t *testing.T) {
	for _, operation := range []string{"create", "restore", "rm"} {
		t.Run(operation, func(t *testing.T) {
			f := newCommandRecoveryFixture(t)
			selector := f.journal.View.VM + ":sleep"
			args := []string{"snap", operation, selector}
			if operation == "create" {
				f.status = "Paused"
				args = []string{"snap", "create", "sleep", "--sandbox", f.journal.View.VM, "--full"}
			} else {
				f.snapshotID, f.digest = "immutable-snapshot", "immutable-digest"
				f.journal.SnapshotIDs[selector] = f.snapshotID
				if operation == "restore" {
					f.vmID, f.parent = "", f.snapshotID
					f.journal.RestoredParent = f.snapshotID
					args = append(args, "--name", f.journal.View.VM)
				}
				f.writeJournal(t)
			}
			record := f.intent(t, args...)
			switch operation {
			case "create":
				f.snapshotID, f.digest = "immutable-snapshot", "immutable-digest"
			case "restore":
				f.vmID, f.status = "restored-vm", "Running"
			case "rm":
				f.snapshotID, f.digest = "", ""
			}
			f.terminal(t, &record, false)
			if operation != "rm" {
				f.digest = "changed-digest"
				if err := f.runner.recoverCommand(t.Context()); err == nil {
					t.Fatal("replacement snapshot accepted")
				}
				f.digest = "immutable-digest"
			}
			if operation == "restore" {
				f.parent = "different-parent"
				if err := f.runner.recoverCommand(t.Context()); err == nil {
					t.Fatal("different restore lineage accepted")
				}
				f.parent = f.snapshotID
			}
			if err := f.runner.recoverCommand(t.Context()); err != nil || f.mutations != 0 {
				t.Fatal("immutable terminal snapshot did not recover", err)
			}
		})
	}
}

func TestTerminalLaunchAndNonDispatchRecordsKeepArgumentsPrivate(t *testing.T) {
	for _, refused := range []bool{false, true} {
		t.Run(fmt.Sprintf("refused=%t", refused), func(t *testing.T) {
			f := newCommandRecoveryFixture(t)
			f.vmID = ""
			args := []string{"run", "-d", "--name", f.journal.View.VM, "--env", "SECRET=must-not-store", "--entrypoint", "/bin/sh", "private/image", "--", "-c", "secret-command"}
			record := f.intent(t, args...)
			if !refused {
				f.vmID = "new-immutable-id"
			}
			f.terminal(t, &record, refused)
			data, err := os.ReadFile(filepath.Join(f.runner.cfg.state, "native-command.pending"))
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"must-not-store", "private/image", "secret-command"} {
				if strings.Contains(string(data), secret) {
					t.Fatal("private arguments persisted")
				}
			}
			if err := f.runner.recoverCommand(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTerminalCommandRecoveryRejectsSymlinkMarker(t *testing.T) {
	f := newCommandRecoveryFixture(t)
	path := filepath.Join(t.TempDir(), "unowned")
	if err := os.WriteFile(path, []byte("legacy"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(path, filepath.Join(f.runner.cfg.state, "native-command.pending")); err != nil {
		t.Fatal(err)
	}
	if err := f.runner.recoverCommand(t.Context()); err == nil {
		t.Fatal("symlink marker accepted")
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "legacy" {
		t.Fatal("unowned file changed", err)
	}
}

func TestTerminalCommandRecoveryRejectsReplacementDuringInspection(t *testing.T) {
	f := newCommandRecoveryFixture(t)
	record := f.intent(t, "stop", f.journal.View.VM)
	f.status = "Stopped"
	f.terminal(t, &record, false)
	original := f.runner.runtime
	f.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, input []byte) (runtime.Result, error) {
		result, err := original.Execute(ctx, args, input)
		if args[0] == "inspect" {
			f.vmID = "concurrent-replacement"
		}
		return result, err
	}}
	if err := f.runner.recoverCommand(t.Context()); err == nil {
		t.Fatal("VM replacement between listing and inspection was accepted")
	}
	if err := f.runner.checkPending(); err == nil || f.mutations != 0 {
		t.Fatal("concurrent replacement lost its fence or triggered a mutation")
	}
}
