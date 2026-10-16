package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func frozenFixture() *applicationFreeze {
	return &applicationFreeze{Version: 1, Phase: "FROZEN", Operation: "ABCDEFGHIJKLMNOPQRSTUVWX23", Tasks: []frozenTask{{Task: "application", PID: 101, StartTicks: 123}}}
}

func TestFreezeOnlyCapturesManagedApplicationsAndRestartableSidecars(t *testing.T) {
	e := &podEntry{pod: validPod()}
	e.pod.Spec.Containers = append(e.pod.Spec.Containers, corev1.Container{Name: "completed"})
	e.pod.Spec.InitContainers = []corev1.Container{{Name: "init"}, {Name: "sidecar", RestartPolicy: new(corev1.ContainerRestartPolicyAlways)}}
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "completed", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}}}
	names := freezeTaskNames(e)
	if strings.Join(names, ",") != "app-0,init-1" {
		t.Fatalf("wrong application cgroups: %v", names)
	}
	for _, output := range []string{"", "app-0 101 0\ninit-1 102 456", "other 101 123\ninit-1 102 456", "app-0 -1 123\ninit-1 102 456", "app-0 101 123"} {
		if _, err := parseFrozenTasks(output, names); err == nil {
			t.Fatal("accepted missing or changed process inventory")
		}
	}
	tasks, err := parseFrozenTasks("app-0 101 123\ninit-1 102 456", names)
	if err != nil || tasks[1].StartTicks != 456 {
		t.Fatal("valid process inventory rejected")
	}
}

func TestLegacyIdentitySnapshotsAndCorruptFreezeOwnershipAreRejected(t *testing.T) {
	_, e, _ := projectedFixture(t)
	if err := requireFrozenSnapshot(e); err == nil {
		t.Fatal("old token snapshot could run before projection refresh")
	}
	e.freeze = frozenFixture()
	if err := requireFrozenSnapshot(e); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*applicationFreeze){
		func(f *applicationFreeze) { f.Tasks[0].Task = "replacement" },
		func(f *applicationFreeze) { f.Operation = "../../another-marker" },
		func(f *applicationFreeze) { f.Tasks[0].PID = 0 },
		func(f *applicationFreeze) { f.Version = 2 },
		func(f *applicationFreeze) { f.Phase = "FREEZING" },
	} {
		e.freeze = frozenFixture()
		mutate(e.freeze)
		if requireFrozenSnapshot(e) == nil {
			t.Fatal("accepted incomplete or foreign freeze journal")
		}
	}
	e.freeze = nil
	e.pod.Spec.Containers[0].VolumeMounts = nil
	if requireFrozenSnapshot(e) != nil {
		t.Fatal("nonidentity checkpoints must remain compatible")
	}
}

func TestFreezeIntentIsDurableBeforeAcceptedGuestPause(t *testing.T) {
	p, e, _ := projectedFixture(t)
	pauses := 0
	p.runner.runtime = stubNativeRuntime{execute: func(ctx context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		script := args[len(args)-1]
		if strings.Contains(script, "tasks pause") {
			pauses++
			data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
			if err != nil {
				t.Fatal(err)
			}
			var record journalRecord
			if json.Unmarshal(data, &record) != nil || record.Freeze == nil || record.Freeze.Phase != "FREEZING" || record.Freeze.Tasks[0].PID != 101 {
				t.Fatal("pause ran without durable identity intent")
			}
			if ctx.Done() != nil {
				t.Fatal("accepted pause can be abandoned by request cancellation")
			}
			return msbruntime.Result{}, nil
		}
		if strings.HasPrefix(script, "cat ") {
			return msbruntime.Result{Stdout: "0"}, nil
		}
		return msbruntime.Result{Stdout: "application 101 123"}, nil
	}}
	if err := p.freezeApplications(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if pauses != 1 || e.freeze.Phase != "FROZEN" {
		t.Fatal("freeze was not completed")
	}
}

func TestLostGuestFreezeResponseDoesNotPermitConcurrentThaw(t *testing.T) {
	p, e, _ := projectedFixture(t)
	e.freeze = frozenFixture()
	e.freeze.Phase = "FREEZING"
	commands := 0
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
		commands++
		if !strings.HasPrefix(args[len(args)-1], "cat ") {
			t.Fatal("uncertain pause was followed by a mutation")
		}
		return msbruntime.Result{}, errors.New("transport lost")
	}}
	if p.thawApplications(t.Context(), e) == nil || commands != 1 || e.freeze.Phase != "FREEZING" {
		t.Fatal("missing completion proof admitted thaw")
	}
}

func TestTokenRefreshMustSucceedBeforeThawAndNeverUsesCommandArguments(t *testing.T) {
	for _, unavailable := range []bool{false, true} {
		p, e, tokens := projectedFixture(t)
		e.freeze = frozenFixture()
		if unavailable {
			tokens.failure = errors.New("private-api-response")
		}
		written, thawed := false, false
		p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, input []byte) (msbruntime.Result, error) {
			script := args[len(args)-1]
			if len(input) > 0 {
				written = true
				if strings.Contains(strings.Join(args, " "), "eyJ") {
					t.Fatal("token reached guest argv")
				}
			}
			if strings.Contains(script, "tasks resume") {
				if !written || len(tokens.requests) != 1 {
					t.Fatal("application resumed before a fresh projected identity")
				}
				thawed = true
			}
			if strings.HasPrefix(script, "cat ") {
				return msbruntime.Result{Stdout: "0"}, nil
			}
			return msbruntime.Result{}, nil
		}}
		err := p.refreshAndThaw(t.Context(), e)
		if unavailable {
			if err == nil || written || thawed || e.freeze == nil {
				t.Fatal("API failure released frozen applications")
			}
		} else if err != nil || !written || !thawed || e.freeze != nil {
			t.Fatalf("fresh identity not followed by thaw: %v", err)
		}
	}
}

// Execute the actual shell plan against a deterministic task/proc inventory.
// Native qualification separately proves cgroup freezer and RAM persistence.
func freezeShellFixture(t *testing.T, states string) (func(string) (string, error), string) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "tasks"), []byte(states), 0600); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{101, 102} {
		directory := filepath.Join(root, "proc", fmt.Sprint(pid))
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		stat := fmt.Sprintf("%d (command ) with spaces) S %s123 0\n", pid, strings.Repeat("0 ", 18))
		if err := os.WriteFile(filepath.Join(directory, "stat"), []byte(stat), 0600); err != nil {
			t.Fatal(err)
		}
	}
	prefix := `fake_ctr() { case "$1 $2" in
"tasks list") cat "$fixture/tasks";;
"tasks pause"|"tasks resume")
 printf '%s %s\n' "$2" "$3" >>"$fixture/actions"
 status=PAUSED; test "$2" = pause || status=RUNNING
 awk -v task="$3" -v state="$status" '$1==task {$3=state} {print}' "$fixture/tasks" >"$fixture/pending"
 mv "$fixture/pending" "$fixture/tasks";;
*) exit 125;; esac; }; `
	return func(script string) (string, error) {
		script = strings.ReplaceAll(script, ctr, "fake_ctr")
		script = strings.ReplaceAll(script, "/proc/", root+"/proc/")
		script = strings.ReplaceAll(script, "/run/floci-freeze", root+"/markers")
		cmd := exec.CommandContext(t.Context(), "sh", "-ec", prefix+script)
		cmd.Env = append(os.Environ(), "fixture="+root)
		output, err := cmd.CombinedOutput()
		return string(output), err
	}, root
}

func TestFreezeShellPreservesProcessIdentityAndOnlyThawsOwnedPausedTasks(t *testing.T) {
	run, root := freezeShellFixture(t, "TASK PID STATUS\napplication 101 RUNNING\nother 102 PAUSED\n")
	freeze := frozenFixture()
	observation, err := run(freezeObservationScript([]string{"application"}))
	if err != nil {
		t.Fatalf("observe: %s %v", observation, err)
	}
	if strings.TrimSpace(observation) != "application 101 123" {
		t.Fatalf("bad /proc starttime parsing: %s", observation)
	}
	if output, err := run(freezeMutationScript(freeze, "pause")); err != nil {
		t.Fatalf("pause: %s %v", output, err)
	}
	if output, err := run(freezeMutationScript(freeze, "resume")); err != nil {
		t.Fatalf("resume: %s %v", output, err)
	}
	actions, _ := os.ReadFile(filepath.Join(root, "actions"))
	if string(actions) != "pause application\nresume application\n" {
		t.Fatalf("foreign process mutated: %s", actions)
	}
	tasks, _ := os.ReadFile(filepath.Join(root, "tasks"))
	if !strings.Contains(string(tasks), "other 102 PAUSED") {
		t.Fatal("foreign task thawed")
	}
	for _, action := range []string{"pause", "resume"} {
		proof, err := os.ReadFile(filepath.Join(root, "markers", freeze.Operation+"."+action))
		if err != nil || string(proof) != "0\n" {
			t.Fatal("missing guest completion proof")
		}
	}
}

func TestFreezeShellRefusesReplacementProcessBeforeResume(t *testing.T) {
	for _, change := range []func(*applicationFreeze){func(f *applicationFreeze) { f.Tasks[0].PID = 102 }, func(f *applicationFreeze) { f.Tasks[0].StartTicks = 999 }} {
		run, root := freezeShellFixture(t, "TASK PID STATUS\napplication 101 PAUSED\n")
		freeze := frozenFixture()
		change(freeze)
		if _, err := run(freezeMutationScript(freeze, "resume")); err == nil {
			t.Fatal("replacement PID was resumed")
		}
		if _, err := os.Stat(filepath.Join(root, "actions")); !os.IsNotExist(err) {
			t.Fatal("resume executed before identity validation")
		}
		proof, err := os.ReadFile(filepath.Join(root, "markers", freeze.Operation+".resume"))
		if err != nil || string(proof) == "0\n" {
			t.Fatal("failed mutation lacks terminal completion proof")
		}
	}
}

func TestRecoveryRefreshesFrozenIdentityBeforeRestartingApplicationMonitoring(t *testing.T) {
	_, entry, tokens := projectedFixture(t)
	entry.pod.Spec.NodeName = "native-node"
	if _, err := tokens.client.CoreV1().Pods(entry.pod.Namespace).Update(t.Context(), entry.pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	p, _ := recoveryStatusFixture(t, journalRecord{Pod: entry.pod, Freeze: frozenFixture(), View: lifecycleView{State: "RESUMING"}}, "Running")
	p.client = tokens.client
	vm := "fvp-" + stringsWithoutHyphens(string(entry.pod.UID))[:20]
	projected, thawed := false, false
	p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, input []byte) (msbruntime.Result, error) {
		switch args[0] {
		case "ls":
			return msbruntime.Result{Stdout: fmt.Sprintf(`[{"name":%q}]`, vm)}, nil
		case "inspect":
			return msbruntime.Result{Stdout: fmt.Sprintf(`{"status":"Running","config":{"labels":{%q:"recovery-fixture","kubernetes.io/pod-uid":%q}}}`, ownerLabel, entry.pod.UID)}, nil
		case "exec":
			script := args[len(args)-1]
			if len(input) > 0 {
				projected = true
			}
			if strings.Contains(script, "tasks resume") {
				if !projected {
					t.Fatal("recovery thawed before refreshing identity")
				}
				thawed = true
			}
			if strings.HasPrefix(script, "cat ") {
				return msbruntime.Result{Stdout: "0"}, nil
			}
			return msbruntime.Result{}, nil
		default:
			t.Fatalf("unexpected native operation during recovery: %s", args[0])
			return msbruntime.Result{}, nil
		}
	}}
	if err := p.recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, e := range p.pods {
		e.cancel()
	}
	p.jobs.Wait()
	recovered := p.pods[entry.pod.Namespace+"/"+entry.pod.Name]
	if !projected || !thawed || recovered.freeze != nil || len(tokens.requests) != 1 {
		t.Fatal("recovery did not refresh and release the frozen identity")
	}
}

func TestInterruptedThawRequiresItsOwnSuccessfulCompletionProof(t *testing.T) {
	for _, proof := range []string{"0", "1", "unfinished"} {
		p, e, _ := projectedFixture(t)
		e.freeze = frozenFixture()
		e.freeze.Phase = "THAWING"
		p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			if !strings.HasPrefix(args[len(args)-1], "cat ") || !strings.Contains(args[len(args)-1], ".resume") {
				t.Fatal("recovery replayed an interrupted thaw")
			}
			return msbruntime.Result{Stdout: proof}, nil
		}}
		err := p.thawApplications(t.Context(), e)
		if proof == "0" {
			if err != nil || e.freeze != nil {
				t.Fatal("completed thaw not reconciled")
			}
		} else if err == nil || e.freeze == nil {
			t.Fatal("unknown or partial thaw treated as complete")
		}
	}
}
