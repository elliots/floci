package main

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRecoveryNeverReplaysAnAbsentOrUncommittedSource(t *testing.T) {
	cases := []struct {
		journal, runtime string
		exists, verified bool
		want             string
	}{
		{"COLD", "", false, false, "COLD"},
		{"WAITINGVOLUMES", "", false, false, "WAITINGVOLUMES"},
		{"WAITINGVOLUMES", "Running", true, false, "FAILED"},
		{"STARTING", "", false, false, "FAILED"},
		{"STARTING", "Running", true, false, "FAILED"},
		{"STARTING", "Paused", true, false, "FAILED"},
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
		{"RESTARTING", "Running", true, false, "FAILED"},
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

func recoveryStatusFixture(t *testing.T, record journalRecord, runtimeStatus string) (*provider, string) {
	t.Helper()
	directory := t.TempDir()
	current := record.Pod.DeepCopy()
	current.Spec.NodeName = "native-node"
	record.Pod.Spec.NodeName = current.Spec.NodeName
	record.DeclaredSpec = current.Spec.DeepCopy()
	vm := "fvp-" + stringsWithoutHyphens(string(current.UID))[:20]
	record.Version = podJournalVersion(record.Pod)
	record.Allocation = podAllocation{Version: 1, VCPUs: 2, MemoryMiB: 1280}
	record.View.UID, record.View.Namespace, record.View.Name = string(current.UID), current.Namespace, current.Name
	record.View.VM, record.View.PodIP, record.View.UDPPort = vm, "10.42.200.2", 30000
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(directory, string(current.UID)+".json")
	if err := os.WriteFile(journal, data, 0600); err != nil {
		t.Fatal(err)
	}
	list := fmt.Sprintf(`[{"name":%q}]`, vm)
	if runtimeStatus == "" {
		list = "[]"
	}
	inspection := fmt.Sprintf(`{"status":%q,"config":{"labels":{%q:"recovery-fixture","kubernetes.io/pod-uid":%q}}}`, runtimeStatus, ownerLabel, current.UID)
	log := filepath.Join(directory, "commands")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(log) + "\ncase \"$1\" in\n" +
		"ls) printf '%s\\n' " + quote(list) + ";;\n" +
		"inspect) printf '%s\\n' " + quote(inspection) + ";;\n" +
		"*) exit 9;;\nesac\n"
	binary := filepath.Join(directory, "msb")
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	p := &provider{
		ctx: t.Context(), client: fake.NewClientset(current), pods: make(map[string]*podEntry),
		network: &network{prefix: netip.MustParsePrefix("10.42.200.0/24")},
		runner:  testCommandRunner(config{state: directory, msb: binary, nodeName: current.Spec.NodeName, peerOwner: "recovery-fixture", maxActiveMemoryMiB: 2048}),
	}
	t.Cleanup(func() {
		for _, entry := range p.pods {
			entry.cancel()
		}
	})
	return p, journal
}

func TestRecoveryFailsUncommittedLaunchesAndAccountsObservedGuestMemory(t *testing.T) {
	for _, state := range []string{"STARTING", "RESTARTING"} {
		for _, runtimeStatus := range []string{"Running", "Paused", "Stopped", ""} {
			t.Run(state+"/"+runtimeStatus, func(t *testing.T) {
				pod := validPod()
				pod.Status = corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{
					{Name: "app", Ready: true, Started: new(true), State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
				}}
				p, journal := recoveryStatusFixture(t, journalRecord{Pod: pod, View: lifecycleView{State: state}}, runtimeStatus)
				if err := p.recover(t.Context()); err != nil {
					t.Fatal(err)
				}
				entry := p.pods["default/app"]
				status := entry.pod.Status
				if entry.state != "FAILED" || status.Phase != corev1.PodFailed || status.Reason != "NativeRuntimeUncertain" || status.Message == "" {
					t.Fatalf("uncommitted launch was not terminal and diagnosable: %s, %+v", entry.state, status)
				}
				container := status.ContainerStatuses[0]
				if container.Ready || container.Started == nil || *container.Started || container.State.Running != nil ||
					container.State.Waiting == nil || container.State.Waiting.Reason != "NativeRuntimeUncertain" {
					t.Fatalf("uncommitted application was reported running or ready: %+v", container)
				}
				active := runtimeStatus == "Running" || runtimeStatus == "Paused"
				wantMemory := 0
				if active {
					wantMemory = 1280
				}
				if entry.allocated != active || p.activeMemoryMiB != wantMemory {
					t.Fatalf("observed guest RAM was incorrectly released or charged: allocated=%t memory=%d", entry.allocated, p.activeMemoryMiB)
				}
				select {
				case <-entry.done:
				default:
					t.Fatal("failed launch started an application monitor")
				}
				data, err := os.ReadFile(journal)
				if err != nil {
					t.Fatal(err)
				}
				var saved journalRecord
				if err := json.Unmarshal(data, &saved); err != nil || saved.View.State != "FAILED" || saved.Pod.Status.Phase != status.Phase ||
					saved.Pod.Status.Reason != status.Reason || saved.Pod.Status.Message != status.Message ||
					!equality.Semantic.DeepEqual(saved.Pod.Status.ContainerStatuses, status.ContainerStatuses) {
					t.Fatalf("failed recovery status was not durably recorded: %v", err)
				}
			})
		}
	}
}

func TestRecoveryPreservesRecordedFailureDiagnostics(t *testing.T) {
	for _, reason := range []string{"NativeRuntimeFailed", "NativeRuntimeUncertain", "ControllerRecovering", ""} {
		t.Run(reason, func(t *testing.T) {
			pod := validPod()
			pod.Status = corev1.PodStatus{Phase: corev1.PodPending, Reason: reason, Message: "original image pull failed",
				ContainerStatuses: []corev1.ContainerStatus{{Name: "app"}}}
			p, _ := recoveryStatusFixture(t, journalRecord{Pod: pod, View: lifecycleView{State: "FAILED"}}, "Stopped")
			if err := p.recover(t.Context()); err != nil {
				t.Fatal(err)
			}
			wantReason := reason
			if wantReason == "" || wantReason == "ControllerRecovering" {
				wantReason = "NativeRuntimeUncertain"
			}
			status := p.pods["default/app"].pod.Status
			if status.Phase != corev1.PodFailed || status.Reason != wantReason || status.Message != "original image pull failed" ||
				status.ContainerStatuses[0].State.Waiting.Reason != wantReason {
				t.Fatalf("recorded failure diagnostics were lost: %+v", status)
			}
		})
	}
}

func TestRecoveryPreservesTerminalOOMStatusWithoutReservingStoppedGuest(t *testing.T) {
	pod := validPod()
	started, finished := metav1.NewTime(time.Unix(1000, 0).UTC()), metav1.NewTime(time.Unix(2000, 0).UTC())
	pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, ContainerStatuses: []corev1.ContainerStatus{{Name: "app", Started: new(false),
		State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 137, Reason: "OOMKilled", StartedAt: started, FinishedAt: finished}}}},
	}
	want := pod.Status.DeepCopy()
	p, journal := recoveryStatusFixture(t, journalRecord{Pod: pod, View: lifecycleView{State: "TERMINATED"}, TerminationReason: "OOMKilled"}, "Stopped")
	if err := p.recover(t.Context()); err != nil {
		t.Fatal(err)
	}
	entry := p.pods["default/app"]
	if !equality.Semantic.DeepEqual(entry.pod.Status, *want) || entry.terminationReason != "OOMKilled" || entry.allocated || p.activeMemoryMiB != 0 {
		t.Fatalf("terminal OOM status or released memory changed on recovery: %+v", entry.pod.Status)
	}
	data, err := os.ReadFile(journal)
	if err != nil {
		t.Fatal(err)
	}
	var saved journalRecord
	if err := json.Unmarshal(data, &saved); err != nil || !equality.Semantic.DeepEqual(saved.Pod.Status, *want) || saved.TerminationReason != "OOMKilled" {
		t.Fatalf("terminal OOM status was not preserved in the journal: %v", err)
	}
}

func TestJournalAllocationPreservesLegacySizeAndRejectsMissingNewAllocation(t *testing.T) {
	legacy := journalRecord{Version: 1}
	allocation, err := legacy.guestAllocation()
	if err != nil || allocation != (podAllocation{Version: 2, VCPUs: 1, MemoryMiB: 768, RootDiskMiB: 4096}) {
		t.Fatalf("legacy guest changed size: %+v, %v", allocation, err)
	}
	wanted := podAllocation{Version: 2, VCPUs: 2, MemoryMiB: 1536, RootDiskMiB: 8192}
	allocation, err = (journalRecord{Version: 2, Allocation: wanted}).guestAllocation()
	if err != nil || allocation != wanted {
		t.Fatalf("stored allocation changed: %+v, %v", allocation, err)
	}
	for _, record := range []journalRecord{{Version: 2}, {Version: 3, Allocation: wanted}, {Version: 1, Allocation: wanted}, {Version: 7, Allocation: wanted}} {
		if _, err := record.guestAllocation(); err == nil {
			t.Fatalf("invalid journal silently accepted: %+v", record)
		}
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
