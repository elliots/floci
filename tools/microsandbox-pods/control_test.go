package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestControlRequiresBearerAndGenerationBeforeMutation(t *testing.T) {
	token := strings.Repeat("a", 32)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	port, err := availableTCPPort()
	if err != nil {
		t.Fatal(err)
	}
	entry := &podEntry{pod: validPod(), state: "COLD", generation: 7, activitySequence: 3, lifetime: t.Context()}
	p := &provider{pods: map[string]*podEntry{"default/app": entry}}
	server, err := p.serveControl(fmt.Sprintf("127.0.0.1:%d", port), file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	for _, test := range []struct {
		auth, body, operation string
		want                  int
	}{
		{"", "{\"expectedGeneration\":7}", "resume", 401},
		{token, "{\"expectedGeneration\":6}", "resume", 401},
		{"Bearer " + token, "{}", "resume", 400},
		{"Bearer " + token, "{\"expectedGeneration\":6}", "resume", 409},
		{"Bearer " + token, `{"expectedGeneration":7,"expectedActivitySequence":2}`, "hibernate", 409},
		{"Bearer " + token, `{"expectedGeneration":7,"expectedActivitySequence":-1}`, "hibernate", 400},
	} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fmt.Sprintf("http://127.0.0.1:%d/v1/pods/%s/%s", port, entry.pod.UID, test.operation), strings.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", test.auth)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != test.want {
			t.Fatalf("status %d, want %d", response.StatusCode, test.want)
		}
	}
}

func TestGuestCommandFailureDoesNotExposeOutput(t *testing.T) {
	_, err := (commandRunner{}).run(t.Context(), "sh", "-c", "printf private-secret >&2; exit 1")
	if err == nil || strings.Contains(err.Error(), "private-secret") {
		t.Fatalf("unsafe command error: %v", err)
	}
}

func TestSelectedNetworkPolicyBlocksNativeCreation(t *testing.T) {
	policy := &networkingv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Name: "deny", Namespace: "default"}, Spec: networkingv1.NetworkPolicySpec{PodSelector: metav1.LabelSelector{}}}
	p := &provider{client: fake.NewClientset(policy)}
	if err := p.CreatePod(t.Context(), validPod()); err == nil {
		t.Fatal("selected policy bypassed")
	}
}

func TestColdAdmissionTimeoutLeavesGenerationRetryable(t *testing.T) {
	p := &provider{client: fake.NewClientset(), runner: commandRunner{cfg: config{maxActiveMemoryMiB: vmMemoryMiB}}, activeMemoryMiB: vmMemoryMiB}
	e := &podEntry{pod: validPod(), state: "COLD", generation: 9}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.resume(ctx, e); err == nil {
		t.Fatal("full budget admitted another guest")
	}
	if e.state != "COLD" || e.generation != 9 || e.allocated {
		t.Fatal("cold admission failure consumed runtime identity")
	}
}

func TestUnsettledNativeCommandRetainsColdJournalAndBudget(t *testing.T) {
	directory := t.TempDir()
	pod := validPod()
	ctx, cancel := context.WithCancel(t.Context())
	e := &podEntry{pod: pod, state: "COLD", allocated: true, lifetime: ctx, cancel: cancel, done: make(chan struct{})}
	close(e.done)
	p := &provider{runner: commandRunner{cfg: config{state: directory}}, pods: map[string]*podEntry{"default/app": e}, activeMemoryMiB: vmMemoryMiB}
	if err := p.journal(e); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "native-command.pending"), []byte("run"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := p.DeletePod(t.Context(), pod); err == nil {
		t.Fatal("uncertain native operation was forgotten")
	}
	if p.activeMemoryMiB != vmMemoryMiB {
		t.Fatal("uncertain budget released")
	}
	if _, err := os.Stat(filepath.Join(directory, string(pod.UID)+".json")); err != nil {
		t.Fatal("ownership journal lost", err)
	}
}

func TestSnapshotDeletionIsIdempotentAndRejectsReplacement(t *testing.T) {
	for _, currentID := range []string{"", "replacement"} {
		t.Run(currentID, func(t *testing.T) {
			directory := t.TempDir()
			entries := []map[string]string{}
			if currentID != "" {
				entries = append(entries, map[string]string{"group": "owned", "name": "sleep", "snapshot_id": currentID})
			}
			data, _ := json.Marshal(entries)
			script := filepath.Join(directory, "msb")
			if err := os.WriteFile(script, []byte("#!/bin/sh\nif [ \"$1 $2\" != 'snap ls' ]; then exit 77; fi\nprintf '%s' "+quote(string(data))+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			p := &provider{runner: testCommandRunner(config{state: directory, msb: script})}
			err := p.deleteSnapshot(t.Context(), &podEntry{snapshotIDs: map[string]string{"owned:sleep": "original"}}, "owned:sleep")
			if (err != nil) != (currentID != "") {
				t.Fatalf("unexpected deletion result: %v", err)
			}
		})
	}
}

func TestGuestProfileAcceptsLargerCPUAndMemoryRequests(t *testing.T) {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		pod := validPod()
		quantity := resource.MustParse("2")
		if name == corev1.ResourceMemory {
			quantity = resource.MustParse("1Gi")
		}
		pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{name: quantity}
		if err := validatePod(pod); err != nil {
			t.Fatalf("supported %s request rejected: %v", name, err)
		}
	}
}
