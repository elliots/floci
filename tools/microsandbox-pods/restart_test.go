package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRestartPersistsAndPublishesTerminationBeforeBackoff(t *testing.T) {
	for _, reason := range []string{"OOMKilled", ""} {
		t.Run(reason, func(t *testing.T) {
			p, entry := probeFixture(t)
			entry.started = time.Now().Add(-time.Minute).Truncate(time.Second)
			entry.terminationReason, entry.probeFailure = reason, "LivenessProbeFailed"
			entry.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", Ready: true, RestartCount: 4,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(entry.started)}}}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var published *corev1.Pod
			var persisted journalRecord
			p.notify = func(pod *corev1.Pod) {
				published = pod
				defer cancel()
				data, err := os.ReadFile(filepath.Join(p.runner.cfg.state, string(entry.pod.UID)+".json"))
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &persisted); err != nil {
					t.Fatal(err)
				}
			}
			entry.op.Lock()
			err := p.restart(ctx, entry, 137)
			entry.op.Unlock()
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("restart did not reach its backoff: %v", err)
			}
			if published == nil || persisted.Pod == nil {
				t.Fatal("restart did not publish after persisting status")
			}
			wantReason := cmp.Or(reason, "LivenessProbeFailed")
			for name, pod := range map[string]*corev1.Pod{"published": published, "persisted": persisted.Pod} {
				status := pod.Status.ContainerStatuses[0]
				termination := status.LastTerminationState.Terminated
				if termination == nil || termination.ExitCode != 137 || termination.Reason != wantReason ||
					!termination.StartedAt.Time.Equal(entry.started) || termination.FinishedAt.IsZero() || termination.FinishedAt.Before(&termination.StartedAt) {
					t.Fatalf("%s restart lost termination details: %+v", name, termination)
				}
				if status.RestartCount != 4 {
					t.Fatalf("%s restart count changed before backoff completed: %d", name, status.RestartCount)
				}
			}
			if published.Status.ContainerStatuses[0].State.Waiting == nil || published.Status.Reason != "ContainerRestarting" {
				t.Fatal("published restart did not become unavailable")
			}
			if persisted.View.State != "RESTARTING" || persisted.TerminationReason != reason {
				t.Fatalf("restart journal lost its lifecycle or termination reason: %+v", persisted)
			}
		})
	}
}
