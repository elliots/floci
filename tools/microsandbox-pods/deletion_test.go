package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPlainPodDeletionDrainsBeforeRemovingGuest(t *testing.T) {
	for _, mode := range []string{"graceful", "zero-grace", "sleeping", "cold"} {
		t.Run(mode, func(t *testing.T) {
			p, e := probeFixture(t)
			e.vm, e.lifetime, e.cancel, e.done = "owned-plain", t.Context(), func() {}, make(chan struct{})
			close(e.done)
			e.pod.Spec.TerminationGracePeriodSeconds = new(int64(2))
			e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", State: corev1.ContainerState{
				Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(e.started)}}}}
			p.pods = map[string]*podEntry{"default/app": e}
			status, exists := "Running", true
			signalled, observedExit := false, false
			if mode == "zero-grace" {
				e.pod.DeletionGracePeriodSeconds = new(int64(0))
			}
			if mode == "sleeping" {
				e.state, status = "SLEEPING", "Stopped"
			}
			if mode == "cold" {
				e.state, exists = "COLD", false
			}
			p.runner.runtime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
				switch args[0] {
				case "ls":
					if !exists {
						return msbruntime.Result{Stdout: "[]"}, nil
					}
					return msbruntime.Result{Stdout: `[{"name":"owned-plain"}]`}, nil
				case "inspect":
					value, err := json.Marshal(map[string]any{"status": status, "config": map[string]any{"labels": map[string]string{
						ownerLabel: p.runner.cfg.peerOwner, "kubernetes.io/pod-uid": string(e.pod.UID)}}})
					return msbruntime.Result{Stdout: string(value)}, err
				case "exec":
					if mode != "graceful" {
						t.Fatalf("%s deletion executed guest code: %v", mode, args)
					}
					script := args[len(args)-1]
					switch {
					case strings.Contains(script, "tasks kill --signal SIGTERM"):
						signalled = true
					case strings.Contains(script, "application.records"):
						return msbruntime.Result{Stdout: "EMPTY\n"}, nil
					case strings.Contains(script, "tail -c"):
						return msbruntime.Result{}, nil
					case strings.Contains(script, "application.exit"):
						if !signalled {
							t.Fatal("application exit checked before SIGTERM")
						}
						observedExit = true
						return msbruntime.Result{Stdout: "EXIT 0"}, nil
					default:
						t.Fatalf("unexpected guest operation: %v", args)
					}
				case "stop":
					if !observedExit {
						t.Fatal("guest stopped before application exit was observed")
					}
					status = "Stopped"
				case "rm":
					if mode == "graceful" && !observedExit {
						t.Fatal("plain application was removed without graceful shutdown")
					}
					exists = false
				default:
					t.Fatalf("unexpected native operation: %v", args)
				}
				return msbruntime.Result{}, nil
			}}
			started := time.Now()
			if err := p.DeletePod(t.Context(), e.pod); err != nil {
				t.Fatal(err)
			}
			if mode != "graceful" && time.Since(started) > time.Second {
				t.Fatal("inactive or forced deletion waited for application grace")
			}
			if exists || len(p.pods) != 0 || signalled != (mode == "graceful") {
				t.Fatalf("unexpected deletion result: exists=%t pods=%d signalled=%t", exists, len(p.pods), signalled)
			}
		})
	}
}
