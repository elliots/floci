package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func TestAcceptedHTTPWakeOutlivesClientButKeepsPodLifetimeAndDeadline(t *testing.T) {
	entry := availabilityEntry(t)
	lifetime, deletePod := context.WithCancel(t.Context())
	defer deletePod()
	entry.lifetime = lifetime
	request, disconnect := context.WithCancel(context.WithValue(t.Context(), httpWakeKey{}, true))
	defer disconnect()
	accepted, cancel, err := acceptHTTPWake(request, entry)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	deadline, ok := accepted.Deadline()
	if !ok || time.Until(deadline) > 4*time.Minute || time.Until(deadline) < 3*time.Minute {
		t.Fatalf("accepted transition lost its four-minute deadline: %v", deadline)
	}
	disconnect()
	if accepted.Err() != nil {
		t.Fatal("client disconnect cancelled an accepted wake")
	}
	deletePod()
	select {
	case <-accepted.Done():
	case <-time.After(time.Second):
		t.Fatal("Pod deletion or controller shutdown did not cancel accepted wake")
	}
}

func TestHTTPWakeCancellationBeforeAdmissionDoesNotCreateGuestOrRetainPermits(t *testing.T) {
	for _, stage := range []string{"inspection", "admission", "accepted admission"} {
		t.Run(stage, func(t *testing.T) {
			entry := availabilityEntry(t)
			entry.vm = "owned-cold"
			request, disconnect := context.WithCancel(context.WithValue(t.Context(), httpWakeKey{}, true))
			defer disconnect()
			cancelledPermits := 0
			p := &provider{starts: make(chan struct{}, 1), runner: commandRunner{
				cfg: config{state: t.TempDir(), peerOwner: "owner", maxActiveMemoryMiB: 768},
				runtime: stubNativeRuntime{
					prepare: func(ctx context.Context, request msbruntime.AdmissionRequest) (string, error) {
						disconnect()
						if stage == "accepted admission" {
							return "reserved", nil
						}
						return "", ctx.Err()
					},
					cancel: func(ctx context.Context, token string) error {
						if ctx.Err() != nil || token != "reserved" {
							t.Fatal("abandoned reservation could not be released")
						}
						cancelledPermits++
						return nil
					},
					execute: func(ctx context.Context, args []string, _ []byte) (msbruntime.Result, error) {
						if args[0] != "ls" {
							t.Fatalf("cancelled preflight dispatched a native command: %v", args)
						}
						if stage == "inspection" {
							disconnect()
							return msbruntime.Result{}, ctx.Err()
						}
						return msbruntime.Result{Stdout: "[]"}, nil
					},
				},
			}}
			if err := p.resume(request, entry); !errors.Is(err, context.Canceled) {
				t.Fatalf("preflight cancellation was not preserved: %v", err)
			}
			if entry.state != "COLD" || entry.created || entry.allocated || p.activeMemoryMiB != 0 || len(p.starts) != 0 {
				t.Fatalf("cancelled preflight consumed guest identity or memory: state=%s memory=%d", entry.state, p.activeMemoryMiB)
			}
			if (cancelledPermits == 1) != (stage == "accepted admission") {
				t.Fatalf("accepted permit was not released exactly once: %d", cancelledPermits)
			}
			if err := p.runner.checkPending(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestColdHTTPWakeKeepsContextForGuestSetupAfterClientDisconnect(t *testing.T) {
	entry := availabilityEntry(t)
	entry.vm = "owned-cold"
	request, disconnect := context.WithCancel(context.WithValue(t.Context(), httpWakeKey{}, true))
	defer disconnect()
	setupReached := errors.New("guest setup reached")
	prepared, created := false, false
	p := &provider{runner: commandRunner{cfg: config{state: t.TempDir(), maxActiveMemoryMiB: 768}, runtime: stubNativeRuntime{
		prepare: func(ctx context.Context, request msbruntime.AdmissionRequest) (string, error) {
			if ctx.Err() != nil || request.Arguments[0] != "run" || request.SourceName != "" {
				t.Fatal("cold admission was not requested before native creation")
			}
			prepared = true
			return "cold-permit", nil
		},
		execute: func(ctx context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			switch args[0] {
			case "ls":
				return msbruntime.Result{Stdout: "[]"}, nil
			case "run":
				if !prepared || ctx.Value(nativeAdmissionKey{}) != "cold-permit" {
					t.Fatal("VM creation did not use its secured admission")
				}
				created = true
				disconnect()
				return msbruntime.Result{}, nil
			case "exec":
				if !created || ctx.Err() != nil || ctx.Value(nativeAdmissionKey{}) != nil {
					t.Fatal("guest setup inherited request cancellation or an already-consumed admission")
				}
				return msbruntime.Result{}, setupReached
			default:
				t.Fatalf("unexpected native operation: %v", args)
				return msbruntime.Result{}, nil
			}
		},
	}}}
	if err := p.startAdmitted(request, entry); !errors.Is(err, setupReached) || errors.Is(err, errColdWakeNotDispatched) || !entry.created {
		t.Fatalf("client disconnect interrupted accepted guest setup: %v", err)
	}
}

func TestSleepingHTTPWakeCancellationBeforeSourceRemovalKeepsSnapshot(t *testing.T) {
	entry := availabilityEntry(t)
	entry.vm, entry.state = "owned", "SLEEPING"
	entry.snapshots = []string{"owned:sleep"}
	entry.snapshotIDs = map[string]string{"owned:sleep": "original"}
	request, disconnect := context.WithCancel(context.WithValue(t.Context(), httpWakeKey{}, true))
	defer disconnect()
	cancelledPermit := false
	p := &provider{starts: make(chan struct{}, 1), runner: commandRunner{
		cfg: config{state: t.TempDir(), peerOwner: "owner", maxActiveMemoryMiB: 768},
		runtime: stubNativeRuntime{
			prepare: func(ctx context.Context, request msbruntime.AdmissionRequest) (string, error) {
				if request.SourceName != entry.vm || request.Arguments[1] != "restore" {
					t.Fatal("restore admission lost source identity")
				}
				disconnect()
				return "restore-permit", nil
			},
			cancel: func(ctx context.Context, token string) error {
				cancelledPermit = token == "restore-permit" && ctx.Err() == nil
				return nil
			},
			execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
				var value any
				switch args[0] {
				case "ls":
					value = []map[string]string{{"name": entry.vm}}
				case "inspect":
					value = map[string]any{"status": "Stopped", "config": map[string]any{"labels": map[string]string{ownerLabel: "owner", "kubernetes.io/pod-uid": string(entry.pod.UID)}}}
				case "snap":
					if args[1] == "verify" {
						return msbruntime.Result{}, nil
					}
					if args[1] != "ls" {
						t.Fatal("cancelled wake restored its snapshot")
					}
					value = []map[string]string{{"group": "owned", "name": "sleep", "snapshot_id": "original"}}
				default:
					t.Fatalf("cancelled wake removed its source: %v", args)
				}
				data, err := json.Marshal(value)
				return msbruntime.Result{Stdout: string(data)}, err
			},
		},
	}}
	if err := p.resume(request, entry); !errors.Is(err, context.Canceled) {
		t.Fatalf("restore preflight cancellation was not preserved: %v", err)
	}
	if !cancelledPermit || entry.state != "SLEEPING" || entry.generation != 9 || entry.allocated || p.activeMemoryMiB != 0 || len(p.starts) != 0 {
		t.Fatalf("cancelled restore consumed snapshot, generation, or memory: state=%s memory=%d", entry.state, p.activeMemoryMiB)
	}
}

func TestAcceptedHTTPWakeNativeCancellationRetainsUncertainOutcomeFence(t *testing.T) {
	for _, cause := range []string{"Pod lifetime", "deadline"} {
		t.Run(cause, func(t *testing.T) {
			entry := availabilityEntry(t)
			lifetime, stop := context.WithCancel(t.Context())
			defer stop()
			entry.lifetime = lifetime
			accepted, cancel, err := acceptHTTPWake(context.WithValue(t.Context(), httpWakeKey{}, true), entry)
			if err != nil {
				t.Fatal(err)
			}
			defer cancel()
			if cause == "deadline" {
				limited, finish := context.WithTimeout(accepted, 20*time.Millisecond)
				defer finish()
				accepted = limited
			}
			calls := 0
			runner := commandRunner{cfg: config{state: t.TempDir()}, runtime: stubNativeRuntime{
				execute: func(ctx context.Context, _ []string, _ []byte) (msbruntime.Result, error) {
					calls++
					if cause == "Pod lifetime" {
						stop()
					}
					select {
					case <-ctx.Done():
						return msbruntime.Result{}, ctx.Err()
					case <-time.After(time.Second):
						t.Fatal("native mutation lost accepted wake cancellation")
						return msbruntime.Result{}, nil
					}
				},
			}}
			if _, err := runner.vm(accepted, "run", "--name", "owned"); err == nil {
				t.Fatal("unknown native outcome was reported successful")
			}
			if _, err := os.Stat(filepath.Join(runner.cfg.state, "native-command.pending")); err != nil {
				t.Fatal("cancelled native mutation lost its reconciliation fence", err)
			}
			if _, err := runner.vm(t.Context(), "run", "--name", "owned"); err == nil || calls != 1 {
				t.Fatal("uncertain accepted mutation was replayed")
			}
		})
	}
}

func TestAcceptedHTTPWakeDoesNotWaitPastDeadlineForAnotherNativeMutation(t *testing.T) {
	entry := availabilityEntry(t)
	accepted, cancel, err := acceptHTTPWake(context.WithValue(t.Context(), httpWakeKey{}, true), entry)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	limited, finish := context.WithTimeout(accepted, 20*time.Millisecond)
	defer finish()
	runner := commandRunner{cfg: config{state: t.TempDir()}, runtime: stubNativeRuntime{
		execute: func(context.Context, []string, []byte) (msbruntime.Result, error) {
			t.Fatal("timed-out waiter dispatched a native mutation")
			return msbruntime.Result{}, nil
		},
	}}
	nativeMutations.Lock()
	defer nativeMutations.Unlock()
	if _, err := runner.vm(limited, "run", "--name", "owned"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("accepted wake ignored its native dispatch deadline: %v", err)
	}
	if err := runner.checkPending(); err != nil {
		t.Fatal("undispatched waiter wrote an uncertain operation marker", err)
	}
}

func TestColdHTTPWakeReleasesPreparedPermitWhenPodStopsBeforeNativeDispatch(t *testing.T) {
	entry := availabilityEntry(t)
	entry.vm = "owned-cold"
	lifetime, stop := context.WithCancel(t.Context())
	defer stop()
	entry.lifetime = lifetime
	request := context.WithValue(t.Context(), httpWakeKey{}, true)
	cancelledPermit := false
	p := &provider{runner: commandRunner{cfg: config{state: t.TempDir(), maxActiveMemoryMiB: 768}, runtime: stubNativeRuntime{
		prepare: func(context.Context, msbruntime.AdmissionRequest) (string, error) {
			// All filesystem preflight is complete. Let the accepted wake
			// reach the held native lock before cancelling its Pod lifetime.
			timer := time.AfterFunc(200*time.Millisecond, stop)
			t.Cleanup(func() { timer.Stop() })
			return "cold-permit", nil
		},
		cancel: func(ctx context.Context, token string) error {
			if ctx.Value(acceptedHTTPWakeKey{}) != true || ctx.Err() != nil {
				t.Fatal("accepted dispatch cancellation did not use bounded independent permit cleanup")
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("permit cleanup has no deadline")
			}
			cancelledPermit = token == "cold-permit"
			return nil
		},
		execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			if args[0] != "ls" {
				t.Fatalf("cancelled Pod dispatched a native mutation: %v", args)
			}
			return msbruntime.Result{Stdout: "[]"}, nil
		},
	}}}
	nativeMutations.Lock()
	defer nativeMutations.Unlock()
	if err := p.startAdmitted(request, entry); !errors.Is(err, context.Canceled) {
		t.Fatalf("Pod lifetime cancellation was not preserved: %v", err)
	}
	if !cancelledPermit || entry.created {
		t.Fatal("cancelled dispatch retained an unused reservation or created a guest")
	}
	if err := p.runner.checkPending(); err != nil {
		t.Fatal("undispatched cancelled Pod left an uncertain operation fence", err)
	}
}
