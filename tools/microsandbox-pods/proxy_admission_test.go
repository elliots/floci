package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

func TestProxyWaitsForSharedAdmissionWithoutRetainingLocalPermits(t *testing.T) {
	entry := availabilityEntry(t)
	entry.vm = "owned-cold"
	attempts := 0
	p := &provider{starts: make(chan struct{}, 1), runner: commandRunner{
		cfg: config{state: t.TempDir(), peerOwner: "owner", maxActiveMemoryMiB: 768},
		runtime: stubNativeRuntime{prepare: func(context.Context, msbruntime.AdmissionRequest) (string, error) {
			attempts++
			return "", msbruntime.ErrCapacityUnavailable
		}, execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			switch args[0] {
			case "ls":
				return msbruntime.Result{Stdout: "[]"}, nil
			default:
				t.Fatalf("unexpected operation while native admission refuses: %v", args)
				return msbruntime.Result{}, nil
			}
		}},
	}}
	ctx, cancel := context.WithTimeout(t.Context(), 650*time.Millisecond)
	defer cancel()
	var requests sync.WaitGroup
	for range 12 {
		requests.Go(func() {
			_, release, err := p.acquireProxyActivity(ctx, entry, 8080, true)
			if release == nil {
				t.Error("waiting HTTP request lost its activity release")
				return
			}
			release()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("known shared refusal should wait until request cancellation: %v", err)
			}
		})
	}
	requests.Wait()
	if attempts < 2 || attempts > 3 {
		t.Fatalf("waiting requests must share one bounded retry deadline: attempts=%d", attempts)
	}
	if entry.state != "COLD" || entry.created || entry.allocated || p.activeMemoryMiB != 0 || len(p.starts) != 0 || entry.activeRequests != 0 {
		t.Fatalf("refused request leaked a guest, permit or activity: state=%s created=%v allocated=%v memory=%d starts=%d activity=%d", entry.state, entry.created, entry.allocated, p.activeMemoryMiB, len(p.starts), entry.activeRequests)
	}
	if err := p.runner.checkPending(); err != nil {
		t.Fatalf("known refusal left an uncertain native command: %v", err)
	}
}
