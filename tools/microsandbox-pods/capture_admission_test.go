package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	runtime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type captureAdmissionRuntime struct {
	stubNativeRuntime
	cancelled int
}

func (r *captureAdmissionRuntime) CancelAdmission(_ context.Context, token string) error {
	if token != "capture-permit" {
		return errors.New("wrong capture token")
	}
	r.cancelled++
	return nil
}

func TestCaptureAdmissionRefusesBeforeFreezePauseOrJournalChange(t *testing.T) {
	e := &podEntry{pod: validPod(), vm: "owned-pod", state: "READY", generation: 7, actualReady: true}
	prepared := false
	c := &captureAdmissionRuntime{stubNativeRuntime: stubNativeRuntime{
		execute: func(_ context.Context, args []string, _ []byte) (runtime.Result, error) {
			switch args[0] {
			case "ls":
				return runtime.Result{Stdout: `[{"name":"owned-pod"}]`}, nil
			case "inspect":
				return runtime.Result{Stdout: fmt.Sprintf(`{"status":"Running","config":{"labels":{"%s":"owner","kubernetes.io/pod-uid":%q}}}`, ownerLabel, e.pod.UID)}, nil
			default:
				t.Fatalf("capacity refusal executed a mutation: %v", args)
				return runtime.Result{}, nil
			}
		},
		prepare: func(_ context.Context, request runtime.AdmissionRequest) (string, error) {
			prepared = true
			if !slices.Equal(request.Arguments, []string{"snap", "create", "sleep-8", "--sandbox", e.vm, "--full", "--integrity"}) || request.SourceName != "" {
				t.Fatalf("wrong capture reservation: %+v", request)
			}
			return "", runtime.ErrCapacityUnavailable
		},
	}}
	state := t.TempDir()
	p := &provider{runner: commandRunner{cfg: config{state: state, peerOwner: "owner"}, runtime: c}}
	if err := p.hibernate(t.Context(), e); !errors.Is(err, runtime.ErrCapacityUnavailable) {
		t.Fatal(err)
	}
	if !prepared || e.state != "READY" || e.generation != 7 || !e.actualReady || e.freeze != nil || c.cancelled != 0 {
		t.Fatalf("refused capture changed workload: %+v", view(e))
	}
	entries, err := os.ReadDir(state)
	if err != nil || len(entries) != 0 {
		t.Fatalf("refused capture wrote a journal: %v %v", entries, err)
	}
}

func TestCaptureAdmissionCancelsWhenJournalFailsBeforeMutation(t *testing.T) {
	e := &podEntry{pod: validPod(), vm: "owned-pod", state: "READY", generation: 7}
	c := &captureAdmissionRuntime{stubNativeRuntime: stubNativeRuntime{
		execute: func(_ context.Context, args []string, _ []byte) (runtime.Result, error) {
			switch args[0] {
			case "ls":
				return runtime.Result{Stdout: `[{"name":"owned-pod"}]`}, nil
			case "inspect":
				return runtime.Result{Stdout: fmt.Sprintf(`{"status":"Running","config":{"labels":{"%s":"owner","kubernetes.io/pod-uid":%q}}}`, ownerLabel, e.pod.UID)}, nil
			default:
				t.Fatalf("journal failure executed a mutation: %v", args)
				return runtime.Result{}, nil
			}
		},
		prepare: func(context.Context, runtime.AdmissionRequest) (string, error) { return "capture-permit", nil },
	}}
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	p := &provider{runner: commandRunner{cfg: config{state: file, peerOwner: "owner"}, runtime: c}}
	if err := p.hibernate(t.Context(), e); err == nil || c.cancelled != 1 {
		t.Fatalf("undispatched reservation was not cancelled: %d %v", c.cancelled, err)
	}
}
