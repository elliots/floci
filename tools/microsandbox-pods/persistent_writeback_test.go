package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	native "github.com/floci-io/floci/tools/microsandbox-runtime"
	corev1 "k8s.io/api/core/v1"
)

func writebackFixture(t *testing.T) (*provider, *podEntry, *podDiskFixture) {
	t.Helper()
	p, e, f := persistentPodFixture(t)
	if err := p.preparePersistentVolumes(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	binary := make([]byte, 64)
	copy(binary, []byte{127, 'E', 'L', 'F', 2, 1})
	binary[18] = 183
	// The fixture follows readGuestFS's host-architecture validation.
	if runtime.GOARCH == "amd64" {
		binary[18] = 62
	}
	p.runner.cfg.guestFSBinary = filepath.Join(t.TempDir(), "guestfs")
	if err := os.WriteFile(p.runner.cfg.guestFSBinary, binary, 0700); err != nil {
		t.Fatal(err)
	}
	e.state = "RUNNING"
	e.freeze = frozenFixture()
	return p, e, f
}

func TestPersistentCheckpointRequiresFrozenWritersAndInvalidatesBeforeThaw(t *testing.T) {
	p, e, f := writebackFixture(t)
	writes := 0
	f.nativeRuntime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (native.Result, error) {
		script := args[len(args)-1]
		if strings.Contains(script, "writeback") {
			writes++
		}
		if strings.Contains(script, "tasks resume") {
			data, _ := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
			var record journalRecord
			if json.Unmarshal(data, &record) != nil || record.Writeback != nil {
				t.Fatal("writer thaw preceded durable proof invalidation")
			}
		}
		if strings.HasPrefix(script, "cat ") {
			return native.Result{Stdout: "0"}, nil
		}
		return native.Result{}, nil
	}}
	if renewableIdentity(e) {
		t.Fatal("test must cover PVC-only Pod")
	}
	if err := p.syncPersistentVolumes(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if writes != 1 || requirePersistentWriteback(e) != nil {
		t.Fatal("checkpoint writeback not recorded")
	}
	saved := *e.writeback
	e.persistentVolumes[0].Disk.Inode++
	if requirePersistentWriteback(e) == nil {
		t.Fatal("proof accepted different backing inode")
	}
	e.persistentVolumes[0].Disk.Inode--
	if err := p.thawApplications(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if e.writeback != nil || e.freeze != nil {
		t.Fatal("stale proof survived writer thaw")
	}
	e.writeback = &saved
	if requirePersistentWriteback(e) == nil {
		t.Fatal("old checkpoint authorized dirty running disk")
	}
}

func TestPersistentWritebackFailureLeavesNoCheckpointProof(t *testing.T) {
	p, e, f := writebackFixture(t)
	f.nativeRuntime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (native.Result, error) {
		if strings.Contains(args[len(args)-1], "writeback") {
			return native.Result{}, errors.New("writeback failed")
		}
		return native.Result{}, nil
	}}
	if p.syncPersistentVolumes(t.Context(), e) == nil || e.writeback != nil {
		t.Fatal("failed sync admitted capture")
	}
	e.freeze = nil
	if p.syncPersistentVolumes(t.Context(), e) == nil {
		t.Fatal("sync admitted unfrozen writers")
	}
}

func TestPersistentSealIntentAndLostResponseRemainFencedUntilOriginalCompletion(t *testing.T) {
	for _, completion := range []string{"", "1", "0"} {
		t.Run("completion-"+completion, func(t *testing.T) {
			p, e, f := writebackFixture(t)
			commands := 0
			f.nativeRuntime = stubNativeRuntime{execute: func(ctx context.Context, args []string, _ []byte) (native.Result, error) {
				script := args[len(args)-1]
				if strings.Contains(script, "writeback") {
					commands++
					data, _ := os.ReadFile(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json"))
					var record journalRecord
					if json.Unmarshal(data, &record) != nil || record.Writeback == nil || record.Writeback.Phase != "SEALING" || ctx.Done() != nil {
						t.Fatal("seal without durable intent/noncancellable execution")
					}
					return native.Result{}, errors.New("lost response")
				}
				if strings.HasPrefix(script, "cat ") {
					return native.Result{Stdout: completion}, nil
				}
				return native.Result{}, nil
			}}
			if p.sealPersistentVolumes(t.Context(), e, "delete") == nil || e.writeback.Phase != "SEALING" {
				t.Fatal("unknown seal returned success")
			}
			if p.invalidatePersistentWriteback(e) == nil {
				t.Fatal("seal allowed writers to resume")
			}
			err := p.sealPersistentVolumes(t.Context(), e, "delete")
			if commands != 1 {
				t.Fatal("uncertain seal was replayed")
			}
			if completion == "0" {
				if err != nil || requirePersistentWriteback(e) != nil {
					t.Fatal("original completion not reconciled", err)
				}
			} else if err == nil || requirePersistentWriteback(e) == nil {
				t.Fatal("incomplete seal permitted destructive cleanup")
			}
		})
	}
}

func TestPersistentSealFailureRetainsGuestCheckpointsAndLease(t *testing.T) {
	for _, operation := range []string{"delete", "terminal"} {
		t.Run(operation, func(t *testing.T) {
			p, e, f := writebackFixture(t)
			e.freeze = nil
			e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: e.pod.Spec.Containers[0].Name}}
			e.created = true
			e.snapshots = []string{e.vm + ":sleep-1"}
			e.cancel = func() {}
			e.done = make(chan struct{})
			close(e.done)
			e.pod.DeletionGracePeriodSeconds = new(int64(0))
			p.pods = map[string]*podEntry{e.pod.Namespace + "/" + e.pod.Name: e}
			f.release = func(native.NamedDiskIdentity, native.NamedDiskAttachment) error {
				t.Fatal("uncertain disk lease released")
				return nil
			}
			f.nativeRuntime = stubNativeRuntime{execute: func(_ context.Context, args []string, _ []byte) (native.Result, error) {
				switch args[0] {
				case "ls":
					data, _ := json.Marshal([]map[string]string{{"name": e.vm}})
					return native.Result{Stdout: string(data)}, nil
				case "inspect":
					data, _ := json.Marshal(map[string]any{"status": "Running", "config": map[string]any{"labels": map[string]string{ownerLabel: p.runner.cfg.peerOwner, "kubernetes.io/pod-uid": string(e.pod.UID)}}})
					return native.Result{Stdout: string(data)}, nil
				case "exec":
					if strings.Contains(args[len(args)-1], "writeback") {
						return native.Result{}, errors.New("storage writeback failed")
					}
					return native.Result{}, nil
				default:
					t.Fatal("destructive native command followed failed seal", args[0])
					return native.Result{}, nil
				}
			}}
			var err error
			if operation == "delete" {
				err = p.DeletePod(t.Context(), e.pod)
			} else {
				err = p.terminated(e, 0)
			}
			if err == nil || e.writeback == nil || e.writeback.Phase != "SEALING" || len(e.snapshots) != 1 {
				t.Fatal("failed seal discarded recovery state", err)
			}
			if operation == "terminal" && e.pod.Status.ContainerStatuses[0].State.Terminated.ExitCode != 0 {
				t.Fatal("runtime cleanup changed observed application exit")
			}
			if _, err := os.Stat(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json")); err != nil {
				t.Fatal("failed seal lost durable ownership", err)
			}
		})
	}
}
