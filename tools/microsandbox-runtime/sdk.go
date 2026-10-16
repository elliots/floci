package microsandboxruntime

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

type sdkBackend struct{ config Config }

func (b *sdkBackend) initialize() error { return initializeSDK(b.config) }

var sdkProcess struct {
	sync.Mutex
	initialized bool
	config      Config
	err         error
}

// The native SDK caches its environment and backend. Configure it exactly once
// before loading the library, and refuse a second client's incompatible paths.
func initializeSDK(config Config) error {
	sdkProcess.Lock()
	defer sdkProcess.Unlock()
	path, err := exec.LookPath(config.Executable)
	if err != nil {
		return errors.New("microsandbox executable unavailable")
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return errors.New("cannot resolve microsandbox executable")
	}
	config.Executable = path
	if sdkProcess.initialized {
		if sdkProcess.config != config {
			return errors.New("microsandbox SDK process already uses different runtime paths")
		}
		return sdkProcess.err
	}
	sdkProcess.initialized = true
	sdkProcess.config = config
	sdkProcess.err = func() error {
		if err := os.Setenv("MSB_HOME", config.Home); err != nil {
			return err
		}
		if err := os.Setenv("MSB_PATH", config.Executable); err != nil {
			return err
		}
		version, err := msb.RuntimeVersion()
		if err != nil {
			return err
		}
		if version != "0.7.8" || msb.SDKVersion() != "0.7.8" {
			return errors.New("microsandbox SDK runtime version mismatch")
		}
		info, err := msb.DefaultBackendInfo()
		if err != nil {
			return err
		}
		if info.Kind != msb.BackendLocal {
			return errors.New("Floci requires the local microsandbox backend")
		}
		return nil
	}()
	return sdkProcess.err
}

var mutationLocks = sync.OnceValue(func() []chan struct{} {
	locks := make([]chan struct{}, 256)
	for i := range locks {
		locks[i] = make(chan struct{}, 1)
		locks[i] <- struct{}{}
	}
	return locks
})

func lockMutation(ctx context.Context, r command) (func(), error) {
	name := r.name
	switch r.op {
	case "run", "start", "stop", "rm", "pause", "resume", "snap restore":
	case "snap create":
		name = r.source
	case "snap rm":
		name = r.selector
	default:
		return func() {}, nil
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(name))
	lock := mutationLocks()[hash.Sum32()%256]
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock:
		return func() { lock <- struct{}{} }, nil
	}
}

func (b *sdkBackend) execute(ctx context.Context, r command, stdin io.Reader, stdout, stderr io.Writer) (code int, resultErr error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := initializeSDK(b.config); err != nil {
		return 0, err
	}
	unlock, err := lockMutation(ctx, r)
	if err != nil {
		return 0, err
	}
	defer unlock()
	switch r.op {
	case "ls":
		return 0, listSandboxes(ctx, stdout)
	case "snap create":
		h, err := msb.GetSandbox(ctx, r.source)
		if err != nil {
			return 0, err
		}
		if _, err = h.Refresh(ctx); err != nil {
			return 0, err
		}
		var labels map[string]string
		var boundaryRecord restoredBoundary
		if r.boundary != nil {
			boundaryRecord, err = r.boundary.guestRecord(h, b.config.Home)
			if err != nil {
				return 0, err
			}
			labels = map[string]string{networkPolicyLabel: r.boundary.fingerprint}
		}
		artifact, err := msb.Snapshot.Create(ctx, msb.SnapshotCreateOptions{Name: r.selector, FromSandbox: r.source, Full: r.full, RecordIntegrity: r.integrity, Labels: labels})
		if err != nil {
			return 0, err
		}
		if _, err = h.Refresh(ctx); err != nil {
			return 0, err
		}
		if r.boundary != nil {
			if err := r.boundary.recordSnapshot(b.config.Home, artifact.ID(), boundaryRecord); err != nil {
				return 0, err
			}
		}
		_, err = io.WriteString(stdout, artifact.ID()+"\n")
		return 0, err
	case "snap verify":
		artifact, err := msb.Snapshot.Open(ctx, r.selector)
		if err != nil {
			return 0, err
		}
		report, err := artifact.Verify(ctx)
		if err != nil {
			return 0, err
		}
		return 0, writeJSON(stdout, report)
	case "snap ls":
		return 0, listSnapshots(ctx, stdout)
	case "snap rm":
		var recordPath string
		if r.boundary != nil {
			artifact, openErr := msb.Snapshot.Open(ctx, r.selector)
			if openErr == nil {
				recordPath = snapshotBoundaryPath(b.config.Home, artifact.ID())
			}
			// Removing a broken checkpoint must still work when its parent or
			// manifest cannot be opened. An orphan record cannot authorize a VM.
		}
		if err := msb.Snapshot.Remove(ctx, r.selector, r.force); err != nil {
			return 0, err
		}
		if recordPath != "" {
			if err := os.Remove(recordPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return 0, err
			}
		}
		return 0, nil
	case "snap restore":
		var policyParent string
		var verifiedArtifact *msb.SnapshotArtifact
		policy := r.config.Network
		if r.boundary != nil {
			artifact, err := msb.Snapshot.Open(ctx, r.selector)
			if err != nil {
				return 0, err
			}
			if err := r.boundary.requireIdentity(artifact.Labels()); err != nil {
				return 0, err
			}
			policyParent = artifact.ID()
			verifiedArtifact = artifact
			stored, err := r.boundary.snapshotPolicy(b.config.Home, policyParent)
			if err != nil {
				return 0, err
			}
			if policy == nil {
				policy = stored
			}
			policy, err = r.boundary.restorePolicy(policy)
			if err != nil {
				return 0, err
			}
		}
		if policy != nil {
			// Restore accepts rules only. Guest DNS routing survives in RAM and
			// the policy denies the SDK's unconfigured default DNS gateway.
			policy = &msb.NetworkConfig{Rules: policy.Rules, DefaultEgress: policy.DefaultEgress, DefaultIngress: policy.DefaultIngress}
		}
		options := msb.WithRestoreConfig(msb.RestoreConfig{
			Ports: r.config.PortBindings, NetworkPolicy: policy,
		})
		var sandbox *msb.Sandbox
		if verifiedArtifact != nil {
			sandbox, err = msb.RestoreSandbox(ctx, verifiedArtifact, r.name, options)
		} else {
			sandbox, err = msb.RestoreSandbox(ctx, r.selector, r.name, options)
		}
		if err != nil {
			return 0, err
		}
		if r.boundary != nil {
			ownedID := sandbox.ID()
			defer func() {
				if resultErr != nil {
					resultErr = errors.Join(resultErr, stopFailedNetworkBoot(ctx, r.name, ownedID, b.config.Home))
				}
			}()
		}
		// Restore returns a borrowed detached handle. Releasing it never stops
		// the restored workload, which is already running from captured RAM.
		if err = sandbox.Close(); err != nil {
			return 0, err
		}
		if r.boundary != nil {
			handle, err := msb.GetSandbox(ctx, r.name)
			if err != nil {
				return 0, err
			}
			parent, err := snapshotParent(handle.ConfigJSON())
			if err != nil || parent != policyParent {
				return 0, errors.Join(errors.New("restored network source differs"), err)
			}
			if err := r.boundary.completeBoot(ctx, r.name, b.config.Home, policy); err != nil {
				return 0, err
			}
		}
		_, err = io.WriteString(stdout, r.name+"\n")
		return 0, err
	}
	h, err := msb.GetSandbox(ctx, r.name)
	if err != nil {
		return 0, err
	}
	if r.boundary != nil && (r.op == "start" || r.op == "resume" || r.op == "exec") {
		if err := r.boundary.requireGuest(h, b.config.Home); err != nil {
			return 0, err
		}
	}
	switch r.op {
	case "start":
		var network *msb.NetworkConfig
		if r.boundary != nil {
			record, err := r.boundary.guestRecord(h, b.config.Home)
			if err != nil {
				return 0, err
			}
			network = record.Network
		}
		if r.boundary != nil {
			ownedID := h.ID()
			defer func() {
				if resultErr != nil {
					resultErr = errors.Join(resultErr, stopFailedNetworkBoot(ctx, r.name, ownedID, b.config.Home))
				}
			}()
		}
		sandbox, err := h.StartDetached(ctx)
		if err != nil {
			return 0, err
		}
		if err := sandbox.Detach(ctx); err != nil {
			return 0, err
		}
		if r.boundary != nil {
			return 0, r.boundary.completeBoot(ctx, r.name, b.config.Home, network)
		}
		return 0, nil
	case "stop":
		if r.force {
			return 0, h.Kill(ctx)
		}
		if r.timeout != nil {
			return 0, h.Stop(ctx, msb.WithStopTimeout(*r.timeout))
		}
		return 0, h.Stop(ctx)
	case "rm":
		if r.force {
			err = h.Destroy(ctx, msb.WithDestroyForce())
		} else {
			err = h.Remove(ctx)
		}
		if err != nil {
			return 0, err
		}
		if err := os.Remove(boundaryRecordPath(b.config.Home, h.ID())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		return 0, nil
	case "pause":
		return 0, h.PauseWithGuestFlush(ctx, msb.GuestFlushSkip)
	case "resume":
		// This binding is name-addressed. Refresh detects replacement, but
		// cannot make this atomic against an external process reusing a name.
		if _, err = h.Refresh(ctx); err != nil {
			return 0, err
		}
		if err = h.Resume(ctx); err != nil {
			return 0, err
		}
		_, err = h.Refresh(ctx)
		return 0, err
	case "exec":
		sandbox, err := h.Connect(ctx)
		if err != nil {
			return 0, err
		}
		defer sandbox.Close()
		if r.detached {
			job, err := sandbox.ExecDetached(ctx, r.argv[0], r.argv[1:], msb.WithExecStdinNull(), msb.WithExecUser(r.user))
			if err != nil {
				return 0, err
			}
			if _, err = io.WriteString(stdout, job.ID()+"\n"); err != nil {
				_ = job.Close()
				return 0, err
			}
			return 0, job.Close()
		}
		return streamExec(ctx, sandbox, r, stdin, stdout, stderr)
	default:
		return 0, errors.New("unsupported SDK operation")
	}
}

func listSandboxes(ctx context.Context, stdout io.Writer) error {
	return collectSandboxes(ctx, stdout, func(ctx context.Context, cursor string, limit uint32) (*msb.SandboxPage, error) {
		opts := []msb.SandboxListOption{msb.WithListLimit(limit)}
		if cursor != "" {
			opts = append(opts, msb.WithListCursor(cursor))
		}
		return msb.ListSandboxesWith(ctx, opts...)
	})
}

func collectSandboxes(ctx context.Context, stdout io.Writer, fetch func(context.Context, string, uint32) (*msb.SandboxPage, error)) error {
	rows := []map[string]any{}
	cursor := ""
	seen := map[string]bool{}
	for {
		page, err := fetch(ctx, cursor, 100)
		if err != nil {
			return err
		}
		for _, h := range page.Sandboxes {
			rows = append(rows, map[string]any{"name": h.Name(), "id": h.ID(), "status": cliStatus(h.Status()), "created_at": h.CreatedAt(), "updated_at": h.UpdatedAt()})
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			break
		}
		cursor = *page.NextCursor
		if seen[cursor] {
			return errors.New("microsandbox repeated list cursor")
		}
		seen[cursor] = true
	}
	return writeJSON(stdout, rows)
}

func cliStatus(status msb.SandboxStatus) string {
	s := string(status)
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func listSnapshots(ctx context.Context, stdout io.Writer) error {
	snapshots, err := msb.Snapshot.List(ctx)
	if err != nil {
		return err
	}
	rows := make([]map[string]any, 0, len(snapshots))
	for _, s := range snapshots {
		rows = append(rows, map[string]any{"snapshot_id": s.ID(), "name": s.Name(), "group": s.Group(), "digest": s.Digest(), "scope": s.Scope(), "parent_digest": s.ParentDigest(), "created_at": s.CreatedAt(), "path": s.Path()})
	}
	return writeJSON(stdout, rows)
}

func writeJSON(w io.Writer, value any) error { return json.NewEncoder(w).Encode(value) }
