package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
)

type terminalRuntime struct {
	stubNativeRuntime
	terminal func(context.Context, []string, io.Reader, io.Writer, io.Writer, msbruntime.StreamOptions) (int, error)
}

func TestExecLeaseAllowsOwnServiceHTTPAndBlocksHibernation(t *testing.T) {
	e := &podEntry{pod: validPod(), state: "READY", vm: "owned", lifetime: t.Context(), actualReady: true,
		proxyAvailability: true, proxyRoutesReady: true, idleOwner: "controller", idleTimeout: time.Second}
	p := &provider{pods: map[string]*podEntry{"default/app": e}}
	p.runner.runtime = stubNativeRuntime{stream: func(ctx context.Context, _ []string, _ io.Reader, _, _ io.Writer) (int, error) {
		// A command inside the Pod can call its own Service. That HTTP request
		// must acquire the same Pod's availability lease while exec is running.
		address, release, err := p.acquireHTTP(ctx, e, 8123)
		if release != nil {
			release()
		}
		if err != nil || address != "127.0.0.1:8123" {
			t.Errorf("self-Service HTTP failed during exec: %q %v", address, err)
		}
		e.op.Lock()
		defer e.op.Unlock()
		e.lastActivity = time.Now().Add(-time.Hour)
		if len(e.execs) != 1 || e.publishedView.Load().ActiveExecs != 1 || e.activeRequests != 0 || e.activeConnections != 0 {
			t.Error("exec lease was lost or conflated with HTTP/TCP activity")
		}
		if idleEligible(e) {
			t.Error("automatic hibernation accepted an active exec")
		}
		if err := p.hibernate(ctx, e); err == nil || !strings.Contains(err.Error(), "exec sessions are active") {
			t.Errorf("explicit hibernation did not reject active exec: %v", err)
		}
		return 0, nil
	}}
	result := make(chan error, 1)
	go func() {
		result <- p.RunInContainer(t.Context(), "default", "app", "app", []string{"curl", "http://app"}, &sdkAttachIO{})
	}()
	if err := execResult(t, result); err != nil {
		t.Fatal(err)
	}
	if len(e.execs) != 0 || e.publishedView.Load().ActiveExecs != 0 || time.Since(e.lastActivity) > time.Second {
		t.Fatal("completed exec retained its lease or failed to reset idle time")
	}
	e.lastActivity = time.Now().Add(-time.Hour)
	if !idleEligible(e) {
		t.Fatal("completed exec permanently prevented hibernation")
	}
}

func TestDeletingPodDrainsExecBeforeRemovingVM(t *testing.T) {
	p, e := probeFixture(t)
	e.vm = "owned"
	e.lifetime, e.cancel = context.WithCancel(t.Context())
	defer e.cancel()
	e.done = make(chan struct{})
	close(e.done)
	e.pod.DeletionGracePeriodSeconds = new(int64(0))
	p.pods = map[string]*podEntry{"default/app": e}
	started, cleaning, allowCleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cleanupDone, removed := false, false
	p.runner.runtime = stubNativeRuntime{
		stream: func(ctx context.Context, _ []string, _ io.Reader, _, _ io.Writer) (int, error) {
			close(started)
			<-ctx.Done()
			return -1, ctx.Err()
		},
		execute: func(ctx context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			if args[0] == "exec" {
				if ctx.Err() != nil || !strings.Contains(args[len(args)-1], "tasks kill --exec-id 'exec-") || !strings.HasSuffix(args[len(args)-1], "--signal SIGKILL 'application'") {
					t.Errorf("deletion did not clean up only the cancelled exec: %v", args)
				}
				close(cleaning)
				<-allowCleanup
				cleanupDone = true
				return msbruntime.Result{}, nil
			}
			if !cleanupDone {
				t.Error("VM deletion began before exec cleanup finished")
			}
			switch args[0] {
			case "ls":
				if removed {
					return msbruntime.Result{Stdout: "[]"}, nil
				}
				return msbruntime.Result{Stdout: `[{"name":"owned"}]`}, nil
			case "inspect":
				data, err := json.Marshal(map[string]any{"status": "Running", "config": map[string]any{"labels": map[string]string{
					ownerLabel: p.runner.cfg.peerOwner, "kubernetes.io/pod-uid": string(e.pod.UID)}}})
				return msbruntime.Result{Stdout: string(data)}, err
			case "rm":
				removed = true
				return msbruntime.Result{}, nil
			default:
				return msbruntime.Result{}, errors.New("unexpected native operation: " + strings.Join(args, " "))
			}
		},
	}
	execDone, deleteDone := make(chan error, 1), make(chan error, 1)
	deleteContext, cancelDelete := context.WithCancel(t.Context())
	defer cancelDelete()
	go func() {
		execDone <- p.RunInContainer(t.Context(), "default", "app", "app", []string{"sleep", "600"}, &sdkAttachIO{})
	}()
	<-started
	go func() { deleteDone <- p.DeletePod(deleteContext, e.pod) }()
	<-cleaning
	cancelDelete()
	if err := execResult(t, deleteDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("deletion did not respect cancellation while draining exec: %v", err)
	}
	e.op.Lock()
	if len(e.execs) != 1 || p.hibernate(t.Context(), e) == nil {
		t.Error("cancelled deletion dropped its unfinished exec lease")
	}
	e.op.Unlock()
	go func() { deleteDone <- p.DeletePod(t.Context(), e.pod) }()
	select {
	case err := <-deleteDone:
		t.Fatalf("deletion returned before exec cleanup: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowCleanup)
	if err := execResult(t, execDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("deletion did not cancel exec: %v", err)
	}
	if err := execResult(t, deleteDone); err != nil {
		t.Fatal(err)
	}
	if !removed || len(e.execs) != 0 || len(p.pods) != 0 {
		t.Fatal("deletion retained an exec lease, VM, or Pod")
	}
}

func TestReplacingContainerDrainsOnlyItsExecs(t *testing.T) {
	e := &podEntry{pod: validPod(), state: "RUNNING", vm: "owned", lifetime: t.Context()}
	e.pod.Spec.Containers = append(e.pod.Spec.Containers, corev1.Container{Name: "other", Image: "busybox:1.37.0"})
	for _, name := range []string{"app", "other"} {
		e.pod.Status.ContainerStatuses = append(e.pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: name,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
	}
	started := make(chan string, 2)
	otherStopped := make(chan struct{})
	imageFailure := errors.New("stop fixture before image preparation")
	cleaned := make(chan string, 2)
	p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{runtime: stubNativeRuntime{
		stream: func(ctx context.Context, args []string, _ io.Reader, _, _ io.Writer) (int, error) {
			task := args[11]
			started <- task
			<-ctx.Done()
			if task == "app-1" {
				close(otherStopped)
			}
			return -1, ctx.Err()
		},
		execute: func(_ context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			if args[0] == "exec" && strings.Contains(args[len(args)-1], "tasks kill --exec-id") {
				cleaned <- args[len(args)-1]
				return msbruntime.Result{}, nil
			}
			return msbruntime.Result{}, imageFailure
		},
	}}}
	firstDone, secondDone := make(chan error, 1), make(chan error, 1)
	otherContext, cancelOther := context.WithCancel(t.Context())
	defer cancelOther()
	go func() {
		firstDone <- p.RunInContainer(t.Context(), "default", "app", "app", []string{"sleep", "600"}, &sdkAttachIO{})
	}()
	go func() {
		secondDone <- p.RunInContainer(otherContext, "default", "app", "other", []string{"sleep", "600"}, &sdkAttachIO{})
	}()
	<-started
	<-started
	e.op.Lock()
	_, _, err := p.containerLaunch(t.Context(), e, e.pod.Spec.Containers[0], "app-0", "/tmp/floci-app-0")
	e.op.Unlock()
	if !errors.Is(err, imageFailure) {
		t.Fatalf("replacement did not reach image preparation after draining exec: %v", err)
	}
	if err := execResult(t, firstDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("replaced container retained exec: %v", err)
	}
	if script := <-cleaned; !strings.HasSuffix(script, "--signal SIGKILL 'app-0'") {
		t.Fatalf("replacement killed the wrong container exec: %s", script)
	}
	select {
	case <-otherStopped:
		t.Fatal("replacing one container cancelled another container's exec")
	default:
	}
	cancelOther()
	if err := execResult(t, secondDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("client cancellation did not release remaining exec: %v", err)
	}
}

func execResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("exec or lifecycle operation did not complete")
		return nil
	}
}

func (r terminalRuntime) ExecuteStreamWithOptions(ctx context.Context, args []string, input io.Reader, output, stderr io.Writer, options msbruntime.StreamOptions) (int, error) {
	return r.terminal(ctx, args, input, output, stderr, options)
}

func TestTerminalExecPreservesContainerArgumentsAndLiveResize(t *testing.T) {
	resize := make(chan api.TermSize, 3)
	resize <- api.TermSize{Width: 0, Height: 40}
	resize <- api.TermSize{Width: 120, Height: 40}
	resize <- api.TermSize{Width: 80, Height: 24}
	close(resize)
	attach := &sdkAttachIO{tty: true, resize: resize, stdin: strings.NewReader("terminal input")}
	e := &podEntry{pod: validPod(), state: "RUNNING", vm: "owned", lifetime: t.Context()}
	p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{runtime: terminalRuntime{
		terminal: func(ctx context.Context, args []string, input io.Reader, output, stderr io.Writer, options msbruntime.StreamOptions) (int, error) {
			if !options.TTY || !slices.Equal(args[11:], []string{"--tty", "application", "sh", "-c", "literal $(secret)"}) {
				t.Fatalf("TTY or literal command lost: %v", args)
			}
			for _, want := range []msbruntime.TerminalSize{{Rows: 40, Cols: 120}, {Rows: 24, Cols: 80}} {
				if got := <-options.Resize; got != want {
					t.Fatalf("terminal resize: %+v, wanted %+v", got, want)
				}
			}
			if _, ok := <-options.Resize; ok {
				t.Fatal("closed resize input remained open")
			}
			_, err := io.Copy(output, input)
			return 0, err
		},
	}}}
	if err := p.RunInContainer(t.Context(), "default", "app", "app", []string{"sh", "-c", "literal $(secret)"}, attach); err != nil || attach.stdout.String() != "terminal input" {
		t.Fatalf("terminal exec: %v, output %q", err, attach.stdout.String())
	}
}

func TestPodCancellationStopsExecAndKillsOnlyItsContainerProcess(t *testing.T) {
	lifetime, deletePod := context.WithCancel(t.Context())
	defer deletePod()
	e := &podEntry{pod: validPod(), state: "RUNNING", vm: "owned", lifetime: lifetime}
	started := make(chan struct{})
	cleaned := false
	p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{runtime: stubNativeRuntime{
		stream: func(ctx context.Context, _ []string, _ io.Reader, _, _ io.Writer) (int, error) {
			close(started)
			<-ctx.Done()
			return -1, ctx.Err()
		},
		execute: func(ctx context.Context, args []string, _ []byte) (msbruntime.Result, error) {
			if ctx.Err() != nil || !strings.Contains(args[len(args)-1], "tasks kill --exec-id 'exec-") || !strings.Contains(args[len(args)-1], "--signal SIGKILL 'application'") {
				t.Errorf("unsafe cancellation cleanup: %v", args)
			}
			cleaned = true
			return msbruntime.Result{}, nil
		},
	}}}
	result := make(chan error, 1)
	go func() {
		result <- p.RunInContainer(t.Context(), "default", "app", "app", []string{"sleep", "600"}, &sdkAttachIO{})
	}()
	<-started
	deletePod()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !cleaned {
			t.Fatalf("exec did not terminate with cleanup: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Pod deletion waited indefinitely for exec")
	}
	if !e.op.TryLock() {
		t.Fatal("exec retained lifecycle lock")
	}
	e.op.Unlock()
}

func TestPortForwardUsesPodLoopbackPinsRAMAndClosesOnDeletion(t *testing.T) {
	lifetime, deletePod := context.WithCancel(t.Context())
	defer deletePod()
	e := &podEntry{pod: validPod(), state: "RUNNING", vm: "owned", lifetime: lifetime}
	binary := filepath.Join(t.TempDir(), "relay")
	if err := os.WriteFile(binary, []byte("fixture-relay"), 0700); err != nil {
		t.Fatal(err)
	}
	installed := false
	started := make(chan struct{})
	p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{
		cfg: config{relayBinary: binary, peerOwner: "owner", nodeName: "node"}, runtime: stubNativeRuntime{
			execute: func(_ context.Context, args []string, input []byte) (msbruntime.Result, error) {
				switch args[0] {
				case "ls":
					return msbruntime.Result{Stdout: `[{"name":"owned"}]`}, nil
				case "inspect":
					data, _ := json.Marshal(map[string]any{"status": "Running", "config": map[string]any{"labels": map[string]string{ownerLabel: "owner", "kubernetes.io/pod-uid": string(e.pod.UID), "io.floci.node": "node"}}})
					return msbruntime.Result{Stdout: string(data)}, nil
				case "exec":
					if len(input) > 0 {
						installed = string(input) == "fixture-relay"
					}
					return msbruntime.Result{}, nil
				default:
					t.Errorf("unexpected operation %v", args)
					return msbruntime.Result{}, errors.New("unexpected operation")
				}
			},
			stream: func(ctx context.Context, args []string, input io.Reader, output, stderr io.Writer) (int, error) {
				e.op.Lock()
				if !installed || e.activeConnections != 1 || e.publishedView.Load().ActiveConnections != 1 {
					t.Error("forward did not install connector and pin active memory")
				}
				e.op.Unlock()
				if !slices.Equal(args, []string{"exec", "--stream", "owned", "--", guestConnector, "connect", "--address", "127.0.0.1", "--port", "8123"}) || any(input) != any(output) {
					t.Errorf("forward did not use the Pod loopback stream: %v", args)
				}
				close(started)
				<-ctx.Done()
				return -1, ctx.Err()
			},
		}}}
	server, client := net.Pipe()
	defer client.Close()
	result := make(chan error, 1)
	go func() { result <- p.PortForward(t.Context(), "default", "app", 8123, server) }()
	select {
	case <-started:
	case err := <-result:
		t.Fatalf("forward failed before starting: %v", err)
	}
	deletePod()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("port-forward did not cancel: %v", err)
	}
	if e.activeConnections != 0 {
		t.Fatal("port-forward retained active memory pin")
	}
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("deleted Pod left client socket open: %v", err)
	}
}
