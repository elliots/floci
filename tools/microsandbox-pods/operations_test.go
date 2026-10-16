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
)

type terminalRuntime struct {
	stubNativeRuntime
	terminal func(context.Context, []string, io.Reader, io.Writer, io.Writer, msbruntime.StreamOptions) (int, error)
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
