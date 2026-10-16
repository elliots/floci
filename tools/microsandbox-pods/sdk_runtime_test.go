package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	utilexec "k8s.io/utils/exec"
)

// Existing guest-script fixtures model containerd and native failure boundaries.
// Their CLI transport is test-only; production always constructs an SDK client.
func testCommandRunner(cfg config) commandRunner {
	return commandRunner{cfg: cfg, runtime: scriptNativeRuntime{cfg: cfg}}
}

type scriptNativeRuntime struct{ cfg config }

func (r scriptNativeRuntime) Execute(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
	var stdout, stderr bytes.Buffer
	exitCode, err := r.ExecuteStream(ctx, args, bytes.NewReader(input), &stdout, &stderr)
	return msbruntime.Result{ExitCode: exitCode, Stdout: stdout.String(), Stderr: stderr.String()}, err
}

func (r scriptNativeRuntime) ExecuteStream(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	cmd := (commandRunner{cfg: r.cfg}).command(ctx, r.cfg.msb, args...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdin, stdout, stderr
	err := cmd.Run()
	if err == nil {
		return 0, nil
	}
	if ctx.Err() != nil {
		return -1, ctx.Err()
	}
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return exit.ExitCode(), nil
	}
	return -1, err
}

type stubNativeRuntime struct {
	execute func(context.Context, []string, []byte) (msbruntime.Result, error)
	stream  func(context.Context, []string, io.Reader, io.Writer, io.Writer) (int, error)
}

func (r stubNativeRuntime) Execute(ctx context.Context, args []string, input []byte) (msbruntime.Result, error) {
	return r.execute(ctx, args, input)
}

func (r stubNativeRuntime) ExecuteStream(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
	return r.stream(ctx, args, stdin, stdout, stderr)
}

func TestNativeRuntimeRequiresExplicitClient(t *testing.T) {
	runner := commandRunner{cfg: config{msb: "must-not-execute"}}
	if _, err := runner.vm(t.Context(), "ls"); err == nil {
		t.Fatal("missing SDK client accepted")
	}
	if err := runner.guestInput(t.Context(), "pod", "cat", []byte("private")); err == nil {
		t.Fatal("guest stdin fell back to a native CLI")
	}
	if err := runner.nativeStream(t.Context(), []string{"exec"}, nil, io.Discard, io.Discard); err == nil {
		t.Fatal("streaming exec fell back to a native CLI")
	}
}

func TestNativeSDKOutputPreservesBytesAndSeparatesStderr(t *testing.T) {
	want := "  first\n\nlast  \n"
	runner := commandRunner{runtime: stubNativeRuntime{execute: func(_ context.Context, args []string, input []byte) (msbruntime.Result, error) {
		return msbruntime.Result{Stdout: want, Stderr: "warning"}, nil
	}}}
	output, err := runner.nativeOutput(t.Context(), []string{"exec"}, nil)
	if err != nil || string(output) != want {
		t.Fatalf("raw output changed: %q, %v", output, err)
	}
	metadata, err := runner.vm(t.Context(), "ls")
	if err != nil || metadata != strings.TrimSpace(want) {
		t.Fatalf("metadata was not normalized: %q, %v", metadata, err)
	}
}

func TestNativeSDKGuestInputDoesNotPutSecretsInArguments(t *testing.T) {
	secret := []byte("private-registry-password\n")
	runner := commandRunner{runtime: stubNativeRuntime{execute: func(_ context.Context, args []string, input []byte) (msbruntime.Result, error) {
		if !slices.Equal(input, secret) || strings.Contains(strings.Join(args, " "), "private-registry-password") {
			t.Fatalf("guest input was not transported privately")
		}
		if !slices.Equal(args, []string{"exec", "--stream", "pod", "--", "sh", "-ec", "cat > /tmp/input"}) {
			t.Fatalf("unexpected guest command: %v", args)
		}
		return msbruntime.Result{}, nil
	}}}
	if err := runner.guestInput(t.Context(), "pod", "cat > /tmp/input", secret); err != nil {
		t.Fatal(err)
	}
}

func TestNativeSDKFailureRedactsOutputArgumentsAndCause(t *testing.T) {
	for _, transport := range []bool{false, true} {
		runner := commandRunner{runtime: stubNativeRuntime{execute: func(context.Context, []string, []byte) (msbruntime.Result, error) {
			result := msbruntime.Result{ExitCode: 7, Stdout: "private-stdout", Stderr: "private-stderr"}
			if transport {
				return result, errors.Join(context.Canceled, errors.New("private-cause"))
			}
			return result, nil
		}}}
		_, err := runner.nativeOutput(t.Context(), []string{"exec", "private-argument"}, []byte("private-input"))
		if err == nil || strings.Contains(err.Error(), "private") {
			t.Fatalf("unsafe native error: %v", err)
		}
		if errors.Is(err, context.Canceled) != transport {
			t.Fatal("transport cancellation cause was lost")
		}
		failure, ok := errors.AsType[*nativeCommandError](err)
		if !ok || failure.Exited() == transport || (!transport && failure.ExitStatus() != 7) {
			t.Fatalf("guest exit status was lost: %v", err)
		}
	}
}

func TestNativeSDKMutationFenceRetainsUncertainResults(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "uncertain"}[fail], func(t *testing.T) {
			directory := t.TempDir()
			marker := filepath.Join(directory, "native-command.pending")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			calls := 0
			runner := commandRunner{cfg: config{state: directory}, runtime: stubNativeRuntime{execute: func(ctx context.Context, args []string, _ []byte) (msbruntime.Result, error) {
				calls++
				if ctx.Err() != nil {
					t.Fatal("accepted native mutation inherited request cancellation")
				}
				data, err := os.ReadFile(marker)
				if err != nil || string(data) != "snap restore\n" {
					t.Fatalf("native mutation ran without durable fence: %q, %v", data, err)
				}
				if fail {
					return msbruntime.Result{}, errors.New("uncertain transport")
				}
				return msbruntime.Result{Stdout: "restored\n"}, nil
			}}}
			output, err := runner.vm(ctx, "snap", "restore", "owned:sleep")
			if (err != nil) != fail || (!fail && output != "restored") {
				t.Fatalf("unexpected mutation outcome: %q, %v", output, err)
			}
			_, statErr := os.Stat(marker)
			if fail && statErr != nil || !fail && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("incorrect fence after mutation: %v", statErr)
			}
			if fail {
				if _, err := runner.vm(t.Context(), "rm", "owned"); err == nil || calls != 1 {
					t.Fatal("uncertain mutation did not block the next SDK operation")
				}
			}
		})
	}
}

func TestNativeSDKReadOnlyRequestsKeepCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runner := commandRunner{runtime: stubNativeRuntime{execute: func(ctx context.Context, _ []string, _ []byte) (msbruntime.Result, error) {
		return msbruntime.Result{}, ctx.Err()
	}}}
	if _, err := runner.vm(ctx, "ls"); !errors.Is(err, context.Canceled) {
		t.Fatalf("read-only operation lost cancellation: %v", err)
	}
}

type sdkAttachIO struct {
	stdin          io.Reader
	stdout, stderr bytes.Buffer
}

type sdkWriteCloser struct{ io.Writer }

func (sdkWriteCloser) Close() error           { return nil }
func (a *sdkAttachIO) Stdin() io.Reader       { return a.stdin }
func (a *sdkAttachIO) Stdout() io.WriteCloser { return sdkWriteCloser{&a.stdout} }
func (a *sdkAttachIO) Stderr() io.WriteCloser { return sdkWriteCloser{&a.stderr} }
func (a *sdkAttachIO) TTY() bool              { return false }
func (a *sdkAttachIO) Resize() <-chan api.TermSize {
	return nil
}

func TestNativeSDKContainerExecPassesLiveStreamsAndExitStatus(t *testing.T) {
	attach := &sdkAttachIO{stdin: strings.NewReader("stream input\n")}
	args := []string{"sh", "-c", "printf '%s' 'literal $(command)'"}
	pod := validPod()
	e := &podEntry{pod: pod, vm: "native-pod", state: "RUNNING"}
	p := &provider{pods: map[string]*podEntry{"default/app": e}, runner: commandRunner{runtime: stubNativeRuntime{
		stream: func(ctx context.Context, command []string, stdin io.Reader, stdout, stderr io.Writer) (int, error) {
			if stdin != attach.stdin || stdout != attach.Stdout() || stderr != attach.Stderr() {
				t.Fatal("exec streams were buffered or replaced")
			}
			if !slices.Equal(command[:10], []string{"exec", "--stream", "native-pod", "--", "ctr", "--address", "/run/containerd/containerd.sock", "tasks", "exec", "--exec-id"}) ||
				!strings.HasPrefix(command[10], "exec-") || command[11] != "application" || !slices.Equal(command[12:], args) {
				t.Fatalf("exec arguments changed: %v", command)
			}
			if _, err := io.Copy(stdout, stdin); err != nil {
				return -1, err
			}
			if _, err := io.WriteString(stderr, "guest error\n"); err != nil {
				return -1, err
			}
			return 7, nil
		},
	}}}
	err := p.RunInContainer(t.Context(), "default", "app", "app", args, attach)
	exit, ok := err.(utilexec.ExitError)
	if !ok || !exit.Exited() || exit.ExitStatus() != 7 {
		t.Fatalf("exec protocol lost guest exit code: %v", err)
	}
	if attach.stdout.String() != "stream input\n" || attach.stderr.String() != "guest error\n" {
		t.Fatalf("exec changed its separate streams: stdout=%q stderr=%q", attach.stdout.String(), attach.stderr.String())
	}
}
