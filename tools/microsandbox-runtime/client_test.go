package microsandboxruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

type fakeBackend func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error)

func (fakeBackend) initialize() error { return nil }

func (f fakeBackend) execute(ctx context.Context, c command, in io.Reader, out, errs io.Writer) (int, error) {
	return f(ctx, c, in, out, errs)
}

func TestNewDoesNotTouchRuntimeOrHome(t *testing.T) {
	home := filepath.Join(t.TempDir(), "not-created")
	c, err := New(Config{Executable: "/missing/msb", Home: home})
	if err != nil || c == nil {
		t.Fatalf("New: %v", err)
	}
	if _, err = os.Stat(home); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("New touched home: %v", err)
	}
}

func TestReadOnlyFallbackAllowlistPreservesDiagnosticsAndReports(t *testing.T) {
	c, err := New(Config{Home: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	c.cli = func(_ context.Context, args []string, out, errs io.Writer) (int, error) {
		calls = append(calls, "cli "+args[0])
		_, _ = io.WriteString(out, "exact\n")
		return 0, nil
	}
	c.core = fakeBackend(func(_ context.Context, r command, _ io.Reader, out, errs io.Writer) (int, error) {
		calls = append(calls, "sdk "+r.op)
		return 0, nil
	})
	for _, args := range [][]string{{"--version"}, {"doctor"}, {"inspect", "--format", "json", "owned"}, {"metrics", "--format", "json", "owned"}, {"ls", "--format", "json"}, {"pause", "owned"}, {"snap", "verify", "owned:sleep"}} {
		if _, err = c.Execute(t.Context(), args, nil); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	want := []string{"cli --version", "cli doctor", "cli inspect", "cli metrics", "sdk ls", "sdk pause", "sdk snap verify"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls %v", calls)
	}
	before := len(calls)
	if _, err = c.Execute(t.Context(), []string{"arbitrary-secret-command"}, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if len(calls) != before {
		t.Fatal("invalid operation reached runtime")
	}
}

func TestImageLaunchRetainsSnapshotCompatibleCLIPath(t *testing.T) {
	args := []string{"run", "-d", "--name", "owned", "--label", "floci.owner=test", "-e", "PASSWORD=a'$(literal)", "distroless/image:1", "--", "literal argument"}
	c := &Client{core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
		t.Fatal("image was launched as SDK managed job, blocking full snapshots")
		return 0, nil
	})}
	c.cli = func(_ context.Context, got []string, out, errs io.Writer) (int, error) {
		if !reflect.DeepEqual(got, args) {
			t.Fatal("image launch argv changed")
		}
		_, err := io.WriteString(out, "owned\n")
		return 0, err
	}
	result, err := c.Execute(t.Context(), args, nil)
	if err != nil || result.Stdout != "owned\n" {
		t.Fatalf("launch: %#v %v", result, err)
	}
	c.cli = func(context.Context, []string, io.Writer, io.Writer) (int, error) { return 1, nil }
	if result, err = c.Execute(t.Context(), args, nil); err == nil || errors.Is(err, ErrInvalidArgument) || result != (Result{}) {
		t.Fatalf("failed launch lost uncertainty: %#v %v", result, err)
	}
}

func TestGuestStdinAndExitStatusStaySeparateFromTransportErrors(t *testing.T) {
	const secret = "pass'$()\nword\x00"
	c := &Client{core: fakeBackend(func(ctx context.Context, r command, in io.Reader, out, errs io.Writer) (int, error) {
		data, err := io.ReadAll(in)
		if err != nil || string(data) != secret {
			t.Fatalf("stdin mismatch: %v", err)
		}
		if r.user != "postgres" || !reflect.DeepEqual(r.argv, []string{"sh", "-c", "printf '%s' '$()'"}) {
			t.Fatal("guest argv changed")
		}
		_, _ = io.WriteString(out, "line\n\n")
		_, _ = io.WriteString(errs, "diagnostic\n")
		return 17, nil
	})}
	r, err := c.Execute(t.Context(), []string{"exec", "--no-tty", "--user", "postgres", "db", "--", "sh", "-c", "printf '%s' '$()'"}, []byte(secret))
	if err != nil || r.ExitCode != 17 || r.Stdout != "line\n\n" || r.Stderr != "diagnostic\n" {
		t.Fatalf("result %#v error %v", r, err)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "{\"exitCode\":17,\"stdout\":\"line\\n\\n\",\"stderr\":\"diagnostic\\n\"}" {
		t.Fatalf("wire shape: %s", encoded)
	}
	sentinel := errors.New("native error includes " + secret)
	c.core = fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) { return 0, sentinel })
	r, err = c.Execute(t.Context(), []string{"exec", "db", "--", "sh", "-c", secret[:5]}, nil)
	if err == nil || !errors.Is(err, sentinel) || r != (Result{}) || strings.Contains(err.Error(), "pass") {
		t.Fatalf("unsafe error/result: %#v %v", r, err)
	}
	if errors.Is(err, ErrInvalidArgument) {
		t.Fatal("uncertain native outcome classified as input rejection")
	}
}

func TestInputRejectedBeforeAnyRuntimeAction(t *testing.T) {
	c := &Client{core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
		t.Fatal("runtime called")
		return 0, nil
	})}
	for _, args := range [][]string{{"run", "-d", "--name", "owned", "alpine"}, {"exec", "--no-stdin", "owned", "--", "cat"}, {"exec", "-d", "owned", "--", "cat"}, {"snap", "restore", "owned:sleep", "--name", "restored"}} {
		if _, err := c.Execute(t.Context(), args, []byte("secret")); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%v: %v", args, err)
		}
	}
}

func TestBufferedOutputLimitCannotBecomeSuccessfulExit(t *testing.T) {
	c := &Client{cli: func(_ context.Context, _ []string, out, errs io.Writer) (int, error) {
		out.(*boundedWriter).written = MaxOutputBytes
		_, _ = io.WriteString(out, "overflow")
		return 1, nil
	}}
	if result, err := c.Execute(t.Context(), []string{"doctor"}, nil); !errors.Is(err, errOutputLimit) || result != (Result{}) {
		t.Fatalf("overflow result %#v error %v", result, err)
	}
}

func TestLiveStreamingHasNoTotalOutputLimit(t *testing.T) {
	c := &Client{core: fakeBackend(func(_ context.Context, _ command, in io.Reader, out, errs io.Writer) (int, error) {
		if in != nil {
			t.Fatal("nil input acquired a pipe")
		}
		chunk := make([]byte, 1<<20)
		for range 65 {
			if _, err := out.Write(chunk); err != nil {
				return 0, err
			}
		}
		_, err := io.WriteString(errs, "discard me")
		return 23, err
	})}
	var output countWriter
	code, err := c.ExecuteStream(t.Context(), []string{"exec", "owned", "--", "cat"}, nil, &output, nil)
	if err != nil || code != 23 || output.n != 65<<20 {
		t.Fatalf("code=%d bytes=%d err=%v", code, output.n, err)
	}
}

type countWriter struct{ n int }

func (w *countWriter) Write(p []byte) (int, error) { w.n += len(p); return len(p), nil }

func TestAcceptedMutationContextIsNotReplacedOrRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ctx = context.WithoutCancel(ctx)
	calls := 0
	c := &Client{core: fakeBackend(func(got context.Context, r command, _ io.Reader, _ io.Writer, _ io.Writer) (int, error) {
		calls++
		if got != ctx || got.Err() != nil {
			t.Fatal("mutation context changed")
		}
		return 0, errors.New("uncertain")
	})}
	if _, err := c.Execute(ctx, []string{"stop", "--force", "owned"}, nil); err == nil || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestCallerCommandShapesAndNetworkPolicy(t *testing.T) {
	cases := [][]string{
		{"run", "-d", "--name", "db", "--cpus", "1", "--memory", "512M", "--root-disk", "4G", "-e", "POSTGRES_PASSWORD=pa'ss", "-p", "127.0.0.1:54321:5432", "postgres:16"},
		{"run", "--detach", "--entrypoint", "/bin/sh", "--name", "pod", "--label", "floci.owner=fixture", "--cpus", "2", "--memory", "1024M", "--root-disk", "flat:4G", "--net", "host,private,public", "--port", "127.0.0.1:30000:8472/udp", "controller:1", "--", "-ec", "containerd & wait"},
		{"run", "-d", "--name", "ec2", "--entrypoint", "/bin/sh", "--net-default", "deny", "--net-rule", "allow:ingress@10.0.0.0/8:tcp:8080-8082", "--net-rule", "allow:egress@0.0.0.0/0", "alpine", "--", "-c", "exec tail -f /dev/null"},
		{"stop", "-t", "30", "db"}, {"stop", "db", "--timeout", "30"}, {"rm", "--force", "pod"},
		{"snap", "create", "sleep", "--sandbox", "pod", "--full", "--integrity"},
		{"snap", "restore", "pod:sleep", "--name", "pod", "--port", "127.0.0.1:30000:8472/udp", "--net-default", "deny", "--net-rule", "allow:egress@host,allow:egress@private,allow:egress@public", "--net-rule", "allow:ingress@any"},
		{"exec", "--no-tty", "--user", "postgres", "db", "--", "sh", "-c", "pg_ctl stop"},
		{"exec", "--stream", "pod", "--", "sh", "-ec", "cat > /run/archive"},
	}
	for _, args := range cases {
		if _, err := parse(args); err != nil {
			t.Fatalf("operation %s rejected: %v", args[0], err)
		}
	}
	r, err := parse(cases[7])
	if err != nil {
		t.Fatal(err)
	}
	n := r.config.Network
	if len(n.Rules) != 4 || n.DefaultEgress != msb.PolicyActionDeny || n.DefaultIngress != msb.PolicyActionDeny || n.Rules[3].Destination != "*" || n.Rules[3].Direction != msb.PolicyDirectionIngress {
		t.Fatalf("restore rules %#v", n)
	}
	if r.config.PortBindings[0].Protocol != msb.PortProtocolUDP {
		t.Fatal("lost UDP VXLAN mapping")
	}
	r, err = parse(cases[1])
	if err != nil {
		t.Fatal(err)
	}
	if r.config.RootDisk.Kind() != msb.RootDiskKindFlat || r.config.RootDisk.SizeMiB != 4096 || r.config.MemoryMiB != 1024 || r.config.CPUs != 2 {
		t.Fatalf("lost launch resources: %#v", r.config)
	}
	if !reflect.DeepEqual(r.config.Cmd, []string{"-ec", "containerd & wait"}) {
		t.Fatal("lost literal command")
	}
}

func TestInvalidOptionsDoNotLeakTheirValues(t *testing.T) {
	cases := [][]string{{"run", "-d", "--name", "owned", "--cpus", "secret", "alpine"}, {"exec", "--password", "secret", "owned", "--", "sh"}, {"run", "-d", "--name", "owned", "--memory", "999999999999G", "alpine"}, {"run", "-d", "--name", "owned", "--net", "public", "--net-default", "deny", "alpine"}, {"snap", "restore", "a:b", "--name", "owned", "--net-rule", "secret"}, {"exec", "owned", "--", "a\x00secret"}, {"stop", "--timeout", "-1", "owned"}, {"run", "-d", "--name", "owned", "--cpus", "1", "--cpus", "2", "alpine"}}
	for _, args := range cases {
		_, err := parse(args)
		if !errors.Is(err, ErrInvalidArgument) || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe rejection: %v", err)
		}
	}
}

func TestInventoryPreservesCLIStatusCase(t *testing.T) {
	if cliStatus(msb.SandboxStatusPaused) != "Paused" {
		t.Fatal("CLI status shape changed")
	}
}

type fakeEvents struct {
	events []*msb.ExecEvent
	err    error
}

func (f *fakeEvents) Recv(ctx context.Context) (*msb.ExecEvent, error) {
	if len(f.events) == 0 {
		if f.err != nil {
			return nil, f.err
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	e := f.events[0]
	f.events = f.events[1:]
	return e, nil
}
func (*fakeEvents) Kill(context.Context) error { return nil }

func TestExecEventsRequireTerminalStatusAndKeepStreamsSeparate(t *testing.T) {
	stream := &fakeEvents{events: []*msb.ExecEvent{{Kind: msb.ExecEventStdout, Data: []byte("a\n")}, {Kind: msb.ExecEventStderr, Data: []byte("b\n")}, {Kind: msb.ExecEventExited, ExitCode: 9}, {Kind: msb.ExecEventStdout, Data: []byte("tail\n")}, {Kind: msb.ExecEventDone}}}
	var out, errs bytes.Buffer
	code, err := consumeEvents(t.Context(), stream, &out, &errs, nil)
	if err != nil || code != 9 || out.String() != "a\ntail\n" || errs.String() != "b\n" {
		t.Fatalf("events: %d %q %q %v", code, out.String(), errs.String(), err)
	}
	for _, kind := range []msb.ExecEventKind{msb.ExecEventDone, msb.ExecEventFailed, msb.ExecEventStdinError} {
		if _, err := consumeEvents(t.Context(), &fakeEvents{events: []*msb.ExecEvent{{Kind: kind}}}, io.Discard, io.Discard, nil); err == nil {
			t.Fatalf("event %v falsely succeeded", kind)
		}
	}
}

func TestExecCancellationAndShortWritesAreTransportFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := consumeEvents(ctx, &fakeEvents{}, io.Discard, io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := consumeEvents(t.Context(), &fakeEvents{events: []*msb.ExecEvent{{Kind: msb.ExecEventStdout, Data: []byte("abc")}}}, shortWriter{}, io.Discard, nil); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	release, err := lockMutation(ctx, command{op: "stop", name: "owned"})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err = lockMutation(canceled, command{op: "rm", name: "owned"}); !errors.Is(err, context.Canceled) {
		t.Fatal("blocked mutation ignored cancellation")
	}
}

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }

func TestRecoveryListsAllFiveHundredSandboxesAcrossSDKPages(t *testing.T) {
	var output bytes.Buffer
	calls := 0
	err := collectSandboxes(t.Context(), &output, func(_ context.Context, cursor string, limit uint32) (*msb.SandboxPage, error) {
		if limit > 100 || limit == 0 {
			t.Fatalf("SDK rejects page limit %d", limit)
		}
		if calls == 0 && cursor != "" || calls > 0 && cursor != strconv.Itoa(calls) {
			t.Fatalf("unexpected recovery cursor %q", cursor)
		}
		calls++
		page := &msb.SandboxPage{Sandboxes: make([]*msb.SandboxHandle, 100)}
		for i := range page.Sandboxes {
			page.Sandboxes[i] = &msb.SandboxHandle{}
		}
		if calls < 5 {
			page.NextCursor = new(strconv.Itoa(calls))
		}
		return page, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]any
	if err = json.Unmarshal(output.Bytes(), &rows); err != nil || len(rows) != 500 || calls != 5 {
		t.Fatalf("recovery rows=%d calls=%d error=%v", len(rows), calls, err)
	}
}

func TestRecoveryRejectsBrokenPaginationWithoutReturningPartialSuccess(t *testing.T) {
	var output bytes.Buffer
	err := collectSandboxes(t.Context(), &output, func(context.Context, string, uint32) (*msb.SandboxPage, error) {
		return &msb.SandboxPage{NextCursor: new("same")}, nil
	})
	if err == nil || output.Len() != 0 {
		t.Fatalf("broken cursor returned partial success: %s %v", output.String(), err)
	}
}
