package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guestlog"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
)

func logSnapshot() containerLogSnapshot {
	at := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	return containerLogSnapshot{Version: 1, Records: []guestlog.Record{
		{At: at, Offset: 0, Data: []byte("first\n"), LineStart: true},
		{At: at.Add(time.Second), Offset: 6, Data: []byte("sec"), LineStart: true},
		{At: at.Add(time.Second), Offset: 9, Data: []byte("ond\n")},
		{At: at.Add(2 * time.Second), Offset: 13, Data: []byte("partial"), LineStart: true},
	}}
}
func TestLogFiltersPreserveBytesFragmentsAndOriginalTimestamps(t *testing.T) {
	snapshot := logSnapshot()
	data, end, err := renderContainerLogs(snapshot, api.ContainerLogOpts{}, 0, true)
	if err != nil || string(data) != "first\nsecond\npartial" || end != 20 {
		t.Fatalf("raw bytes changed: %q %d %v", data, end, err)
	}
	data, _, err = renderContainerLogs(snapshot, api.ContainerLogOpts{Timestamps: true, Tail: 2, SinceTime: snapshot.Records[1].At}, 0, true)
	if err != nil || string(data) != "2026-10-17T00:00:01Z second\n2026-10-17T00:00:02Z partial" {
		t.Fatalf("filter/timestamps changed: %q %v", data, err)
	}
	data, _, err = renderContainerLogs(snapshot, api.ContainerLogOpts{Timestamps: true}, 9, false)
	if err != nil || string(data) != "ond\n2026-10-17T00:00:02Z partial" {
		t.Fatalf("follow repeated prefix/bytes: %q %v", data, err)
	}
	if _, _, err := renderContainerLogs(containerLogSnapshot{Raw: []byte("legacy")}, api.ContainerLogOpts{Timestamps: true}, 0, true); err == nil {
		t.Fatal("fabricated timestamps for legacy logs")
	}
	if _, _, err := renderContainerLogs(containerLogSnapshot{Version: 2}, api.ContainerLogOpts{}, 0, true); err == nil {
		t.Fatal("accepted unknown log format")
	}
	if _, _, err := renderContainerLogs(containerLogSnapshot{Raw: make([]byte, 1<<20)}, api.ContainerLogOpts{Follow: true}, 0, true); err == nil {
		t.Fatal("follow accepted legacy capture without reliable rotation offsets")
	}
}

func TestSleepingLogsSupportPreviousAndFollowWithoutWaking(t *testing.T) {
	p, e := probeFixture(t)
	e.state = "SLEEPING"
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", RestartCount: 1, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 2}}}}
	p.pods = map[string]*podEntry{"default/app": e}
	first := logSnapshot()
	first.Attempt = 1
	if err := p.saveLogs(e, logContainer{}, first); err != nil {
		t.Fatal(err)
	}
	before := e.lastActivity
	reader, err := p.GetContainerLogs(t.Context(), "default", "app", "app", api.ContainerLogOpts{Timestamps: true, Tail: 1})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || !bytes.HasSuffix(data, []byte("partial")) || e.state != "SLEEPING" || e.lastActivity != before {
		t.Fatal("sleeping logs woke or changed activity", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream, err := p.GetContainerLogs(ctx, "default", "app", "app", api.ContainerLogOpts{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	initial := make([]byte, 20)
	if _, err := io.ReadFull(stream, initial); err != nil {
		t.Fatal(err)
	}
	e.op.Lock()
	first.Records = append(first.Records, guestlog.Record{At: first.Records[2].At, Offset: 20, Data: []byte(" end\n")})
	if err := p.saveLogs(e, logContainer{}, first); err != nil {
		t.Fatal(err)
	}
	e.op.Unlock()
	next := make([]byte, 5)
	if _, err := io.ReadFull(stream, next); err != nil || string(next) != " end\n" {
		t.Fatalf("follow missing bytes: %q %v", next, err)
	}
	stream.Close()
	if e.state != "SLEEPING" || e.lastActivity != before {
		t.Fatal("follow changed sleeping lifecycle")
	}
}

func TestLogTailAndByteLimitIncludeFinalPartialLine(t *testing.T) {
	for _, test := range []struct {
		input string
		count int
		want  string
	}{{"a\nb\n", 1, "b\n"}, {"a\nb", 1, "b"}, {"\n\n", 1, "\n"}, {"", 1, ""}} {
		if got := string(tailLogLines([]byte(test.input), test.count)); got != test.want {
			t.Fatalf("tail %q = %q", test.input, got)
		}
	}
	p, e := probeFixture(t)
	e.state = "SLEEPING"
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app"}}
	p.pods = map[string]*podEntry{"default/app": e}
	if err := p.saveLogs(e, logContainer{}, logSnapshot()); err != nil {
		t.Fatal(err)
	}
	reader, err := p.GetContainerLogs(t.Context(), "default", "app", "app", api.ContainerLogOpts{LimitBytes: 4, Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || string(data) != "firs" {
		t.Fatal("byte limit did not finish stream", err)
	}
	if _, err := p.GetContainerLogs(t.Context(), "default", "app", "app", api.ContainerLogOpts{Previous: true}); err == nil || !strings.Contains(err.Error(), "previous") {
		t.Fatal("missing previous attempt accepted")
	}
}

func TestFollowCancellationClosesAnUnreadPipeAndExplicitZeroTail(t *testing.T) {
	p, e := probeFixture(t)
	e.state = "SLEEPING"
	e.pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app"}}
	p.pods = map[string]*podEntry{"default/app": e}
	if err := p.saveLogs(e, logContainer{}, logSnapshot()); err != nil {
		t.Fatal(err)
	}
	zero := context.WithValue(t.Context(), logTailZeroKey{}, true)
	reader, err := p.GetContainerLogs(zero, "default", "app", "app", api.ContainerLogOpts{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	reader.Close()
	if err != nil || len(data) != 0 {
		t.Fatal("explicit zero tail included history", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := p.GetContainerLogs(ctx, "default", "app", "app", api.ContainerLogOpts{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(stream); done <- err }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation left log writer blocked")
	}
	stream.Close()
}
