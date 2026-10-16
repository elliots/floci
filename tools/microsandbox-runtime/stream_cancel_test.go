package microsandboxruntime

import (
	"context"
	"errors"
	"io"
	"testing"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

type canceledExec struct {
	started chan struct{}
	killed  chan struct{}
	events  []*msb.ExecEvent
	killErr error
}

func (e *canceledExec) Recv(ctx context.Context) (*msb.ExecEvent, error) {
	select {
	case e.started <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.killed:
		if len(e.events) == 0 {
			return nil, io.EOF
		}
		event := e.events[0]
		e.events = e.events[1:]
		return event, nil
	}
}

func (e *canceledExec) Kill(context.Context) error {
	if e.killErr != nil {
		return e.killErr
	}
	close(e.killed)
	return nil
}

func TestAttachedCancelKeepsOneReaderUntilActualExit(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []*msb.ExecEvent
		kill   error
		known  bool
	}{
		{"observed", []*msb.ExecEvent{{Kind: msb.ExecEventStdout, Data: []byte("tail")}, {Kind: msb.ExecEventExited, ExitCode: 137}, {Kind: msb.ExecEventDone}}, nil, true},
		{"missing exit", []*msb.ExecEvent{{Kind: msb.ExecEventDone}}, nil, false},
		{"kill failure", nil, errors.New("kill failed"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream := &canceledExec{started: make(chan struct{}, 1), killed: make(chan struct{}), events: test.events, killErr: test.kill}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() { <-stream.started; cancel() }()
			code, err := consumeAttachedEvents(ctx, stream, io.Discard, io.Discard)
			if test.known && (err != nil || code != 137) {
				t.Fatalf("lost observed exit: %d %v", code, err)
			}
			if !test.known && err == nil {
				t.Fatal("invented exit status")
			}
		})
	}
}
