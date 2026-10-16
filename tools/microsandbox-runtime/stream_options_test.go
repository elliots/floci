package microsandboxruntime

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestInteractiveOptionsReachAttachedExec(t *testing.T) {
	sizes := make(chan TerminalSize)
	want := StreamOptions{TTY: true, Size: TerminalSize{Rows: 24, Cols: 80}, Resize: sizes}
	c := &Client{core: fakeBackend(func(_ context.Context, request command, _ io.Reader, _, _ io.Writer) (int, error) {
		if request.stream != want {
			t.Fatalf("options lost: %#v", request.stream)
		}
		return 17, nil
	})}
	code, err := c.ExecuteStreamWithOptions(t.Context(), []string{"exec", "owned", "--", "sh"}, nil, nil, nil, want)
	if err != nil || code != 17 {
		t.Fatalf("exit %d: %v", code, err)
	}
}

func TestInvalidTerminalOptionsCannotExecute(t *testing.T) {
	c := &Client{core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
		t.Fatal("invalid request executed")
		return 0, nil
	})}
	for _, options := range []StreamOptions{
		{Size: TerminalSize{Rows: 24, Cols: 80}},
		{Resize: make(chan TerminalSize)},
		{TTY: true, Size: TerminalSize{Cols: 80}},
	} {
		if _, err := c.ExecuteStreamWithOptions(t.Context(), []string{"exec", "owned", "--", "sh"}, nil, nil, nil, options); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("accepted %#v: %v", options, err)
		}
	}
	if _, err := c.ExecuteStreamWithOptions(t.Context(), []string{"exec", "--detach", "owned", "--", "sh"}, nil, nil, nil, StreamOptions{TTY: true}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("accepted detached terminal: %v", err)
	}
}

type resizeFunc func(context.Context, uint16, uint16) error

func (f resizeFunc) Resize(ctx context.Context, rows, cols uint16) error { return f(ctx, rows, cols) }

func TestTerminalResizesPreserveRowColumnOrderAndStopOnClose(t *testing.T) {
	want := []TerminalSize{{Rows: 24, Cols: 80}, {Rows: 40, Cols: 120}}
	sizes := make(chan TerminalSize, len(want))
	for _, size := range want {
		sizes <- size
	}
	close(sizes)
	var got []TerminalSize
	err := forwardResizes(t.Context(), resizeFunc(func(_ context.Context, rows, cols uint16) error {
		got = append(got, TerminalSize{Rows: rows, Cols: cols})
		return nil
	}), sizes)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("sizes %#v: %v", got, err)
	}
}

func TestTerminalResizeFailureAndCancellationPropagate(t *testing.T) {
	failure := errors.New("resize unavailable")
	sizes := make(chan TerminalSize, 1)
	sizes <- TerminalSize{Rows: 24, Cols: 80}
	err := forwardResizes(t.Context(), resizeFunc(func(context.Context, uint16, uint16) error { return failure }), sizes)
	if !errors.Is(err, failure) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := forwardResizes(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	sizes <- TerminalSize{Rows: 24}
	if err := forwardResizes(t.Context(), nil, sizes); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
}
