package microsandboxruntime

import (
	"context"
	"errors"
	"io"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

// Keep stream consumption behind a narrow seam so termination and malformed
// event sequences can be tested without booting a VM.
type eventStream interface {
	Recv(context.Context) (*msb.ExecEvent, error)
	Kill(context.Context) error
}

// TerminalSize uses rows then columns, matching the native SDK's Resize API.
type TerminalSize struct {
	Rows uint16 `json:"rows"`
	Cols uint16 `json:"cols"`
}

type StreamOptions struct {
	TTY    bool
	Size   TerminalSize
	Resize <-chan TerminalSize
}

func (o StreamOptions) validate(r command) error {
	if !o.TTY && (o.Size != (TerminalSize{}) || o.Resize != nil) {
		return invalid("terminal dimensions require TTY")
	}
	if o.TTY && (r.op != "exec" || r.detached) {
		return invalid("TTY requires attached exec")
	}
	if (o.Size.Rows == 0) != (o.Size.Cols == 0) {
		return invalid("terminal rows and columns must both be positive")
	}
	return nil
}

type terminalResizer interface {
	Resize(context.Context, uint16, uint16) error
}

func forwardResizes(ctx context.Context, stream terminalResizer, sizes <-chan TerminalSize) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case size, ok := <-sizes:
			if !ok {
				return nil
			}
			if size.Rows == 0 || size.Cols == 0 {
				return invalid("terminal rows and columns must both be positive")
			}
			if err := stream.Resize(ctx, size.Rows, size.Cols); err != nil {
				return err
			}
		}
	}
}

func streamExec(ctx context.Context, sandbox *msb.Sandbox, r command, input io.Reader, stdout, stderr io.Writer) (code int, resultErr error) {
	opts := []msb.ExecOption{msb.WithExecUser(r.user), msb.WithExecTTY(r.stream.TTY)}
	if input != nil {
		opts = append(opts, msb.WithExecStdinPipe())
	}
	stream, err := sandbox.ExecStream(ctx, r.argv[0], r.argv[1:], opts...)
	if err != nil {
		return 0, err
	}
	ctx, cancelCause := context.WithCancelCause(ctx)
	cancel := func() { cancelCause(context.Canceled) }
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, context.Cause(ctx))
		}
		cancel()
		if resultErr != nil {
			// Close only releases the transport. Kill a failed attached session
			// before release, including failures while acquiring its stdin sink.
			killCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			resultErr = errors.Join(resultErr, stream.Kill(killCtx))
			stop()
		}
		resultErr = errors.Join(resultErr, stream.Close())
	}()
	if r.stream.Size != (TerminalSize{}) {
		if err := stream.Resize(ctx, r.stream.Size.Rows, r.stream.Size.Cols); err != nil {
			return 0, err
		}
	}
	if r.stream.Resize != nil {
		go func() {
			if err := forwardResizes(ctx, stream, r.stream.Resize); err != nil {
				cancelCause(err)
			}
		}()
	}
	if input != nil {
		sink := stream.TakeStdin()
		if sink == nil {
			return 0, errors.New("guest stdin pipe unavailable")
		}
		if closer, ok := input.(io.ReadCloser); ok {
			stop := context.AfterFunc(ctx, func() { _ = closer.Close() })
			defer func() {
				cancel()
				if stop() {
					_ = closer.Close()
				}
			}()
		}
		go func() {
			buf := make([]byte, 32<<10)
			var copyErr error
			for {
				n, err := input.Read(buf)
				if n > 0 {
					if _, writeErr := sink.WriteCtx(ctx, buf[:n]); writeErr != nil {
						copyErr = writeErr
						break
					}
				}
				if err != nil {
					if !errors.Is(err, io.EOF) {
						copyErr = err
					}
					break
				}
				if ctx.Err() != nil {
					copyErr = ctx.Err()
					break
				}
			}
			closeErr := sink.Close()
			copyErr = errors.Join(copyErr, closeErr)
			if copyErr != nil {
				cancelCause(copyErr)
			}
		}()
	}
	return consumeAttachedEvents(ctx, stream, stdout, stderr)
}

func consumeAttachedEvents(ctx context.Context, stream eventStream, stdout, stderr io.Writer) (int, error) {
	// A canceled SDK Recv can continue in the FFI and consume the eventual exit
	// event. Keep a single reader alive while cancellation kills this process,
	// then allow a bounded drain to observe its real exit status.
	events, cancelEvents := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelEvents(context.Canceled)
	finished := make(chan struct{})
	defer close(finished)
	stop := context.AfterFunc(ctx, func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := stream.Kill(cleanup); err != nil {
			cancelEvents(err)
			return
		}
		select {
		case <-finished:
		case <-cleanup.Done():
			cancelEvents(errors.New("canceled exec did not report its exit status"))
		}
	})
	defer stop()
	code, err := consumeEvents(events, stream, stdout, stderr, nil)
	if err != nil {
		return 0, errors.Join(err, context.Cause(events))
	}
	return code, nil
}

func consumeEvents(ctx context.Context, stream eventStream, stdout, stderr io.Writer, inputErr <-chan error) (int, error) {
	exited := false
	code := 0
	for {
		select {
		case err := <-inputErr:
			if err != nil {
				return 0, err
			}
			inputErr = nil
		default:
		}
		event, err := stream.Recv(ctx)
		if err != nil {
			return 0, err
		}
		if event == nil {
			return 0, errors.New("guest exec returned an empty event")
		}
		switch event.Kind {
		case msb.ExecEventStdout:
			if err = writeChunk(stdout, event.Data); err != nil {
				return 0, err
			}
		case msb.ExecEventStderr:
			if err = writeChunk(stderr, event.Data); err != nil {
				return 0, err
			}
		case msb.ExecEventExited:
			code = event.ExitCode
			exited = true
		case msb.ExecEventFailed:
			return 0, errors.New("guest command failed to start")
		case msb.ExecEventStdinError:
			return 0, errors.New("guest stdin transport failed")
		case msb.ExecEventDone:
			if !exited {
				return 0, errors.New("guest exec ended without an exit status")
			}
			return code, nil
		}
	}
}

func writeChunk(w io.Writer, data []byte) error {
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		return io.ErrShortWrite
	}
	return err
}
