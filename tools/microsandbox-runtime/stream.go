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

func streamExec(ctx context.Context, sandbox *msb.Sandbox, r command, input io.Reader, stdout, stderr io.Writer) (code int, resultErr error) {
	opts := []msb.ExecOption{msb.WithExecUser(r.user), msb.WithExecTTY(false)}
	if input != nil {
		opts = append(opts, msb.WithExecStdinPipe())
	}
	stream, err := sandbox.ExecStream(ctx, r.argv[0], r.argv[1:], opts...)
	if err != nil {
		return 0, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
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
	var inputErr <-chan error
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
		finished := make(chan error, 1)
		inputErr = finished
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
			finished <- copyErr
			if copyErr != nil {
				cancel()
			}
		}()
	}
	return consumeEvents(ctx, stream, stdout, stderr, inputErr)
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
