package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	microsandboxruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	"github.com/gorilla/websocket"
)

type streamingRuntime struct {
	executor
	stream func(context.Context, []string, io.Reader, io.Writer, io.Writer, microsandboxruntime.StreamOptions) (int, error)
}

func (f streamingRuntime) ExecuteStreamWithOptions(ctx context.Context, args []string, in io.Reader, out, stderr io.Writer, options microsandboxruntime.StreamOptions) (int, error) {
	return f.stream(ctx, args, in, out, stderr, options)
}

func openExec(t *testing.T, runtime streamingRuntime) *websocket.Conn {
	t.Helper()
	config := testConfig()
	config.timeout = 5 * time.Second
	server := httptest.NewServer(newHandler(config, runtime))
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/exec", http.Header{"Authorization": {"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestExecStreamsBinaryDataResizeEOFAndNonzeroExit(t *testing.T) {
	failures := make(chan error, 1)
	conn := openExec(t, streamingRuntime{stream: func(ctx context.Context, args []string, input io.Reader, stdout, stderr io.Writer, options microsandboxruntime.StreamOptions) (int, error) {
		if !options.TTY || options.Size != (microsandboxruntime.TerminalSize{Rows: 24, Cols: 80}) {
			failures <- errors.New("TTY options lost")
			return 0, errors.New("invalid")
		}
		select {
		case size := <-options.Resize:
			if size != (microsandboxruntime.TerminalSize{Rows: 40, Cols: 120}) {
				failures <- errors.New("resize lost")
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		data, err := io.ReadAll(input)
		if err != nil {
			return 0, err
		}
		if _, err := stdout.Write(data); err != nil {
			return 0, err
		}
		if _, err := io.WriteString(stderr, "separate-error"); err != nil {
			return 0, err
		}
		return 23, nil
	}})
	for _, frame := range []any{
		execStart{Type: "start", Arguments: []string{"exec", "vm", "--", "cat"}, Stdin: true, TTY: true, Rows: 24, Cols: 80},
		execFrame{Type: "resize", Rows: 40, Cols: 120},
		execFrame{Type: "stdin", Data: []byte{0, 1, 255}},
		execFrame{Type: "eof"},
	} {
		if err := conn.WriteJSON(frame); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr []byte
	for {
		var frame execFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatal(err)
		}
		switch frame.Type {
		case "stdout":
			stdout = append(stdout, frame.Data...)
		case "stderr":
			stderr = append(stderr, frame.Data...)
		case "exit":
			if frame.ExitCode == nil || *frame.ExitCode != 23 || string(stdout) != string([]byte{0, 1, 255}) || string(stderr) != "separate-error" {
				t.Fatalf("lost result: %#v", frame)
			}
			select {
			case err := <-failures:
				t.Fatal(err)
			default:
			}
			return
		default:
			t.Fatalf("unexpected frame: %#v", frame)
		}
	}
}

func TestExecDisconnectCancelsOnlyTheAttachedExecution(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	conn := openExec(t, streamingRuntime{executor: unexpectedExecutor(t), stream: func(ctx context.Context, _ []string, _ io.Reader, _, _ io.Writer, _ microsandboxruntime.StreamOptions) (int, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return 0, ctx.Err()
	}})
	if err := conn.WriteJSON(execStart{Type: "start", Arguments: []string{"exec", "vm", "--", "sh"}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("exec did not start")
	}
	_ = conn.Close()
	select {
	case <-canceled:
	case <-time.After(3 * time.Second):
		t.Fatal("disconnect did not cancel")
	}
}

func TestExecFailureDoesNotExposeArgumentsOrInventExitCode(t *testing.T) {
	conn := openExec(t, streamingRuntime{stream: func(context.Context, []string, io.Reader, io.Writer, io.Writer, microsandboxruntime.StreamOptions) (int, error) {
		return 0, errors.New("secret-password")
	}})
	if err := conn.WriteJSON(execStart{Type: "start", Arguments: []string{"exec", "vm", "--", "sh"}}); err != nil {
		t.Fatal(err)
	}
	var frame execFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatal(err)
	}
	if frame.Type != "error" || frame.ExitCode != nil || strings.Contains(frame.Error, "secret") {
		t.Fatalf("unsafe failure: %#v", frame)
	}
}

func TestExecUpgradeRequiresAuthorization(t *testing.T) {
	w := request(newHandler(testConfig(), streamingRuntime{}), http.MethodGet, "/v1/exec", "", "wrong-token")
	if w.Code != http.StatusUnauthorized {
		t.Fatal(w.Code)
	}
}
