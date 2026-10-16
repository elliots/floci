package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	microsandboxruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
	"github.com/gorilla/websocket"
)

type streamExecutor interface {
	ExecuteStreamWithOptions(context.Context, []string, io.Reader, io.Writer, io.Writer, microsandboxruntime.StreamOptions) (int, error)
}

type execStart struct {
	Type      string   `json:"type"`
	Arguments []string `json:"arguments"`
	Stdin     bool     `json:"stdin"`
	TTY       bool     `json:"tty"`
	Rows      uint16   `json:"rows"`
	Cols      uint16   `json:"cols"`
}

type execFrame struct {
	Type     string `json:"type"`
	Data     []byte `json:"data,omitempty"`
	Rows     uint16 `json:"rows,omitzero"`
	Cols     uint16 `json:"cols,omitzero"`
	ExitCode *int   `json:"exitCode,omitempty"`
	Error    string `json:"error,omitempty"`
}

type execSocket struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (s *execSocket) write(frame execFrame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	return s.conn.WriteJSON(frame)
}

type socketOutput struct {
	socket *execSocket
	kind   string
}

func (w socketOutput) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		n := min(len(data), 32<<10)
		if err := w.socket.write(execFrame{Type: w.kind, Data: data[:n]}); err != nil {
			return written, err
		}
		written += n
		data = data[n:]
	}
	return written, nil
}

type socketInput struct {
	ctx     context.Context
	chunks  <-chan []byte
	pending []byte
}

func (r *socketInput) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case chunk, ok := <-r.chunks:
			if !ok {
				return 0, io.EOF
			}
			r.pending = chunk
		}
	}
	n := copy(data, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (h *hostHandler) execStream(w http.ResponseWriter, r *http.Request) {
	if !h.authorize(w, r) {
		return
	}
	runtime, ok := h.runtime.(streamExecutor)
	if !ok {
		writeError(w, http.StatusNotImplemented, "Runtime does not support streaming exec")
		return
	}
	upgrader := websocket.Upgrader{ReadBufferSize: 4096, WriteBufferSize: 4096}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	socket := &execSocket{conn: conn}
	conn.SetReadLimit(1 << 20)
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return
	}
	var start execStart
	if err := conn.ReadJSON(&start); err != nil || start.Type != "start" || len(start.Arguments) == 0 || start.Arguments[0] != "exec" {
		_ = socket.write(execFrame{Type: "error", Error: "An attached exec start frame is required"})
		return
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	conn.SetReadLimit(96 << 10)
	ctx, cancel := context.WithTimeout(r.Context(), h.config.timeout)
	defer cancel()
	chunks := make(chan []byte, 4)
	sizes := make(chan microsandboxruntime.TerminalSize, 16)
	go func() {
		defer cancel()
		defer close(sizes)
		if err := readExecFrames(conn, chunks, sizes, start.Stdin, start.TTY); err != nil {
			_ = socket.write(execFrame{Type: "error", Error: "Exec input disconnected or rejected"})
		}
	}()
	var input io.Reader
	if start.Stdin {
		input = &socketInput{ctx: ctx, chunks: chunks}
	}
	options := microsandboxruntime.StreamOptions{TTY: start.TTY, Size: microsandboxruntime.TerminalSize{Rows: start.Rows, Cols: start.Cols}}
	if start.TTY {
		options.Resize = sizes
	}
	code, err := runtime.ExecuteStreamWithOptions(ctx, start.Arguments, input,
		socketOutput{socket, "stdout"}, socketOutput{socket, "stderr"}, options)
	if err != nil {
		_ = socket.write(execFrame{Type: "error", Error: "Attached guest exec failed"})
		return
	}
	_ = socket.write(execFrame{Type: "exit", ExitCode: new(code)})
}

func readExecFrames(conn *websocket.Conn, chunks chan<- []byte, sizes chan<- microsandboxruntime.TerminalSize, stdin, tty bool) error {
	inputClosed := false
	defer func() {
		if !inputClosed {
			close(chunks)
		}
	}()
	for {
		var frame execFrame
		if err := conn.ReadJSON(&frame); err != nil {
			return err
		}
		switch frame.Type {
		case "stdin":
			if !stdin || inputClosed || len(frame.Data) > 64<<10 {
				return errors.New("invalid stdin frame")
			}
			select {
			case chunks <- frame.Data:
			default:
				return errors.New("stdin backlog exceeded")
			}
		case "eof":
			if !stdin || inputClosed {
				return errors.New("invalid stdin EOF")
			}
			close(chunks)
			inputClosed = true
		case "resize":
			if !tty || frame.Rows == 0 || frame.Cols == 0 {
				return errors.New("invalid terminal resize")
			}
			select {
			case sizes <- microsandboxruntime.TerminalSize{Rows: frame.Rows, Cols: frame.Cols}:
			default:
				return errors.New("resize backlog exceeded")
			}
		case "cancel":
			return nil
		default:
			return errors.New("unknown exec frame")
		}
	}
}
