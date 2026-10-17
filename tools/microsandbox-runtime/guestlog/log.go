// Package guestlog captures container output with event timestamps and bounded
// storage. Its process belongs to the VM supervisor tree and is checkpointed
// with the container, so log offsets and partial lines survive RAM resume.
package guestlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const SegmentBytes = 1 << 20

type Record struct {
	At        time.Time `json:"at"`
	Offset    int64     `json:"offset"`
	Data      []byte    `json:"data"`
	LineStart bool      `json:"lineStart,omitzero"`
}

func Run(root string, args []string) (int, error) {
	if !filepath.IsAbs(root) || len(args) == 0 {
		return 0, errors.New("log capture requires an absolute root and a command")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return 0, err
	}
	output, input, err := os.Pipe()
	if err != nil {
		return 0, err
	}
	defer output.Close()
	defer input.Close()
	command := exec.Command(args[0], args[1:]...)
	command.Stdin, command.Stdout, command.Stderr = nil, input, input
	if err := command.Start(); err != nil {
		return 0, err
	}
	input.Close()
	captureErr := Capture(output, root, time.Now)
	if captureErr != nil {
		_ = command.Process.Kill()
	}
	waitErr := command.Wait()
	if captureErr != nil {
		return 0, captureErr
	}
	if exit, ok := errors.AsType[*exec.ExitError](waitErr); ok {
		return exit.ExitCode(), nil
	}
	return 0, waitErr
}

func Capture(input io.Reader, root string, now func() time.Time) error {
	records := &rotatingWriter{path: filepath.Join(root, "application.records")}
	raw := &rotatingWriter{path: filepath.Join(root, "application.log")}
	defer records.close()
	defer raw.close()
	buffer := make([]byte, 16<<10)
	offset, lineStart := int64(0), true
	for {
		n, readErr := input.Read(buffer)
		data := buffer[:n]
		for len(data) > 0 {
			size := len(data)
			if newline := bytes.IndexByte(data, '\n'); newline >= 0 {
				size = newline + 1
			}
			chunk := data[:size]
			record := Record{At: now().UTC(), Offset: offset, Data: chunk, LineStart: lineStart}
			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if _, err := records.Write(append(encoded, '\n')); err != nil {
				return err
			}
			if _, err := raw.Write(chunk); err != nil {
				return err
			}
			offset += int64(len(chunk))
			lineStart = chunk[len(chunk)-1] == '\n'
			data = data[size:]
		}
		if errors.Is(readErr, io.EOF) {
			return errors.Join(records.sync(), raw.sync())
		}
		if readErr != nil {
			return readErr
		}
	}
}

type rotatingWriter struct {
	path string
	file *os.File
	size int
}

func (w *rotatingWriter) Write(data []byte) (int, error) {
	if w.file != nil && w.size+len(data) > SegmentBytes {
		if err := w.close(); err != nil {
			return 0, err
		}
		if err := os.Rename(w.path, w.path+".1"); err != nil {
			return 0, err
		}
	}
	if w.file == nil {
		file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
		if err != nil {
			return 0, err
		}
		w.file, w.size = file, 0
	}
	n, err := w.file.Write(data)
	w.size += n
	if n != len(data) && err == nil {
		err = fmt.Errorf("short container log write")
	}
	return n, err
}
func (w *rotatingWriter) sync() error {
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}
func (w *rotatingWriter) close() error {
	if w.file == nil {
		return nil
	}
	err := errors.Join(w.file.Sync(), w.file.Close())
	w.file = nil
	return err
}
