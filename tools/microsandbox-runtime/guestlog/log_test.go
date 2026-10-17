package guestlog

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCapturePreservesPartialBinaryOutputAndTimes(t *testing.T) {
	root := t.TempDir()
	clock := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	input := []byte("first\n \x00\xffpartial")
	if err := Capture(bytes.NewReader(input), root, func() time.Time { clock = clock.Add(time.Millisecond); return clock }); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "application.log"))
	if err != nil || !bytes.Equal(raw, input) {
		t.Fatalf("raw changed: %q %v", raw, err)
	}
	file, err := os.Open(filepath.Join(root, "application.records"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	var got []byte
	var offset int64
	for decoder.More() {
		var record Record
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record.Offset != offset || record.At.IsZero() || !record.LineStart {
			t.Fatalf("invalid metadata: %+v", record)
		}
		got = append(got, record.Data...)
		offset += int64(len(record.Data))
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("records changed output: %q", got)
	}
}

func TestCapturePublishesPartialOutputBeforeEOF(t *testing.T) {
	root := t.TempDir()
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	go func() { done <- Capture(reader, root, time.Now) }()
	if _, err := writer.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		data, _ := os.ReadFile(filepath.Join(root, "application.records"))
		if bytes.Contains(data, []byte("cGFydGlhbA==")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partial output blocked behind newline")
		}
		time.Sleep(time.Millisecond)
	}
	writer.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCaptureRotatesCompleteRecordsAndRunPreservesExit(t *testing.T) {
	root := t.TempDir()
	if err := Capture(strings.NewReader(strings.Repeat(strings.Repeat("x", 511)+"\n", 8192)), root, time.Now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"application.records", "application.records.1", "application.log", "application.log.1"} {
		info, err := os.Stat(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > SegmentBytes+32<<10 {
			t.Fatalf("unbounded log %s: %d", name, info.Size())
		}
	}
	code, err := Run(t.TempDir(), []string{"sh", "-c", "printf stdout; printf stderr >&2; exit 19"})
	if err != nil || code != 19 {
		t.Fatalf("exit status changed: %d %v", code, err)
	}
}
