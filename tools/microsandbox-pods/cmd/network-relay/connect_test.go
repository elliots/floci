package main

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStdioConnectorPreservesHalfCloseAndFinalReply(t *testing.T) {
	listener := tcpListener(t)
	finished := make(chan error, 1)
	go func() {
		connection, err := listener.AcceptTCP()
		if err != nil {
			finished <- err
			return
		}
		defer connection.Close()
		connection.SetDeadline(time.Now().Add(3 * time.Second))
		data, err := io.ReadAll(connection)
		if err == nil {
			_, err = connection.Write(append([]byte("reply after EOF: "), data...))
		}
		finished <- err
	}()
	var output bytes.Buffer
	err := run([]string{"connect", "--address", "127.0.0.1", "--port", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)}, strings.NewReader("literal request\x00\n"), &output)
	if err != nil || output.String() != "reply after EOF: literal request\x00\n" {
		t.Fatalf("forwarded bytes or TCP half-close changed: %q %v", output.String(), err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestStdioConnectorClosesBlockedInputWhenServerFinishes(t *testing.T) {
	listener := tcpListener(t)
	input, writer := io.Pipe()
	defer writer.Close()
	go func() {
		connection, err := listener.AcceptTCP()
		if err == nil {
			connection.Close()
		}
	}()
	finished := make(chan error, 1)
	go func() {
		finished <- connectStdio([]string{"--address", "127.0.0.1", "--port", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)}, input, io.Discard)
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("upstream EOF left the connector waiting for stdin")
	}
	if _, err := writer.Write([]byte("late input")); err != io.ErrClosedPipe {
		t.Fatalf("connector did not close its input: %v", err)
	}
}

func TestStdioConnectorRejectsAmbiguousDestinations(t *testing.T) {
	for _, args := range [][]string{
		{"--address", "localhost", "--port", "80"},
		{"--address", "0.0.0.0", "--port", "80"},
		{"--address", "127.0.0.1", "--port", "0"},
		{"--address", "127.0.0.1", "--port", "65536"},
		{"--address", "127.0.0.1", "--port", "80", "unexpected"},
	} {
		if err := connectStdio(args, strings.NewReader(""), io.Discard); err == nil {
			t.Fatalf("invalid destination accepted: %v", args)
		}
	}
}
