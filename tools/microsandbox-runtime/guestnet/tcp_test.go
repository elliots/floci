package guestnet

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func listener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	return listener
}

func TestForwardDrainsReplyAfterInputEOF(t *testing.T) {
	listener := listener(t)
	server := make(chan error, 1)
	go func() {
		connection, err := listener.Accept()
		if err != nil {
			server <- err
			return
		}
		defer connection.Close()
		connection.SetDeadline(time.Now().Add(3 * time.Second))
		data, err := io.ReadAll(connection)
		if err == nil {
			_, err = connection.Write(append([]byte("response:"), data...))
		}
		server <- err
	}()
	var output bytes.Buffer
	err := Forward(t.Context(), listener.Addr().String(), strings.NewReader("request\x00\n"), &output)
	if err != nil || output.String() != "response:request\x00\n" {
		t.Fatalf("half-close changed bytes: %q %v", output.String(), err)
	}
	if err := <-server; err != nil {
		t.Fatal(err)
	}
}

func TestForwardCancellationClosesNetworkAndOwnedInput(t *testing.T) {
	listener := listener(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	result := make(chan error, 1)
	go func() { result <- Forward(ctx, listener.Addr().String(), input, io.Discard) }()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled forward retained blocked streams")
	}
	assertInputClosed(t, writer)
	connection.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := connection.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("network connection remained open: %v", err)
	}
}

func assertInputClosed(t *testing.T, writer *io.PipeWriter) {
	t.Helper()
	written := make(chan error, 1)
	go func() {
		_, err := writer.Write([]byte("late"))
		written <- err
	}()
	select {
	case err := <-written:
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("owned input remained open: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owned input retained a blocked writer after completion")
	}
}

func TestForwardClosesOwnedInputWhenDialIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input, writer := io.Pipe()
	defer input.Close()
	defer writer.Close()
	err := Forward(ctx, "127.0.0.1:1", input, io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dial cancellation lost: %v", err)
	}
	assertInputClosed(t, writer)
}

func TestForwardRejectsDNSAndInvalidDestinations(t *testing.T) {
	for _, address := range []string{"localhost:80", "127.0.0.1:0", "[::1]:80", "0.0.0.0:80", "224.0.0.1:80", "127.0.0.1:65536"} {
		if err := Forward(t.Context(), address, nil, nil); err == nil {
			t.Fatalf("accepted invalid address: %s", address)
		}
	}
}
