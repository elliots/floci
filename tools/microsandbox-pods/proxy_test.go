package main

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func testWakeProxy(t *testing.T, maxConnections int, startupTimeout time.Duration) *wakeProxy {
	t.Helper()
	proxy, err := newWakeProxy(t.Context(), maxConnections, startupTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := proxy.close(ctx); err != nil {
			t.Error(err)
		}
	})
	return proxy
}

func testTCPListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func connectWakeProxy(t *testing.T, port int) *net.TCPConn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return connection.(*net.TCPConn)
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
	}
}

func TestWakeProxyWaitsForActualReadinessAndPreservesWriteHalfClose(t *testing.T) {
	backend := testTCPListener(t)
	accepted := make(chan struct{})
	backendResult := make(chan error, 1)
	go func() {
		connection, err := backend.Accept()
		if err != nil {
			backendResult <- err
			return
		}
		defer connection.Close()
		close(accepted)
		request, err := io.ReadAll(connection)
		if err == nil {
			_, err = connection.Write(append([]byte("reply:"), request...))
		}
		backendResult <- err
	}()
	proxy := testWakeProxy(t, 2, time.Second)
	waking, ready, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	port, err := proxy.listen("pod-a", 8080, "127.0.0.1", 0, func(ctx context.Context) (string, func(), error) {
		close(waking)
		select {
		case <-ready:
			return backend.Addr().String(), func() { close(released) }, nil
		case <-ctx.Done():
			return "", nil, ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	if _, err := client.Write([]byte("first request")); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, waking)
	select {
	case <-accepted:
		t.Fatal("backend was contacted before application readiness")
	case <-time.After(30 * time.Millisecond):
	}
	close(ready)
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "reply:first request" {
		t.Fatalf("response %q, error %v", response, err)
	}
	awaitSignal(t, released)
	if err := <-backendResult; err != nil {
		t.Fatal(err)
	}
}

func TestWakeProxyBoundsPendingConnectionsAcrossPods(t *testing.T) {
	proxy := testWakeProxy(t, 1, time.Minute)
	entered, released := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	acquire := func(ctx context.Context) (string, func(), error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-ctx.Done()
		return "", func() { close(released) }, ctx.Err()
	}
	first, err := proxy.listen("pod-a", 8080, "127.0.0.1", 0, acquire)
	if err != nil {
		t.Fatal(err)
	}
	second, err := proxy.listen("pod-b", 8080, "127.0.0.1", 0, acquire)
	if err != nil {
		t.Fatal(err)
	}
	firstClient := connectWakeProxy(t, first)
	awaitSignal(t, entered)
	secondClient := connectWakeProxy(t, second)
	if bytes, err := io.ReadAll(secondClient); err != nil || len(bytes) != 0 {
		t.Fatalf("excess connection was not closed: %q, %v", bytes, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("capacity allowed %d activation calls", calls.Load())
	}
	if err := proxy.closePod(t.Context(), "pod-a"); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, released)
	if bytes, err := io.ReadAll(firstClient); err != nil || len(bytes) != 0 {
		t.Fatalf("deleted pod connection remained open: %q, %v", bytes, err)
	}
	if proxy.ready("pod-a") || !proxy.ready("pod-b") {
		t.Fatal("deletion changed another Pod UID's listener")
	}
}

func TestWakeProxyStartupFailureReleasesLeaseAndDoesNotInventResponse(t *testing.T) {
	proxy := testWakeProxy(t, 1, time.Second)
	released := make(chan struct{})
	port, err := proxy.listen("pod-a", 443, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		return "must-not-dial:1", func() { close(released) }, errors.New("application failed readiness")
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	response, err := io.ReadAll(client)
	if err != nil || len(response) != 0 {
		t.Fatalf("proxy invented a protocol response: %q, %v", response, err)
	}
	awaitSignal(t, released)
}

func TestWakeProxyStartupDeadlineCancelsAdmission(t *testing.T) {
	proxy := testWakeProxy(t, 1, 40*time.Millisecond)
	canceled := make(chan struct{})
	port, err := proxy.listen("pod-a", 8080, "127.0.0.1", 0, func(ctx context.Context) (string, func(), error) {
		<-ctx.Done()
		close(canceled)
		return "", nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	awaitSignal(t, canceled)
	if bytes, err := io.ReadAll(client); err != nil || len(bytes) != 0 {
		t.Fatalf("timed out connection remained open: %q, %v", bytes, err)
	}
}

func TestWakeProxyDeletionClosesActiveStreamsAndReleasesActivity(t *testing.T) {
	backend := testTCPListener(t)
	backendClosed := make(chan struct{})
	go func() {
		connection, err := backend.Accept()
		if err != nil {
			close(backendClosed)
			return
		}
		defer connection.Close()
		defer close(backendClosed)
		_, _ = io.Copy(connection, connection)
	}()
	proxy := testWakeProxy(t, 1, time.Second)
	released := make(chan struct{})
	port, err := proxy.listen("pod-a", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		return backend.Addr().String(), func() { close(released) }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	if _, err := client.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, 4)
	if _, err := io.ReadFull(client, response); err != nil || string(response) != "ping" {
		t.Fatalf("stream failed: %q, %v", response, err)
	}
	select {
	case <-released:
		t.Fatal("activity lease ended while the stream remained open")
	default:
	}
	if err := proxy.closePod(t.Context(), "pod-a"); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, released)
	awaitSignal(t, backendClosed)
}

func TestWakeProxyListenerOwnershipAndShutdown(t *testing.T) {
	proxy := testWakeProxy(t, 2, time.Second)
	acquire := func(context.Context) (string, func(), error) { return "", nil, errors.New("not started") }
	if _, err := proxy.listen("pod-a", 8080, "127.0.0.1", 0, acquire); err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.listen("pod-a", 8080, "127.0.0.1", 0, acquire); err == nil {
		t.Fatal("duplicate Pod UID and container port was accepted")
	}
	if err := proxy.closePod(t.Context(), "stale-pod-uid"); err != nil || !proxy.ready("pod-a") {
		t.Fatalf("stale deletion changed a live owner: %v", err)
	}
	if err := proxy.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if proxy.ready("pod-a") {
		t.Fatal("stopped listener reported ready")
	}
	if _, err := proxy.listen("pod-b", 8080, "127.0.0.1", 0, acquire); err == nil {
		t.Fatal("shutdown accepted another listener")
	}
}
