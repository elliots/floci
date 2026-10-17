package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testHTTPBackend(t *testing.T, handler http.HandlerFunc, configure ...func(*http.Server)) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	for _, apply := range configure {
		apply(server.Config)
	}
	server.Start()
	t.Cleanup(server.Close)
	return server
}

func testHTTPProxyClient(t *testing.T, http2 bool) *http.Client {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetHTTP1(!http2)
	protocols.SetUnencryptedHTTP2(http2)
	transport := &http.Transport{Protocols: protocols}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 3 * time.Second}
}

func proxyTestURL(port int) string {
	return "http://127.0.0.1:" + strconv.Itoa(port)
}

func TestWakeHTTPIdleKeepAliveReacquiresBackendAndPreservesRequest(t *testing.T) {
	var address atomic.Value
	var acquired atomic.Int32
	released := make(chan struct{}, 2)
	backendClosed := make(chan struct{}, 1)
	requestObserved := make(chan string, 2)
	first := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		requestObserved <- r.Host + " " + r.RequestURI
		w.Write([]byte("first"))
	}, func(server *http.Server) {
		server.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				backendClosed <- struct{}{}
			}
		}
	})
	second := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		requestObserved <- r.Host + " " + r.RequestURI
		w.Write([]byte("second"))
	})
	address.Store(first.Listener.Addr().String())
	proxy := testWakeProxy(t, 1, time.Second)
	port, err := proxy.listenHTTP("http-pod", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		acquired.Add(1)
		return address.Load().(string), func() { released <- struct{}{} }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	time.Sleep(20 * time.Millisecond)
	if acquired.Load() != 0 {
		t.Fatal("an idle client connection acquired the application")
	}
	reader := bufio.NewReader(client)
	for i, want := range []string{"first", "second"} {
		if i == 1 {
			address.Store(second.Listener.Addr().String())
		}
		uri := "/a%2fb//c?x=one;two&signed=%2f%2F&bad=%zz"
		if _, err := fmt.Fprintf(client, "GET %s HTTP/1.1\r\nHost: original.example:8080\r\n\r\n", uri); err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(body) != want || response.Close {
			t.Fatalf("keepalive response: %q, close=%t, error=%v", body, response.Close, err)
		}
		if got := <-requestObserved; got != "original.example:8080 "+uri {
			t.Fatalf("request changed: %q", got)
		}
		awaitSignal(t, released)
		if i == 0 {
			awaitSignal(t, backendClosed)
		}
	}
	if acquired.Load() != 2 || !proxy.ready("http-pod") || len(proxy.slots) != 0 {
		t.Fatal("requests did not independently acquire and release the live listener")
	}
}

func TestWakeHTTPPreservesExistingForwardingHeadersWithoutInventingAHop(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%t", http2), func(t *testing.T) {
			observed := make(chan http.Header, 2)
			backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
				observed <- r.Header.Clone()
				w.WriteHeader(http.StatusNoContent)
			})
			proxy := testWakeProxy(t, 2, time.Second)
			port, err := proxy.listenHTTP("forwarded", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
				return backend.Listener.Addr().String(), func() {}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			client := testHTTPProxyClient(t, http2)
			forwarding := http.Header{
				"Forwarded":         {"for=192.0.2.10;proto=https;host=external.example", "for=198.51.100.20"},
				"X-Forwarded-For":   {"192.0.2.10, 198.51.100.20", "203.0.113.30"},
				"X-Forwarded-Host":  {"external.example:443"},
				"X-Forwarded-Proto": {"https"},
			}
			for _, headers := range []http.Header{forwarding, {}} {
				request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxyTestURL(port), nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header = headers.Clone()
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != http.StatusNoContent {
					t.Fatalf("request failed: %d", response.StatusCode)
				}
				actual := <-observed
				for name := range forwarding {
					if !slices.Equal(actual.Values(name), headers.Values(name)) {
						t.Errorf("%s changed: got %q, want %q", name, actual.Values(name), headers.Values(name))
					}
				}
			}
		})
	}
}

func TestWakeHTTPDoesNotRestoreHopByHopForwardingHeaders(t *testing.T) {
	observed := make(chan http.Header, 1)
	backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Header.Clone()
		w.WriteHeader(http.StatusNoContent)
	})
	proxy := testWakeProxy(t, 1, time.Second)
	port, err := proxy.listenHTTP("hop-headers", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		return backend.Listener.Addr().String(), func() {}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, proxyTestURL(port), nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header = http.Header{
		"Connection":        {"keep-alive, FoRwArDeD", "X-FORWARDED-FOR, X-Other-Hop"},
		"Forwarded":         {"for=192.0.2.10"},
		"X-Forwarded-For":   {"192.0.2.10"},
		"X-Forwarded-Host":  {"external.example:443"},
		"X-Forwarded-Proto": {"https"},
		"X-Other-Hop":       {"private-hop-value"},
	}
	response, err := testHTTPProxyClient(t, false).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("request failed: %d", response.StatusCode)
	}
	actual := <-observed
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Other-Hop"} {
		if actual.Get(name) != "" {
			t.Errorf("hop-by-hop %s was restored: %q", name, actual.Values(name))
		}
	}
	for _, name := range []string{"X-Forwarded-Host", "X-Forwarded-Proto"} {
		if !slices.Equal(actual.Values(name), request.Header.Values(name)) {
			t.Errorf("end-to-end %s changed: %q", name, actual.Values(name))
		}
	}
}

func TestWakeHTTPAsteriskOptionsAcquiresAndForwardsToApplication(t *testing.T) {
	backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodOptions || r.RequestURI != "*" || r.Host != "application.example" {
			t.Errorf("OPTIONS request changed: %s %s Host=%s", r.Method, r.RequestURI, r.Host)
		}
		w.Header().Set("X-Application-Options", "handled")
		w.WriteHeader(http.StatusAccepted)
	}, func(server *http.Server) {
		server.DisableGeneralOptionsHandler = true
	})
	proxy := testWakeProxy(t, 1, time.Second)
	var acquired atomic.Int32
	released := make(chan struct{})
	port, err := proxy.listenHTTP("options", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		acquired.Add(1)
		return backend.Listener.Addr().String(), func() { close(released) }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	fmt.Fprint(client, "OPTIONS * HTTP/1.1\r\nHost: application.example\r\n\r\n")
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || response.Header.Get("X-Application-Options") != "handled" || acquired.Load() != 1 {
		t.Fatalf("OPTIONS bypassed application activation: status=%d headers=%v acquired=%d", response.StatusCode, response.Header, acquired.Load())
	}
	awaitSignal(t, released)
}

func TestWakeHTTPStreamingResponseHoldsLeaseAndForwardsTrailers(t *testing.T) {
	finish := make(chan struct{})
	backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Trailer", "X-Result")
		w.Write([]byte("first\n"))
		w.(http.Flusher).Flush()
		select {
		case <-finish:
			w.Write([]byte("last\n"))
			w.Header().Set("X-Result", "complete")
		case <-r.Context().Done():
		}
	})
	proxy := testWakeProxy(t, 1, time.Second)
	released := make(chan struct{})
	port, err := proxy.listenHTTP("stream", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		return backend.Listener.Addr().String(), func() { close(released) }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := testHTTPProxyClient(t, false).Get(proxyTestURL(port))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	if line, err := reader.ReadString('\n'); err != nil || line != "first\n" {
		t.Fatalf("stream did not flush: %q, %v", line, err)
	}
	select {
	case <-released:
		t.Fatal("streaming response released its lease before completion")
	default:
	}
	close(finish)
	body, err := io.ReadAll(reader)
	if err != nil || string(body) != "last\n" || response.Trailer.Get("X-Result") != "complete" {
		t.Fatalf("stream end %q, trailer %v, error %v", body, response.Trailer, err)
	}
	awaitSignal(t, released)
}

func TestWakeHTTPStreamsRequestBodyAndRequestTrailers(t *testing.T) {
	for _, http2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%t", http2), func(t *testing.T) {
			received := make(chan string, 1)
			backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor == 1 {
					if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
						t.Error(err)
						return
					}
				}
				first := make([]byte, 4)
				if _, err := io.ReadFull(r.Body, first); err != nil {
					t.Error(err)
					return
				}
				w.Write(append(first, '\n'))
				w.(http.Flusher).Flush()
				rest, err := io.ReadAll(r.Body)
				if err != nil {
					return
				}
				received <- string(rest) + ":" + r.Trailer.Get("X-Upload-Result")
				w.Write([]byte("complete"))
			})
			proxy := testWakeProxy(t, 1, 40*time.Millisecond)
			released := make(chan struct{})
			port, err := proxy.listenHTTP("upload", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
				return backend.Listener.Addr().String(), func() { close(released) }, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			input, output := io.Pipe()
			defer output.Close()
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, proxyTestURL(port), input)
			if err != nil {
				t.Fatal(err)
			}
			request.Trailer = http.Header{"X-Upload-Result": nil}
			responseReady := make(chan *http.Response, 1)
			errors := make(chan error, 1)
			client := testHTTPProxyClient(t, http2)
			go func() {
				response, err := client.Do(request)
				if err != nil {
					errors <- err
					return
				}
				responseReady <- response
			}()
			if _, err := output.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			var response *http.Response
			select {
			case response = <-responseReady:
			case err := <-errors:
				t.Fatal(err)
			case <-time.After(3 * time.Second):
				t.Fatal("response waited for the request body to end")
			}
			defer response.Body.Close()
			reader := bufio.NewReader(response.Body)
			if line, err := reader.ReadString('\n'); err != nil || line != "ping\n" {
				t.Fatalf("streamed upload response %q, %v", line, err)
			}
			time.Sleep(60 * time.Millisecond)
			select {
			case <-released:
				t.Fatal("upload lost its lease or inherited the startup deadline")
			default:
			}
			request.Trailer.Set("X-Upload-Result", "complete")
			output.Write([]byte("end"))
			output.Close()
			if body, err := io.ReadAll(reader); err != nil || string(body) != "complete" {
				t.Fatalf("upload response %q, %v", body, err)
			}
			if got := <-received; got != "end:complete" {
				t.Fatalf("request body/trailer changed: %q", got)
			}
			awaitSignal(t, released)
		})
	}
}

func TestWakeHTTP2ConcurrentRequestsCancellationAndGrpcTrailers(t *testing.T) {
	firstDone := make(chan struct{})
	secondCanceled := make(chan struct{})
	backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("backend protocol changed to %s", r.Proto)
		}
		w.Header().Set("Content-Type", "application/grpc")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
		w.Write([]byte{0, 0, 0, 0, 1, 'a'})
		w.(http.Flusher).Flush()
		switch r.URL.Path {
		case "/first":
			select {
			case <-firstDone:
			case <-r.Context().Done():
			}
		case "/second":
			<-r.Context().Done()
			close(secondCanceled)
		}
		w.Header().Set("Grpc-Status", "0")
		w.Header().Set("Grpc-Message", "done")
	})
	proxy := testWakeProxy(t, 2, time.Second)
	var acquired atomic.Int32
	released := make(chan struct{}, 3)
	port, err := proxy.listenHTTP("grpc", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		acquired.Add(1)
		return backend.Listener.Addr().String(), func() { released <- struct{}{} }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := testHTTPProxyClient(t, true)
	connections := make(chan net.Conn, 4)
	request := func(ctx context.Context, path string) *http.Response {
		t.Helper()
		ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { connections <- info.Conn }})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyTestURL(port)+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/grpc")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	first := request(t.Context(), "/first")
	defer first.Body.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	second := request(ctx, "/second")
	defer second.Body.Close()
	if first.ProtoMajor != 2 || second.ProtoMajor != 2 || <-connections != <-connections {
		t.Fatal("requests did not share a cleartext HTTP2 connection")
	}
	excess := request(t.Context(), "/excess")
	excess.Body.Close()
	if excess.StatusCode != http.StatusOK || excess.Header.Get("Grpc-Status") != "14" || acquired.Load() != 2 {
		t.Fatalf("excess stream was not bounded with gRPC unavailable: %v", excess.Header)
	}
	select {
	case <-released:
		t.Fatal("an active HTTP2 stream lost its request lease")
	default:
	}
	cancel()
	awaitSignal(t, secondCanceled)
	awaitSignal(t, released)
	close(firstDone)
	body, err := io.ReadAll(first.Body)
	if err != nil || string(body) != "\x00\x00\x00\x00\x01a" || first.Trailer.Get("Grpc-Status") != "0" || first.Trailer.Get("Grpc-Message") != "done" {
		t.Fatalf("gRPC body %q, trailers %v, error %v", body, first.Trailer, err)
	}
	awaitSignal(t, released)
	next := request(t.Context(), "/next")
	io.Copy(io.Discard, next.Body)
	next.Body.Close()
	awaitSignal(t, released)
	if acquired.Load() != 3 || len(proxy.slots) != 0 {
		t.Fatal("canceled HTTP2 stream poisoned the connection or retained capacity")
	}
}

func TestWakeHTTPWebSocketLeaseEndsOnPodDeletion(t *testing.T) {
	backendClosed := make(chan struct{})
	backend := testHTTPBackend(t, func(w http.ResponseWriter, r *http.Request) {
		connection, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		defer close(backendClosed)
		fmt.Fprint(buffered, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		buffered.Flush()
		io.Copy(connection, buffered)
	})
	proxy := testWakeProxy(t, 1, time.Second)
	released := make(chan struct{})
	port, err := proxy.listenHTTP("websocket", 8080, "127.0.0.1", 0, func(context.Context) (string, func(), error) {
		return backend.Listener.Addr().String(), func() { close(released) }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	fmt.Fprint(client, "GET /socket HTTP/1.1\r\nHost: socket.example\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(client)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade failed: %v, %v", response, err)
	}
	client.Write([]byte("ping"))
	data := make([]byte, 4)
	if _, err := io.ReadFull(reader, data); err != nil || string(data) != "ping" {
		t.Fatalf("upgraded stream failed: %q, %v", data, err)
	}
	select {
	case <-released:
		t.Fatal("upgraded stream lost its lease")
	default:
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := proxy.closePod(ctx, "websocket"); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, released)
	awaitSignal(t, backendClosed)
	if proxy.ready("websocket") || len(proxy.slots) != 0 {
		t.Fatal("deleted HTTP route retained readiness or a request slot")
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("deleted upgraded connection remained open: %v", err)
	}
}

func TestWakeHTTPDeletionCancelsPendingAcquisition(t *testing.T) {
	proxy := testWakeProxy(t, 1, time.Minute)
	entered, released := make(chan struct{}), make(chan struct{})
	port, err := proxy.listenHTTP("pending", 8080, "127.0.0.1", 0, func(ctx context.Context) (string, func(), error) {
		close(entered)
		<-ctx.Done()
		return "", func() { close(released) }, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	client := connectWakeProxy(t, port)
	fmt.Fprint(client, "GET / HTTP/1.1\r\nHost: app\r\n\r\n")
	awaitSignal(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := proxy.closePod(ctx, "pending"); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, released)
	if len(proxy.slots) != 0 {
		t.Fatal("canceled acquisition retained a request slot")
	}
}

func TestWakeHTTPAndRawTCPShareCapacityAndListenerOwnership(t *testing.T) {
	proxy := testWakeProxy(t, 1, time.Minute)
	entered := make(chan struct{})
	raw, err := proxy.listen("raw", 443, "127.0.0.1", 0, func(ctx context.Context) (string, func(), error) {
		close(entered)
		<-ctx.Done()
		return "", nil, ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	var called atomic.Bool
	acquire := func(context.Context) (string, func(), error) {
		called.Store(true)
		return "", nil, errors.New("not running")
	}
	port, err := proxy.listenHTTP("http", 8080, "127.0.0.1", 0, acquire)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.listen("http", 8080, "127.0.0.1", 0, acquire); err == nil {
		t.Fatal("raw listener replaced an HTTP listener with the same owner")
	}
	connectWakeProxy(t, raw)
	awaitSignal(t, entered)
	response, err := testHTTPProxyClient(t, false).Get(proxyTestURL(port))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || called.Load() {
		t.Fatal("HTTP acquisition bypassed raw TCP's shared capacity")
	}
	if err := proxy.closePod(t.Context(), "raw"); err != nil || !proxy.ready("http") {
		t.Fatalf("raw deletion affected HTTP listener: %v", err)
	}
}

func TestWakeHTTPBoundsIdleConnectionsAcrossPodsWithoutAcquiring(t *testing.T) {
	proxy := testWakeProxy(t, 1, time.Second)
	var acquired atomic.Int32
	acquire := func(context.Context) (string, func(), error) {
		acquired.Add(1)
		return "", nil, errors.New("not running")
	}
	first, err := proxy.listenHTTP("first", 8080, "127.0.0.1", 0, acquire)
	if err != nil {
		t.Fatal(err)
	}
	second, err := proxy.listenHTTP("second", 8080, "127.0.0.1", 0, acquire)
	if err != nil {
		t.Fatal(err)
	}
	connectWakeProxy(t, first)
	deadline := time.Now().Add(time.Second)
	for len(proxy.httpConnections) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(proxy.httpConnections) != 1 {
		t.Fatal("idle client was not accounted")
	}
	excess := connectWakeProxy(t, second)
	if data, err := io.ReadAll(excess); err != nil || len(data) != 0 {
		t.Fatalf("excess idle connection was not closed: %q, %v", data, err)
	}
	if acquired.Load() != 0 || len(proxy.slots) != 0 || !proxy.ready("first") || !proxy.ready("second") {
		t.Fatal("idle HTTP connection acquired a guest or changed listener availability")
	}
	if err := proxy.closePod(t.Context(), "first"); err != nil {
		t.Fatal(err)
	}
	if len(proxy.httpConnections) != 0 {
		t.Fatal("deletion retained an idle connection slot")
	}
	response, err := testHTTPProxyClient(t, false).Get(proxyTestURL(second))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || acquired.Load() != 1 {
		t.Fatal("released connection slot could not be reused")
	}
}

func TestWakeHTTPStartupTimeoutAndBackendFailure(t *testing.T) {
	for _, scenario := range []string{"startup timeout", "backend refused"} {
		t.Run(scenario, func(t *testing.T) {
			proxy := testWakeProxy(t, 1, 30*time.Millisecond)
			released := make(chan struct{})
			port, err := proxy.listenHTTP("failed", 8080, "127.0.0.1", 0, func(ctx context.Context) (string, func(), error) {
				if scenario == "startup timeout" {
					<-ctx.Done()
					return "", func() { close(released) }, ctx.Err()
				}
				return "127.0.0.1:0", func() { close(released) }, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			response, err := testHTTPProxyClient(t, false).Get(proxyTestURL(port))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			want := http.StatusBadGateway
			if scenario == "startup timeout" {
				want = http.StatusGatewayTimeout
			}
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != want || !strings.Contains(string(body), "Application") {
				t.Fatalf("failure response status=%d body=%q error=%v", response.StatusCode, body, err)
			}
			awaitSignal(t, released)
		})
	}
}
