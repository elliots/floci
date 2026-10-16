package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

const testToken = "private-relay-token-with-at-least-thirty-two-bytes"

func fixture(t *testing.T, listeners, connections int) *relay {
	t.Helper()
	r := newRelay("owned-relay", testToken, netip.MustParsePrefix("127.0.0.0/8"), netip.MustParseAddr("127.0.0.1"), listeners, connections)
	t.Cleanup(r.close)
	return r
}

func request(t *testing.T, r *relay, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&encoded).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &encoded)
	req.Header.Set("Authorization", "Bearer "+testToken)
	response := httptest.NewRecorder()
	r.handler().ServeHTTP(response, req)
	return response
}

func tcpListener(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	return listener
}

func availableAddress(t *testing.T) string {
	t.Helper()
	listener := tcpListener(t)
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func bind(t *testing.T, r *relay, id, target string) string {
	t.Helper()
	address := availableAddress(t)
	response := request(t, r, "PUT", "/listeners/"+id, binding{ListenAddress: address, TargetAddress: target})
	if response.Code != http.StatusOK {
		t.Fatalf("create: %d %s", response.Code, response.Body)
	}
	return address
}

func connect(t *testing.T, address string) *net.TCPConn {
	t.Helper()
	connection, err := net.DialTimeout("tcp4", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	tcp := connection.(*net.TCPConn)
	t.Cleanup(func() { tcp.Close() })
	if err := tcp.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return tcp
}

func TestControlAuthenticationOwnershipAndBindingRecovery(t *testing.T) {
	r := fixture(t, 4, 4)
	unauthorized := httptest.NewRecorder()
	r.handler().ServeHTTP(unauthorized, httptest.NewRequest("GET", "/health", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status: %d", unauthorized.Code)
	}
	health := request(t, r, "GET", "/health", nil)
	if health.Code != http.StatusOK || !strings.Contains(health.Body.String(), `"owner":"owned-relay"`) {
		t.Fatalf("health: %d %s", health.Code, health.Body)
	}
	target := tcpListener(t)
	address := bind(t, r, "pod-uid:8080", target.Addr().String())
	same := request(t, r, "PUT", "/listeners/pod-uid:8080", binding{address, target.Addr().String()})
	if same.Code != http.StatusOK {
		t.Fatalf("idempotent PUT: %d %s", same.Code, same.Body)
	}
	changed := request(t, r, "PUT", "/listeners/pod-uid:8080", binding{availableAddress(t), target.Addr().String()})
	if changed.Code != http.StatusConflict {
		t.Fatalf("changed binding: %d", changed.Code)
	}
	duplicate := request(t, r, "PUT", "/listeners/another-id", binding{address, target.Addr().String()})
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate socket: %d", duplicate.Code)
	}
	listing := request(t, r, "GET", "/listeners", nil)
	var state struct {
		Owner     string         `json:"owner"`
		Listeners []listenerView `json:"listeners"`
	}
	if err := json.Unmarshal(listing.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if state.Owner != r.owner || len(state.Listeners) != 1 || state.Listeners[0].ID != "pod-uid:8080" || state.Listeners[0].ListenAddress != address {
		t.Fatalf("listing: %s", listing.Body)
	}
	for index := range 2 {
		deleted := request(t, r, "DELETE", "/listeners/pod-uid:8080", nil)
		if deleted.Code != http.StatusOK || !strings.Contains(deleted.Body.String(), `"deleted":`+[]string{"true", "false"}[index]) {
			t.Fatalf("delete: %d %s", deleted.Code, deleted.Body)
		}
	}
}

func TestRejectsBindingsOutsideOwnedNetworkAndMalformedBodies(t *testing.T) {
	r := fixture(t, 4, 4)
	for _, desired := range []binding{
		{"0.0.0.0:8080", "127.0.0.1:80"}, {"10.1.2.3:8080", "127.0.0.1:80"},
		{"127.0.0.1:0", "127.0.0.1:80"}, {"localhost:8080", "127.0.0.1:80"},
		{"[::1]:8080", "127.0.0.1:80"}, {"127.0.0.1:8080", "127.0.0.2:80"},
		{"127.0.0.1:8080", "localhost:80"}, {"127.0.0.1:8080", "127.0.0.1:0"},
		{"127.0.0.1:8080", "[::ffff:127.0.0.1]:80"},
	} {
		response := request(t, r, "PUT", "/listeners/pod", desired)
		if response.Code != http.StatusBadRequest {
			t.Errorf("accepted invalid binding %v: %d", desired, response.Code)
		}
	}
	for _, body := range []string{`{"listenAddress":"127.0.0.1:8080","targetAddress":"127.0.0.1:80","unknown":true}`, `{} {}`, "null"} {
		req := httptest.NewRequest("PUT", "/listeners/pod", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+testToken)
		response := httptest.NewRecorder()
		r.handler().ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest {
			t.Errorf("accepted malformed request: %d %s", response.Code, body)
		}
	}
}

func TestForwardsOpaquePayloadAndClientHalfClose(t *testing.T) {
	r := fixture(t, 4, 4)
	target := tcpListener(t)
	address := bind(t, r, "client-half-close", target.Addr().String())
	payload := bytes.Repeat([]byte{0, 1, 2, 255, 13, 10, 99}, 32000)
	result := make(chan error, 1)
	go func() {
		server, err := target.AcceptTCP()
		if err != nil {
			result <- err
			return
		}
		defer server.Close()
		server.SetDeadline(time.Now().Add(5 * time.Second))
		bytes, err := io.ReadAll(server)
		if err == nil {
			_, err = server.Write(bytes)
		}
		result <- err
	}()
	client := connect(t, address)
	if _, err := client.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	response, err := io.ReadAll(client)
	if err != nil || !bytes.Equal(payload, response) {
		t.Fatalf("payload mismatch: got %d bytes, error %v", len(response), err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestServerHalfCloseStillAllowsClientToSend(t *testing.T) {
	r := fixture(t, 4, 4)
	target := tcpListener(t)
	address := bind(t, r, "server-half-close", target.Addr().String())
	received := make(chan string, 1)
	go func() {
		server, err := target.AcceptTCP()
		if err != nil {
			received <- err.Error()
			return
		}
		defer server.Close()
		server.SetDeadline(time.Now().Add(5 * time.Second))
		server.Write([]byte("first response"))
		server.CloseWrite()
		body, err := io.ReadAll(server)
		if err != nil {
			received <- err.Error()
			return
		}
		received <- string(body)
	}()
	client := connect(t, address)
	response, err := io.ReadAll(client)
	if err != nil || string(response) != "first response" {
		t.Fatalf("server half-close: %q %v", response, err)
	}
	if _, err := client.Write([]byte("after server EOF")); err != nil {
		t.Fatal(err)
	}
	client.CloseWrite()
	if actual := <-received; actual != "after server EOF" {
		t.Fatalf("client tail: %q", actual)
	}
}

func TestConnectionCapIsGlobalAndDeleteDrainsSessions(t *testing.T) {
	r := fixture(t, 2, 1)
	target := tcpListener(t)
	firstAddress := bind(t, r, "first", target.Addr().String())
	secondAddress := bind(t, r, "second", target.Addr().String())
	first := connect(t, firstAddress)
	if err := target.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	upstream, err := target.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	upstream.SetDeadline(time.Now().Add(3 * time.Second))
	upstream.Write([]byte("a"))
	var one [1]byte
	if _, err := io.ReadFull(first, one[:]); err != nil {
		t.Fatal(err)
	}
	rejected := connect(t, secondAddress)
	if count, err := rejected.Read(one[:]); count != 0 || err == nil {
		t.Fatalf("connection cap failed: %d %v", count, err)
	}
	started := time.Now()
	if response := request(t, r, "DELETE", "/listeners/first", nil); response.Code != http.StatusOK {
		t.Fatalf("delete: %d", response.Code)
	}
	if time.Since(started) > time.Second || len(r.slots) != 0 {
		t.Fatalf("delete did not drain promptly: elapsed=%v slots=%d", time.Since(started), len(r.slots))
	}
	for _, connection := range []*net.TCPConn{first, upstream} {
		if count, err := connection.Read(one[:]); count != 0 || err == nil {
			t.Fatalf("delete left session open: %d %v", count, err)
		}
	}
	accepted := connect(t, secondAddress)
	secondUpstream, err := target.AcceptTCP()
	if err != nil {
		t.Fatalf("capacity not released: %v", err)
	}
	defer secondUpstream.Close()
	secondUpstream.Write([]byte("b"))
	if _, err := io.ReadFull(accepted, one[:]); err != nil || one[0] != 'b' {
		t.Fatalf("replacement session: %q %v", one, err)
	}
	r.close()
	if len(r.slots) != 0 {
		t.Fatalf("shutdown leaked %d sessions", len(r.slots))
	}
}

func TestListenerCapacityAndClosedRelayRejectNewBindings(t *testing.T) {
	r := fixture(t, 1, 1)
	target := tcpListener(t)
	bind(t, r, "first", target.Addr().String())
	response := request(t, r, "PUT", "/listeners/second", binding{availableAddress(t), target.Addr().String()})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("listener capacity: %d", response.Code)
	}
	r.close()
	response = request(t, r, "PUT", "/listeners/second", binding{availableAddress(t), target.Addr().String()})
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed relay accepted a listener: %d", response.Code)
	}
}

func TestShutdownRequiresAuthenticationAndIsIdempotent(t *testing.T) {
	r := fixture(t, 1, 1)
	unauthorized := httptest.NewRecorder()
	r.handler().ServeHTTP(unauthorized, httptest.NewRequest("POST", "/shutdown", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated shutdown: %d", unauthorized.Code)
	}
	select {
	case <-r.shutdown:
		t.Fatal("unauthenticated request shut down the relay")
	default:
	}
	for range 2 {
		response := request(t, r, "POST", "/shutdown", nil)
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"shutdown":true`) {
			t.Fatalf("shutdown: %d %s", response.Code, response.Body)
		}
	}
	select {
	case <-r.shutdown:
	default:
		t.Fatal("shutdown did not signal daemon exit")
	}
}

func TestUnexpectedAcceptExitMarksListenerUnavailableUntilOwnedDeletion(t *testing.T) {
	r := fixture(t, 1, 1)
	target := tcpListener(t)
	address := bind(t, r, "failed-listener", target.Addr().String())
	r.mu.Lock()
	entry := r.listeners["failed-listener"]
	r.mu.Unlock()
	if !entry.view().Ready {
		t.Fatal("new listener is not ready")
	}
	if err := entry.listener.Close(); err != nil {
		t.Fatal(err)
	}
	entry.wg.Wait()
	listing := request(t, r, "GET", "/listeners", nil)
	var state struct {
		Listeners []listenerView `json:"listeners"`
	}
	if err := json.Unmarshal(listing.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	if len(state.Listeners) != 1 || state.Listeners[0].Ready || state.Listeners[0].ListenAddress != address {
		t.Fatalf("failed accept loop must retain ownership without claiming readiness: %s", listing.Body)
	}
	desired := binding{address, target.Addr().String()}
	if response := request(t, r, "PUT", "/listeners/failed-listener", desired); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("same-binding PUT hid dead accept loop: %d", response.Code)
	}
	if response := request(t, r, "DELETE", "/listeners/failed-listener", nil); response.Code != http.StatusOK {
		t.Fatalf("cannot delete unavailable owned listener: %d", response.Code)
	}
	if response := request(t, r, "PUT", "/listeners/failed-listener", desired); response.Code != http.StatusOK {
		t.Fatalf("cannot recreate deleted listener: %d", response.Code)
	}
}
