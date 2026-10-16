package healthprobe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestGRPCProbeChecksNamedServiceAndIgnoresEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	checks := health.NewServer()
	checks.SetServingStatus("ready", healthpb.HealthCheckResponse_SERVING)
	checks.SetServingStatus("unhealthy", healthpb.HealthCheckResponse_NOT_SERVING)
	healthpb.RegisterHealthServer(server, checks)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	spec := Request{Address: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, Scheme: "GRPC", TimeoutSeconds: 1}
	for _, service := range []string{"", "ready", "unhealthy", "missing"} {
		spec.Service = service
		result, err := Check(t.Context(), spec)
		if err != nil || result.Success != (service == "" || service == "ready") {
			t.Fatalf("gRPC service %q: %+v %v", service, result, err)
		}
	}
	server.Stop()
	result, err := Check(t.Context(), spec)
	if err != nil || result.Success {
		t.Fatalf("stopped gRPC listener: %+v %v", result, err)
	}
}

type blockingHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (blockingHealthServer) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestGRPCProbeEnforcesRPCDeadline(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, blockingHealthServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	start := time.Now()
	result, err := Check(t.Context(), Request{Address: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, Scheme: "GRPC", TimeoutSeconds: 1})
	if err != nil || result.Success || time.Since(start) > 3*time.Second {
		t.Fatalf("gRPC deadline: %+v %v after %s", result, err, time.Since(start))
	}
}

func serverRequest(server *httptest.Server) Request {
	address := netip.MustParseAddrPort(strings.TrimPrefix(strings.TrimPrefix(server.URL, "http://"), "https://"))
	return Request{Address: address.Addr().String(), Port: int(address.Port()), Scheme: "HTTP", TimeoutSeconds: 2}
}

func TestHTTPProbesUseKubernetesHeadersPathsAndStatusCodes(t *testing.T) {
	for _, code := range []int{200, 204, 302, 400, 503} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.RequestURI() != "/ready?full=true" || r.Host != "application.test" || r.Header.Get("User-Agent") != "kube-probe/1.35" || r.Header.Get("Accept") != "*/*" || len(r.Header.Values("X-Check")) != 2 {
					t.Errorf("wrong probe request: %s %s %v", r.URL, r.Host, r.Header)
				}
				w.WriteHeader(code)
			}))
			defer server.Close()
			spec := serverRequest(server)
			spec.Path, spec.Headers = "/ready?full=true", http.Header{"Host": {"application.test"}, "X-Check": {"one", "two"}}
			result, err := Check(t.Context(), spec)
			if err != nil || result.Success != (code < 400) {
				t.Fatalf("status %d: %+v %v", code, result, err)
			}
		})
	}
}

func TestHTTPSProbesAcceptSelfSignedCertificates(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer server.Close()
	spec := serverRequest(server)
	spec.Scheme = "HTTPS"
	result, err := Check(t.Context(), spec)
	if err != nil || !result.Success {
		t.Fatalf("HTTPS probe: %+v %v", result, err)
	}
}

func TestTCPProbeChecksApplicationSocketAndClosesIt(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	address := listener.Addr().(*net.TCPAddr)
	spec := Request{Address: "127.0.0.1", Port: address.Port, Scheme: "TCP", TimeoutSeconds: 1}
	result, err := Check(t.Context(), spec)
	if err != nil || !result.Success {
		t.Fatalf("open socket: %+v %v", result, err)
	}
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetReadDeadline(time.Now().Add(time.Second))
	if n, err := connection.Read(make([]byte, 1)); n != 0 || err == nil {
		t.Fatal("probe retained its connection")
	}
	listener.Close()
	result, err = Check(t.Context(), spec)
	if err != nil || result.Success {
		t.Fatalf("closed socket: %+v %v", result, err)
	}
}

func TestTimeoutAndRedirectHandling(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/slow":
			<-r.Context().Done()
		case "/loop":
			http.Redirect(w, r, "/loop", 302)
		case "/external":
			http.Redirect(w, r, "http://different.invalid/private", 302)
		case "/local":
			http.Redirect(w, r, "/fail", 302)
		default:
			w.WriteHeader(503)
		}
	}))
	defer server.Close()
	spec := serverRequest(server)
	spec.TimeoutSeconds, spec.Path = 1, "/slow"
	started := time.Now()
	result, err := Check(t.Context(), spec)
	if err != nil || result.Success || time.Since(started) > 3*time.Second {
		t.Fatalf("timeout: %+v %v", result, err)
	}
	for _, path := range []string{"/loop", "/external"} {
		requests.Store(0)
		spec.Path = path
		result, err = Check(t.Context(), spec)
		if err != nil || !result.Success || result.Warning == "" || requests.Load() > 10 {
			t.Fatalf("redirect %s: %+v %v", path, result, err)
		}
	}
	spec.Path = "/local"
	result, err = Check(t.Context(), spec)
	if err != nil || result.Success {
		t.Fatalf("same-host redirect did not inspect the final status: %+v %v", result, err)
	}
}

func TestHTTPProbeDoesNotUseEnvironmentProxy(t *testing.T) {
	// An invalid proxy must not affect a node-local health check.
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	result, err := Check(t.Context(), serverRequest(server))
	if err != nil || !result.Success {
		t.Fatalf("probe inherited environment proxy: %+v %v", result, err)
	}
}
