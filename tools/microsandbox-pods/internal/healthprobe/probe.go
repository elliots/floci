// Package healthprobe checks actual guest listeners from the Linux network peer.
package healthprobe

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type Request struct {
	Lifecycle      bool        `json:"lifecycle,omitzero"`
	Address        string      `json:"address"`
	Port           int         `json:"port"`
	Scheme         string      `json:"scheme"`
	Path           string      `json:"path,omitempty"`
	Service        string      `json:"service,omitempty"`
	Headers        http.Header `json:"headers,omitempty"`
	TimeoutSeconds int32       `json:"timeoutSeconds"`
}

type Result struct {
	Success bool   `json:"success"`
	Warning string `json:"warning,omitempty"`
}

func Check(ctx context.Context, spec Request) (Result, error) {
	address, err := netip.ParseAddr(spec.Address)
	if err != nil || !address.Is4() || address.IsUnspecified() || address.IsMulticast() || spec.Port < 1 || spec.Port > 65535 || spec.TimeoutSeconds < 1 {
		return Result{}, fmt.Errorf("invalid probe address, port or timeout")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(spec.TimeoutSeconds)*time.Second)
	defer cancel()
	target := net.JoinHostPort(spec.Address, strconv.Itoa(spec.Port))
	dialer := net.Dialer{}
	if spec.Scheme == "GRPC" {
		client, err := grpc.NewClient("passthrough:///"+target,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp4", target)
			}))
		if err != nil {
			return Result{}, err
		}
		defer client.Close()
		response, err := healthpb.NewHealthClient(client).Check(ctx, &healthpb.HealthCheckRequest{Service: spec.Service})
		return Result{Success: err == nil && response.GetStatus() == healthpb.HealthCheckResponse_SERVING}, nil
	}
	if spec.Scheme == "TCP" {
		connection, err := dialer.DialContext(ctx, "tcp4", target)
		if err != nil {
			return Result{}, nil
		}
		connection.Close()
		return Result{Success: true}, nil
	}
	if spec.Scheme != "HTTP" && spec.Scheme != "HTTPS" {
		return Result{}, fmt.Errorf("unsupported probe scheme")
	}
	path, err := url.Parse(spec.Path)
	if err != nil {
		return Result{}, fmt.Errorf("invalid HTTP probe path: %w", err)
	}
	// The path must not replace the destination selected by the Pod specification.
	path.Scheme, path.Host, path.User, path.Opaque = strings.ToLower(spec.Scheme), target, nil, ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, path.String(), nil)
	if err != nil {
		return Result{}, err
	}
	request.Header = spec.Headers.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	if _, exists := request.Header["User-Agent"]; !exists {
		agent := "kube-probe/1.35"
		if spec.Lifecycle {
			agent = "kube-lifecycle/1.35"
		}
		request.Header.Set("User-Agent", agent)
	}
	if _, exists := request.Header["Accept"]; !exists {
		request.Header.Set("Accept", "*/*")
	}
	request.Host = request.Header.Get("Host")
	transport := &http.Transport{
		Proxy: nil, DisableKeepAlives: true, DisableCompression: true,
		DialContext: dialer.DialContext,
		// Kubernetes HTTPS probes intentionally skip certificate verification.
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	defer transport.CloseIdleConnections()
	warning := ""
	client := http.Client{Transport: transport, CheckRedirect: func(next *http.Request, via []*http.Request) error {
		if next.URL.Hostname() != via[0].URL.Hostname() || len(via) >= 10 {
			warning = "Probe terminated redirects"
			return http.ErrUseLastResponse
		}
		return nil
	}}
	response, err := client.Do(request)
	if spec.Lifecycle && err != nil && strings.Contains(err.Error(), "server gave HTTP response to HTTPS client") {
		retry := request.Clone(ctx)
		retry.URL.Scheme = "http"
		retry.Header.Del("Authorization")
		response, err = client.Do(retry)
		if err == nil {
			warning = "LifecycleHTTPSFallback"
		}
	}
	if err != nil {
		return Result{}, nil
	}
	response.Body.Close()
	return Result{Success: spec.Lifecycle || response.StatusCode >= 200 && response.StatusCode < 400, Warning: warning}, nil
}
