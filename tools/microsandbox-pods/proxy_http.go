package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"slices"
	"strings"
	"sync"
	"time"
)

type proxyHTTPConnectionKey struct{}

type proxyHTTPListener struct {
	net.Listener
	slots chan struct{}
}

func (l *proxyHTTPListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		select {
		case l.slots <- struct{}{}:
			return &proxyHTTPConnection{Conn: conn, release: sync.OnceFunc(func() { <-l.slots })}, nil
		default:
			_ = conn.Close()
		}
	}
}

type proxyHTTPConnection struct {
	net.Conn
	release func()
}

func (c *proxyHTTPConnection) Close() error {
	defer c.release()
	return c.Conn.Close()
}

func (c *proxyHTTPConnection) CloseWrite() error {
	if conn, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return http.ErrNotSupported
}

type proxyHTTPHandlers struct {
	mu      sync.Mutex
	closing bool
	jobs    sync.WaitGroup
}

func (p *wakeProxy) listenHTTP(uid string, containerPort int, bindHost string, requestedPort int, acquire proxyAcquire) (int, error) {
	return p.listenWith(uid, containerPort, bindHost, requestedPort, acquire, p.serveHTTP)
}

func (p *wakeProxy) serveHTTP(binding *wakeProxyBinding) {
	var handlers proxyHTTPHandlers
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Protocols: protocols, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 10 * time.Minute,
		DisableGeneralOptionsHandler: true,
		BaseContext:                  func(net.Listener) context.Context { return binding.ctx },
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, proxyHTTPConnectionKey{}, conn)
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlers.mu.Lock()
			if handlers.closing || binding.ctx.Err() != nil {
				handlers.mu.Unlock()
				proxyHTTPError(w, r, http.StatusServiceUnavailable, "Application route is unavailable")
				return
			}
			handlers.jobs.Add(1)
			handlers.mu.Unlock()
			defer handlers.jobs.Done()
			// Server.Close does not close hijacked WebSocket connections. Closing the
			// underlying connection also unblocks a stream stalled writing to its client.
			stop := context.AfterFunc(binding.ctx, func() {
				if conn, ok := r.Context().Value(proxyHTTPConnectionKey{}).(net.Conn); ok {
					_ = conn.Close()
				}
			})
			defer stop()
			p.forwardHTTP(binding, w, r)
		}),
	}
	closeServer := func() {
		handlers.mu.Lock()
		handlers.closing = true
		handlers.mu.Unlock()
		_ = server.Close()
	}
	stop := context.AfterFunc(binding.ctx, closeServer)
	defer stop()
	_ = server.Serve(&proxyHTTPListener{Listener: binding.listener, slots: p.httpConnections})
	binding.cancel()
	closeServer()
	// No new handler can enter after closing is set. Serve returning alone does
	// not mean active handlers, including hijacked streams, have released leases.
	handlers.jobs.Wait()
}

func (p *wakeProxy) forwardHTTP(binding *wakeProxyBinding, w http.ResponseWriter, r *http.Request) {
	select {
	case p.slots <- struct{}{}:
		defer func() { <-p.slots }()
	default:
		proxyHTTPError(w, r, http.StatusServiceUnavailable, "Application request capacity is full")
		return
	}
	startup, cancel := context.WithTimeout(r.Context(), p.timeout)
	backend, release, err := binding.acquire(startup)
	if release != nil {
		defer release()
	}
	startupErr := startup.Err()
	cancel()
	if err != nil || startupErr != nil {
		status := http.StatusServiceUnavailable
		message := "Application could not become ready"
		if errors.Is(startupErr, context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			status, message = http.StatusGatewayTimeout, "Application startup timed out"
		}
		proxyHTTPError(w, r, status, message)
		return
	}
	protocols := new(http.Protocols)
	protocols.SetHTTP1(r.ProtoMajor == 1)
	protocols.SetUnencryptedHTTP2(r.ProtoMajor == 2)
	transport := &http.Transport{
		Protocols: protocols, DisableKeepAlives: true, DisableCompression: true,
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
	}
	defer transport.CloseIdleConnections()
	proxy := &httputil.ReverseProxy{
		Transport: transport, FlushInterval: -1,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme = "http"
			request.Out.URL.Host = backend
			request.Out.Host = request.In.Host
			// Incoming trailers are filled as Body reaches EOF. The outbound writer
			// reads them afterward, so retain that map instead of Clone's empty copy.
			request.Out.Trailer = request.In.Trailer
			// ReverseProxy sanitizes queries before Rewrite. This route must preserve
			// the application's literal query, including signing-sensitive encodings.
			request.Out.URL.RawQuery = request.In.URL.RawQuery
			preserveProxyForwardingHeaders(request)
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, _ error) {
			proxyHTTPError(w, r, http.StatusBadGateway, "Application request failed")
		},
	}
	if r.ProtoMajor == 1 {
		if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
			proxyHTTPError(w, r, http.StatusBadGateway, "Application streaming is unavailable")
			return
		}
	}
	proxy.ServeHTTP(w, r)
}

func preserveProxyForwardingHeaders(request *httputil.ProxyRequest) {
	// Retain existing ingress context without inventing a new forwarding hop.
	// ReverseProxy removed these headers before Rewrite, including hop-by-hop ones.
	for _, name := range []string{"Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		hopByHop := false
		for _, connection := range request.In.Header.Values("Connection") {
			for token := range strings.SplitSeq(connection, ",") {
				if strings.EqualFold(strings.TrimSpace(token), name) {
					hopByHop = true
				}
			}
		}
		if values, exists := request.In.Header[name]; exists && !hopByHop {
			request.Out.Header[name] = slices.Clone(values)
		}
	}
}

func proxyHTTPError(w http.ResponseWriter, r *http.Request, status int, message string) {
	contentType := r.Header.Get("Content-Type")
	if r.ProtoMajor == 2 && (contentType == "application/grpc" || strings.HasPrefix(contentType, "application/grpc+")) {
		w.Header().Set("Content-Type", "application/grpc")
		code := "14"
		if status == http.StatusGatewayTimeout {
			code = "4"
		}
		w.Header().Set("Grpc-Status", code)
		w.Header().Set("Grpc-Message", message)
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Error(w, message, status)
}
