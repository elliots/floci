package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, input io.Reader, output io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: network-relay serve|client|probe [flags]")
	}
	switch args[0] {
	case "probe":
		if len(args) != 1 {
			return errors.New("probe accepts its request on stdin only")
		}
		return probe(input, output)
	case "serve":
		return serve(args[1:])
	case "client":
		return client(args[1:], input, output)
	default:
		return errors.New("expected serve, client or probe")
	}
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	socket := flags.String("socket", "", "private control socket path")
	tokenFile := flags.String("token-file", "", "private bearer token file")
	owner := flags.String("owner", "", "relay ownership identity")
	cidr := flags.String("listen-cidr", "", "owned IPv4 listener prefix")
	targetHost := flags.String("target-host", "", "allowed target IPv4 host")
	maxConnections := flags.Int("max-connections", 1024, "maximum concurrent TCP sessions")
	maxListeners := flags.Int("max-listeners", 2048, "maximum TCP listeners")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !validID(*owner) || !filepath.IsAbs(*socket) || *maxConnections < 1 || *maxListeners < 1 {
		return errors.New("serve requires an absolute socket path, owner and positive capacity limits")
	}
	prefix, err := netip.ParsePrefix(*cidr)
	if err != nil || !prefix.Addr().Is4() || prefix.Bits() == 0 {
		return errors.New("listen-cidr must be a specific IPv4 prefix")
	}
	target, err := netip.ParseAddr(*targetHost)
	if err != nil || !target.Is4() || target.IsUnspecified() || target.IsMulticast() {
		return errors.New("target-host must be an explicit unicast IPv4 address")
	}
	token, err := readToken(*tokenFile)
	if err != nil {
		return err
	}
	listener, err := controlListener(*socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	forwarding := newRelay(*owner, token, prefix.Masked(), target, *maxListeners, *maxConnections)
	defer forwarding.close()
	server := &http.Server{Handler: forwarding.handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second,
		MaxHeaderBytes: 8192}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	select {
	case err := <-finished:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		return server.Close()
	case <-forwarding.shutdown:
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			return errors.Join(err, server.Close())
		}
		return nil
	}
}

func controlListener(path string) (*net.UnixListener, error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, errors.New("control socket directory must be private with mode 0700")
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("cannot bind control socket; inspect existing relay ownership before recovery: %w", err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func readToken(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 4096 {
		return "", errors.New("token file must be a regular file with mode 0600 and at most 4096 bytes")
	}
	bytes, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(bytes))
	if len(token) < 32 || strings.ContainsAny(token, "\r\n\t ") {
		return "", errors.New("token must contain at least 32 bytes without whitespace")
	}
	return token, nil
}

func client(args []string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	socket := flags.String("socket", "", "private control socket path")
	tokenFile := flags.String("token-file", "", "private bearer token file")
	method := flags.String("method", "GET", "HTTP method")
	path := flags.String("path", "/health", "control resource path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || !filepath.IsAbs(*socket) || (*method != "GET" && *method != "PUT" && *method != "DELETE" && *method != "POST") ||
		!strings.HasPrefix(*path, "/") || strings.ContainsAny(*path, "?#\r\n") {
		return errors.New("invalid client socket, method or resource path")
	}
	token, err := readToken(*tokenFile)
	if err != nil {
		return err
	}
	var body io.Reader
	if *method == "PUT" {
		bytes, err := io.ReadAll(io.LimitReader(input, 4097))
		if err != nil {
			return err
		}
		if len(bytes) > 4096 {
			return errors.New("request body exceeds 4096 bytes")
		}
		body = strings.NewReader(string(bytes))
	}
	request, err := http.NewRequest(*method, "http://relay"+*path, body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	dialer := net.Dialer{Timeout: 3 * time.Second}
	transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", *socket)
	}}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: 20 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	bytes, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024+1))
	if err != nil {
		return err
	}
	if len(bytes) > 1024*1024 {
		return errors.New("response exceeds one MiB")
	}
	if _, err := output.Write(bytes); err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("relay request failed with HTTP %d", response.StatusCode)
	}
	return nil
}
