package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

// An acquired backend is actually ready. Its lease remains held until both TCP
// directions finish, including a response following a client's write half-close.
type proxyAcquire func(context.Context) (backend string, release func(), err error)

type proxyKey struct {
	uid  string
	port int
}

type wakeProxy struct {
	ctx      context.Context
	cancel   context.CancelFunc
	timeout  time.Duration
	slots    chan struct{}
	mu       sync.Mutex
	closed   bool
	bindings map[proxyKey]*wakeProxyBinding
}

type wakeProxyBinding struct {
	ctx      context.Context
	cancel   context.CancelFunc
	listener net.Listener
	acquire  proxyAcquire
	jobs     sync.WaitGroup
	done     chan struct{}
}

func newWakeProxy(ctx context.Context, maxConnections int, startupTimeout time.Duration) (*wakeProxy, error) {
	if maxConnections < 1 || startupTimeout <= 0 {
		return nil, errors.New("wake proxy requires positive connection and startup limits")
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &wakeProxy{ctx: lifetime, cancel: cancel, timeout: startupTimeout,
		slots: make(chan struct{}, maxConnections), bindings: make(map[proxyKey]*wakeProxyBinding)}, nil
}

func (p *wakeProxy) listen(uid string, containerPort int, bindHost string, requestedPort int, acquire proxyAcquire) (int, error) {
	if uid == "" || containerPort < 1 || containerPort > 65535 || net.ParseIP(bindHost) == nil ||
		requestedPort < 0 || requestedPort > 65535 || acquire == nil {
		return 0, errors.New("invalid wake proxy listener")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.ctx.Err() != nil {
		return 0, errors.New("wake proxy is shutting down")
	}
	key := proxyKey{uid: uid, port: containerPort}
	if _, exists := p.bindings[key]; exists {
		return 0, errors.New("pod port already has a wake listener")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(bindHost, strconv.Itoa(requestedPort)))
	if err != nil {
		return 0, fmt.Errorf("cannot bind wake listener: %w", err)
	}
	ctx, cancel := context.WithCancel(p.ctx)
	binding := &wakeProxyBinding{ctx: ctx, cancel: cancel, listener: listener, acquire: acquire, done: make(chan struct{})}
	p.bindings[key] = binding
	binding.jobs.Go(func() { p.accept(binding) })
	go func() {
		binding.jobs.Wait()
		close(binding.done)
	}()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func (p *wakeProxy) ready(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	found := false
	for key, binding := range p.bindings {
		if key.uid == uid {
			found = true
			if binding.ctx.Err() != nil {
				return false
			}
		}
	}
	return found && !p.closed && p.ctx.Err() == nil
}

func (p *wakeProxy) accept(binding *wakeProxyBinding) {
	stop := context.AfterFunc(binding.ctx, func() { _ = binding.listener.Close() })
	defer stop()
	defer binding.cancel()
	for {
		client, err := binding.listener.Accept()
		if err != nil {
			return
		}
		if binding.ctx.Err() != nil {
			_ = client.Close()
			return
		}
		select {
		case p.slots <- struct{}{}:
			binding.jobs.Go(func() {
				defer func() { <-p.slots }()
				p.forward(binding, client)
			})
		default:
			// A raw TCP listener cannot safely invent an HTTP or TLS response.
			_ = client.Close()
		}
	}
}

func (p *wakeProxy) forward(binding *wakeProxyBinding, client net.Conn) {
	defer client.Close()
	stopClient := context.AfterFunc(binding.ctx, func() { _ = client.Close() })
	defer stopClient()
	startup, cancel := context.WithTimeout(binding.ctx, p.timeout)
	defer cancel()
	backend, release, err := binding.acquire(startup)
	if release != nil {
		defer release()
	}
	if err != nil || startup.Err() != nil {
		return
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	upstream, err := dialer.DialContext(startup, "tcp", backend)
	if err != nil {
		return
	}
	defer upstream.Close()
	cancel()
	stopUpstream := context.AfterFunc(binding.ctx, func() { _ = upstream.Close() })
	defer stopUpstream()
	var streams sync.WaitGroup
	streams.Go(func() { copyTCP(upstream, client) })
	streams.Go(func() { copyTCP(client, upstream) })
	streams.Wait()
}

func copyTCP(destination, source net.Conn) {
	_, err := io.CopyBuffer(destination, source, make([]byte, 32*1024))
	if err != nil {
		_ = source.Close()
		_ = destination.Close()
		return
	}
	if tcp, ok := destination.(*net.TCPConn); ok {
		_ = tcp.CloseWrite()
	}
}

func (p *wakeProxy) closePod(ctx context.Context, uid string) error {
	p.mu.Lock()
	bindings := make(map[proxyKey]*wakeProxyBinding)
	for key, binding := range p.bindings {
		if key.uid == uid {
			bindings[key] = binding
			binding.cancel()
		}
	}
	p.mu.Unlock()
	for key, binding := range bindings {
		select {
		case <-binding.done:
			p.mu.Lock()
			if p.bindings[key] == binding {
				delete(p.bindings, key)
			}
			p.mu.Unlock()
		case <-ctx.Done():
			return fmt.Errorf("wake proxy connections did not drain: %w", ctx.Err())
		}
	}
	return nil
}

func (p *wakeProxy) close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.cancel()
	bindings := make([]*wakeProxyBinding, 0, len(p.bindings))
	for _, binding := range p.bindings {
		bindings = append(bindings, binding)
	}
	p.mu.Unlock()
	for _, binding := range bindings {
		select {
		case <-binding.done:
		case <-ctx.Done():
			return fmt.Errorf("wake proxy shutdown did not drain: %w", ctx.Err())
		}
	}
	return nil
}
