package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"
)

type binding struct {
	ListenAddress string `json:"listenAddress"`
	TargetAddress string `json:"targetAddress"`
}

type listenerView struct {
	ID string `json:"id"`
	binding
	Connections int  `json:"connections"`
	Ready       bool `json:"ready"`
}

type relay struct {
	owner        string
	token        string
	listenPrefix netip.Prefix
	targetHost   netip.Addr
	maxListeners int
	slots        chan struct{}
	shutdown     chan struct{}
	signalStop   func()
	mu           sync.Mutex
	listeners    map[string]*forwarder
	closed       bool
}

type forwarder struct {
	id string
	binding
	listener *net.TCPListener
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	sessions map[*session]struct{}
	closed   bool
	ready    bool
	wg       sync.WaitGroup
}

type session struct {
	client   *net.TCPConn
	upstream *net.TCPConn
}

func newRelay(owner, token string, prefix netip.Prefix, target netip.Addr, maxListeners, maxConnections int) *relay {
	shutdown := make(chan struct{})
	return &relay{owner: owner, token: token, listenPrefix: prefix, targetHost: target,
		maxListeners: maxListeners, slots: make(chan struct{}, maxConnections), listeners: make(map[string]*forwarder),
		shutdown: shutdown, signalStop: sync.OnceFunc(func() { close(shutdown) })}
}

func (r *relay) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, req *http.Request) {
		reply(w, http.StatusOK, map[string]any{"status": "ok", "name": "network-relay", "contract": 1,
			"probeContract": 2, "owner": r.owner, "maxListeners": r.maxListeners, "maxConnections": cap(r.slots)})
	})
	mux.HandleFunc("GET /listeners", func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		views := make([]listenerView, 0, len(r.listeners))
		for _, listener := range r.listeners {
			views = append(views, listener.view())
		}
		r.mu.Unlock()
		slices.SortFunc(views, func(a, b listenerView) int { return strings.Compare(a.ID, b.ID) })
		reply(w, http.StatusOK, map[string]any{"owner": r.owner, "listeners": views})
	})
	mux.HandleFunc("PUT /listeners/{id}", r.put)
	mux.HandleFunc("DELETE /listeners/{id}", r.remove)
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, req *http.Request) {
		r.close()
		reply(w, http.StatusOK, map[string]any{"owner": r.owner, "shutdown": true})
		r.signalStop()
	})
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		token, ok := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(r.token)) != 1 {
			reply(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, req)
	})
}

func (r *relay) put(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	if !validID(id) {
		reply(w, http.StatusBadRequest, map[string]string{"error": "invalid listener id"})
		return
	}
	var desired binding
	decoder := json.NewDecoder(http.MaxBytesReader(w, req.Body, 4096))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&desired); err != nil {
		reply(w, http.StatusBadRequest, map[string]string{"error": "invalid binding"})
		return
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) || !r.validBinding(desired) {
		reply(w, http.StatusBadRequest, map[string]string{"error": "binding must use the owned IPv4 prefix and target host"})
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		reply(w, http.StatusServiceUnavailable, map[string]string{"error": "relay is closing"})
		return
	}
	if existing := r.listeners[id]; existing != nil {
		if existing.binding != desired {
			reply(w, http.StatusConflict, map[string]string{"error": "listener id already has another binding"})
			return
		}
		view := existing.view()
		if !view.Ready {
			reply(w, http.StatusServiceUnavailable, map[string]string{"error": "owned listener is unavailable; delete before recreating"})
			return
		}
		reply(w, http.StatusOK, view)
		return
	}
	if len(r.listeners) >= r.maxListeners {
		reply(w, http.StatusServiceUnavailable, map[string]string{"error": "listener capacity exhausted"})
		return
	}
	address, err := net.ResolveTCPAddr("tcp4", desired.ListenAddress)
	if err != nil {
		reply(w, http.StatusBadRequest, map[string]string{"error": "invalid listen address"})
		return
	}
	listener, err := net.ListenTCP("tcp4", address)
	if err != nil {
		reply(w, http.StatusConflict, map[string]string{"error": "listen address unavailable"})
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	entry := &forwarder{id: id, binding: desired, listener: listener, ctx: ctx, cancel: cancel,
		sessions: make(map[*session]struct{}), ready: true}
	r.listeners[id] = entry
	entry.wg.Go(func() { entry.accept(r.slots) })
	view := entry.view()
	reply(w, http.StatusOK, view)
}

func (r *relay) validBinding(value binding) bool {
	listen, err := netip.ParseAddrPort(value.ListenAddress)
	if err != nil || !listen.Addr().Is4() || listen.Port() == 0 || listen.Addr().IsUnspecified() ||
		!r.listenPrefix.Contains(listen.Addr()) {
		return false
	}
	target, err := netip.ParseAddrPort(value.TargetAddress)
	return err == nil && target.Addr().Is4() && target.Port() != 0 && target.Addr() == r.targetHost
}

func validID(id string) bool {
	if len(id) == 0 || len(id) > 200 {
		return false
	}
	for _, character := range id {
		if character != '-' && character != '_' && character != '.' && character != ':' &&
			(character < '0' || character > '9') && (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') {
			return false
		}
	}
	return id != "." && id != ".."
}

func (r *relay) remove(w http.ResponseWriter, req *http.Request) {
	id := req.PathValue("id")
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.listeners[id]
	if entry != nil {
		entry.close()
		delete(r.listeners, id)
	}
	reply(w, http.StatusOK, map[string]any{"id": id, "deleted": entry != nil})
}

func (r *relay) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, entry := range r.listeners {
		entry.close()
	}
	clear(r.listeners)
}

func (f *forwarder) view() listenerView {
	f.mu.Lock()
	defer f.mu.Unlock()
	return listenerView{ID: f.id, binding: f.binding, Connections: len(f.sessions), Ready: f.ready && !f.closed}
}

func (f *forwarder) accept(slots chan struct{}) {
	defer func() {
		f.mu.Lock()
		f.ready = false
		f.mu.Unlock()
	}()
	for {
		client, err := f.listener.AcceptTCP()
		if err != nil {
			return
		}
		select {
		case slots <- struct{}{}:
		default:
			client.Close()
			continue
		}
		f.mu.Lock()
		if f.closed {
			f.mu.Unlock()
			client.Close()
			<-slots
			return
		}
		connection := &session{client: client}
		f.sessions[connection] = struct{}{}
		f.wg.Go(func() { f.forward(connection, slots) })
		f.mu.Unlock()
	}
}

func (f *forwarder) forward(connection *session, slots chan struct{}) {
	defer func() {
		f.mu.Lock()
		delete(f.sessions, connection)
		connection.client.Close()
		if connection.upstream != nil {
			connection.upstream.Close()
		}
		f.mu.Unlock()
		<-slots
	}()
	dialer := net.Dialer{Timeout: 10 * time.Second}
	upstream, err := dialer.DialContext(f.ctx, "tcp4", f.TargetAddress)
	if err != nil {
		return
	}
	server := upstream.(*net.TCPConn)
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		server.Close()
		return
	}
	connection.upstream = server
	f.mu.Unlock()
	var copies sync.WaitGroup
	copies.Go(func() {
		if _, err := io.Copy(server, connection.client); err != nil {
			server.Close()
			connection.client.Close()
		}
		server.CloseWrite()
	})
	if _, err := io.Copy(connection.client, server); err != nil {
		server.Close()
		connection.client.Close()
	}
	connection.client.CloseWrite()
	copies.Wait()
}

func (f *forwarder) close() {
	f.mu.Lock()
	f.closed = true
	f.cancel()
	f.listener.Close()
	for connection := range f.sessions {
		connection.client.Close()
		if connection.upstream != nil {
			connection.upstream.Close()
		}
	}
	f.mu.Unlock()
	f.wg.Wait()
}

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}
