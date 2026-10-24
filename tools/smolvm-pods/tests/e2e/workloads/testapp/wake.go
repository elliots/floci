package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The endpoints of the wake test, which hold serves on its address beside its
// own:
//
//	/health              "ok", or 500 once /health/fail was called or with -unready
//	/health/fail, /health/ok
//	                     switch /health, the HTTPS /health and the gRPC health service
//	/remote              the client address that the server saw
//	/stream?seconds=N&every=S
//	                     a chunk every S seconds (1) for N seconds (10)
//	/ws?ping=S           a WebSocket echo that sends a ping every S seconds (0: none)
//	/call?url=URL        GET URL from this pod and answer with what came back
//	/busy?seconds=N      keep one CPU busy for N seconds, answering at once
//	/follow?name=NAME&url=URL
//	                     GET URL from this pod in the background, on a connection
//	                     without TCP keepalive, and keep its lines as the follow
//	                     NAME (default: empty): a stream that another pod or the
//	                     Mac pushes to this one
//	/followed?name=NAME  what the follow NAME received so far: its lines, the last
//	                     one, how long ago, whether it ended, and its error
//
// With -https ADDR, hold also serves its endpoints over TLS with a self-signed
// certificate; with -grpc ADDR, a gRPC health service (grpc.health.v1.Health/Check)
// over unencrypted HTTP/2; with -udp ADDR, an echo of UDP datagrams.
//
// The servers turn TCP keepalive off on the connections they accept, so that an
// idle connection sends no packets from the pod.

// healthy is what the health endpoints answer.
var healthy atomic.Bool

// wsConnections numbers the WebSocket connections, so that a client can tell
// that two answers came on one connection.
var wsConnections atomic.Int64

// busySink keeps the busy loop's result, so that the loop is not removed.
var busySink atomic.Uint64

// registerWakeEndpoints adds these endpoints to the default mux.
func registerWakeEndpoints(unready bool) {
	healthy.Store(!unready)
	http.HandleFunc("GET /health", health)
	http.HandleFunc("/health/fail", func(w http.ResponseWriter, r *http.Request) {
		healthy.Store(false)
		fmt.Fprintln(w, "health=failing")
	})
	http.HandleFunc("/health/ok", func(w http.ResponseWriter, r *http.Request) {
		healthy.Store(true)
		fmt.Fprintln(w, "health=ok")
	})
	http.HandleFunc("GET /remote", remote)
	http.HandleFunc("GET /stream", stream)
	http.HandleFunc("GET /ws", websocketEcho)
	http.HandleFunc("GET /call", call)
	http.HandleFunc("GET /busy", busy)
	http.HandleFunc("GET /follow", follow)
	http.HandleFunc("GET /followed", followed)
}

// startWakeServers starts the optional HTTPS, gRPC and UDP servers.
func startWakeServers(httpsAddr, grpcAddr, udpAddr string) {
	if httpsAddr != "" {
		cert, err := selfSigned()
		if err != nil {
			fail(err)
		}
		server := &http.Server{TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
		ln := listen(httpsAddr)
		go func() { fail(server.ServeTLS(ln, "", "")) }()
	}
	if grpcAddr != "" {
		var protocols http.Protocols
		protocols.SetHTTP1(true)
		protocols.SetUnencryptedHTTP2(true)
		server := &http.Server{Handler: http.HandlerFunc(grpcHealth), Protocols: &protocols}
		ln := listen(grpcAddr)
		go func() { fail(server.Serve(ln)) }()
	}
	if udpAddr != "" {
		conn, err := net.ListenPacket("udp", udpAddr)
		if err != nil {
			fail(err)
		}
		go udpEcho(conn)
	}
}

// listen listens on addr with TCP keepalive off on accepted connections.
func listen(addr string) net.Listener {
	config := net.ListenConfig{KeepAlive: -1}
	ln, err := config.Listen(context.Background(), "tcp", addr)
	if err != nil {
		fail(err)
	}
	return ln
}

func health(w http.ResponseWriter, r *http.Request) {
	if !healthy.Load() {
		http.Error(w, "failing", http.StatusInternalServerError)
		return
	}
	fmt.Fprintln(w, "ok")
}

func remote(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	fmt.Fprintf(w, "remote=%s\nremote_ip=%s\n", r.RemoteAddr, host)
}

// numberParam reads a number from the query, or def.
func numberParam(r *http.Request, name string, def float64) (float64, error) {
	text := r.URL.Query().Get(name)
	if text == "" {
		return def, nil
	}
	return strconv.ParseFloat(text, 64)
}

func seconds(s float64) time.Duration {
	return time.Duration(s * float64(time.Second))
}

func stream(w http.ResponseWriter, r *http.Request) {
	total, err1 := numberParam(r, "seconds", 10)
	every, err2 := numberParam(r, "every", 1)
	if err := errors.Join(err1, err2); err != nil || every <= 0 {
		http.Error(w, "stream takes seconds=N and every=S", http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "cannot stream", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	start := time.Now()
	chunks := int(total/every) + 1
	for n := 1; n <= chunks; n++ {
		select {
		case <-time.After(time.Until(start.Add(seconds(every * float64(n-1))))):
		case <-r.Context().Done():
			return
		}
		fmt.Fprintf(w, "chunk=%d\nchunk_ms=%d\n", n, time.Since(start).Milliseconds())
		flusher.Flush()
	}
	fmt.Fprintf(w, "chunks=%d\n", chunks)
}

// call GETs a URL from this pod, on a new connection without keepalive.
func call(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "call takes url=URL", http.StatusBadRequest)
		return
	}
	client := http.Client{Timeout: time.Minute, Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext:       (&net.Dialer{KeepAlive: -1}).DialContext,
	}}
	start := time.Now()
	resp, err := client.Get(url)
	var body []byte
	if err == nil {
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	fmt.Fprintf(w, "call_ms=%.1f\n", millis(time.Since(start)))
	if err != nil {
		fmt.Fprintf(w, "call_error=%s\n", oneLine(err.Error()))
		return
	}
	fmt.Fprintf(w, "call_status=%d\n", resp.StatusCode)
	for _, line := range bodyLines(body) {
		fmt.Fprintf(w, "callee.%s\n", line)
	}
}

func busy(w http.ResponseWriter, r *http.Request) {
	total, err := numberParam(r, "seconds", 10)
	if err != nil || total <= 0 {
		http.Error(w, "busy takes seconds=N", http.StatusBadRequest)
		return
	}
	go func() {
		end := time.Now().Add(seconds(total))
		x := uint64(1)
		for time.Now().Before(end) {
			for range 1_000_000 {
				x = x*6364136223846793005 + 1442695040888963407
			}
		}
		busySink.Store(x)
	}()
	fmt.Fprintf(w, "busy_seconds=%g\n", total)
}

// selfSigned makes a certificate for the HTTPS server. kubelet does not verify
// it.
func selfSigned() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "testapp"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"testapp", "localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}

// grpcHealth serves grpc.health.v1.Health/Check: SERVING (1) while /health is
// ok, NOT_SERVING (2) otherwise. Other methods are UNIMPLEMENTED (12).
func grpcHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/grpc")
	if r.Method != http.MethodPost || r.URL.Path != "/grpc.health.v1.Health/Check" {
		w.Header().Set("Grpc-Status", "12")
		w.WriteHeader(http.StatusOK)
		return
	}
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		return
	}
	status := byte(1)
	if !healthy.Load() {
		status = 2
	}
	w.Header().Set("Trailer", "Grpc-Status")
	w.WriteHeader(http.StatusOK)
	// A message of 2 bytes, not compressed: field 1 (status), varint.
	w.Write([]byte{0, 0, 0, 0, 2, 0x08, status})
	w.Header().Set("Grpc-Status", "0")
}

func udpEcho(conn net.PacketConn) {
	buf := make([]byte, 2048)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			fail(err)
		}
		conn.WriteTo(append([]byte("echo "), buf[:n]...), from)
	}
}

// The WebSocket opcodes (RFC 6455).
const (
	wsText  = 0x1
	wsClose = 0x8
	wsPing  = 0x9
	wsPong  = 0xa
)

// wsAccept is the Sec-WebSocket-Accept value for a key.
func wsAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// readFrame reads one unfragmented frame and unmasks its payload.
func readFrame(r *bufio.Reader) (byte, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return 0, nil, err
	}
	op := head[0] & 0x0f
	size := uint64(head[1] & 0x7f)
	switch size {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, nil, err
		}
		size = uint64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, nil, err
		}
		size = binary.BigEndian.Uint64(b[:])
	}
	if size > 1<<20 {
		return 0, nil, fmt.Errorf("frame of %d bytes", size)
	}
	var mask [4]byte
	masked := head[1]&0x80 != 0
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return op, payload, nil
}

// appendFrame appends a final frame, masked when mask is not nil.
func appendFrame(frame []byte, op byte, payload []byte, mask []byte) []byte {
	frame = append(frame, 0x80|op)
	bit := byte(0)
	if mask != nil {
		bit = 0x80
	}
	switch n := len(payload); {
	case n < 126:
		frame = append(frame, bit|byte(n))
	case n < 1<<16:
		frame = binary.BigEndian.AppendUint16(append(frame, bit|126), uint16(n))
	default:
		frame = binary.BigEndian.AppendUint64(append(frame, bit|127), uint64(n))
	}
	if mask == nil {
		return append(frame, payload...)
	}
	frame = append(frame, mask...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	return frame
}

// websocketEcho answers each text message with "conn=ID;n=N;text=TEXT", and
// sends a ping every ping= seconds.
func websocketEcho(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Sec-WebSocket-Key")
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || key == "" {
		http.Error(w, "not a WebSocket upgrade", http.StatusBadRequest)
		return
	}
	every, err := numberParam(r, "ping", 0)
	if err != nil {
		http.Error(w, "ws takes ping=SECONDS", http.StatusBadRequest)
		return
	}
	conn, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"+
		"Connection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", wsAccept(key))
	if err := rw.Flush(); err != nil {
		return
	}
	id := wsConnections.Add(1)
	var mu sync.Mutex
	send := func(op byte, payload []byte) error {
		mu.Lock()
		defer mu.Unlock()
		_, err := conn.Write(appendFrame(nil, op, payload, nil))
		return err
	}
	done := make(chan struct{})
	defer close(done)
	if every > 0 {
		go func() {
			ticker := time.NewTicker(seconds(every))
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					if send(wsPing, []byte("ping")) != nil {
						return
					}
				}
			}
		}()
	}
	for n := 0; ; {
		op, payload, err := readFrame(rw.Reader)
		if err != nil {
			return
		}
		switch op {
		case wsText:
			n++
			send(wsText, fmt.Appendf(nil, "conn=%d;n=%d;text=%s", id, n, payload))
		case wsPing:
			send(wsPong, payload)
		case wsClose:
			send(wsClose, payload)
			return
		}
	}
}

// A follow of /follow.
type following struct {
	sync.Mutex
	lines int
	last  string
	at    time.Time
	ended bool
	err   string
}

// The follows by name.
var follows struct {
	sync.Mutex
	byName map[string]*following
}

func follow(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("url")
	if url == "" {
		http.Error(w, "follow takes url=URL", http.StatusBadRequest)
		return
	}
	f := &following{at: time.Now()}
	follows.Lock()
	if follows.byName == nil {
		follows.byName = map[string]*following{}
	}
	follows.byName[r.URL.Query().Get("name")] = f
	follows.Unlock()
	go func() {
		client := http.Client{Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext:       (&net.Dialer{KeepAlive: -1}).DialContext,
		}}
		resp, err := client.Get(url)
		if err == nil {
			lines := bufio.NewScanner(resp.Body)
			for lines.Scan() {
				f.Lock()
				f.lines++
				f.last = lines.Text()
				f.at = time.Now()
				f.Unlock()
			}
			err = lines.Err()
			resp.Body.Close()
		}
		f.Lock()
		f.ended = true
		if err != nil {
			f.err = oneLine(err.Error())
		}
		f.Unlock()
	}()
	fmt.Fprintln(w, "follow=started")
}

func followed(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	follows.Lock()
	f := follows.byName[name]
	follows.Unlock()
	if f == nil {
		http.Error(w, fmt.Sprintf("no follow %q", name), http.StatusNotFound)
		return
	}
	f.Lock()
	defer f.Unlock()
	fmt.Fprintf(w, "lines=%d\nlast=%s\nlast_ago_ms=%d\nended=%t\nerror=%s\n",
		f.lines, f.last, time.Since(f.at).Milliseconds(), f.ended, f.err)
}

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func oneLine(text string) string {
	return strings.ReplaceAll(text, "\n", " ")
}

// bodyLines returns a body's lines, without the empty ones.
func bodyLines(body []byte) []string {
	var lines []string
	for _, line := range strings.Split(string(body), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
