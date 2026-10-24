package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	neturl "net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// The clients of the wake test. Each prints key=value lines and exits with 0
// when it could run, also when a request failed, so that the test judges the
// outcome.

// fetch makes N GET requests of each URL, all at once, each on a new
// connection without keepalive, and prints for request I (the requests of
// the first URL first):
//
//	I.connect_ms     from the start of the request until the TCP connection was up
//	I.first_byte_ms  from the start of the request until the first byte of the answer
//	I.total_ms       from the start of the request until the body was read or it failed
//	I.status, I.body_bytes, and the body as I.body.KEY=VALUE, or I.body=LINE
//	I.error          when it failed
func fetch(args []string) {
	flags := flag.NewFlagSet("fetch", flag.ExitOnError)
	timeout := flags.Duration("timeout", 10*time.Second, "how long each request may take")
	count := flags.Int("n", 1, "how many requests to make at once")
	agent := flags.String("user-agent", "", "the User-Agent header")
	flags.Parse(args)
	if flags.NArg() < 1 || *count < 1 {
		fail(fmt.Errorf("fetch takes [-n N] URL..., not %q", args))
	}
	results := make([]string, *count*flags.NArg())
	var ready, done sync.WaitGroup
	ready.Add(1)
	for i := range results {
		url := flags.Arg(i / *count)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Wait()
			results[i] = fetchOne(i, url, *timeout, *agent)
		}()
	}
	ready.Done()
	done.Wait()
	for _, result := range results {
		fmt.Print(result)
	}
}

func fetchOne(i int, url string, timeout time.Duration, agent string) string {
	var out strings.Builder
	put := func(key, value string) { fmt.Fprintf(&out, "%d.%s=%s\n", i, key, value) }
	client := http.Client{Timeout: timeout, Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext:       (&net.Dialer{KeepAlive: -1}).DialContext,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		put("error", oneLine(err.Error()))
		return out.String()
	}
	if agent != "" {
		req.Header.Set("User-Agent", agent)
	}
	var mu sync.Mutex
	var connected, firstByte time.Duration
	start := time.Now()
	trace := &httptrace.ClientTrace{
		ConnectDone: func(network, addr string, err error) {
			if err == nil {
				mu.Lock()
				connected = time.Since(start)
				mu.Unlock()
			}
		},
		GotFirstResponseByte: func() {
			mu.Lock()
			firstByte = time.Since(start)
			mu.Unlock()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := client.Do(req)
	var body []byte
	if err == nil {
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	total := time.Since(start)
	mu.Lock()
	defer mu.Unlock()
	put("total_ms", fmt.Sprintf("%.1f", millis(total)))
	if connected > 0 {
		put("connect_ms", fmt.Sprintf("%.1f", millis(connected)))
	}
	if firstByte > 0 {
		put("first_byte_ms", fmt.Sprintf("%.1f", millis(firstByte)))
	}
	if resp != nil {
		put("status", fmt.Sprint(resp.StatusCode))
	}
	if err != nil {
		put("error", oneLine(err.Error()))
		return out.String()
	}
	put("body_bytes", fmt.Sprint(len(body)))
	for _, line := range bodyLines(body) {
		if strings.Contains(line, "=") {
			put("body."+line[:strings.IndexByte(line, '=')], line[strings.IndexByte(line, '=')+1:])
		} else {
			put("body", line)
		}
	}
	return out.String()
}

// wsClient opens a WebSocket without TCP keepalive, sends "first" and prints
// the answer; with -hold, keeps the connection open that long, answering the
// server's pings; with -then-file, waits until that file exists, then sends
// "second" on the same connection and prints the answer and how long it took.
// It prints first_conn and second_conn, the server's numbers of the
// connections that answered, and pings, the pings it answered.
func wsClient(args []string) {
	flags := flag.NewFlagSet("ws", flag.ExitOnError)
	timeout := flags.Duration("timeout", 5*time.Minute, "how long the whole exchange may take")
	hold := flags.Duration("hold", 0, "how long to keep the connection after the first answer")
	thenFile := flags.String("then-file", "", "send the second message once this file exists")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("ws takes one URL, not %q", flags.Args()))
	}
	deadline := time.Now().Add(*timeout)
	ws, err := dialWebSocket(flags.Arg(0))
	if err != nil {
		fail(err)
	}
	defer ws.conn.Close()
	reply, err := ws.exchange("first", deadline)
	if err != nil {
		fail(fmt.Errorf("first message: %w", err))
	}
	fmt.Printf("first_reply=%s\nfirst_conn=%s\n", reply, wsField(reply, "conn"))
	if *hold > 0 {
		if err := ws.listen(time.Now().Add(*hold)); err != nil {
			fail(fmt.Errorf("while holding the connection: %w", err))
		}
	}
	fmt.Printf("pings=%d\n", ws.pings)
	if *thenFile != "" {
		if err := waitForFile(*thenFile, deadline); err != nil {
			fail(err)
		}
		start := time.Now()
		reply, err := ws.exchange("second", deadline)
		if err != nil {
			fail(fmt.Errorf("second message: %w", err))
		}
		fmt.Printf("second_reply=%s\nsecond_conn=%s\nsecond_ms=%.1f\n", reply, wsField(reply, "conn"), millis(time.Since(start)))
	}
	ws.send(wsClose, nil)
}

type webSocket struct {
	conn   net.Conn
	reader *bufio.Reader
	pings  int
}

func dialWebSocket(rawURL string) (*webSocket, error) {
	u, err := neturl.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: -1}).Dial("tcp", u.Host)
	if err != nil {
		return nil, err
	}
	var nonce [16]byte
	rand.Read(nonce[:])
	key := base64.StdEncoding.EncodeToString(nonce[:])
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n", u.RequestURI(), u.Host, key)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Sec-WebSocket-Accept") != wsAccept(key) {
		conn.Close()
		return nil, fmt.Errorf("not a WebSocket answer: %s", resp.Status)
	}
	return &webSocket{conn: conn, reader: reader}, nil
}

func (ws *webSocket) send(op byte, payload []byte) error {
	var mask [4]byte
	rand.Read(mask[:])
	_, err := ws.conn.Write(appendFrame(nil, op, payload, mask[:]))
	return err
}

// next reads frames until one that is not a ping, answering pings.
func (ws *webSocket) next(deadline time.Time) (byte, []byte, error) {
	ws.conn.SetReadDeadline(deadline)
	for {
		op, payload, err := readFrame(ws.reader)
		if err != nil {
			return 0, nil, err
		}
		if op != wsPing {
			return op, payload, nil
		}
		ws.pings++
		if err := ws.send(wsPong, payload); err != nil {
			return 0, nil, err
		}
	}
}

func (ws *webSocket) exchange(text string, deadline time.Time) (string, error) {
	if err := ws.send(wsText, []byte(text)); err != nil {
		return "", err
	}
	op, payload, err := ws.next(deadline)
	if err != nil {
		return "", err
	}
	if op != wsText {
		return "", fmt.Errorf("frame with opcode %d, not text", op)
	}
	return string(payload), nil
}

// listen answers pings until the deadline.
func (ws *webSocket) listen(until time.Time) error {
	op, _, err := ws.next(until)
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("unexpected frame with opcode %d", op)
}

// wsField returns a field of the server's "conn=ID;n=N;text=TEXT" answer.
func wsField(reply, name string) string {
	for _, field := range strings.Split(reply, ";") {
		if value, ok := strings.CutPrefix(field, name+"="); ok {
			return value
		}
	}
	return ""
}

func waitForFile(path string, deadline time.Time) error {
	for {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not appear", path)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// pool makes a request through an HTTP client that keeps its connection,
// with Go's default TCP keepalive (probes after 15 s idle, every 15 s); with
// -then-file, waits until that file exists, prints how many keepalive probes
// of the connection are unanswered (keepalive_probes, tcpi_probes), and
// makes a second request. It prints NAME.status, NAME.reused, NAME.local,
// NAME.first_byte_ms, NAME.error and the body as NAME.body.KEY=VALUE for the
// requests first and second.
func pool(args []string) {
	flags := flag.NewFlagSet("pool", flag.ExitOnError)
	timeout := flags.Duration("timeout", 5*time.Minute, "how long to wait for the file")
	thenFile := flags.String("then-file", "", "make the second request once this file exists")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("pool takes one URL, not %q", flags.Args()))
	}
	url := flags.Arg(0)
	var mu sync.Mutex
	var last net.Conn
	dialer := &net.Dialer{}
	client := &http.Client{Timeout: time.Minute, Transport: &http.Transport{
		MaxIdleConnsPerHost: 1,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err == nil {
				mu.Lock()
				last = conn
				mu.Unlock()
			}
			return conn, err
		},
	}}
	request := func(name string) {
		var info httptrace.GotConnInfo
		var firstByte time.Duration
		start := time.Now()
		trace := &httptrace.ClientTrace{
			GotConn:              func(i httptrace.GotConnInfo) { info = i },
			GotFirstResponseByte: func() { firstByte = time.Since(start) },
		}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			fail(err)
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
		resp, err := client.Do(req)
		var body []byte
		if err == nil {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		if info.Conn != nil {
			fmt.Printf("%s.reused=%t\n%s.local=%s\n", name, info.Reused, name, info.Conn.LocalAddr())
		}
		if firstByte > 0 {
			fmt.Printf("%s.first_byte_ms=%.1f\n", name, millis(firstByte))
		}
		if err != nil {
			fmt.Printf("%s.error=%s\n", name, oneLine(err.Error()))
			return
		}
		fmt.Printf("%s.status=%d\n", name, resp.StatusCode)
		for _, line := range bodyLines(body) {
			fmt.Printf("%s.body.%s\n", name, line)
		}
	}
	request("first")
	if *thenFile == "" {
		return
	}
	if err := waitForFile(*thenFile, time.Now().Add(*timeout)); err != nil {
		fail(err)
	}
	mu.Lock()
	conn := last
	mu.Unlock()
	if conn != nil {
		if info, err := tcpInfo(conn); err == nil {
			fmt.Printf("keepalive_probes=%d\ntcp_state=%d\n", info.Probes, info.State)
		} else {
			fmt.Printf("tcp_info_error=%s\n", oneLine(err.Error()))
		}
	}
	request("second")
}

// tcpInfo reads TCP_INFO of a TCP connection.
func tcpInfo(conn net.Conn) (syscall.TCPInfo, error) {
	var info syscall.TCPInfo
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return info, fmt.Errorf("%T is not a TCP connection", conn)
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return info, err
	}
	var errno syscall.Errno
	err = raw.Control(func(fd uintptr) {
		size := uint32(unsafe.Sizeof(info))
		_, _, errno = syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO,
			uintptr(unsafe.Pointer(&info)), uintptr(unsafe.Pointer(&size)), 0)
	})
	if err != nil {
		return info, err
	}
	if errno != 0 {
		return info, errno
	}
	return info, nil
}

// udpClient sends N datagrams to ADDR, each waiting up to -timeout for the
// echo, and prints sent and received.
func udpClient(args []string) {
	flags := flag.NewFlagSet("udp", flag.ExitOnError)
	timeout := flags.Duration("timeout", time.Second, "how long to wait for each echo")
	count := flags.Int("n", 3, "how many datagrams to send")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("udp takes one address, not %q", flags.Args()))
	}
	conn, err := net.Dial("udp", flags.Arg(0))
	if err != nil {
		fail(err)
	}
	defer conn.Close()
	received := 0
	buf := make([]byte, 2048)
	for i := range *count {
		want := fmt.Sprintf("datagram %d", i)
		if _, err := conn.Write([]byte(want)); err != nil {
			fmt.Printf("error=%s\n", oneLine(err.Error()))
			continue
		}
		conn.SetReadDeadline(time.Now().Add(*timeout))
		n, err := conn.Read(buf)
		if err == nil && string(buf[:n]) == "echo "+want {
			received++
		}
	}
	fmt.Printf("sent=%d\nreceived=%d\n", *count, received)
}

// ping sends ICMP echo requests from a raw socket, each waiting up to
// -timeout for its reply, and prints sent and received.
func ping(args []string) {
	flags := flag.NewFlagSet("ping", flag.ExitOnError)
	timeout := flags.Duration("timeout", time.Second, "how long to wait for each reply")
	count := flags.Int("count", 3, "how many echo requests to send")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("ping takes one IPv4 address, not %q", flags.Args()))
	}
	dst := net.ParseIP(flags.Arg(0)).To4()
	if dst == nil {
		fail(fmt.Errorf("%q is not an IPv4 address", flags.Arg(0)))
	}
	conn, err := net.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		fail(err)
	}
	defer conn.Close()
	id := os.Getpid() & 0xffff
	received := 0
	buf := make([]byte, 1500)
	for seq := 1; seq <= *count; seq++ {
		msg := []byte{8, 0, 0, 0, byte(id >> 8), byte(id), byte(seq >> 8), byte(seq), 'r', '4'}
		sum := icmpChecksum(msg)
		msg[2], msg[3] = byte(sum>>8), byte(sum)
		if _, err := conn.WriteTo(msg, &net.IPAddr{IP: dst}); err != nil {
			fmt.Printf("error=%s\n", oneLine(err.Error()))
			continue
		}
		deadline := time.Now().Add(*timeout)
		conn.SetReadDeadline(deadline)
		for {
			n, from, err := conn.ReadFrom(buf)
			if err != nil {
				break
			}
			reply := buf[:n]
			if n >= 8 && reply[0] == 0 && from.(*net.IPAddr).IP.Equal(dst) &&
				int(reply[4])<<8|int(reply[5]) == id && int(reply[6])<<8|int(reply[7]) == seq {
				received++
				break
			}
		}
	}
	fmt.Printf("sent=%d\nreceived=%d\n", *count, received)
}

func icmpChecksum(msg []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(msg); i += 2 {
		sum += uint32(msg[i])<<8 | uint32(msg[i+1])
	}
	if len(msg)%2 == 1 {
		sum += uint32(msg[len(msg)-1]) << 8
	}
	for sum>>16 != 0 {
		sum = sum&0xffff + sum>>16
	}
	return ^uint16(sum)
}

// grpcHealthClient calls grpc.health.v1.Health/Check at ADDR over
// unencrypted HTTP/2, as kubelet's gRPC probe does, and prints the HTTP
// status, grpc-status and the serving status.
func grpcHealthClient(args []string) {
	flags := flag.NewFlagSet("grpchealth", flag.ExitOnError)
	timeout := flags.Duration("timeout", 5*time.Second, "how long the call may take")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("grpchealth takes one address, not %q", flags.Args()))
	}
	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	client := http.Client{Timeout: *timeout, Transport: &http.Transport{Protocols: &protocols}}
	url := "http://" + flags.Arg(0) + "/grpc.health.v1.Health/Check"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte{0, 0, 0, 0, 0}))
	if err != nil {
		fail(err)
	}
	req.Header.Set("Content-Type", "application/grpc")
	req.Header.Set("TE", "trailers")
	start := time.Now()
	resp, err := client.Do(req)
	var body []byte
	if err == nil {
		body, err = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	fmt.Printf("total_ms=%.1f\n", millis(time.Since(start)))
	if err != nil {
		fmt.Printf("error=%s\n", oneLine(err.Error()))
		return
	}
	status := resp.Trailer.Get("Grpc-Status")
	if status == "" {
		status = resp.Header.Get("Grpc-Status")
	}
	serving := "UNKNOWN"
	if len(body) == 7 && body[5] == 0x08 {
		switch body[6] {
		case 1:
			serving = "SERVING"
		case 2:
			serving = "NOT_SERVING"
		}
	}
	fmt.Printf("http_status=%d\nproto=%s\ngrpc_status=%s\nserving=%s\n", resp.StatusCode, resp.Proto, status, serving)
}

// dial opens a TCP connection to ADDR and closes it, as kubelet's TCP probe
// does, and prints connect_ms or error.
func dial(args []string) {
	flags := flag.NewFlagSet("dial", flag.ExitOnError)
	timeout := flags.Duration("timeout", 2*time.Second, "how long the connect may take")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("dial takes one address, not %q", flags.Args()))
	}
	start := time.Now()
	conn, err := net.DialTimeout("tcp", flags.Arg(0), *timeout)
	if err != nil {
		fmt.Printf("total_ms=%.1f\nerror=%s\n", millis(time.Since(start)), oneLine(err.Error()))
		return
	}
	fmt.Printf("connect_ms=%.1f\n", millis(time.Since(start)))
	conn.Close()
}
