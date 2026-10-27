// Command testapp is the workload of the smolvm-pods end-to-end tests.
//
//	testapp serve NAME ADDR            serve this process's state on ADDR until SIGTERM
//	testapp hold [-mib N] [-https ADDR] [-grpc ADDR] [-udp ADDR] [-unready] NAME ADDR
//	                                   fill N MiB (256), serve its hash, this process's identity
//	                                   and the clocks on ADDR, and print a counter line every
//	                                   100 ms, until SIGTERM (see hold.go); also serve the
//	                                   endpoints of wake.go
//	testapp get [-timeout D] [-times] URL
//	                                   print the body that URL returns, waiting at most D (5s);
//	                                   with -times, then the clocks before and after the request
//	testapp send [-timeout D] -bytes N URL
//	                                   POST N random bytes to URL, waiting at most D (5m)
//	testapp lookup [-timeout D] NAME   print the addresses of NAME, waiting at most D (10s)
//	testapp iface NAME                 print the MTU, MAC and addresses of a network interface
//	testapp echo WORD...               print the words
//	testapp cat                        copy stdin to stdout
//	testapp append PATH LINE           append LINE to the file at PATH, creating it, then
//	                                   print the file
//	testapp exit CODE                  print a line to stdout and to stderr, then exit with CODE
//	testapp sleep SECONDS              wait, printing a line before and after
//	testapp now                        print the realtime and monotonic clocks
//	testapp firstanswer [-timeout D] [-out FILE] URL CMD [ARG...]
//	                                   run CMD and poll URL from just before it starts; print when
//	                                   CMD started and ended and when URL first answered
//	testapp ipv6probe                  time smolvm's IPv6 probe in this network namespace
//	testapp fetch [-timeout D] [-n N] [-user-agent UA] URL...
//	                                   make N GETs of each URL at once on new connections and
//	                                   print each one's timings, status and body (see clients.go)
//	testapp ws [-timeout D] [-hold D] [-then-file PATH] URL
//	                                   talk to a WebSocket echo, keeping the connection
//	testapp pool [-timeout D] [-then-file PATH] URL
//	                                   make two GETs on one kept connection with TCP keepalive
//	testapp udp [-timeout D] [-n N] ADDR
//	                                   send datagrams to a UDP echo and count the answers
//	testapp ping [-timeout D] [-count N] IP
//	                                   send ICMP echo requests and count the replies
//	testapp grpchealth [-timeout D] ADDR
//	                                   call grpc.health.v1.Health/Check over h2c
//	testapp dial [-timeout D] ADDR     open and close a TCP connection
//	testapp burn THREADS SECONDS       keep THREADS threads busy for SECONDS, then print how
//	                                   many loops they ran
//
// GET / answers with the state of the serving process, one key=value pair per
// line, and prints a numbered line to stdout. GET /healthz answers ok and
// prints nothing. GET /stream serves the stream of wake.go, which VM pods
// follow. POST /sink reads the body and answers with its size and SHA-256.
// send prints the size and SHA-256 of what it sent, those that the server
// answered, and how long it took, one key=value pair per line, as do lookup
// and iface.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"debug/elf"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func main() {
	args := os.Args[1:]
	switch {
	case len(args) == 3 && args[0] == "serve":
		serve(args[1], args[2])
	case len(args) >= 3 && args[0] == "hold":
		hold(args[1:])
	case len(args) >= 2 && args[0] == "get":
		get(args[1:])
	case len(args) >= 2 && args[0] == "send":
		send(args[1:])
	case len(args) >= 2 && args[0] == "lookup":
		lookup(args[1:])
	case len(args) == 2 && args[0] == "iface":
		iface(args[1])
	case len(args) >= 1 && args[0] == "echo":
		fmt.Println(strings.Join(args[1:], " "))
	case len(args) == 1 && args[0] == "cat":
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			fail(err)
		}
	case len(args) == 3 && args[0] == "append":
		appendLine(args[1], args[2])
	case len(args) == 2 && args[0] == "sleep":
		sleep(args[1])
	case len(args) == 1 && args[0] == "now":
		now()
	case len(args) >= 3 && args[0] == "firstanswer":
		firstAnswer(args[1:])
	case len(args) == 1 && args[0] == "ipv6probe":
		ipv6Probe()
	case len(args) >= 2 && args[0] == "fetch":
		fetch(args[1:])
	case len(args) >= 2 && args[0] == "ws":
		wsClient(args[1:])
	case len(args) >= 2 && args[0] == "pool":
		pool(args[1:])
	case len(args) >= 2 && args[0] == "udp":
		udpClient(args[1:])
	case len(args) >= 2 && args[0] == "ping":
		ping(args[1:])
	case len(args) >= 2 && args[0] == "grpchealth":
		grpcHealthClient(args[1:])
	case len(args) >= 2 && args[0] == "dial":
		dial(args[1:])
	case len(args) == 3 && args[0] == "burn":
		burn(args[1], args[2])
	case len(args) == 2 && args[0] == "exit":
		code, err := strconv.Atoi(args[1])
		if err != nil {
			fail(err)
		}
		fmt.Println("stdout before exit", code)
		fmt.Fprintln(os.Stderr, "stderr before exit", code)
		os.Exit(code)
	default:
		fail(fmt.Errorf("unknown command %q", args))
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "testapp:", err)
	os.Exit(1)
}

// appendLine appends a line to a file, then prints the file as it reads back.
func appendLine(path, line string) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
	if err != nil {
		fail(err)
	}
	_, err = f.WriteString(line + "\n")
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		fail(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		fail(err)
	}
	os.Stdout.Write(data)
	if !strings.HasSuffix(string(data), line+"\n") {
		fail(fmt.Errorf("%s does not end with the appended line %q", path, line))
	}
}

// burn keeps threads busy for a time, each on an OS thread of its own.
func burn(threadsArg, secondsArg string) {
	threads, err := strconv.Atoi(threadsArg)
	if err != nil || threads < 1 {
		fail(fmt.Errorf("threads %q", threadsArg))
	}
	seconds, err := strconv.Atoi(secondsArg)
	if err != nil || seconds < 1 {
		fail(fmt.Errorf("seconds %q", secondsArg))
	}
	runtime.GOMAXPROCS(threads + 1)
	end := time.Now().Add(time.Duration(seconds) * time.Second)
	loops := make([]uint64, threads)
	var wg sync.WaitGroup
	for i := range threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			var n uint64
			for time.Now().Before(end) {
				for range 100000 {
					n++
				}
			}
			loops[i] = n
		}()
	}
	wg.Wait()
	var total uint64
	for _, n := range loops {
		total += n
	}
	fmt.Printf("threads=%d\nseconds=%d\nloops=%d\n", threads, seconds, total)
}

func serve(name, addr string) {
	// mu keeps the numbered lines in the order of the answers.
	var mu sync.Mutex
	var requests int
	http.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		state, err := describe(name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mu.Lock()
		requests++
		n := requests
		fmt.Printf("GET / request=%d\n", n)
		mu.Unlock()
		fmt.Fprintf(w, "%srequest=%d\n", state, n)
	})
	// VM pods follow a stream that this pod pushes.
	http.HandleFunc("GET /stream", stream)
	http.HandleFunc("POST /sink", func(w http.ResponseWriter, r *http.Request) {
		sum := sha256.New()
		n, err := io.Copy(sum, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "bytes=%d\nsha256=%x\n", n, sum.Sum(nil))
	})
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM)
		<-stop
		os.Exit(0)
	}()
	fmt.Printf("%s listening on %s\n", name, addr)
	fmt.Fprintf(os.Stderr, "%s started\n", name)
	fail(http.ListenAndServe(addr, nil))
}

// describe returns what this process sees of its executable and its kernel.
func describe(name string) (string, error) {
	exe, err := elf.Open("/proc/self/exe")
	if err != nil {
		return "", err
	}
	defer exe.Close()
	var b strings.Builder
	fmt.Fprintf(&b, "name=%s\npid=%d\narch=%s\nmachine=%d\n", name, os.Getpid(), runtime.GOARCH, int(exe.Machine))
	for _, f := range []struct{ key, path string }{
		{"boot_id", "/proc/sys/kernel/random/boot_id"},
		{"kernel", "/proc/version"},
		{"cmdline", "/proc/cmdline"},
	} {
		data, err := os.ReadFile(f.path)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(&b, "%s=%s\n", f.key, strings.TrimSpace(string(data)))
	}
	return b.String(), nil
}

func get(args []string) {
	flags := flag.NewFlagSet("get", flag.ExitOnError)
	timeout := flags.Duration("timeout", 5*time.Second, "how long to wait for the answer")
	times := flags.Bool("times", false, "print the clocks before and after the request")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("get takes one URL, not %q", flags.Args()))
	}
	url := flags.Arg(0)
	client := http.Client{Timeout: *timeout}
	startRealtime, startMonotonic := clock(clockRealtime), clock(clockMonotonic)
	resp, err := client.Get(url)
	if err != nil {
		fail(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		fail(err)
	}
	endRealtime, endMonotonic := clock(clockRealtime), clock(clockMonotonic)
	if resp.StatusCode != http.StatusOK {
		fail(fmt.Errorf("GET %s: %s", url, resp.Status))
	}
	os.Stdout.Write(body)
	if *times {
		fmt.Printf("request_start_realtime_ns=%d\nrequest_start_monotonic_ns=%d\n", startRealtime, startMonotonic)
		fmt.Printf("request_end_realtime_ns=%d\nrequest_end_monotonic_ns=%d\n", endRealtime, endMonotonic)
	}
}

// send streams random bytes to a sink in one request.
func send(args []string) {
	flags := flag.NewFlagSet("send", flag.ExitOnError)
	timeout := flags.Duration("timeout", 5*time.Minute, "how long the request may take")
	size := flags.Int64("bytes", 0, "how many bytes to send")
	flags.Parse(args)
	if flags.NArg() != 1 || *size <= 0 {
		fail(fmt.Errorf("send takes -bytes N and one URL, not %q", args))
	}
	url := flags.Arg(0)
	var seed [32]byte
	if _, err := cryptorand.Read(seed[:]); err != nil {
		fail(err)
	}
	sent := sha256.New()
	body := io.TeeReader(io.LimitReader(rand.NewChaCha8(seed), *size), sent)
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		fail(err)
	}
	req.ContentLength = *size
	client := http.Client{Timeout: *timeout}
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		fail(err)
	}
	answer, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if err != nil {
		fail(err)
	}
	if resp.StatusCode != http.StatusOK {
		fail(fmt.Errorf("POST %s: %s: %s", url, resp.Status, answer))
	}
	fmt.Printf("sent_bytes=%d\nsent_sha256=%x\nseconds=%.3f\n", *size, sent.Sum(nil), elapsed.Seconds())
	for _, line := range strings.Split(strings.TrimSpace(string(answer)), "\n") {
		fmt.Printf("received_%s\n", line)
	}
}

// lookup resolves a name as an app in the container would.
func lookup(args []string) {
	flags := flag.NewFlagSet("lookup", flag.ExitOnError)
	timeout := flags.Duration("timeout", 10*time.Second, "how long to wait for the answer")
	flags.Parse(args)
	if flags.NArg() != 1 {
		fail(fmt.Errorf("lookup takes one name, not %q", flags.Args()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupHost(ctx, flags.Arg(0))
	if err != nil {
		fail(err)
	}
	fmt.Printf("addrs=%s\n", strings.Join(addrs, " "))
}

// iface describes a network interface, with its addresses in CIDR form.
func iface(name string) {
	link, err := net.InterfaceByName(name)
	if err != nil {
		fail(err)
	}
	addrs, err := link.Addrs()
	if err != nil {
		fail(err)
	}
	var ipv4, ipv6 []string
	for _, addr := range addrs {
		ip, ok := addr.(*net.IPNet)
		switch {
		case !ok:
		case ip.IP.To4() != nil:
			ipv4 = append(ipv4, ip.String())
		default:
			ipv6 = append(ipv6, ip.String())
		}
	}
	fmt.Printf("mtu=%d\nmac=%s\nipv4=%s\nipv6=%s\n", link.MTU, link.HardwareAddr, strings.Join(ipv4, " "), strings.Join(ipv6, " "))
}
