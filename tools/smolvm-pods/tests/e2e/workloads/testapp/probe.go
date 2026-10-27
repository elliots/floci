package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"time"
)

// now prints the realtime and monotonic clocks in ns.
func now() {
	fmt.Printf("realtime_ns=%d\nmonotonic_ns=%d\n", clock(clockRealtime), clock(clockMonotonic))
}

// sleep waits for a number of seconds.
func sleep(arg string) {
	seconds, err := strconv.Atoi(arg)
	if err != nil {
		fail(err)
	}
	fmt.Printf("sleeping %d s\n", seconds)
	time.Sleep(time.Duration(seconds) * time.Second)
	fmt.Println("slept")
}

// firstAnswer runs a command and, from just before it starts, polls a URL
// until it answers 200. Each attempt is a new connection that may take
// -attempt, and a new attempt starts every -interval, so an attempt whose
// SYN was lost does not delay the first answer. It prints when the command
// started and ended and when the first answer came, on the monotonic clock,
// with the command's exit code and the number of attempts.
func firstAnswer(args []string) {
	flags := flag.NewFlagSet("firstanswer", flag.ExitOnError)
	timeout := flags.Duration("timeout", 30*time.Second, "how long to poll")
	attempt := flags.Duration("attempt", 200*time.Millisecond, "how long one attempt may take")
	interval := flags.Duration("interval", 5*time.Millisecond, "time between the starts of attempts")
	out := flags.String("out", "", "file for the command's stdout (default: discard)")
	flags.Parse(args)
	if flags.NArg() < 2 {
		fail(fmt.Errorf("firstanswer takes a URL and a command, not %q", flags.Args()))
	}
	url, command := flags.Arg(0), flags.Args()[1:]
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stderr = os.Stderr
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fail(err)
		}
		defer f.Close()
		cmd.Stdout = f
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var attempts atomic.Int64
	answered := make(chan int64, 1)
	start := clock(clockMonotonic)
	go poll(ctx, url, *attempt, *interval, &attempts, answered)
	if err := cmd.Start(); err != nil {
		fail(err)
	}
	code := 0
	var exitErr *exec.ExitError
	if err := cmd.Wait(); errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	} else if err != nil {
		fail(err)
	}
	end := clock(clockMonotonic)
	fmt.Printf("command_start_monotonic_ns=%d\ncommand_end_monotonic_ns=%d\ncommand_exit=%d\n", start, end, code)
	select {
	case first := <-answered:
		fmt.Printf("first_answer_monotonic_ns=%d\nattempts=%d\n", first, attempts.Load())
	case <-ctx.Done():
		fmt.Printf("attempts=%d\n", attempts.Load())
		fail(fmt.Errorf("no answer from %s within %v", url, *timeout))
	}
}

// poll starts an attempt every interval until one answers 200, and sends the
// monotonic time of that answer.
func poll(ctx context.Context, url string, attempt, interval time.Duration, attempts *atomic.Int64, answered chan<- int64) {
	client := http.Client{Timeout: attempt, Transport: &http.Transport{DisableKeepAlives: true}}
	inFlight := make(chan struct{}, 64)
	var done atomic.Bool
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for !done.Load() {
		select {
		case <-ctx.Done():
			return
		case inFlight <- struct{}{}:
		}
		attempts.Add(1)
		go func() {
			defer func() { <-inFlight }()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && done.CompareAndSwap(false, true) {
				answered <- clock(clockMonotonic)
			}
		}()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// ipv6Probe times smolvm's IPv6 probe (host_has_ipv6_route in its
// smolvm-network crate) in this process's network namespace: a UDP connect
// to an IPv6 address, and only if that works, TCP handshakes to two IPv6
// hosts raced with a 1 s timeout.
func ipv6Probe() {
	start := time.Now()
	route := false
	udp, err := net.DialUDP("udp6", &net.UDPAddr{IP: net.IPv6unspecified}, &net.UDPAddr{
		IP: net.ParseIP("2606:4700:4700::1111"), Port: 53})
	if err == nil {
		udp.Close()
		ok := make(chan bool, 2)
		for _, target := range []string{"[2606:4700:4700::1111]:443", "[2001:4860:4860::8888]:443"} {
			go func() {
				conn, err := net.DialTimeout("tcp6", target, time.Second)
				if err == nil {
					conn.Close()
				}
				ok <- err == nil
			}()
		}
		route = <-ok || <-ok
	}
	fmt.Printf("ms=%.3f\nroute=%t\n", float64(time.Since(start).Microseconds())/1000, route)
}
