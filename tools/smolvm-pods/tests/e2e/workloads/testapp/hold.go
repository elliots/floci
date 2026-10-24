package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// The clocks of clock_gettime, from linux/time.h.
const (
	clockRealtime  = 0
	clockMonotonic = 1
	clockBoottime  = 7
)

// hold is the app of the hibernation, wake and scale tests. It fills a memory
// region with pseudo-random words from a random seed, then serves on ADDR:
//
//	/hash   the SHA-256 of the region
//	/id     its pid, its start time in clock ticks after boot, and the boot id
//	/clock  the realtime, monotonic and boot clocks, in ns
//	/ticks  the last counter printed
//	/ping   a quick answer
//
// and the endpoints of wake.go, and prints "tick N" to stdout every
// 100 ms, N counting from 1.
func hold(args []string) {
	flags := flag.NewFlagSet("hold", flag.ExitOnError)
	mib := flags.Int("mib", 256, "MiB of memory to fill and hold")
	httpsAddr := flags.String("https", "", "also serve the endpoints over TLS on this address")
	grpcAddr := flags.String("grpc", "", "serve a gRPC health service over h2c on this address")
	udpAddr := flags.String("udp", "", "echo UDP datagrams on this address")
	unready := flags.Bool("unready", false, "answer /health with 500 from the start")
	flags.Parse(args)
	if flags.NArg() != 2 || *mib <= 0 {
		fail(fmt.Errorf("hold takes [-mib N] NAME ADDR, not %q", args))
	}
	name, addr := flags.Arg(0), flags.Arg(1)
	region, err := syscall.Mmap(-1, 0, *mib<<20, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		fail(err)
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		fail(err)
	}
	start := time.Now()
	fill(region, binary.LittleEndian.Uint64(seed[:])|1)
	fmt.Fprintf(os.Stderr, "%s filled %d MiB in %v\n", name, *mib, time.Since(start).Round(time.Millisecond))
	id, err := identity(name)
	if err != nil {
		fail(err)
	}

	var ticks atomic.Int64
	http.HandleFunc("GET /ticks", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "tick=%d\n", ticks.Load())
	})
	http.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})
	http.HandleFunc("GET /id", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, id)
	})
	http.HandleFunc("GET /hash", func(w http.ResponseWriter, r *http.Request) {
		t := time.Now()
		sum := sha256.Sum256(region)
		fmt.Fprintf(w, "sha256=%x\nmib=%d\nms=%.1f\n", sum, *mib, float64(time.Since(t).Microseconds())/1000)
	})
	http.HandleFunc("GET /clock", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "realtime_ns=%d\nmonotonic_ns=%d\nboottime_ns=%d\n",
			clock(clockRealtime), clock(clockMonotonic), clock(clockBoottime))
	})
	registerWakeEndpoints(*unready)
	startWakeServers(*httpsAddr, *grpcAddr, *udpAddr)
	go func() {
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, syscall.SIGTERM)
		<-stop
		os.Exit(0)
	}()
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for n := 1; ; n++ {
			<-ticker.C
			fmt.Printf("tick %d\n", n)
			ticks.Store(int64(n))
		}
	}()
	fmt.Printf("%s holding %d MiB, listening on %s\n", name, *mib, addr)
	fail(http.Serve(listen(addr), nil))
}

// fill writes xorshift64 words from seed into region.
func fill(region []byte, seed uint64) {
	words := unsafe.Slice((*uint64)(unsafe.Pointer(&region[0])), len(region)/8)
	x := seed
	for i := range words {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		words[i] = x
	}
}

// identity returns what /id answers: this process and its kernel.
func identity(name string) (string, error) {
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return "", err
	}
	// The command name can hold spaces, so the fields start after its ")".
	fields := strings.Fields(string(stat[strings.LastIndexByte(string(stat), ')')+1:]))
	if len(fields) < 20 {
		return "", fmt.Errorf("/proc/self/stat has %d fields after the name", len(fields))
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("name=%s\npid=%d\nstart_ticks=%s\nboot_id=%s\n",
		name, os.Getpid(), fields[19], strings.TrimSpace(string(bootID))), nil
}

// clock reads a clock of clock_gettime in ns.
func clock(id uintptr) int64 {
	var ts syscall.Timespec
	if _, _, e := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, id, uintptr(unsafe.Pointer(&ts)), 0); e != 0 {
		fail(fmt.Errorf("clock_gettime(%d): %v", id, e))
	}
	return ts.Nano()
}
