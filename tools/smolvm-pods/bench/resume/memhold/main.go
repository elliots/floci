// memhold maps N MiB of anonymous memory, fills it with pseudo-random words
// (the first touch), and serves HTTP while it holds them:
//
//	/stats   first-touch and warm-rewrite times
//	/ping    a quick answer
//	/read    reads every word and checks the sum
//	/random  random 8-byte reads across the buffer
//	/hash    the SHA-256 of the whole buffer
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const seed = 0x9e3779b97f4a7c15

type holder struct {
	mu        sync.Mutex
	words     []uint64
	sum       uint64
	firstMs   float64
	rewriteMs float64
	chunks    map[string][]float64
	started   time.Time
}

// chunk is the unit of the per-chunk timings: 128 MiB of words.
const chunk = 128 << 20 / 8

// fill writes the words and returns their sum and the time of each chunk in ms.
func fill(words []uint64) (uint64, []float64) {
	x := uint64(seed)
	var sum uint64
	var chunks []float64
	t := time.Now()
	for i := range words {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		words[i] = x
		sum += x
		if (i+1)%chunk == 0 {
			chunks = append(chunks, ms(time.Since(t)))
			t = time.Now()
		}
	}
	return sum, chunks
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Connection", "close")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("reply: %v", err)
	}
}

func main() {
	mib := flag.Int("mib", 1024, "memory to hold, in MiB")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	h := &holder{started: time.Now()}
	size := *mib << 20
	mem, err := syscall.Mmap(-1, 0, size, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		log.Fatalf("mmap %d MiB: %v", *mib, err)
	}
	h.words = unsafe.Slice((*uint64)(unsafe.Pointer(&mem[0])), size/8)

	t := time.Now()
	var first, rewrite []float64
	h.sum, first = fill(h.words)
	h.firstMs = ms(time.Since(t))
	t = time.Now()
	sum, rewrite := fill(h.words)
	if sum != h.sum {
		log.Fatal("rewrite sum differs")
	}
	h.rewriteMs = ms(time.Since(t))
	h.chunks = map[string][]float64{"first_touch": first, "rewrite": rewrite}
	log.Printf("memhold: %d MiB first touch %.1f ms, rewrite %.1f ms", *mib, h.firstMs, h.rewriteMs)

	http.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"pid": os.Getpid(), "uptime_s": time.Since(h.started).Seconds()})
	})
	http.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		reply(w, map[string]any{"mib": *mib, "first_touch_ms": h.firstMs, "rewrite_ms": h.rewriteMs,
			"chunk_ms": h.chunks})
	})
	http.HandleFunc("/read", func(w http.ResponseWriter, _ *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		var sum uint64
		var chunks []float64
		t := time.Now()
		c := time.Now()
		for i, v := range h.words {
			sum += v
			if (i+1)%chunk == 0 {
				chunks = append(chunks, ms(time.Since(c)))
				c = time.Now()
			}
		}
		reply(w, map[string]any{"ms": ms(time.Since(t)), "ok": sum == h.sum, "chunk_ms": chunks})
	})
	http.HandleFunc("/random", func(w http.ResponseWriter, _ *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		// 8 Mi reads, or as many as fit in 10 s.
		const max = 8 << 20
		n := uint64(len(h.words))
		x := uint64(seed)
		var sum uint64
		reads := 0
		t := time.Now()
		for reads < max && (reads%4096 != 0 || time.Since(t) < 10*time.Second) {
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			sum += h.words[x%n]
			reads++
		}
		d := time.Since(t)
		reply(w, map[string]any{"ms": ms(d), "reads": reads, "ns_per_read": float64(d.Nanoseconds()) / float64(reads),
			"sum": fmt.Sprintf("%x", sum)})
	})
	http.HandleFunc("/hash", func(w http.ResponseWriter, _ *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		t := time.Now()
		sum := sha256.Sum256(mem)
		reply(w, map[string]any{"ms": ms(time.Since(t)), "sha256": hex.EncodeToString(sum[:])})
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}
