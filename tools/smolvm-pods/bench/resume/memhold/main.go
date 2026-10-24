// memhold maps N MiB of anonymous memory, fills it with pseudo-random words
// (the first touch), and serves HTTP while it holds them. It also maps a
// reserve that it does not touch until a dirtying request asks for it.
//
//	/stats   first-touch and warm-rewrite times
//	/ping    a quick answer
//	/read    reads every word and checks the sum
//	/random  random 8-byte reads across the buffer
//	/hash    the SHA-256 of the whole buffer
//	/dirty   changes memory with a pattern, then records the SHA-256 of the
//	         memory in use as the expected one (see dirty)
//	/verify  compares the SHA-256 of the memory in use with the expected one
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

const seed = 0x9e3779b97f4a7c15

// block is the unit in which smolvm saves guest RAM, page the guest's page.
const (
	block = 2 << 20
	page  = 4 << 10
)

// patterns are the dirtying patterns that mixed takes in turn.
var patterns = []string{"scattered", "blocks", "zero", "free", "fresh"}

type holder struct {
	mu        sync.Mutex
	mem       []byte // the held memory, then the reserve
	all       []uint64
	words     []uint64 // the held memory
	used      int      // bytes in use: the held memory and the reserve touched so far
	zeroed    []bool   // held blocks that dirty zeroed
	gen       uint64   // dirtying requests so far
	sealed    bool
	expected  [sha256.Size]byte
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

// mapAligned maps size bytes of anonymous memory at a 2 MiB boundary, so that
// a block of the buffer is a block of a huge page.
func mapAligned(size int) ([]byte, error) {
	raw, err := syscall.Mmap(-1, 0, size+block, syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	skip := (block - int(uintptr(unsafe.Pointer(&raw[0]))%block)) % block
	return raw[skip : skip+size], nil
}

// write fills words with new pseudo-random words, never zero, that depend on
// the request and the offset, and returns the bytes written.
func (h *holder) write(from, n int) int {
	x := (seed ^ h.gen*0xbf58476d1ce4e5b9 ^ uint64(from)*0x94d049bb133111eb) | 1
	words := h.all[from/8 : (from+n)/8]
	for i := range words {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		words[i] = x
	}
	return n
}

// pick returns k distinct random numbers below n (Floyd's sampling, which
// allocates only for the k).
func pick(rng *rand.Rand, n, k int) []int {
	k = min(k, n)
	seen := make(map[int]bool, k)
	out := make([]int, 0, k)
	for j := n - k; j < n; j++ {
		t := rng.IntN(j + 1)
		if seen[t] {
			t = j
		}
		seen[t] = true
		out = append(out, t)
	}
	return out
}

// dirty changes memory with pattern; percent is a share of the held memory:
//
//	scattered  4 KiB pages at random places, percent of the held pages
//	blocks     whole 2 MiB blocks, percent of the held blocks
//	all        every held block (blocks with 100)
//	zero       zeros over percent of the held blocks, among those that hold data
//	free       frees percent of the held blocks (MADV_DONTNEED, so that the
//	           guest kernel can report them free), waits settle, then writes
//	           them again, which allocates new pages
//	fresh      memory of the reserve never touched before, percent of the
//	           held memory's size, up to what the reserve has left
//	mixed      one of scattered, blocks, zero, free and fresh, in turn
//	none       nothing
//
// It returns what it did.
func (h *holder) dirty(pattern string, percent float64, settle time.Duration) (map[string]any, error) {
	h.gen++
	if pattern == "mixed" {
		pattern = patterns[(h.gen-1)%uint64(len(patterns))]
	}
	if pattern == "all" {
		pattern, percent = "blocks", 100
	}
	rng := rand.New(rand.NewPCG(seed, h.gen))
	held := len(h.words) * 8
	blocks := held / block
	count := func(units int) int { return int(float64(units)*percent/100 + 0.5) }
	touched := map[int]bool{}
	written := 0
	t := time.Now()
	switch pattern {
	case "none":
	case "scattered":
		for _, p := range pick(rng, held/page, count(held/page)) {
			written += h.write(p*page, page)
			touched[p*page/block] = true
		}
	case "blocks":
		for _, b := range pick(rng, blocks, count(blocks)) {
			written += h.write(b*block, block)
			touched[b] = true
			h.zeroed[b] = false
		}
	case "zero":
		var data []int
		for b, z := range h.zeroed {
			if !z {
				data = append(data, b)
			}
		}
		for _, i := range pick(rng, len(data), count(blocks)) {
			b := data[i]
			clear(h.mem[b*block : (b+1)*block])
			written += block
			touched[b] = true
			h.zeroed[b] = true
		}
	case "free":
		chosen := pick(rng, blocks, count(blocks))
		for _, b := range chosen {
			if err := syscall.Madvise(h.mem[b*block:(b+1)*block], syscall.MADV_DONTNEED); err != nil {
				return nil, fmt.Errorf("madvise: %v", err)
			}
		}
		time.Sleep(settle)
		for _, b := range chosen {
			written += h.write(b*block, block)
			touched[b] = true
			h.zeroed[b] = false
		}
	case "fresh":
		n := min((count(held)+block-1)/block*block, len(h.mem)-h.used)
		written += h.write(h.used, n)
		for b := h.used / block; b < (h.used+n)/block; b++ {
			touched[b] = true
		}
		h.used += n
	default:
		return nil, fmt.Errorf("unknown pattern %q", pattern)
	}
	writeMs := ms(time.Since(t))
	t = time.Now()
	h.sum = 0
	for _, v := range h.words {
		h.sum += v
	}
	h.expected = sha256.Sum256(h.mem[:h.used])
	h.sealed = true
	return map[string]any{"pattern": pattern, "percent": percent, "gen": h.gen, "bytes_written": written,
		"blocks_touched": len(touched), "write_ms": writeMs, "hash_ms": ms(time.Since(t)), "used": h.used,
		"reserve_left": len(h.mem) - h.used}, nil
}

func main() {
	mib := flag.Int("mib", 1024, "memory to hold, in MiB")
	reserve := flag.Int("reserve", 0, "memory to map but leave untouched until /dirty?pattern=fresh, in MiB")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	h := &holder{started: time.Now()}
	size := *mib << 20
	mem, err := mapAligned(size + *reserve<<20)
	if err != nil {
		log.Fatalf("mmap %d MiB: %v", *mib+*reserve, err)
	}
	h.mem = mem
	h.all = unsafe.Slice((*uint64)(unsafe.Pointer(&mem[0])), len(mem)/8)
	h.words = h.all[:size/8]
	h.used = size
	h.zeroed = make([]bool, size/block)

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
		sum := sha256.Sum256(mem[:size])
		reply(w, map[string]any{"ms": ms(time.Since(t)), "sha256": hex.EncodeToString(sum[:])})
	})
	// /dirty?pattern=P&percent=N&settle_ms=MS: see dirty. pattern=none only
	// records the expected SHA-256.
	http.HandleFunc("/dirty", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		percent, err := strconv.ParseFloat(q.Get("percent"), 64)
		if q.Get("percent") == "" {
			percent, err = 0, nil
		}
		if err != nil || percent < 0 || percent > 100 {
			http.Error(w, "percent must be a number from 0 to 100", http.StatusBadRequest)
			return
		}
		settle := 3 * time.Second
		if v := q.Get("settle_ms"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				http.Error(w, "settle_ms must be a whole number of ms", http.StatusBadRequest)
				return
			}
			settle = time.Duration(n) * time.Millisecond
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		v, err := h.dirty(q.Get("pattern"), percent, settle)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		reply(w, v)
	})
	http.HandleFunc("/verify", func(w http.ResponseWriter, _ *http.Request) {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.sealed {
			http.Error(w, "no expected hash: request /dirty first", http.StatusConflict)
			return
		}
		t := time.Now()
		sum := sha256.Sum256(h.mem[:h.used])
		reply(w, map[string]any{"ms": ms(time.Since(t)), "ok": sum == h.expected, "used": h.used,
			"sha256": hex.EncodeToString(sum[:]), "expected": hex.EncodeToString(h.expected[:])})
	})
	log.Fatal(http.ListenAndServe(*addr, nil))
}
