// Command litmus counts message-passing reorderings between two threads.
//
// A writer stores x = i and then y = i for i from 1 to n. A reader loads y and
// then x, and counts the reads that see x < y. Total store order (x86, and Arm
// CPUs in Apple's TSO mode) forbids that outcome; the default Arm memory model
// allows it. The writer runs on CPU 0 and the reader on CPU 1.
//
//	litmus [-n stores] [-runs runs] [-min-racing reads]
//
// The last line of the output is
//
//	RESULT arch=ARCH violations=V racing=R
//
// where V counts the violations in all runs, and R is the least number of
// racing reads in a run: reads that saw y change since the reader's previous
// read, so both threads ran between them. The exit code is 0 when V is 0 and R
// reaches -min-racing, 3 when V is not 0, and 4 when the threads overlapped
// too little for the result to mean anything.
package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// writer and reader are written in assembly, so that each access is one plain
// load or store, in program order.
func writer(x, y *uint64, n uint64)
func reader(x, y *uint64, n, maxReads uint64) (reads, racing, violations uint64)

// shared keeps x and y on separate cache lines.
type shared struct {
	_ [128]byte
	x uint64
	_ [120]byte
	y uint64
	_ [120]byte
}

func main() {
	n := flag.Uint64("n", 20_000_000, "stores per run")
	runs := flag.Int("runs", 3, "runs")
	minRacing := flag.Uint64("min-racing", 10_000, "racing reads needed in each run")
	flag.Parse()
	if runtime.NumCPU() < 2 {
		fmt.Fprintln(os.Stderr, "litmus: needs 2 CPUs, has", runtime.NumCPU())
		os.Exit(2)
	}
	racing, violations := uint64(math.MaxUint64), uint64(0)
	for run := range *runs {
		r, ra, v := once(*n)
		fmt.Printf("run=%d reads=%d racing=%d violations=%d\n", run, r, ra, v)
		racing = min(racing, ra)
		violations += v
	}
	fmt.Printf("RESULT arch=%s violations=%d racing=%d\n", runtime.GOARCH, violations, racing)
	switch {
	case violations > 0:
		os.Exit(3)
	case racing < *minRacing:
		os.Exit(4)
	}
}

// once runs the writer and the reader once, starting them together.
func once(n uint64) (reads, racing, violations uint64) {
	s := new(shared)
	var ready atomic.Int32
	var wg sync.WaitGroup
	start := func(cpu int, f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.LockOSThread()
			if err := pin(cpu); err != nil {
				fmt.Fprintf(os.Stderr, "litmus: pin to CPU %d: %v\n", cpu, err)
				os.Exit(2)
			}
			ready.Add(1)
			for ready.Load() < 2 {
			}
			f()
		}()
	}
	start(0, func() { writer(&s.x, &s.y, n) })
	start(1, func() { reads, racing, violations = reader(&s.x, &s.y, n, 8*n) })
	wg.Wait()
	return reads, racing, violations
}

// pin binds the calling thread to cpu.
func pin(cpu int) error {
	var mask [16]uint64
	mask[cpu/64] = 1 << uint(cpu%64)
	_, _, e := syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, uintptr(len(mask)*8), uintptr(unsafe.Pointer(&mask[0])))
	if e != 0 {
		return e
	}
	return nil
}
