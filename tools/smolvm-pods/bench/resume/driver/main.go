// driver measures one smolvm machine through resume-harness, which drives it
// with smolvm as a library: it starts it, waits for its first answer, measures
// the workload, pauses and resumes it, and stops it. It prints one JSON record
// per repetition. Each step has a limit of 5 minutes; a step that reaches it
// ends the repetition. With -series, it runs one series of pauses instead and
// prints one record for it (see series.go).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type driver struct {
	lib      *harness
	vmmLog   string
	name     string
	workload string
	base     string
	node     bool
	warm     int
	quick    *http.Client
	slow     *http.Client
	idle     time.Duration
	dirty    string
	settle   time.Duration
	disk     *disk
}

type record map[string]any

const limit = 5 * time.Minute

var errLimit = errors.New("did not finish in 5 min")

// limited names the step that failed, and turns a timeout into errLimit.
func limited(step string, err error) error {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errLimit) || (errors.As(err, &ne) && ne.Timeout()) {
		return fmt.Errorf("%s: %w", step, errLimit)
	}
	return fmt.Errorf("%s: %w", step, err)
}

func secs(d time.Duration) float64 { return float64(d.Milliseconds()) / 1000 }

// run runs the harness command "CMD NAME" on the driver's machine.
func (d *driver) run(cmd string) (time.Duration, error) {
	t := time.Now()
	reply, err := d.lib.call(cmd + " " + d.name)
	el := time.Since(t)
	log.Printf("harness %s %s: %.3f s: %s", cmd, d.name, secs(el), reply)
	if err != nil {
		return el, limited("harness "+cmd, err)
	}
	return el, nil
}

func (d *driver) get(c *http.Client, path string) (time.Duration, []byte, error) {
	t := time.Now()
	resp, err := c.Get(d.base + path)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	el := time.Since(t)
	if err != nil {
		return el, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return el, body, fmt.Errorf("GET %s: status %d", path, resp.StatusCode)
	}
	return el, body, nil
}

// poll requests path until it answers 200 and returns the time from since,
// and the duration of the request that answered.
func (d *driver) poll(path string, since time.Time, limit time.Duration) (float64, float64, []byte, error) {
	var last error
	for time.Since(since) < limit {
		el, body, err := d.get(d.quick, path)
		if err == nil {
			return secs(time.Since(since)), float64(el.Microseconds()) / 1000, body, nil
		}
		last = err
		time.Sleep(10 * time.Millisecond)
	}
	return 0, 0, nil, fmt.Errorf("no answer from %s (last: %v): %w", path, last, errLimit)
}

func (d *driver) getJSON(path string) (record, error) {
	_, body, err := d.get(d.slow, path)
	if err != nil {
		return nil, limited("GET "+path, err)
	}
	var r record
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("GET %s: %v: %s", path, err, body)
	}
	return r, nil
}

// guestClock is how far memhold's wall clock is from the node's, in ms: its
// time against the middle of the request that read it.
func (d *driver) guestClock() (float64, error) {
	t := time.Now()
	r, err := d.getJSON("/clock")
	if err != nil {
		return 0, err
	}
	ns, ok := r["realtime_ns"].(float64)
	if !ok {
		return 0, fmt.Errorf("GET /clock: no realtime_ns in %v", r)
	}
	mid := t.Add(time.Since(t) / 2)
	return (ns - float64(mid.UnixNano())) / 1e6, nil
}

// latencies requests path n times and summarises the durations in ms.
func (d *driver) latencies(path string, n int) (record, error) {
	var ms []float64
	for i := 0; i < n; i++ {
		el, _, err := d.get(d.slow, path)
		if err != nil {
			return nil, limited("GET "+path, err)
		}
		ms = append(ms, float64(el.Microseconds())/1000)
	}
	first := ms[0]
	sort.Float64s(ms)
	return record{"n": n, "first_ms": first, "median_ms": ms[n/2], "p90_ms": ms[n*9/10], "max_ms": ms[n-1]}, nil
}

func (d *driver) machine() (record, error) {
	reply, err := d.lib.call("pid " + d.name)
	if err != nil {
		return nil, err
	}
	pid, err := strconv.Atoi(reply)
	if err != nil {
		return nil, fmt.Errorf("machine %s has no pid", d.name)
	}
	return record{"name": d.name, "pid": float64(pid)}, nil
}

func (d *driver) dataDir() (string, error) {
	return d.lib.call("datadir " + d.name)
}

// kib reads "Key: N kB" or "key N" lines into a map.
func kib(path string, keep func(string) bool) record {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	r := record{}
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(strings.Replace(s.Text(), ":", " ", 1))
		if len(fields) < 2 || !keep(fields[0]) {
			continue
		}
		if v, err := strconv.ParseInt(fields[1], 10, 64); err == nil {
			r[fields[0]] = v
		}
	}
	return r
}

// nodeStats reads the VM process's memory and the node's THP counters.
func (d *driver) nodeStats() record {
	if !d.node {
		return nil
	}
	r := record{}
	if m, err := d.machine(); err == nil {
		if pid, ok := m["pid"].(float64); ok {
			r["pid"] = int(pid)
			r["smaps"] = kib(fmt.Sprintf("/proc/%d/smaps_rollup", int(pid)), func(string) bool { return true })
		}
	}
	r["vmstat"] = kib("/proc/vmstat", func(k string) bool {
		return strings.HasPrefix(k, "thp_") || k == "pgfault" || k == "pgmajfault"
	})
	r["meminfo"] = kib("/proc/meminfo", func(k string) bool {
		return strings.Contains(k, "Huge") || strings.Contains(k, "Pmd") || k == "MemFree" || k == "Cached" ||
			k == "Dirty" || k == "AnonPages"
	})
	khugepaged := record{}
	for _, name := range []string{"pages_collapsed", "full_scans"} {
		raw, err := os.ReadFile("/sys/kernel/mm/transparent_hugepage/khugepaged/" + name)
		if err != nil {
			continue
		}
		if v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err == nil {
			khugepaged[name] = v
		}
	}
	r["khugepaged"] = khugepaged
	return r
}

func fileSize(path string) record {
	st, err := os.Stat(path)
	if err != nil {
		return nil
	}
	if st.IsDir() {
		// A local checkpoint: the sum of its files.
		var bytes, allocated int64
		filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
			if err == nil && info.Mode().IsRegular() {
				bytes += info.Size()
				if sys, ok := info.Sys().(*syscall.Stat_t); ok {
					allocated += int64(sys.Blocks) * 512
				}
			}
			return nil
		})
		return record{"bytes": bytes, "allocated": allocated, "dir": true}
	}
	r := record{"bytes": st.Size()}
	if sys, ok := st.Sys().(*syscall.Stat_t); ok {
		r["allocated"] = int64(sys.Blocks) * 512
	}
	return r
}

func (d *driver) checkpointSize(pattern string) record {
	dir, err := d.dataDir()
	if err != nil {
		return nil
	}
	matches, _ := filepath.Glob(filepath.Join(dir, pattern))
	if len(matches) == 0 {
		return nil
	}
	r := fileSize(matches[len(matches)-1])
	r["file"] = filepath.Base(matches[len(matches)-1])
	return r
}

func (d *driver) answerPath(resumed bool) string {
	switch {
	case d.workload == "memhold" && resumed:
		return "/ping"
	case d.workload == "memhold":
		return "/stats"
	case resumed:
		return "/owners?lastName="
	default:
		return "/"
	}
}

// measure records the workload's own numbers; resumed says it follows a resume.
func (d *driver) measure(r record, resumed bool) error {
	if d.workload == "memhold" {
		var chunks []any
		r["read_sum_ok"] = true
		for _, key := range []string{"read_ms", "read2_ms", "read3_ms"} {
			v, err := d.getJSON("/read")
			if err != nil {
				return err
			}
			if v["ok"] != true {
				log.Printf("memhold read: sum differs")
				r["read_sum_ok"] = false
			}
			r[key] = v["ms"]
			chunks = append(chunks, v["chunk_ms"])
		}
		r["read_chunk_ms"] = chunks
		v, err := d.getJSON("/random")
		if err != nil {
			return err
		}
		r["random_ns"] = v["ns_per_read"]
		r["random_reads"] = v["reads"]
		v, err = d.getJSON("/hash")
		if err != nil {
			return err
		}
		r["sha256"] = v["sha256"]
		r["hash_ms"] = v["ms"]
		return nil
	}
	if !resumed {
		el, _, err := d.get(d.slow, "/owners?lastName=")
		if err != nil {
			return limited("GET /owners", err)
		}
		r["first_owners_ms"] = float64(el.Microseconds()) / 1000
	}
	lat, err := d.latencies("/owners?lastName=", d.warm)
	if err != nil {
		return err
	}
	r["warm"] = lat
	return nil
}

func (d *driver) rep(n, cycles int) record {
	r := record{"rep": n}
	fail := func(err error) record {
		r["error"] = err.Error()
		r["limit_reached"] = errors.Is(err, errLimit)
		log.Printf("rep %d: %v", n, err)
		_, _ = d.run("stop")
		return r
	}
	before := d.nodeStats()
	t := time.Now()
	el, err := d.run("start")
	if err != nil {
		return fail(err)
	}
	r["start_cmd_s"] = secs(el)
	answer, _, body, err := d.poll(d.answerPath(false), t, limit)
	if err != nil {
		return fail(limited("first answer", err))
	}
	r["first_answer_s"] = answer
	if d.workload == "memhold" {
		var stats record
		if err := json.Unmarshal(body, &stats); err != nil {
			return fail(err)
		}
		r["first_touch_ms"] = stats["first_touch_ms"]
		r["rewrite_ms"] = stats["rewrite_ms"]
		r["fill_chunk_ms"] = stats["chunk_ms"]
	}
	if err := d.measure(r, false); err != nil {
		return fail(err)
	}
	r["node_before"] = before
	r["node"] = d.nodeStats()
	var cs []record
	for c := 1; c <= cycles; c++ {
		cr := record{"cycle": c}
		cs = append(cs, cr)
		r["cycles"] = cs
		cr["pause_start_epoch"] = float64(time.Now().UnixNano()) / 1e9
		el, err := d.run("pause")
		if err != nil {
			return fail(err)
		}
		cr["pause_s"] = secs(el)
		cr["checkpoint"] = d.checkpointSize("pause-*.smolcheckpoint")
		cr["node_paused"] = d.nodeStats()
		t := time.Now()
		cr["resume_start_epoch"] = float64(t.UnixNano()) / 1e9
		el, err = d.run("resume")
		if err != nil {
			return fail(err)
		}
		cr["resume_s"] = secs(el)
		answer, reqMs, _, err := d.poll(d.answerPath(true), t, limit)
		if err != nil {
			return fail(limited("first answer after resume", err))
		}
		cr["first_answer_s"] = answer
		cr["first_answer_request_ms"] = reqMs
		// The VM process's memory right after the resume, before the
		// measurements touch the guest's RAM.
		cr["node_answered"] = d.nodeStats()
		d.saveVMMLog(n, c)
		if err := d.measure(cr, true); err != nil {
			return fail(err)
		}
		if d.workload == "memhold" {
			cr["hash_ok"] = cr["sha256"] == r["sha256"]
			if cr["hash_ok"] != true {
				log.Printf("rep %d cycle %d: memory differs after resume: %v, before %v", n, c, cr["sha256"], r["sha256"])
			}
		}
		cr["memory_file"] = d.checkpointSize(".portable-checkpoint-memory.bin")
		cr["node"] = d.nodeStats()
	}
	if _, err := d.run("stop"); err != nil {
		r["stop_error"] = err.Error()
	}
	return r
}

func main() {
	d := &driver{
		quick: &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}},
		slow:  &http.Client{Timeout: limit, Transport: &http.Transport{DisableKeepAlives: true}},
	}
	flag.StringVar(&d.name, "name", "", "machine name")
	flag.StringVar(&d.workload, "workload", "memhold", "memhold or petclinic")
	port := flag.Int("port", 8080, "host port of the machine's HTTP port")
	flag.BoolVar(&d.node, "node", false, "record the node's THP counters and the VM process's memory")
	flag.IntVar(&d.warm, "warm", 50, "warm requests (petclinic)")
	reps := flag.Int("reps", 3, "repetitions")
	first := flag.Int("first", 1, "number of the first repetition")
	cycles := flag.Int("cycles", 2, "pause and resume cycles per repetition")
	label := flag.String("label", "", "label for the records")
	lib := flag.String("lib", "", "the resume-harness that drives the machine (required)")
	libLog := flag.String("liblog", "", "append the harness's stderr to this file")
	create := flag.String("create", "", "with -prepare: create the machine first from this image")
	share := flag.String("share", "", "with -create: the directory shared read-only at /resume-check")
	del := flag.Bool("delete", false, "delete the machine and exit")
	flag.StringVar(&d.vmmLog, "vmmlog", "", "after each resume, append the VM process's log to this file")
	prepare := flag.Bool("prepare", false, "start the machine once (image pull, first launch), wait for an answer and stop it")
	series := flag.Int("series", 0, "in place of repetitions: a cold start and a pause, then this many cycles of a "+
		"resume, a dirtying request, an idle period and a pause, then a last resume (see series)")
	flag.DurationVar(&d.idle, "idle", 5*time.Second, "with -series: the idle period of each cycle")
	flag.StringVar(&d.dirty, "dirty", "", "with -series: memhold's dirtying pattern of each cycle, PATTERN[:PERCENT]")
	diskPath := flag.String("disk", "/var/lib/smolvm", "with -series: record the writes of the disk that holds this path")
	flag.DurationVar(&d.settle, "settle", 3*time.Second, "with -series: the wait after a sync before reading the disk's writes")
	flag.Parse()
	d.base = fmt.Sprintf("http://127.0.0.1:%d", *port)
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	if *lib == "" {
		log.Fatal("-lib is required")
	}
	h, err := startHarness(*lib, *libLog)
	if err != nil {
		log.Fatal(err)
	}
	defer h.close()
	d.lib = h
	if *del {
		if _, err := h.call("delete " + d.name); err != nil {
			log.Fatal(err)
		}
		return
	}
	if *create != "" {
		if _, err := h.call(fmt.Sprintf("create %s %s %d %s %s", d.name, d.workload, *port, *share, *create)); err != nil {
			log.Fatal(err)
		}
	}
	if *prepare {
		t := time.Now()
		if _, err := d.run("start"); err != nil {
			log.Fatal(err)
		}
		answer, _, _, err := d.poll(d.answerPath(false), t, limit)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("prepared: first answer after %.1f s", answer)
		if _, err := d.run("stop"); err != nil {
			log.Fatal(err)
		}
		return
	}
	enc := json.NewEncoder(os.Stdout)
	if *series > 0 {
		if *diskPath != "" {
			if d.disk, err = findDisk(*diskPath); err != nil {
				log.Fatal(err)
			}
		}
		r := d.series(*series)
		r["label"] = *label
		r["workload"] = d.workload
		r["time"] = time.Now().UTC().Format(time.RFC3339)
		if err := enc.Encode(r); err != nil {
			log.Fatal(err)
		}
		return
	}
	for n := *first; n < *first+*reps; n++ {
		r := d.rep(n, *cycles)
		r["label"] = *label
		r["workload"] = d.workload
		r["time"] = time.Now().UTC().Format(time.RFC3339)
		if err := enc.Encode(r); err != nil {
			log.Fatal(err)
		}
		time.Sleep(3 * time.Second)
	}
}

// harness is a resume-harness serve process: one command per line on its
// stdin, one reply per line on its stdout.
type harness struct {
	cmd *exec.Cmd
	in  io.WriteCloser
	out *bufio.Reader
}

func startHarness(path, logPath string) (*harness, error) {
	cmd := exec.Command(path, "serve")
	cmd.Stderr = os.Stderr
	if logPath != "" {
		f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, err
		}
		cmd.Stderr = f
	}
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &harness{cmd: cmd, in: in, out: bufio.NewReader(out)}, nil
}

// call sends one command and returns the value of an "ok MS VALUE" reply.
func (h *harness) call(command string) (string, error) {
	_, value, err := h.callMS(command)
	return value, err
}

// callMS is call that also returns the MS of the reply: how long the command
// took inside the harness.
func (h *harness) callMS(command string) (float64, string, error) {
	type answer struct {
		line string
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		if _, err := fmt.Fprintln(h.in, command); err != nil {
			done <- answer{err: err}
			return
		}
		line, err := h.out.ReadString('\n')
		done <- answer{strings.TrimSpace(line), err}
	}()
	select {
	case a := <-done:
		if a.err != nil {
			return 0, "", fmt.Errorf("%s: %w", command, a.err)
		}
		fields := strings.SplitN(a.line, " ", 3)
		value := ""
		if len(fields) == 3 {
			value = fields[2]
		}
		if fields[0] != "ok" {
			return 0, a.line, fmt.Errorf("%s: %s", command, a.line)
		}
		var ms float64
		if len(fields) > 1 {
			ms, _ = strconv.ParseFloat(fields[1], 64)
		}
		return ms, value, nil
	case <-time.After(limit):
		return 0, "", fmt.Errorf("%s: %w", command, errLimit)
	}
}

func (h *harness) close() {
	h.in.Close()
	h.cmd.Wait()
}

// saveVMMLog appends the resumed VM process's log, its stderr, to d.vmmLog.
func (d *driver) saveVMMLog(rep, cycle int) {
	if d.vmmLog == "" {
		return
	}
	dir, err := d.dataDir()
	if err != nil {
		log.Printf("vmm log: %v", err)
		return
	}
	body, err := os.ReadFile(filepath.Join(dir, "agent-startup-error.log"))
	if err != nil {
		log.Printf("vmm log: %v", err)
		return
	}
	f, err := os.OpenFile(d.vmmLog, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("vmm log: %v", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "=== rep %d cycle %d %s\n", rep, cycle, d.name)
	f.Write(body)
}
