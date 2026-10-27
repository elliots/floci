package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// disk is the block device that holds smolvm's data, whose writes a series
// records around each pause.
type disk struct {
	name string
	stat string // /sys/dev/block/MAJ:MIN/stat
	ext4 string // the file system's lifetime_write_kbytes, or ""
}

// diskStat is a settled reading of a disk's writes.
type diskStat struct {
	sectors int64
	ext4KiB int64
	settled bool
	waitS   float64
	at      time.Time
}

// findDisk finds the device of the mount that holds path.
func findDisk(path string) (*disk, error) {
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	mount, dev := "", ""
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) < 5 {
			continue
		}
		m := fields[4]
		if (m == "/" || real == m || strings.HasPrefix(real, m+"/")) && len(m) >= len(mount) {
			mount, dev = m, fields[2]
		}
	}
	k := &disk{stat: "/sys/dev/block/" + dev + "/stat"}
	if _, err := os.Stat(k.stat); dev == "" || err != nil {
		return nil, fmt.Errorf("no block device holds %s (mount %q, device %q)", path, mount, dev)
	}
	uevent, err := os.ReadFile("/sys/dev/block/" + dev + "/uevent")
	if err != nil {
		return nil, err
	}
	for _, line := range strings.Split(string(uevent), "\n") {
		if name, ok := strings.CutPrefix(line, "DEVNAME="); ok {
			k.name = name
		}
	}
	if ext4 := "/sys/fs/ext4/" + k.name + "/lifetime_write_kbytes"; k.name != "" {
		if _, err := os.Stat(ext4); err == nil {
			k.ext4 = ext4
		}
	}
	log.Printf("series: %s is on %s (%s), mount %s", path, k.name, dev, mount)
	return k, nil
}

// read returns the sectors written (field 7 of the stat) and the I/Os in
// flight (field 9), and the file system's KiB written, -1 without ext4.
func (k *disk) read() (int64, int64, int64, error) {
	raw, err := os.ReadFile(k.stat)
	if err != nil {
		return 0, 0, 0, err
	}
	f := strings.Fields(string(raw))
	if len(f) < 9 {
		return 0, 0, 0, fmt.Errorf("%s: %d fields", k.stat, len(f))
	}
	sectors, err := strconv.ParseInt(f[6], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	inflight, err := strconv.ParseInt(f[8], 10, 64)
	if err != nil {
		return 0, 0, 0, err
	}
	ext4 := int64(-1)
	if k.ext4 != "" {
		raw, err := os.ReadFile(k.ext4)
		if err != nil {
			return 0, 0, 0, err
		}
		if ext4, err = strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); err != nil {
			return 0, 0, 0, err
		}
	}
	return sectors, inflight, ext4, nil
}

// settled syncs, waits d.settle for writeback, then reads the disk until two
// readings a second apart, each after a sync, agree with nothing in flight,
// for up to 10 s more: the node's k3s also writes to the disk. It returns nil
// without a disk.
func (d *driver) settled() (*diskStat, error) {
	if d.disk == nil {
		return nil, nil
	}
	t := time.Now()
	syscall.Sync()
	time.Sleep(d.settle)
	prev, _, _, err := d.disk.read()
	if err != nil {
		return nil, err
	}
	for {
		time.Sleep(time.Second)
		syscall.Sync()
		sectors, inflight, ext4, err := d.disk.read()
		if err != nil {
			return nil, err
		}
		stable := sectors == prev && inflight == 0
		if stable || time.Since(t) > d.settle+10*time.Second {
			return &diskStat{sectors: sectors, ext4KiB: ext4, settled: stable, waitS: secs(time.Since(t)),
				at: time.Now()}, nil
		}
		prev = sectors
	}
}

// words reads "key=value" words.
func words(value string) map[string]string {
	kv := map[string]string{}
	for _, w := range strings.Fields(value) {
		if k, v, ok := strings.Cut(w, "="); ok {
			kv[k] = v
		}
	}
	return kv
}

// pause pauses the machine and records how smolvm saved its RAM, the disk's
// writes from last to the pause (the wake), in a quiet window just before the
// pause (the node's background) and during it, and the machine after it. It
// returns the disk's reading after the pause.
func (d *driver) pause(cr record, last *diskStat) (*diskStat, error) {
	woke, err := d.settled()
	if err != nil {
		return nil, fmt.Errorf("disk before the pause: %w", err)
	}
	if woke != nil && last != nil {
		cr["disk_wake_bytes"] = (woke.sectors - last.sectors) * 512
	}
	before, err := d.settled()
	if err != nil {
		return nil, fmt.Errorf("disk before the pause: %w", err)
	}
	if before != nil {
		cr["disk_background_bytes"] = (before.sectors - woke.sectors) * 512
		cr["disk_background_s"] = before.at.Sub(woke.at).Seconds()
	}
	cr["pause_start_epoch"] = float64(time.Now().UnixNano()) / 1e9
	t := time.Now()
	ms, value, err := d.lib.callMS("pause " + d.name)
	cr["pause_s"] = secs(time.Since(t))
	log.Printf("harness pause %s: %.3f s: %s", d.name, secs(time.Since(t)), value)
	if err != nil {
		return nil, limited("harness pause", err)
	}
	kv := words(value)
	written, err := strconv.ParseInt(kv["written"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("harness pause: no written bytes in %q", value)
	}
	cr["pause_ms"] = ms
	cr["save_mode"] = kv["save"]
	cr["written_bytes"] = written
	after, err := d.settled()
	if err != nil {
		return nil, fmt.Errorf("disk after the pause: %w", err)
	}
	if before != nil {
		cr["disk_pause_bytes"] = (after.sectors - before.sectors) * 512
		cr["disk_pause_s"] = after.at.Sub(before.at).Seconds()
		if before.ext4KiB >= 0 {
			cr["ext4_pause_bytes"] = (after.ext4KiB - before.ext4KiB) * 1024
		}
		cr["disk_settled"] = woke.settled && before.settled && after.settled
		cr["disk_settle_s"] = woke.waitS + before.waitS + after.waitS
	}
	if v, err := d.lib.call("inspect " + d.name); err != nil {
		cr["inspect_error"] = err.Error()
	} else {
		kv := words(v)
		cr["state"] = kv["state"]
		for _, key := range []string{"base_bytes", "resume_point_bytes"} {
			if n, err := strconv.ParseInt(kv[key], 10, 64); err == nil {
				cr[key] = n
			}
		}
	}
	return after, nil
}

// wake resumes the machine, waits for its first answer, and has memhold
// check its memory against the hash it recorded before the pause.
func (d *driver) wake(cr record, cycle int) error {
	t := time.Now()
	cr["resume_start_epoch"] = float64(t.UnixNano()) / 1e9
	el, err := d.run("resume")
	if err != nil {
		return err
	}
	cr["resume_s"] = secs(el)
	answer, reqMs, _, err := d.poll(d.answerPath(true), t, limit)
	if err != nil {
		return limited("first answer after resume", err)
	}
	cr["first_answer_s"] = answer
	cr["first_answer_request_ms"] = reqMs
	cr["node_answered"] = d.nodeStats()
	d.saveVMMLog(0, cycle)
	if d.workload != "memhold" {
		return nil
	}
	v, err := d.getJSON("/verify")
	if err != nil {
		return err
	}
	cr["hash_ok"] = v["ok"] == true
	cr["verify_ms"] = v["ms"]
	if v["ok"] != true {
		cr["sha256"], cr["expected"] = v["sha256"], v["expected"]
		log.Printf("cycle %d: memory differs after resume: %v, expected %v", cycle, v["sha256"], v["expected"])
	}
	return nil
}

// series runs a cold start and a pause (cycle 0, the machine's first pause),
// then n cycles of a resume, memhold's check of its memory, a dirtying
// request with d.dirty, an idle period of d.idle and a pause, then a last
// resume with the check, and a stop. The disk settles (see settled) twice
// before each pause, the second time to measure the node's background
// writes, and once after it, so the idle period before a pause is d.idle and
// those waits.
func (d *driver) series(n int) record {
	r := record{"idle_s": d.idle.Seconds(), "dirty": d.dirty, "settle_s": d.settle.Seconds()}
	if v, err := d.lib.call("delta-saves"); err == nil {
		r["delta_saves"] = v
	}
	if d.disk != nil {
		r["disk"] = d.disk.name
	}
	fail := func(err error) record {
		r["error"] = err.Error()
		r["limit_reached"] = errors.Is(err, errLimit)
		log.Printf("series: %v", err)
		_, _ = d.run("stop")
		return r
	}
	t := time.Now()
	if _, err := d.run("start"); err != nil {
		return fail(err)
	}
	answer, _, _, err := d.poll(d.answerPath(false), t, limit)
	if err != nil {
		return fail(limited("first answer", err))
	}
	r["first_answer_s"] = answer
	if d.workload == "memhold" {
		if _, err := d.getJSON("/dirty?pattern=none"); err != nil {
			return fail(err)
		}
	}
	var cs []record
	var last *diskStat
	for c := 0; c <= n; c++ {
		cr := record{"cycle": c}
		cs = append(cs, cr)
		r["cycles"] = cs
		if c > 0 {
			if err := d.wake(cr, c); err != nil {
				return fail(err)
			}
			if d.dirty != "" {
				pattern, percent, _ := strings.Cut(d.dirty, ":")
				v, err := d.getJSON("/dirty?pattern=" + url.QueryEscape(pattern) + "&percent=" + url.QueryEscape(percent))
				if err != nil {
					return fail(err)
				}
				cr["dirty"] = v
			}
		}
		time.Sleep(d.idle)
		if last, err = d.pause(cr, last); err != nil {
			return fail(err)
		}
	}
	final := record{}
	r["final"] = final
	if err := d.wake(final, n+1); err != nil {
		return fail(err)
	}
	if _, err := d.run("stop"); err != nil {
		r["stop_error"] = err.Error()
	}
	return r
}
