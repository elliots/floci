#!/usr/bin/env python3
"""Summarize a resume check and apply its pass criteria.

Usage: summarize.py RESULTS_DIR [REPS] [CYCLES]

RESULTS_DIR is a results-LABEL directory of run-pod.sh, with WORKLOAD.jsonl
from the driver for memhold and petclinic, and node-info.txt and
node-info-end.txt. Prints a table per workload, with the median and, in
brackets, the range over all resumes, then each failed check, and exits 1 if
any check failed. Every resume must:

- answer within 0.5 s of the start of the resume;
- keep memory intact: memhold's hash equals the one before the first pause;
- have its pod VM process's anonymous huge pages at 90% of its RSS or more at
  the first answer;
- serve warm requests at most 1.5 times as slow as the cold-booted VM of the
  same repetition: PetClinic's median, memhold's second read of 1 GiB and its
  random reads.

The node must run the patched libkrun of its build-info, with THP "always",
and the harness must be built from the shim's smolvm commit.
"""
import json
import os
import re
import statistics
import sys

WORKLOADS = ("memhold", "petclinic")
FIRST_ANSWER_S = 0.5
HUGE_SHARE = 0.9
WARM_FACTOR = 1.5
MIB = 1024  # smaps and meminfo are in KiB


def stat(values, digits=0):
    values = [v for v in values if v is not None]
    if not values:
        return "-"
    fmt = f"{{:.{digits}f}}"
    med = fmt.format(statistics.median(values))
    if len(values) == 1 or min(values) == max(values):
        return med
    return f"{med} ({fmt.format(min(values))} to {fmt.format(max(values))})"


def node_info(path):
    """The lines of a node-info file as {key: value}."""
    info = {}
    if not os.path.exists(path):
        return info
    for line in open(path, errors="replace"):
        line = line.rstrip("\n")
        m = re.match(r"^thp (\S+): (.*)$", line)
        if m:
            info["thp " + m.group(1)] = m.group(2)
            continue
        m = re.match(r"^(thp_\S+) (\d+)$", line)
        if m:
            info[m.group(1)] = int(m.group(2))
            continue
        m = re.match(r"^([0-9a-f]{64})  (\S+)$", line)
        if m:
            info["sha256 " + os.path.basename(m.group(2))] = m.group(1)
            continue
        m = re.match(r"^(harness|build-info) (\S+) (\S+)$", line)
        if m:
            info[f"{m.group(1)} {m.group(2)}"] = m.group(3)
    return info


def ram_reads(path):
    """libkrun's RAM read after each resume, in ms, from the VM process logs."""
    if not os.path.exists(path):
        return []
    pattern = re.compile(r"read (\d+) bytes of guest RAM into anonymous memory in (\d+) ms")
    return [int(m.group(2)) for m in map(pattern.search, open(path, errors="replace")) if m]


def collapses(before, after):
    """THP collapses between two node stats of the driver."""
    if not before or not after:
        return None
    vb, va = before.get("vmstat") or {}, after.get("vmstat") or {}
    kb, ka = before.get("khugepaged") or {}, after.get("khugepaged") or {}
    return {
        "thp_collapse_alloc": va.get("thp_collapse_alloc", 0) - vb.get("thp_collapse_alloc", 0),
        "thp_collapse_alloc_failed": va.get("thp_collapse_alloc_failed", 0)
        - vb.get("thp_collapse_alloc_failed", 0),
        "pages_collapsed": ka.get("pages_collapsed", 0) - kb.get("pages_collapsed", 0),
    }


def summarize(results, workload, reps, cycles, failures):
    path = os.path.join(results, workload + ".jsonl")
    if not os.path.exists(path):
        failures.append(f"{workload}: no records ({path})")
        return []
    records = [json.loads(line) for line in open(path) if line.strip()]
    rows = {}

    def add(key, value):
        rows.setdefault(key, []).append(value)

    def fail(rec, cycle, message):
        failures.append(f"{workload} rep {rec.get('rep')} cycle {cycle}: {message}")

    if len(records) != reps:
        failures.append(f"{workload}: {len(records)} repetitions recorded, expected {reps}")
    resume_collapses = []
    for r in records:
        if r.get("error"):
            failures.append(f"{workload} rep {r.get('rep')}: {r['error']}")
        add("cold: first answer after start, s", r.get("first_answer_s"))
        cold_warm = (r.get("warm") or {}).get("median_ms")
        add("cold: warm request median, ms", cold_warm)
        add("cold: second read of 1 GiB, ms", r.get("read2_ms"))
        add("cold: random 8-byte read, ns", r.get("random_ns"))
        cold_smaps = (r.get("node") or {}).get("smaps") or {}
        if cold_smaps:
            add("cold: VM RSS, MiB", cold_smaps.get("Rss", 0) / MIB)
            add("cold: VM AnonHugePages, MiB", cold_smaps.get("AnonHugePages", 0) / MIB)
        cs = r.get("cycles") or []
        if len(cs) != cycles and not r.get("error"):
            failures.append(f"{workload} rep {r.get('rep')}: {len(cs)} cycles, expected {cycles}")
        for c in cs:
            n = c.get("cycle")
            add("pause, s", c.get("pause_s"))
            cp = c.get("checkpoint") or {}
            size = cp.get("allocated") if cp.get("dir") else cp.get("bytes")
            add("checkpoint on disk, MB", size / 1e6 if size else None)
            add("resume call, s", c.get("resume_s"))
            answer = c.get("first_answer_s")
            add("first answer from start of resume, s", answer)
            add("first request after resume, ms", c.get("first_answer_request_ms"))
            if answer is None and "resume_s" in c:
                fail(r, n, "no answer after the resume")
            elif answer is not None and answer >= FIRST_ANSWER_S:
                fail(r, n, f"first answer {answer:.3f} s after the resume started")
            smaps = (c.get("node_answered") or {}).get("smaps") or {}
            if answer is not None:
                rss, huge = smaps.get("Rss", 0), smaps.get("AnonHugePages", 0)
                add("VM RSS at first answer, MiB", rss / MIB if smaps else None)
                add("VM AnonHugePages at first answer, MiB", huge / MIB if smaps else None)
                add("AnonHugePages / RSS at first answer", huge / rss if rss else None)
                if not rss:
                    fail(r, n, "no smaps of the VM process at the first answer")
                elif huge < HUGE_SHARE * rss:
                    fail(r, n, f"AnonHugePages {huge // MIB} MiB of RSS {rss // MIB} MiB")
            if workload == "memhold" and answer is not None:
                add("read of 1 GiB after resume, ms", c.get("read_ms"))
                add("second read of 1 GiB after resume, ms", c.get("read2_ms"))
                add("random 8-byte read after resume, ns", c.get("random_ns"))
                if "hash_ok" in c:
                    add("hash after resume equals hash before", 1 if c["hash_ok"] else 0)
                if c.get("hash_ok") is not True:
                    fail(r, n, f"memory hash {c.get('sha256')} differs from {r.get('sha256')}")
                for key in ("read2_ms", "random_ns"):
                    cold, warm = r.get(key), c.get(key)
                    if cold is None or warm is None:
                        fail(r, n, f"no {key}")
                    elif warm > WARM_FACTOR * cold:
                        fail(r, n, f"{key} {warm:.1f} after the resume, {cold:.1f} cold")
            if workload == "petclinic" and answer is not None:
                warm = (c.get("warm") or {}).get("median_ms")
                add("warm request median after resume, ms", warm)
                if cold_warm is None or warm is None:
                    fail(r, n, "no warm request median")
                elif warm > WARM_FACTOR * cold_warm:
                    fail(r, n, f"warm median {warm:.1f} ms after the resume, {cold_warm:.1f} ms cold")
            col = collapses(c.get("node_paused"), c.get("node_answered"))
            if col:
                resume_collapses.append(col)
        whole = collapses(r.get("node_before"), (cs[-1] if cs else {}).get("node"))
        if whole:
            add("THP collapses in the repetition (thp_collapse_alloc)", whole["thp_collapse_alloc"])
            add("failed THP collapses in the repetition", whole["thp_collapse_alloc_failed"])
            add("khugepaged pages_collapsed in the repetition", whole["pages_collapsed"])
    for col in resume_collapses:
        add("THP collapses from pause to first answer", col["thp_collapse_alloc"])
    reads = ram_reads(os.path.join(results, workload + ".vmm.log"))
    for ms in reads:
        add("libkrun: read RAM into anonymous memory, ms", ms)
    out = [f"### {workload}", "", f"{len(records)} repetitions of {cycles} cycles", ""]
    out += ["| Metric | Value |", "| --- | --- |"]
    for key, values in rows.items():
        if all(v is None for v in values):
            continue
        if key.startswith("hash"):
            vals = [v for v in values if v is not None]
            out.append(f"| {key} | {sum(vals)} of {len(vals)} |")
            continue
        if key.endswith(", s") or "/" in key:
            digits = 2 if key.endswith(", s") else 3
        elif key.endswith(", ns") or key.endswith(", MB") or "request" in key:
            digits = 1
        else:
            digits = 0
        out.append(f"| {key} | {stat(values, digits)} |")
    out.append("")
    return out


def check_node(results, failures):
    start = node_info(os.path.join(results, "node-info.txt"))
    end = node_info(os.path.join(results, "node-info-end.txt"))
    out = ["### Node", ""]
    if not start:
        failures.append("node: no node-info.txt")
        return out
    if "[always]" not in start.get("thp enabled", ""):
        failures.append(f"node: THP enabled is {start.get('thp enabled')!r}, not always")
    libkrun = start.get("sha256 libkrun.so")
    if not libkrun or libkrun != start.get("build-info libkrun"):
        failures.append(f"node: libkrun.so is {libkrun}, build-info names {start.get('build-info libkrun')}")
    harness = start.get("harness smolvm")
    if not harness or harness != start.get("build-info smolvm"):
        failures.append(f"node: the harness is built from smolvm {harness}, the shim from "
                        f"{start.get('build-info smolvm')}")
    out += ["| Item | Start | End |", "| --- | --- | --- |"]
    for key in ("thp enabled", "thp defrag", "thp khugepaged/defrag", "thp khugepaged/pages_to_scan",
                "thp khugepaged/scan_sleep_millisecs", "thp khugepaged/alloc_sleep_millisecs",
                "thp khugepaged/max_ptes_none", "thp khugepaged/full_scans",
                "thp khugepaged/pages_collapsed", "thp_collapse_alloc", "thp_collapse_alloc_failed",
                "thp_fault_alloc", "thp_fault_fallback"):
        out.append(f"| {key} | {start.get(key, '-')} | {end.get(key, '-')} |")
    out += ["", f"libkrun {libkrun}, smolvm {harness}, node kernel {start.get('build-info node-kernel')}", ""]
    return out


def main():
    if len(sys.argv) not in (2, 3, 4):
        print(__doc__.strip(), file=sys.stderr)
        return 2
    results = sys.argv[1]
    reps = int(sys.argv[2]) if len(sys.argv) > 2 else 3
    cycles = int(sys.argv[3]) if len(sys.argv) > 3 else 2
    failures = []
    out = check_node(results, failures)
    for workload in WORKLOADS:
        out += summarize(results, workload, reps, cycles, failures)
    print("\n".join(out))
    if failures:
        print("FAILED:")
        for f in failures:
            print(f"- {f}")
        return 1
    print("PASSED: every check")
    return 0


if __name__ == "__main__":
    sys.exit(main())
