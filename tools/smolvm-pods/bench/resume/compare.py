#!/usr/bin/env python3
"""Summarize the save benchmark of check.sh --delta, one run or two side by side.

Usage: compare.py [--root DIR] [--cycles N] [--workloads LIST] RUN [RUN]

RUN is a run's label, whose results are in DIR/LABEL/results (DIR defaults to
target/resume-check), or a results directory. With two runs, one must have
delta saves on and the other off. Prints a table per workload, with the
pauses after the first ("later") apart from the first one, then each failed
check, and exits 1 if any check failed. Numbers are reported whether or not
a check passed.

Checks of every run:
- each workload has a record with --cycles cycles and no error;
- memhold's memory equals what it held before the pause after every resume;
- with delta saves off, every pause is full; with them on, the first is.

Checks of two runs (the design's bars), per workload, with the run with
delta saves off as today's:
- the first pause writes at most today's first pause plus 1%, by smolvm's
  count and on disk;
- after an idle wake (idle, petclinic), the later pauses' median writes at
  most a quarter of today's median (a target to confirm);
- when memhold rewrites all its memory (all), no later pause writes more
  than today's median plus 1%;
- smolvm's written bytes and the disk's sectors written agree within 5%, or
  within 20 MB, for every later pause: the disk also gets the pause's
  checkpoint state and disk layers, about 15 MB, which smolvm's RAM bytes
  leave out, and a node's first pause also writes ext4's metadata for a new
  image;
- the median pause time is no higher than today's, and at most 10% higher
  for all;
- the median time from the start of a resume to the first answer is at most
  10% higher than today's.

The disk's bytes of a pause are its sectors written from just before the
pause to after it, less the node's background writes (k3s shares the disk)
in as long, at the rate the driver measured in a quiet window just before
the pause. The daily writes project the mean bytes of a later pause to 500
pods that each sleep 10 times a day.
"""
import argparse
import glob
import json
import math
import os
import statistics
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "target", "resume-check")
PODS, SLEEPS = 500, 10
AGREE = 0.05
AGREE_BYTES = 20 * 1000 * 1000
FIRST_SLACK = 1.01
IDLE_SHARE = 0.25
PAUSE_ALL_SLACK = 1.10
RESUME_SLACK = 1.10
SHORT_WAKES = ("memhold-idle", "petclinic")
ALL = "memhold-all"


def workload_id(spec):
    """check.sh's pod VM name part for a --workloads entry."""
    if spec == "petclinic":
        return spec
    if spec in ("idle", "all"):
        return "memhold-" + spec
    return "memhold-" + spec.replace(":", "-", 1).replace(".", "p")


def results_dir(run, root):
    if os.path.isdir(os.path.join(run, "results")):
        return os.path.join(run, "results")
    if os.path.isdir(run):
        return run
    return os.path.join(root, run, "results")


def load(results):
    """{workload: the last series record} of a results directory."""
    runs = {}
    for path in sorted(glob.glob(os.path.join(results, "*.series.jsonl"))):
        with open(path) as f:
            lines = [line for line in f if line.strip()]
        if lines:
            runs[os.path.basename(path)[: -len(".series.jsonl")]] = json.loads(lines[-1])
    return runs


def p95(values):
    values = sorted(values)
    return values[max(0, math.ceil(0.95 * len(values)) - 1)]


def median(values):
    return statistics.median(values) if values else None


def nums(cycles, key):
    return [c[key] for c in cycles if isinstance(c.get(key), (int, float)) and not isinstance(c.get(key), bool)]


def pause_ms(c):
    if isinstance(c.get("pause_ms"), (int, float)):
        return c["pause_ms"]
    if isinstance(c.get("pause_s"), (int, float)):
        return c["pause_s"] * 1000
    return None


def disk_net(c):
    """The disk's bytes written during a pause less the node's background
    writes in as long, at the rate of the quiet window just before it."""
    raw = c.get("disk_pause_bytes")
    bg, bg_s, window = c.get("disk_background_bytes"), c.get("disk_background_s"), c.get("disk_pause_s")
    if not isinstance(raw, int) or not isinstance(bg, int) or not bg_s or not window:
        return raw
    return max(0, round(raw - bg / bg_s * window))


def background_rate(c):
    bg, bg_s = c.get("disk_background_bytes"), c.get("disk_background_s")
    return bg / bg_s if isinstance(bg, int) and bg_s else None


def huge_share(c):
    smaps = (c.get("node_answered") or {}).get("smaps") or {}
    rss = smaps.get("Rss")
    return smaps.get("AnonHugePages", 0) / rss if rss else None


def stats(rec):
    """The numbers of one series record."""
    cycles = rec.get("cycles") or []
    first = cycles[0] if cycles and cycles[0].get("cycle") == 0 else {}
    later = [c for c in cycles if isinstance(c.get("cycle"), int) and c["cycle"] > 0]
    wakes = later + ([rec["final"]] if rec.get("final") else [])
    paused = [c for c in cycles if "written_bytes" in c]
    s = {
        "delta_saves": rec.get("delta_saves"),
        "error": rec.get("error"),
        "cycles": len(later),
        "first_mode": first.get("save_mode"),
        "first_written": first.get("written_bytes"),
        "first_disk": disk_net(first),
        "modes": {},
        "written": nums(later, "written_bytes"),
        "disk": [v for v in map(disk_net, later) if v is not None],
        "disk_raw": nums(later, "disk_pause_bytes"),
        "background": [v for v in map(background_rate, paused) if v is not None],
        "ext4": nums(later, "ext4_pause_bytes"),
        "wake_disk": nums(later, "disk_wake_bytes"),
        "pause_ms": [v for v in map(pause_ms, later) if v is not None],
        "all_pause_ms": [v for v in map(pause_ms, paused) if v is not None],
        "all_written": nums(paused, "written_bytes"),
        "resume_s": nums(wakes, "first_answer_s"),
        "huge": [v for v in map(huge_share, wakes) if v is not None],
        "touched": [c["dirty"]["blocks_touched"] for c in later
                    if isinstance((c.get("dirty") or {}).get("blocks_touched"), int)],
        "base_bytes": (paused[-1] if paused else {}).get("base_bytes"),
        "checked": [c for c in wakes if "hash_ok" in c],
        "unsettled": sum(1 for c in paused if c.get("disk_settled") is False),
        "agree": [],
    }
    for c in later:
        mode = c.get("save_mode") or "-"
        s["modes"][mode] = s["modes"].get(mode, 0) + 1
    for c in later:
        w, d = c.get("written_bytes"), disk_net(c)
        if isinstance(w, int) and isinstance(d, int) and w > 0:
            s["agree"].append((c.get("cycle"), d / w, abs(d - w) <= max(AGREE * w, AGREE_BYTES)))
    s["failed_checks"] = [c for c in s["checked"] if c.get("hash_ok") is not True]
    return s


def mb(v):
    return "-" if v is None else f"{v / 1e6:.1f}"


def kb(v):
    return "-" if v is None else f"{v / 1e3:.1f}"


def ms(v):
    return "-" if v is None else f"{v:.0f}"


def summary(values, fmt):
    if not values:
        return "-"
    return f"{fmt(median(values))} / {fmt(p95(values))}"


def daily(values):
    if not values:
        return "-"
    return f"{statistics.mean(values) * PODS * SLEEPS / 1e9:.1f}"


def ratio(a, b):
    if a is None or not b:
        return "-"
    return f"{a / b:.2f}"


def rows(s):
    """(metric, text, number for the ratio column) of one run."""
    modes = ", ".join(f"{k} {v}" for k, v in sorted(s["modes"].items())) or "-"
    agree = [r for _, r, _ in s["agree"]]
    within = sum(1 for _, _, ok in s["agree"] if ok)
    checked = len(s["checked"])
    return [
        ("first pause: save mode", s["first_mode"] or "-", None),
        ("first pause: written (smolvm), MB", mb(s["first_written"]), s["first_written"]),
        ("first pause: written (disk), MB", mb(s["first_disk"]), s["first_disk"]),
        ("later pauses: save modes", modes, None),
        ("later pauses: written (smolvm), MB, median / p95", summary(s["written"], mb), median(s["written"])),
        ("later pauses: written (disk), MB, median / p95", summary(s["disk"], mb), median(s["disk"])),
        ("later pauses: written (disk, with the background), MB, median / p95", summary(s["disk_raw"], mb),
         median(s["disk_raw"])),
        ("node background writes, KB/s, median / p95", summary(s["background"], kb), median(s["background"])),
        ("later pauses: written (ext4), MB, median / p95", summary(s["ext4"], mb), median(s["ext4"])),
        ("later pauses: disk / smolvm, median", f"{median(agree):.3f}" if agree else "-", None),
        ("later pauses: disk within 5% or 20 MB of smolvm", f"{within} of {len(agree)}" if agree else "-", None),
        ("disk readings that did not settle", str(s["unsettled"]), None),
        ("written between pauses (disk), MB, median / p95", summary(s["wake_disk"], mb), median(s["wake_disk"])),
        ("memhold: 2 MiB blocks it touched per wake, median", ms(median(s["touched"])), median(s["touched"])),
        ("later pauses: pause, ms, median / p95", summary(s["pause_ms"], ms), median(s["pause_ms"])),
        ("resume to first answer, ms, median / p95",
         summary([v * 1000 for v in s["resume_s"]], ms), median(s["resume_s"])),
        ("AnonHugePages / RSS at first answer, median", f"{median(s['huge']):.3f}" if s["huge"] else "-", None),
        ("RAM base after the last pause, MB", mb(s["base_bytes"]), s["base_bytes"]),
        ("memory checks failed", f"{len(s['failed_checks'])} of {checked}" if checked else "-", None),
        (f"projected writes, {PODS} pods x {SLEEPS} sleeps a day (disk), GB/day", daily(s["disk"]),
         statistics.mean(s["disk"]) if s["disk"] else None),
        (f"projected writes, {PODS} pods x {SLEEPS} sleeps a day (smolvm), GB/day", daily(s["written"]),
         statistics.mean(s["written"]) if s["written"] else None),
    ]


def check_run(name, workload, rec, s, cycles, failures):
    where = f"{name} {workload}"
    if rec is None:
        failures.append(f"{where}: no record")
        return
    if s["error"]:
        failures.append(f"{where}: {s['error']}")
    if cycles is not None and s["cycles"] != cycles:
        failures.append(f"{where}: {s['cycles']} cycles, expected {cycles}")
    for c in s["failed_checks"]:
        failures.append(f"{where} cycle {c.get('cycle', 'final')}: memory differs after the resume "
                        f"({c.get('sha256')}, expected {c.get('expected')})")
    if s["delta_saves"] == "off" and set(s["modes"]) - {"full"}:
        failures.append(f"{where}: delta saves are off, but the save modes are {s['modes']}")
    if s["delta_saves"] == "on" and s["first_mode"] not in (None, "full"):
        failures.append(f"{where}: the first pause was {s['first_mode']}, not full")


def check_pair(workload, on, off, failures):
    def fail(message):
        failures.append(f"{workload}: {message}")

    today = median(off["all_written"])
    today_disk = off["first_disk"]
    today_first = off["first_written"]
    if today_first and on["first_written"] is not None and on["first_written"] > FIRST_SLACK * today_first:
        fail(f"the first pause wrote {mb(on['first_written'])} MB, today's first pause {mb(today_first)} MB")
    if today_disk and on["first_disk"] is not None and on["first_disk"] > FIRST_SLACK * today_disk:
        fail(f"the first pause wrote {mb(on['first_disk'])} MB to disk, today's first pause {mb(today_disk)} MB")
    if workload in SHORT_WAKES and today and on["written"]:
        if median(on["written"]) > IDLE_SHARE * today:
            fail(f"later pauses wrote a median {mb(median(on['written']))} MB, more than a quarter of "
                 f"today's {mb(today)} MB (a target to confirm)")
    if workload == ALL and today and on["written"] and max(on["written"]) > FIRST_SLACK * today:
        fail(f"a later pause wrote {mb(max(on['written']))} MB, today's median is {mb(today)} MB")
    for run, s in (("delta on", on), ("delta off", off)):
        bad = [(c, r) for c, r, ok in s["agree"] if not ok]
        if bad:
            fail(f"{run}: disk and smolvm bytes differ by more than 5% and 20 MB in {len(bad)} of {len(s['agree'])} "
                 f"pauses (cycle {bad[0][0]}: disk / smolvm {bad[0][1]:.3f})")
    on_pause, off_pause = median(on["pause_ms"]), median(off["pause_ms"])
    slack = PAUSE_ALL_SLACK if workload == ALL else 1.0
    if on_pause is not None and off_pause and on_pause > slack * off_pause:
        fail(f"median pause {on_pause:.0f} ms, today's {off_pause:.0f} ms")
    on_resume, off_resume = median(on["resume_s"]), median(off["resume_s"])
    if on_resume is not None and off_resume and on_resume > RESUME_SLACK * off_resume:
        fail(f"median first answer {on_resume * 1000:.0f} ms after the resume, today's {off_resume * 1000:.0f} ms")


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--root", default=ROOT)
    parser.add_argument("--cycles", type=int)
    parser.add_argument("--workloads")
    parser.add_argument("runs", nargs="+")
    args = parser.parse_args(argv)
    if len(args.runs) > 2:
        parser.error("one or two runs")
    runs = [(run, load(results_dir(run, args.root))) for run in args.runs]
    failures = []
    for run, data in runs:
        if not data:
            failures.append(f"{run}: no series records in {results_dir(run, args.root)}")
    names = [workload_id(w) for w in args.workloads.split(",")] if args.workloads else []
    for _, data in runs:
        names += [w for w in sorted(data) if w not in names]
    pair = None
    if len(runs) == 2:
        modes = [next((r.get("delta_saves") for r in data.values() if r.get("delta_saves")), None)
                 for _, data in runs]
        if sorted(m or "" for m in modes) != ["off", "on"]:
            failures.append(f"the runs have delta saves {modes[0]} and {modes[1]}, not on and off")
        else:
            pair = runs if modes[0] == "on" else runs[::-1]
            runs = pair
    out = []
    for workload in names:
        recs = [data.get(workload) for _, data in runs]
        ss = [stats(r) if r else None for r in recs]
        for (run, _), rec, s in zip(runs, recs, ss):
            check_run(run, workload, rec, s, args.cycles, failures)
        present = [s for s in ss if s]
        if not present:
            continue
        rec = next(r for r in recs if r)
        out += [f"### {workload}", "",
                f"dirty: {rec.get('dirty') or 'none'}, idle {rec.get('idle_s')} s, "
                f"settle {rec.get('settle_s')} s, disk {rec.get('disk') or '-'}", ""]
        heads = [f"{run} (delta {s['delta_saves'] if s else '-'})" for (run, _), s in zip(runs, ss)]
        table = [rows(s) if s else None for s in ss]
        if len(runs) == 2:
            out += [f"| Metric | {heads[0]} | {heads[1]} | ratio |", "| --- | --- | --- | --- |"]
        else:
            out += [f"| Metric | {heads[0]} |", "| --- | --- |"]
        base = next(t for t in table if t)
        for i, (metric, _, _) in enumerate(base):
            cells = [t[i][1] if t else "-" for t in table]
            line = f"| {metric} | " + " | ".join(cells) + " |"
            if len(runs) == 2:
                a = table[0][i][2] if table[0] else None
                b = table[1][i][2] if table[1] else None
                line += f" {ratio(a, b)} |"
            out.append(line)
        out.append("")
        if pair and all(ss):
            check_pair(workload, ss[0], ss[1], failures)
    print("\n".join(out))
    if failures:
        print("FAILED:")
        for f in failures:
            print(f"- {f}")
        return 1
    print("PASSED: every check")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
