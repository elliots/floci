"""Tests of compare.py on synthetic series records: python3 -m unittest test_compare"""
import contextlib
import io
import json
import os
import tempfile
import unittest

import compare

FULL = 1_270_000_000
MIB2 = 2 << 20


def cycle(n, mode, written, pause_ms=500, answer_s=0.15, hash_ok=True, disk=None):
    c = {"cycle": n, "save_mode": mode, "written_bytes": written, "pause_ms": pause_ms,
         "disk_pause_bytes": written + 4096 if disk is None else disk, "disk_settled": True,
         "base_bytes": FULL if mode != "none" else 0}
    if n > 0:
        c.update({"first_answer_s": answer_s, "hash_ok": hash_ok, "disk_wake_bytes": 1 << 20,
                  "node_answered": {"smaps": {"Rss": 1000, "AnonHugePages": 980}},
                  "dirty": {"blocks_touched": 5}})
    return c


def series(delta, later_written, cycles=3, **kw):
    first = cycle(0, "full", FULL)
    later = [cycle(n, "delta" if delta == "on" else "full", later_written, **kw) for n in range(1, cycles + 1)]
    return {"delta_saves": delta, "dirty": "", "idle_s": 5, "settle_s": 3, "disk": "vdb",
            "cycles": [first] + later, "final": {"first_answer_s": 0.15, "hash_ok": True}}


class CompareTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def write(self, label, records):
        results = os.path.join(self.tmp.name, label, "results")
        os.makedirs(results, exist_ok=True)
        for workload, rec in records.items():
            with open(os.path.join(results, workload + ".series.jsonl"), "w") as f:
                f.write(json.dumps(rec) + "\n")

    def run_compare(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            status = compare.main(["--root", self.tmp.name, "--cycles", "3", *args])
        return status, out.getvalue()

    def good_pair(self):
        self.write("saves-on", {"memhold-idle": series("on", 10 * MIB2),
                                "memhold-all": series("on", FULL, pause_ms=520)})
        self.write("saves-off", {"memhold-idle": series("off", FULL),
                                 "memhold-all": series("off", FULL)})

    def test_a_good_pair_passes_with_a_table_per_workload(self):
        self.good_pair()
        status, out = self.run_compare("saves-on", "saves-off")
        self.assertEqual(status, 0, out)
        self.assertIn("### memhold-idle", out)
        self.assertIn("### memhold-all", out)
        self.assertIn("| saves-on (delta on) | saves-off (delta off) | ratio |", out)
        self.assertIn("| later pauses: save modes | delta 3 | full 3 |", out)
        # 500 pods x 10 sleeps x 1.27 GB
        self.assertIn("| 6350.0 |", out)

    def test_the_run_with_delta_saves_on_comes_first_in_either_order(self):
        self.good_pair()
        status, out = self.run_compare("saves-off", "saves-on")
        self.assertEqual(status, 0, out)
        self.assertIn("| saves-on (delta on) | saves-off (delta off) |", out)

    def test_a_memory_check_failure_fails(self):
        self.write("saves-on", {"memhold-idle": series("on", 10 * MIB2, hash_ok=False)})
        status, out = self.run_compare("saves-on")
        self.assertEqual(status, 1)
        self.assertIn("memory differs after the resume", out)
        self.assertIn("| memory checks failed | 3 of 4 |", out)

    def test_an_idle_wake_that_writes_more_than_a_quarter_fails(self):
        self.write("saves-on", {"memhold-idle": series("on", FULL // 2)})
        self.write("saves-off", {"memhold-idle": series("off", FULL)})
        status, out = self.run_compare("saves-on", "saves-off")
        self.assertEqual(status, 1)
        self.assertIn("more than a quarter", out)

    def test_disagreeing_disk_bytes_and_a_slow_pause_fail(self):
        self.write("saves-on", {"memhold-all": series("on", FULL, pause_ms=700, disk=FULL * 2)})
        self.write("saves-off", {"memhold-all": series("off", FULL)})
        status, out = self.run_compare("saves-on", "saves-off")
        self.assertEqual(status, 1)
        self.assertIn("differ by more than 5%", out)
        self.assertIn("median pause 700 ms, today's 500 ms", out)

    def test_the_disk_bytes_are_less_the_background_writes(self):
        small = 10 * MIB2
        rec = series("on", small)
        for c in rec["cycles"]:
            c.update({"disk_pause_bytes": c["written_bytes"] + 4_000_000, "disk_pause_s": 4.0,
                      "disk_background_bytes": 2_000_000, "disk_background_s": 2.0})
        self.write("saves-on", {"memhold-idle": rec})
        self.write("saves-off", {"memhold-idle": series("off", FULL)})
        status, out = self.run_compare("saves-on", "saves-off")
        self.assertEqual(status, 0, out)
        self.assertIn(f"| later pauses: written (disk), MB, median / p95 | {small / 1e6:.1f} / {small / 1e6:.1f} |", out)
        self.assertIn("| node background writes, KB/s, median / p95 | 1000.0 / 1000.0 |", out)

    def test_two_runs_with_the_same_mode_fail(self):
        self.write("a", {"memhold-idle": series("on", 10 * MIB2)})
        self.write("b", {"memhold-idle": series("on", 10 * MIB2)})
        status, out = self.run_compare("a", "b")
        self.assertEqual(status, 1)
        self.assertIn("not on and off", out)

    def test_delta_pauses_with_delta_saves_off_and_missing_workloads_fail(self):
        self.write("saves-off", {"memhold-idle": series("off", FULL)})
        rec = series("off", FULL)
        rec["cycles"][2]["save_mode"] = "delta"
        self.write("saves-off", {"memhold-idle": rec})
        status, out = self.run_compare("--workloads", "idle,petclinic", "saves-off")
        self.assertEqual(status, 1)
        self.assertIn("delta saves are off", out)
        self.assertIn("saves-off petclinic: no record", out)

    def test_workload_ids_match_check_sh(self):
        self.assertEqual(compare.workload_id("idle"), "memhold-idle")
        self.assertEqual(compare.workload_id("all"), "memhold-all")
        self.assertEqual(compare.workload_id("petclinic"), "petclinic")
        self.assertEqual(compare.workload_id("scattered:0.5"), "memhold-scattered-0p5")


if __name__ == "__main__":
    unittest.main()
