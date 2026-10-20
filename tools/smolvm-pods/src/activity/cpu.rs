//! VM CPU from `/proc/PID/stat`.
//!
//! A pod is busy while its VM process, all its vCPU and pump threads, used
//! more than the CPU threshold (`--cpu-threshold`, half a vCPU by default)
//! on average over the CPU window (`--cpu-window`, 60 s by default), or
//! since the newest reading at least [`RECENT`] old. A pod's annotations
//! override both, and a threshold of `off` turns the rule off. Each poll
//! reads the process's `utime` and `stime`; the window's average runs from
//! the newest reading at least a window old, or from the process's start if
//! it started within the window, as after a wake. The recent average keeps
//! a computation that has just begun from being frozen before it fills the
//! window.

use std::collections::{HashMap, VecDeque};
use std::time::Duration;

use crate::lifecycle::VmProcess;

/// Readings kept per pod at most, whatever the poll interval.
const MAX_SAMPLES: usize = 256;

/// The shortest span of the recent average: long enough that an exec
/// probe's few seconds of CPU do not count as busy for a whole poll.
pub const RECENT: Duration = Duration::from_secs(5);

/// One reading of a VM process. Times are since boot, as `CLOCK_BOOTTIME`
/// counts them and as `/proc/PID/stat` gives the start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub at: Duration,
    /// `utime` plus `stime`, of every thread.
    pub cpu: Duration,
    pub started: Duration,
}

/// The fields of `/proc/PID/stat` that the meter reads, in clock ticks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stat {
    pub utime: u64,
    pub stime: u64,
    pub starttime: u64,
}

/// Fields 14, 15 and 22 of `/proc/PID/stat`. The command name, field 2,
/// may hold spaces and parentheses, so fields are counted from the last `)`.
pub fn parse_stat(text: &str) -> Option<Stat> {
    let fields: Vec<&str> = text[text.rfind(')')? + 1..].split_whitespace().collect();
    let field = |n: usize| fields.get(n - 3)?.parse().ok();
    Some(Stat {
        utime: field(14)?,
        stime: field(15)?,
        starttime: field(22)?,
    })
}

fn ticks(n: u64, per_second: u64) -> Duration {
    Duration::from_nanos((u128::from(n) * 1_000_000_000 / u128::from(per_second.max(1))) as u64)
}

fn boottime() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes the timespec, which lives for the call.
    unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// Reads the VM process. An error if it is gone, or if its start time is
/// not the recorded one, so that the pid is another process's now.
pub fn sample(process: VmProcess) -> Result<Sample, String> {
    let pid = process.pid;
    let text = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|e| format!("VM process {pid}: {e}"))?;
    let at = boottime();
    let stat = parse_stat(&text).ok_or_else(|| format!("VM process {pid}: unexpected stat"))?;
    if process.start_time.is_some_and(|t| t != stat.starttime) {
        return Err(format!("pid {pid} is no longer the VM process"));
    }
    // SAFETY: sysconf has no preconditions.
    let hz = u64::try_from(unsafe { libc::sysconf(libc::_SC_CLK_TCK) }).unwrap_or(100);
    Ok(Sample {
        at,
        cpu: ticks(stat.utime + stat.stime, hz),
        started: ticks(stat.starttime, hz),
    })
}

struct Track {
    process: VmProcess,
    /// `(at, cpu)`, oldest first.
    samples: VecDeque<(Duration, Duration)>,
}

/// The readings of each awake pod's VM process.
#[derive(Default)]
pub struct CpuMeter {
    pods: HashMap<String, Track>,
}

impl CpuMeter {
    /// Adds a reading of the pod's VM process, and returns its CPU in vCPUs:
    /// the larger of its average over `window` and its average since the
    /// newest reading at least [`RECENT`] old. None if it cannot be measured
    /// yet: the process started before the window, and this is the first
    /// reading. A new VM process, after a wake, starts afresh.
    pub fn record(
        &mut self,
        pod: &str,
        process: VmProcess,
        sample: Sample,
        window: Duration,
    ) -> Option<f64> {
        let track = self.pods.entry(pod.to_string()).or_insert_with(|| Track {
            process,
            samples: VecDeque::new(),
        });
        if track.process != process {
            track.process = process;
            track.samples.clear();
        }
        let samples = &mut track.samples;
        if samples.back().is_none_or(|&(at, _)| at < sample.at) {
            samples.push_back((sample.at, sample.cpu));
        }
        let from = sample.at.saturating_sub(window);
        while samples.len() > MAX_SAMPLES || samples.get(1).is_some_and(|&(at, _)| at <= from) {
            samples.pop_front();
        }
        let first = samples.front().copied().filter(|&(at, _)| at < sample.at);
        let (since, cpu) = match first {
            Some((at, cpu)) if at <= from => (at, cpu),
            _ if sample.started >= from => (sample.started, Duration::ZERO),
            Some(first) => first,
            None => return None,
        };
        let average = |since: Duration, cpu: Duration| {
            let span = sample.at.checked_sub(since).filter(|s| !s.is_zero())?;
            Some(sample.cpu.saturating_sub(cpu).as_secs_f64() / span.as_secs_f64())
        };
        let window = average(since, cpu)?;
        let recent_from = sample.at.saturating_sub(RECENT);
        let recent = samples
            .iter()
            .rev()
            .find(|&&(at, _)| at <= recent_from)
            .and_then(|&(at, cpu)| average(at, cpu));
        Some(recent.map_or(window, |recent| recent.max(window)))
    }

    /// Forgets every pod for which `keep` is false.
    pub fn retain(&mut self, keep: impl Fn(&str) -> bool) {
        self.pods.retain(|pod, _| keep(pod));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VM: VmProcess = VmProcess {
        pid: 4242,
        start_time: Some(1000),
    };

    const MINUTE: Duration = Duration::from_secs(60);

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    fn at(at: f64, cpu: f64, started: f64) -> Sample {
        Sample {
            at: secs(at),
            cpu: secs(cpu),
            started: secs(started),
        }
    }

    fn near(got: Option<f64>, want: f64) -> bool {
        got.is_some_and(|g| (g - want).abs() < 1e-9)
    }

    #[test]
    fn stat_fields_are_read_after_the_command_name() {
        // A pod's VM process in the node: libkrun names it "libkrun VM".
        let text = "1067 (libkrun VM) S 1059 1067 0 0 -1 4194560 3139 3209 0 5 70 145 0 0 \
                    20 0 55 0 43633 3306283008 39447 18446744073709551615 187650001797120 \
                    187650009545740 281474057682848 0 0 0 0 4096 134218818 0 0 0 17 1 0 0 0 \
                    58 0 187650009642160 187650010034648 187650324230144 281474057686047 \
                    281474057686139 281474057686139 281474057687017 0\n";
        assert_eq!(
            parse_stat(text),
            Some(Stat {
                utime: 70,
                stime: 145,
                starttime: 43633,
            })
        );
        let odd = "4242 (a) b) S 1 4242 4242 0 -1 4194560 2849 0 0 0 731 214 0 0 20 0 9 0 1000";
        assert_eq!(parse_stat(odd).map(|s| s.starttime), Some(1000));
        assert_eq!(parse_stat("4242 (cut short) S 1 2"), None);
        assert_eq!(ticks(250, 100), Duration::from_millis(2500));
    }

    #[test]
    fn a_new_vm_process_is_measured_from_its_start() {
        let mut meter = CpuMeter::default();
        // Woke at 1000 s; 2 s of CPU in its first 10 s.
        assert!(near(
            meter.record("pod", VM, at(1010.0, 2.0, 1000.0), MINUTE),
            0.2
        ));
        // 15 s later, a busy loop: 15 s of CPU more, 17/25 since the start
        // and a whole vCPU since the last reading.
        assert!(near(
            meter.record("pod", VM, at(1025.0, 17.0, 1000.0), MINUTE),
            1.0
        ));
    }

    #[test]
    fn a_computation_that_has_just_begun_is_busy_at_once() {
        let mut meter = CpuMeter::default();
        // Quiet for 18 s after a wake, then a busy loop for 8 s.
        assert!(near(
            meter.record("pod", VM, at(1018.0, 0.5, 1000.0), MINUTE),
            0.5 / 18.0
        ));
        let busy = meter.record("pod", VM, at(1026.0, 8.5, 1000.0), MINUTE);
        assert!(near(busy, 1.0), "{busy:?}, 8.5/26 over the window");
        // Idle again: the recent average runs from the newest reading at
        // least RECENT old.
        assert!(near(
            meter.record("pod", VM, at(1028.0, 8.5, 1000.0), MINUTE),
            0.8
        ));
        assert!(near(
            meter.record("pod", VM, at(1040.0, 8.5, 1000.0), MINUTE),
            8.5 / 40.0
        ));
    }

    #[test]
    fn the_window_slides_over_the_readings() {
        let mut meter = CpuMeter::default();
        let mut cpu = 0.0;
        for i in 0..=8 {
            // One vCPU busy for the first minute, then idle.
            let t = 1000.0 + 15.0 * f64::from(i);
            if i > 0 && i <= 4 {
                cpu += 15.0;
            }
            let got = meter.record("pod", VM, at(t, cpu, 1000.0), MINUTE);
            match i {
                0 => assert_eq!(got, None, "no time has passed"),
                1..=4 => assert!(near(got, 1.0), "{i}: {got:?}"),
                // From the reading a window back.
                _ => {
                    let want = (60.0 - 15.0 * f64::from(i - 4)) / 60.0;
                    assert!(near(got, want), "{i}: {got:?} want {want}");
                }
            }
        }
        assert!(near(
            meter.record("pod", VM, at(1135.0, 60.0, 1000.0), MINUTE),
            0.0
        ));
        assert!(meter.pods["pod"].samples.len() <= 6);
    }

    #[test]
    fn each_reading_takes_its_pods_window() {
        let mut meter = CpuMeter::default();
        let ten = Duration::from_secs(10);
        meter.record("pod", VM, at(1000.0, 0.0, 0.0), ten);
        // Busy for 10 s, then quiet for 10 s.
        meter.record("pod", VM, at(1010.0, 10.0, 0.0), ten);
        assert!(near(
            meter.record("pod", VM, at(1020.0, 10.0, 0.0), ten),
            0.0
        ));
        let mut minute = CpuMeter::default();
        minute.record("pod", VM, at(1000.0, 0.0, 0.0), MINUTE);
        minute.record("pod", VM, at(1010.0, 10.0, 0.0), MINUTE);
        assert!(near(
            minute.record("pod", VM, at(1020.0, 10.0, 0.0), MINUTE),
            0.5
        ));
    }

    #[test]
    fn an_old_process_needs_a_second_reading() {
        let mut meter = CpuMeter::default();
        // The node service restarted; the VM has run since 10 s after boot.
        assert_eq!(
            meter.record("pod", VM, at(5000.0, 4000.0, 10.0), MINUTE),
            None
        );
        assert!(near(
            meter.record("pod", VM, at(5015.0, 4012.0, 10.0), MINUTE),
            0.8
        ));
        assert!(near(
            meter.record("pod", VM, at(5075.0, 4015.0, 10.0), MINUTE),
            3.0 / 60.0
        ));
    }

    #[test]
    fn a_new_process_or_another_pod_starts_afresh() {
        let mut meter = CpuMeter::default();
        meter.record("pod", VM, at(1010.0, 9.0, 1000.0), MINUTE);
        let woke = VmProcess {
            pid: 5000,
            start_time: Some(2000),
        };
        assert!(near(
            meter.record("pod", woke, at(2010.0, 1.0, 2000.0), MINUTE),
            0.1
        ));
        assert!(near(
            meter.record("other", VM, at(2010.0, 9.0, 2000.0), MINUTE),
            0.9
        ));
        // A reading that does not move forward is ignored.
        assert!(near(
            meter.record("other", VM, at(2010.0, 9.5, 2000.0), MINUTE),
            0.95
        ));
        meter.retain(|pod| pod == "other");
        assert!(meter.pods.contains_key("other") && !meter.pods.contains_key("pod"));
    }

    /// Reads this process while one thread spins for a second.
    #[test]
    #[ignore = "a loaded machine skews it"]
    fn live_a_busy_thread_reads_as_one_vcpu() {
        let pid = std::process::id() as i32;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        let process = VmProcess {
            pid,
            start_time: Some(parse_stat(&stat).unwrap().starttime),
        };
        // A window shorter than the spin, so that it is measured from the
        // first reading rather than from the start of the process.
        let mut meter = CpuMeter::default();
        let window = Duration::from_millis(500);
        meter.record("self", process, sample(process).unwrap(), window);
        let until = std::time::Instant::now() + Duration::from_secs(1);
        while std::time::Instant::now() < until {
            std::hint::spin_loop();
        }
        let vcpus = meter.record("self", process, sample(process).unwrap(), window);
        eprintln!("{vcpus:?} vCPU");
        assert!(vcpus.is_some_and(|v| (0.7..1.5).contains(&v)), "{vcpus:?}");
        let other = VmProcess {
            pid,
            start_time: Some(1),
        };
        assert!(sample(other)
            .unwrap_err()
            .contains("no longer the VM process"));
    }

    /// Reads the VM process `LIVE_VM_PID` twice, `LIVE_INTERVAL` seconds
    /// apart (5), and prints its CPU.
    #[test]
    #[ignore = "needs a VM process"]
    fn live_reads_a_vm_process() {
        let pid: i32 = std::env::var("LIVE_VM_PID")
            .expect("LIVE_VM_PID")
            .parse()
            .unwrap();
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap();
        let process = VmProcess {
            pid,
            start_time: Some(parse_stat(&stat).unwrap().starttime),
        };
        let mut meter = CpuMeter::default();
        let first = sample(process).unwrap();
        let since_start = meter.record("live", process, first, MINUTE);
        let interval = std::env::var("LIVE_INTERVAL").map_or(5, |v| v.parse().unwrap());
        std::thread::sleep(Duration::from_secs(interval));
        let second = sample(process).unwrap();
        let vcpus = meter.record("live", process, second, MINUTE);
        eprintln!("{first:?} {since_start:?} vCPU; {second:?} {vcpus:?} vCPU");
        assert!(vcpus.is_some());
    }
}
