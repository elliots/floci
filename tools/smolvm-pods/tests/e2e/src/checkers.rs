//! Checkers of the hibernation test that need no node: the counter lines in a
//! pod's log, the guest's clocks after a wake, and the settled reading of
//! several samples.

use std::fmt;

/// What the counter lines (`tick N`) of a log show.
#[derive(Debug, PartialEq, Eq)]
pub struct Ticks {
    /// How many counter lines the log has.
    pub count: usize,
    /// The first counter, and the highest.
    pub first: Option<u64>,
    pub last: Option<u64>,
    /// Each place where counters are missing: the counter before the gap and
    /// the one after it.
    pub gaps: Vec<(u64, u64)>,
    /// Each run of counters that came again, first and last: consecutive
    /// lines with counters already seen, each one more than the one before.
    pub replays: Vec<(u64, u64)>,
    /// The replays that do not end at the highest counter before them, so
    /// that they repeat something other than the latest output.
    pub out_of_order: Vec<(u64, u64)>,
}

impl Ticks {
    /// Reads the counter lines of `log`, as `kubectl logs` prints it.
    pub fn parse(log: &str) -> Ticks {
        let mut ticks = Ticks {
            count: 0,
            first: None,
            last: None,
            gaps: Vec::new(),
            replays: Vec::new(),
            out_of_order: Vec::new(),
        };
        let counters = log.lines().filter_map(|line| {
            line.trim_end()
                .strip_prefix("tick ")
                .and_then(|n| n.parse::<u64>().ok())
        });
        // The replay being read, and the highest counter before it.
        let mut replay: Option<(u64, u64, u64)> = None;
        for n in counters {
            ticks.count += 1;
            let Some(last) = ticks.last else {
                ticks.first = Some(n);
                ticks.last = Some(n);
                continue;
            };
            if n <= last {
                replay = match replay {
                    Some((first, end, before)) if n == end + 1 => Some((first, n, before)),
                    Some(run) => {
                        ticks.end_replay(run);
                        Some((n, n, last))
                    }
                    None => Some((n, n, last)),
                };
                continue;
            }
            if let Some(run) = replay.take() {
                ticks.end_replay(run);
            }
            if n > last + 1 {
                ticks.gaps.push((last, n));
            }
            ticks.last = Some(n);
        }
        if let Some(run) = replay {
            ticks.end_replay(run);
        }
        ticks
    }

    fn end_replay(&mut self, (first, end, before): (u64, u64, u64)) {
        self.replays.push((first, end));
        if end != before {
            self.out_of_order.push((first, end));
        }
    }

    /// Fails unless the counters run from 1 without a gap and with at most
    /// `replays` runs of the latest output repeated.
    pub fn check(&self, replays: usize) -> Result<(), String> {
        let mut problems = Vec::new();
        match self.first {
            None => problems.push("no counter lines".to_string()),
            Some(1) => {}
            Some(n) => problems.push(format!("the first counter is {n}, not 1")),
        }
        if !self.gaps.is_empty() {
            problems.push(format!("gaps after and before: {:?}", self.gaps));
        }
        if self.replays.len() > replays {
            problems.push(format!(
                "{} runs of counters repeated, at most {replays} allowed: {:?}",
                self.replays.len(),
                self.replays
            ));
        }
        if !self.out_of_order.is_empty() {
            problems.push(format!("counters out of order: {:?}", self.out_of_order));
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(format!("{self}: {}", problems.join("; ")))
        }
    }
}

impl fmt::Display for Ticks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} counter lines from {:?} to {:?}",
            self.count, self.first, self.last
        )
    }
}

/// How far a clock reading taken during a request is outside the request:
/// zero if `reading` is within `[start, end]`, negative if it is earlier, and
/// positive if later. All three are in ns of the same clock.
pub fn skew(reading: i64, start: i64, end: i64) -> i64 {
    if reading < start {
        reading - start
    } else if reading > end {
        reading - end
    } else {
        0
    }
}

/// A reading of the guest's clocks through a request from the node: the
/// guest's realtime and monotonic clocks, and the node's clocks before and
/// after the request. All in ns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockSample {
    pub guest_realtime: i64,
    pub guest_monotonic: i64,
    pub node_realtime: (i64, i64),
    pub node_monotonic: (i64, i64),
}

impl ClockSample {
    /// How far the guest's realtime is outside the request on the node's
    /// realtime clock (see [`skew`]).
    pub fn realtime_skew(&self) -> i64 {
        skew(
            self.guest_realtime,
            self.node_realtime.0,
            self.node_realtime.1,
        )
    }
}

/// Checks that the guest's monotonic clock advanced between `before` and
/// `after` by the node's time between them, within `tolerance` ns: a guest's
/// clocks move on while it sleeps, as the node's do. The node's time between
/// the samples is at least from the end of the first to the start of the
/// second, and at most from the start of the one to the end of the other.
/// Returns the guest's advance and the range it had to be in.
pub fn check_monotonic(
    before: &ClockSample,
    after: &ClockSample,
    tolerance: i64,
) -> Result<String, String> {
    let advance = after.guest_monotonic - before.guest_monotonic;
    let elapsed_min = after.node_monotonic.0 - before.node_monotonic.1;
    let elapsed_max = after.node_monotonic.1 - before.node_monotonic.0;
    let low = elapsed_min - tolerance;
    let high = elapsed_max + tolerance;
    let ms = |ns: i64| ns as f64 / 1e6;
    let summary = format!(
        "the guest's monotonic clock advanced {:.1} ms while the node's advanced {:.1} to \
         {:.1} ms; expected {:.1} to {:.1} ms",
        ms(advance),
        ms(elapsed_min),
        ms(elapsed_max),
        ms(low),
        ms(high)
    );
    if (low..=high).contains(&advance) {
        Ok(summary)
    } else {
        Err(summary)
    }
}

/// The settled reading of several samples of one measurement: the value seen
/// most often, if it is seen at least twice and no other value is seen as
/// often. Something in flight for a moment changes one sample at most, while
/// a lasting change shows in every sample. Failed samples count for no value.
pub fn settled<T: Copy + PartialEq + fmt::Debug>(
    samples: &[Result<T, String>],
) -> Result<T, String> {
    let mut counts: Vec<(T, usize)> = Vec::new();
    for value in samples.iter().flatten() {
        match counts.iter_mut().find(|(seen, _)| seen == value) {
            Some((_, n)) => *n += 1,
            None => counts.push((*value, 1)),
        }
    }
    let most = counts.iter().map(|&(_, n)| n).max().unwrap_or(0);
    let modes: Vec<T> = counts
        .iter()
        .filter(|&&(_, n)| n == most)
        .map(|&(value, _)| value)
        .collect();
    match modes[..] {
        [value] if most >= 2 => Ok(value),
        _ => Err(format!("no settled value in the samples {samples:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(counters: &[u64]) -> String {
        let mut text = String::from("app holding 256 MiB, listening on :8080\n");
        for n in counters {
            text += &format!("tick {n}\n");
        }
        text
    }

    #[test]
    fn a_whole_sequence_passes() {
        let ticks = Ticks::parse(&log(&[1, 2, 3, 4]));
        assert_eq!(ticks.count, 4);
        assert_eq!((ticks.first, ticks.last), (Some(1), Some(4)));
        assert_eq!(ticks.check(0), Ok(()));
    }

    #[test]
    fn finds_gaps() {
        let ticks = Ticks::parse(&log(&[1, 2, 5, 6, 9]));
        assert_eq!(ticks.gaps, [(2, 5), (6, 9)]);
        let error = ticks.check(0).unwrap_err();
        assert!(
            error.contains("gaps after and before: [(2, 5), (6, 9)]"),
            "{error}"
        );
    }

    #[test]
    fn allows_only_the_replays_it_is_told() {
        let ticks = Ticks::parse(&log(&[1, 2, 2, 3, 4]));
        assert_eq!(ticks.replays, [(2, 2)]);
        assert!(ticks.check(0).is_err());
        assert_eq!(ticks.check(1), Ok(()));
        // A frame of two lines, replayed whole, is one run.
        let ticks = Ticks::parse(&log(&[1, 2, 3, 2, 3, 4]));
        assert_eq!(ticks.replays, [(2, 3)]);
        assert_eq!(ticks.check(1), Ok(()));
        let ticks = Ticks::parse(&log(&[1, 2, 2, 3, 3, 4]));
        assert_eq!(ticks.replays, [(2, 2), (3, 3)]);
        assert!(ticks
            .check(1)
            .unwrap_err()
            .contains("2 runs of counters repeated"));
    }

    #[test]
    fn finds_a_lost_start_and_counters_out_of_order() {
        let ticks = Ticks::parse(&log(&[3, 4]));
        assert!(ticks
            .check(0)
            .unwrap_err()
            .contains("the first counter is 3"));
        let ticks = Ticks::parse(&log(&[1, 2, 3, 1, 4]));
        assert_eq!(ticks.out_of_order, [(1, 1)]);
        assert_eq!(ticks.gaps.len(), 0);
        assert!(ticks
            .check(5)
            .unwrap_err()
            .contains("out of order: [(1, 1)]"));
        let ticks = Ticks::parse(&log(&[1, 2, 3, 3]));
        assert_eq!(
            (ticks.replays.clone(), ticks.out_of_order.len()),
            (vec![(3, 3)], 0)
        );
    }

    #[test]
    fn an_empty_log_fails() {
        let ticks = Ticks::parse("app started\n");
        assert_eq!(ticks.count, 0);
        assert!(ticks.check(0).unwrap_err().contains("no counter lines"));
    }

    #[test]
    fn ignores_other_lines() {
        let text = "tick\ntick x\nticks 3\n tick 2\ntick 1\r\ntick 2\n";
        let ticks = Ticks::parse(text);
        assert_eq!((ticks.count, ticks.last), (2, Some(2)));
        assert_eq!(ticks.check(0), Ok(()));
    }

    #[test]
    fn skew_is_the_distance_outside_the_request() {
        assert_eq!(skew(150, 100, 200), 0);
        assert_eq!(skew(100, 100, 200), 0);
        assert_eq!(skew(40, 100, 200), -60);
        assert_eq!(skew(260, 100, 200), 60);
    }

    fn sample(guest_monotonic: i64, node: i64) -> ClockSample {
        ClockSample {
            guest_realtime: 0,
            guest_monotonic,
            node_realtime: (0, 0),
            node_monotonic: (node, node + 2),
        }
    }

    #[test]
    fn monotonic_advances_with_the_node_through_a_sleep() {
        // Each sample takes 2 on the node's clock, and the second starts 100
        // after the first: the node's time between them is 98 to 102.
        let before = sample(1000, 0);
        for advance in [98, 100, 102] {
            let after = sample(1000 + advance, 100);
            assert!(check_monotonic(&before, &after, 0).is_ok(), "{advance}");
        }
        for advance in [0, 50, 97, 103] {
            let after = sample(1000 + advance, 100);
            let error = check_monotonic(&before, &after, 0).unwrap_err();
            assert!(error.contains("expected"), "{error}");
        }
        let after = sample(1000 + 105, 100);
        assert!(check_monotonic(&before, &after, 3).is_ok());
    }

    #[test]
    fn realtime_skew_uses_the_node_realtime() {
        let mut s = sample(0, 0);
        s.node_realtime = (1_000, 2_000);
        s.guest_realtime = 900;
        assert_eq!(s.realtime_skew(), -100);
    }

    fn ok(samples: &[(u64, u64)]) -> Vec<Result<(u64, u64), String>> {
        samples.iter().map(|&s| Ok(s)).collect()
    }

    #[test]
    fn settled_ignores_one_sample_that_differs() {
        assert_eq!(
            settled(&ok(&[(11, 18), (12, 21), (11, 18), (11, 18), (11, 18)])),
            Ok((11, 18))
        );
        assert_eq!(
            settled(&ok(&[(1, 9), (11, 18), (11, 18), (12, 21), (11, 18)])),
            Ok((11, 18))
        );
        let mut samples = ok(&[(11, 18), (11, 18)]);
        samples.push(Err("ctl agent timed out".into()));
        assert_eq!(settled(&samples), Ok((11, 18)));
    }

    #[test]
    fn settled_keeps_a_lasting_change() {
        assert_eq!(settled(&ok(&[(12, 21); 5])), Ok((12, 21)));
        assert_eq!(
            settled(&ok(&[(12, 21), (12, 21), (11, 18), (12, 21), (12, 21)])),
            Ok((12, 21))
        );
    }

    #[test]
    fn settled_needs_one_value_seen_most_often() {
        let error = settled(&ok(&[(11, 18), (12, 21), (13, 24)])).unwrap_err();
        assert!(error.contains("(13, 24)"), "{error}");
        assert!(settled(&ok(&[(11, 18), (12, 21), (11, 18), (12, 21)])).is_err());
        assert!(settled(&ok(&[(11, 18)])).is_err());
        let failed: Vec<Result<(u64, u64), String>> =
            vec![Err("no smolvm-agent process".into()); 5];
        let error = settled(&failed).unwrap_err();
        assert!(error.contains("no smolvm-agent process"), "{error}");
        assert!(settled::<(u64, u64)>(&[]).is_err());
    }
}
