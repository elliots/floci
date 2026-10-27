//! A small logger for the node service and the front shim: one line per
//! record, with the UTC time, to stderr or to a file. `RUST_LOG` sets the
//! level (default `info`).

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{Level, LevelFilter, Log, Metadata, Record};

/// Where smolvm warns that a VM process it stops has already exited, and how
/// the warning starts.
const VM_EXIT_TARGET: &str = "smolvm::agent::manager";
const VM_EXIT_WARNING: &str = "VM exited without shutdown acknowledgment";

thread_local! {
    /// Whether this thread runs a pause of a pod VM ([`pausing`]).
    static PAUSING: Cell<bool> = const { Cell::new(false) };
}

struct Logger {
    out: Mutex<Option<File>>,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        let level = level_of(record);
        if level > log::max_level() {
            return;
        }
        let line = format!(
            "{} {} {}: {}\n",
            utc_now(),
            level,
            record.target(),
            record.args()
        );
        let Ok(mut out) = self.out.lock() else {
            return;
        };
        let _ = match out.as_mut() {
            Some(file) => file.write_all(line.as_bytes()),
            None => std::io::stderr().write_all(line.as_bytes()),
        };
    }

    fn flush(&self) {}
}

/// Log to `path`, appending, or to stderr if it is `None` or cannot be opened.
/// Only the first call takes effect.
pub fn init(path: Option<&Path>) {
    let file = path.and_then(|path| {
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        OpenOptions::new().create(true).append(true).open(path).ok()
    });
    let logger = Box::new(Logger {
        out: Mutex::new(file),
    });
    if log::set_boxed_logger(logger).is_ok() {
        log::set_max_level(level(std::env::var("RUST_LOG").ok().as_deref()));
    }
}

/// Runs `pause`, a pause of a pod VM, with smolvm's warning that a VM exited
/// without acknowledging its shutdown logged at debug level on this thread. A
/// successful pause stops the paused VM process itself, then drops smolvm's
/// handle of that VM, whose own stop finds the process gone and warns. Outside
/// a pause the warning keeps its level, since there it means a VM that exited
/// by itself.
pub fn pausing<T>(pause: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            PAUSING.with(|pausing| pausing.set(self.0));
        }
    }
    let _restore = Restore(PAUSING.with(|pausing| pausing.replace(true)));
    pause()
}

/// The level `record` is logged at: its own, but debug for smolvm's warning
/// about the VM that a pause on this thread stopped.
fn level_of(record: &Record) -> Level {
    let expected_exit = record.level() == Level::Warn
        && record.target() == VM_EXIT_TARGET
        && PAUSING.with(Cell::get)
        && record.args().to_string().starts_with(VM_EXIT_WARNING);
    if expected_exit {
        Level::Debug
    } else {
        record.level()
    }
}

fn level(spec: Option<&str>) -> LevelFilter {
    spec.and_then(|s| s.trim().parse().ok())
        .unwrap_or(LevelFilter::Info)
}

/// The time as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
fn utc_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format_utc(now.as_secs(), now.subsec_millis())
}

/// Milliseconds since the epoch as `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub(crate) fn format_utc_ms(ms: u64) -> String {
    format_utc(ms / 1000, (ms % 1000) as u32)
}

pub(crate) fn format_utc(secs: u64, millis: u32) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn times_are_utc() {
        assert_eq!(format_utc(0, 0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_utc(1_792_400_645, 7), "2026-10-19T09:04:05.007Z");
        assert_eq!(format_utc(951_782_400, 0), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn a_pause_logs_the_exit_of_the_vm_it_stopped_at_debug() {
        let exited = Record::builder()
            .level(Level::Warn)
            .target("smolvm::agent::manager")
            .args(format_args!(
                "VM exited without shutdown acknowledgment; filesystem synchronization \
                 unconfirmed pid=7 error=connect to agent: No such file or directory"
            ))
            .build();
        assert_eq!(level_of(&exited), Level::Warn);
        assert_eq!(pausing(|| level_of(&exited)), Level::Debug);
        assert_eq!(level_of(&exited), Level::Warn);
    }

    #[test]
    fn a_pause_keeps_the_level_of_other_records() {
        let restart = Record::builder()
            .level(Level::Warn)
            .target("smolvm::agent::manager")
            .args(format_args!("restarting agent VM"))
            .build();
        let ours = Record::builder()
            .level(Level::Warn)
            .target("containerd_shim_smolvm_pods_v2::lifecycle::pod")
            .args(format_args!("VM exited without shutdown acknowledgment"))
            .build();
        let error = Record::builder()
            .level(Level::Error)
            .target("smolvm::agent::manager")
            .args(format_args!("VM exited without shutdown acknowledgment"))
            .build();
        pausing(|| {
            assert_eq!(level_of(&restart), Level::Warn);
            assert_eq!(level_of(&ours), Level::Warn);
            assert_eq!(level_of(&error), Level::Error);
        });
    }

    #[test]
    fn the_level_defaults_to_info() {
        assert_eq!(level(None), LevelFilter::Info);
        assert_eq!(level(Some("debug")), LevelFilter::Debug);
        assert_eq!(level(Some("smolvm=debug,info")), LevelFilter::Info);
    }
}
