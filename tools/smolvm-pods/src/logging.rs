//! A small logger for the node service and the front shim: one line per
//! record, with the UTC time, to stderr or to a file. `RUST_LOG` sets the
//! level (default `info`).

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use log::{LevelFilter, Log, Metadata, Record};

struct Logger {
    out: Mutex<Option<File>>,
}

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let line = format!(
            "{} {} {}: {}\n",
            utc_now(),
            record.level(),
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
    fn the_level_defaults_to_info() {
        assert_eq!(level(None), LevelFilter::Info);
        assert_eq!(level(Some("debug")), LevelFilter::Debug);
        assert_eq!(level(Some("smolvm=debug,info")), LevelFilter::Info);
    }
}
