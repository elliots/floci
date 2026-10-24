//! The node service's settings, from its command line:
//!
//! ```text
//! smolvm-pods node-service [--idle-timeout 5m] [--poll-interval 15s]
//!     [--cpu-threshold 0.5|off] [--cpu-window 60s]
//!     [--max-awake-pods 0] [--max-awake-memory 0] [--admission-wait 10s]
//!     [--api-listen ADDR:PORT] [--api-token-file PATH]
//! ```
//!
//! Durations are Go durations, as in pod annotations. Each flag also takes
//! `--flag=VALUE`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crate::lifecycle::{parse_cpu_threshold, parse_duration};

/// Where `--api-token-file` points by default.
pub const API_TOKEN_FILE: &str = "/etc/floci-node/api-token";

#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    /// How long a pod may be idle before it sleeps, unless its annotation
    /// `smolvm-pods.floci.io/idle-timeout` says otherwise.
    pub idle_timeout: Duration,
    /// How often the node's conntrack table is read for activity.
    pub poll_interval: Duration,
    /// VM CPU, in vCPUs averaged over `cpu_window`, above which a pod is
    /// busy and does not sleep; none for no CPU rule. A pod's annotation
    /// `smolvm-pods.floci.io/cpu-threshold` overrides it.
    pub cpu_threshold: Option<f64>,
    pub cpu_window: Duration,
    /// Pods that hold room at most (creating, awake, pausing or waking); 0 for
    /// no cap.
    pub max_awake_pods: u32,
    /// Their guest RAM in MiB at most; 0 for no cap.
    pub max_awake_memory_mib: u64,
    /// How long a wake waits for room under a cap.
    pub admission_wait: Duration,
    /// The API's TCP listener, which requires a bearer token; none for no
    /// listener.
    pub api_listen: Option<SocketAddr>,
    /// The file with that token, mode 0600.
    pub api_token_file: PathBuf,
}

impl Default for Settings {
    fn default() -> Settings {
        Settings {
            idle_timeout: Duration::from_secs(300),
            poll_interval: Duration::from_secs(15),
            cpu_threshold: Some(0.5),
            cpu_window: Duration::from_secs(60),
            max_awake_pods: 0,
            max_awake_memory_mib: 0,
            admission_wait: Duration::from_secs(10),
            api_listen: None,
            api_token_file: PathBuf::from(API_TOKEN_FILE),
        }
    }
}

impl Settings {
    /// The settings that `args`, the node service's arguments after
    /// `node-service`, give.
    pub fn parse(args: &[String]) -> Result<Settings, String> {
        let mut settings = Settings::default();
        let mut words = args.iter();
        while let Some(word) = words.next() {
            let flag = word
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {word:?}"))?;
            let (flag, value) = match flag.split_once('=') {
                Some((flag, value)) => (flag, value.to_string()),
                None => {
                    let value = words
                        .next()
                        .ok_or_else(|| format!("--{flag} needs a value"))?;
                    (flag, value.clone())
                }
            };
            settings.set(flag, &value)?;
        }
        Ok(settings)
    }

    fn set(&mut self, flag: &str, value: &str) -> Result<(), String> {
        let duration = || match parse_duration(value) {
            Ok(d) if !d.is_zero() => Ok(d),
            Ok(_) => Err(format!("--{flag} must be more than 0")),
            Err(e) => Err(format!("--{flag}: {e}")),
        };
        let number = |what: &str| format!("--{flag}: {value:?} is not {what}");
        match flag {
            "idle-timeout" => self.idle_timeout = duration()?,
            "poll-interval" => self.poll_interval = duration()?,
            "cpu-threshold" => {
                self.cpu_threshold =
                    parse_cpu_threshold(value).map_err(|e| format!("--{flag}: {e}"))?
            }
            "cpu-window" => self.cpu_window = duration()?,
            "max-awake-pods" => {
                self.max_awake_pods = value.parse().map_err(|_| number("a count"))?
            }
            "max-awake-memory" => {
                self.max_awake_memory_mib = value.parse().map_err(|_| number("a size in MiB"))?
            }
            "admission-wait" => self.admission_wait = duration()?,
            "api-listen" => {
                self.api_listen = Some(value.parse().map_err(|_| number("an ADDR:PORT"))?)
            }
            "api-token-file" if !value.is_empty() => self.api_token_file = PathBuf::from(value),
            "api-token-file" => return Err("--api-token-file needs a path".into()),
            other => return Err(format!("unknown flag --{other}")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(str::to_string).collect()
    }

    #[test]
    fn no_flags_give_the_defaults() {
        let settings = Settings::parse(&[]).unwrap();
        assert_eq!(settings, Settings::default());
        assert_eq!(settings.idle_timeout, Duration::from_secs(300));
        assert_eq!(settings.poll_interval, Duration::from_secs(15));
        assert_eq!(
            (settings.cpu_threshold, settings.cpu_window.as_secs()),
            (Some(0.5), 60)
        );
        assert_eq!(
            (settings.max_awake_pods, settings.max_awake_memory_mib),
            (0, 0)
        );
        assert_eq!(settings.admission_wait, Duration::from_secs(10));
        assert_eq!(settings.api_listen, None);
        assert_eq!(settings.api_token_file, PathBuf::from(API_TOKEN_FILE));
    }

    #[test]
    fn every_flag_parses() {
        let settings = Settings::parse(&args(
            "--idle-timeout 10s --poll-interval=5s --cpu-threshold 1.5 --cpu-window 30s \
             --max-awake-pods 2 --max-awake-memory=8192 --admission-wait 1m \
             --api-listen 0.0.0.0:7070 --api-token-file /run/token",
        ))
        .unwrap();
        assert_eq!(
            settings,
            Settings {
                idle_timeout: Duration::from_secs(10),
                poll_interval: Duration::from_secs(5),
                cpu_threshold: Some(1.5),
                cpu_window: Duration::from_secs(30),
                max_awake_pods: 2,
                max_awake_memory_mib: 8192,
                admission_wait: Duration::from_secs(60),
                api_listen: Some("0.0.0.0:7070".parse().unwrap()),
                api_token_file: PathBuf::from("/run/token"),
            }
        );
    }

    #[test]
    fn the_cpu_rule_can_be_turned_off() {
        for line in [
            "--cpu-threshold off",
            "--cpu-threshold=0",
            "--cpu-threshold 0.0",
        ] {
            let settings = Settings::parse(&args(line)).unwrap();
            assert_eq!(settings.cpu_threshold, None, "{line}");
        }
        let later = Settings::parse(&args("--cpu-threshold off --cpu-threshold 2")).unwrap();
        assert_eq!(later.cpu_threshold, Some(2.0), "a later flag wins");
    }

    #[test]
    fn bad_flags_are_refused_with_the_flag() {
        for (line, want) in [
            ("--idle-timeout", "needs a value"),
            ("--idle-timeout 0", "more than 0"),
            ("--idle-timeout soon", "--idle-timeout"),
            ("--cpu-threshold -1", "--cpu-threshold"),
            ("--max-awake-pods many", "--max-awake-pods"),
            ("--api-listen 7070", "--api-listen"),
            ("--api-token-file=", "needs a path"),
            ("--sleep-sooner 1s", "unknown flag --sleep-sooner"),
            ("idle", "unexpected argument"),
        ] {
            let e = Settings::parse(&args(line)).unwrap_err();
            assert!(e.contains(want), "{line}: {e}");
        }
    }
}
