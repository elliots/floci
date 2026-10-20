//! Wake admission: the ledger behind the lifecycle's `may_wake`, with caps on
//! the pods awake and on their guest RAM.
//!
//! A pod holds room from its create, or from the admission of its wake, until
//! it is asleep, failed or deleted, which releases it. A wake that does not
//! fit waits, first come first served, until room is released, for at most
//! `--admission-wait`, and is then refused with the reason, such as
//! `awake pods 2/2`. With no cap, every wake is admitted at once.

use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::settings::Settings;
use crate::sync::{lock, wait_timeout};

/// How often a waiting wake looks again without a release, in case room was
/// freed in a way the ledger did not see.
const RECHECK: Duration = Duration::from_millis(500);

/// A pod that holds room: its sandbox id and guest RAM in MiB.
pub type Holder = (String, u64);

pub struct Admission {
    max_pods: u32,
    max_memory_mib: u64,
    wait: Duration,
    state: Mutex<State>,
    room: Condvar,
}

#[derive(Default)]
struct State {
    /// The pods whose wake was admitted, with their guest RAM, until they
    /// sleep, fail or are deleted.
    admitted: HashMap<String, u64>,
    /// The tickets of the waiting wakes, oldest first.
    queue: VecDeque<u64>,
    next_ticket: u64,
}

impl Admission {
    pub fn new(settings: &Settings) -> Admission {
        Admission {
            max_pods: settings.max_awake_pods,
            max_memory_mib: settings.max_awake_memory_mib,
            wait: settings.admission_wait,
            state: Mutex::new(State::default()),
            room: Condvar::new(),
        }
    }

    fn is_capped(&self) -> bool {
        self.max_pods > 0 || self.max_memory_mib > 0
    }

    /// Admits a wake of `pod`, whose guest RAM is `memory_mib`, waiting up to
    /// the admission wait for room. `holders` lists the pods that hold room
    /// besides the admitted ones: those creating, awake, pausing or waking.
    /// The error is why the wake is refused. Blocking.
    pub fn admit(
        &self,
        pod: &str,
        memory_mib: u64,
        holders: impl Fn() -> Vec<Holder>,
    ) -> Result<(), String> {
        if !self.is_capped() {
            return Ok(());
        }
        let deadline = Instant::now() + self.wait;
        let mut state = lock(&self.state);
        if state.admitted.contains_key(pod) {
            return Ok(());
        }
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        state.queue.push_back(ticket);
        loop {
            let first = state.queue.front() == Some(&ticket);
            let refusal = self.refusal(&state, pod, memory_mib, &holders());
            if first && refusal.is_none() {
                state.queue.pop_front();
                state.admitted.insert(pod.to_string(), memory_mib);
                self.room.notify_all();
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                state.queue.retain(|&t| t != ticket);
                self.room.notify_all();
                return Err(refusal.unwrap_or_else(|| "earlier wakes wait for room".into()));
            }
            state = self.wait_for_room(state, left.min(RECHECK));
        }
    }

    fn wait_for_room<'a>(
        &self,
        state: MutexGuard<'a, State>,
        wait: Duration,
    ) -> MutexGuard<'a, State> {
        wait_timeout(&self.room, state, wait)
    }

    /// Why `pod` does not fit now, if it does not.
    fn refusal(
        &self,
        state: &State,
        pod: &str,
        memory_mib: u64,
        holders: &[Holder],
    ) -> Option<String> {
        let mut held: HashMap<&str, u64> = state
            .admitted
            .iter()
            .map(|(id, mib)| (id.as_str(), *mib))
            .collect();
        for (id, mib) in holders {
            held.entry(id.as_str()).or_insert(*mib);
        }
        held.remove(pod);
        let pods = held.len() as u64;
        let memory: u64 = held.values().sum();
        let max_pods = u64::from(self.max_pods);
        if max_pods > 0 && pods + 1 > max_pods {
            return Some(format!("awake pods {pods}/{max_pods}"));
        }
        if self.max_memory_mib > 0 && memory + memory_mib > self.max_memory_mib {
            return Some(format!(
                "memory {}/{} GiB",
                gib(memory + memory_mib),
                gib(self.max_memory_mib)
            ));
        }
        None
    }

    /// The pod is asleep, failed or deleted: it holds no room.
    pub fn release(&self, pod: &str) {
        let mut state = lock(&self.state);
        if state.admitted.remove(pod).is_some() || !state.queue.is_empty() {
            self.room.notify_all();
        }
    }
}

/// MiB in GiB, with one decimal unless it is whole.
fn gib(mib: u64) -> String {
    if mib.is_multiple_of(1024) {
        (mib / 1024).to_string()
    } else {
        format!("{:.1}", mib as f64 / 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn admission(max_pods: u32, max_memory_mib: u64, wait_ms: u64) -> Arc<Admission> {
        Arc::new(Admission::new(&Settings {
            max_awake_pods: max_pods,
            max_awake_memory_mib: max_memory_mib,
            admission_wait: Duration::from_millis(wait_ms),
            ..Settings::default()
        }))
    }

    fn holders(ids: &[&str]) -> Vec<Holder> {
        ids.iter().map(|id| (id.to_string(), 1024)).collect()
    }

    #[test]
    fn without_a_cap_every_wake_is_admitted() {
        let admission = admission(0, 0, 10_000);
        let many = holders(&["a", "b", "c"]);
        assert_eq!(admission.admit("d", 1024, || many.clone()), Ok(()));
    }

    #[test]
    fn a_wake_past_the_pod_cap_is_refused_after_the_wait() {
        let admission = admission(2, 0, 200);
        let awake = holders(&["a", "b"]);
        let started = Instant::now();
        assert_eq!(
            admission.admit("c", 1024, || awake.clone()),
            Err("awake pods 2/2".into())
        );
        assert!(started.elapsed() >= Duration::from_millis(200));
        // The pod itself does not count, and one under the cap fits.
        assert_eq!(admission.admit("a", 1024, || awake.clone()), Ok(()));
        assert_eq!(admission.admit("c", 1024, || holders(&["a"])), Ok(()));
    }

    #[test]
    fn admitted_wakes_hold_room_until_released() {
        let admission = admission(1, 0, 50);
        assert_eq!(admission.admit("a", 1024, Vec::new), Ok(()));
        assert_eq!(
            admission.admit("b", 1024, Vec::new),
            Err("awake pods 1/1".into())
        );
        admission.release("a");
        assert_eq!(admission.admit("b", 1024, Vec::new), Ok(()));
    }

    #[test]
    fn the_memory_cap_counts_guest_ram() {
        let admission = admission(0, 2048, 50);
        assert_eq!(admission.admit("a", 1024, || holders(&["x"])), Ok(()));
        assert_eq!(
            admission.admit("b", 512, || holders(&["x"])),
            Err("memory 2.5/2 GiB".into())
        );
    }

    #[test]
    fn a_waiting_wake_is_admitted_when_room_is_released() {
        let admission = admission(1, 0, 5_000);
        assert_eq!(admission.admit("a", 1024, Vec::new), Ok(()));
        let waiting = {
            let admission = admission.clone();
            std::thread::spawn(move || {
                let started = Instant::now();
                (admission.admit("b", 1024, Vec::new), started.elapsed())
            })
        };
        std::thread::sleep(Duration::from_millis(100));
        admission.release("a");
        let (admitted, took) = waiting.join().unwrap();
        assert_eq!(admitted, Ok(()));
        assert!(took < Duration::from_secs(2), "{took:?}");
    }

    #[test]
    fn waiting_wakes_are_admitted_first_come_first_served() {
        let admission = admission(1, 0, 5_000);
        assert_eq!(admission.admit("a", 1024, Vec::new), Ok(()));
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut waiting = Vec::new();
        for pod in ["b", "c"] {
            let (admission, order) = (admission.clone(), order.clone());
            waiting.push(std::thread::spawn(move || {
                let admitted = admission.admit(pod, 1024, Vec::new);
                lock(&order).push(pod);
                if admitted.is_ok() {
                    std::thread::sleep(Duration::from_millis(50));
                    admission.release(pod);
                }
                admitted
            }));
            std::thread::sleep(Duration::from_millis(50));
        }
        admission.release("a");
        for wait in waiting {
            assert_eq!(wait.join().unwrap(), Ok(()));
        }
        assert_eq!(*lock(&order), ["b", "c"]);
    }
}
