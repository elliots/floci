//! The front shim: the process containerd starts as
//! `containerd-shim-smolvm-pods-v2` and the only one it talks to.
//!
//! One front shim serves every task of a containerd namespace. It keeps the
//! task table, answers containerd from it, and forwards to the node service
//! ([`crate::node`]) what needs a VM. It never calls smolvm, so that what can
//! end it, and with it every pod, stays small.

mod client;
mod service;
mod task;

pub use service::run_shim;
