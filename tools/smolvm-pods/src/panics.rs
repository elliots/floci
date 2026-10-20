//! Panic isolation. The binary is built with `panic = "unwind"`, so a panic in
//! a request handler can fail that request alone, instead of ending the
//! process and with it every task containerd has.

use std::any::Any;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};

/// Runs a future, catching a panic in it as an error with the panic's message.
pub struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.0.as_mut();
        match catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Err(panic) => Poll::Ready(Err(message(panic.as_ref()))),
        }
    }
}

/// Catch a panic of `future`. A future that panicked is never polled again.
pub fn catch<F: Future>(future: F) -> CatchUnwind<F> {
    CatchUnwind(Box::pin(future))
}

fn message(panic: &(dyn Any + Send)) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else {
        "a panic".to_string()
    }
}

/// Log every panic, with where it happened, before it unwinds.
pub fn log_panics() {
    std::panic::set_hook(Box::new(|info| {
        log::error!("panic: {info}");
    }));
}

/// End the process at a panic, as `panic = "abort"` would: for the VM
/// process, which must not run on with a thread gone.
pub fn abort_on_panic() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        default(info);
        std::process::abort();
    }));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_panic_becomes_an_error() {
        let ok = catch(async { 7 }).await;
        assert_eq!(ok, Ok(7));
        let failed = catch(async {
            tokio::task::yield_now().await;
            panic!("boom {}", 1);
        })
        .await;
        assert_eq!(failed, Err::<(), _>("boom 1".to_string()));
    }
}
