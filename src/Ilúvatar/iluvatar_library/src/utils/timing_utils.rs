// Edited code, based on this
// https://stackoverflow.com/questions/53291554/whats-a-clean-way-to-get-how-long-a-future-takes-to-resolve
use crate::clock::now;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

/// A wrapper around a Future which adds timing data.
#[pin_project]
pub struct Timed<Fut>
where
    Fut: Future,
{
    #[pin]
    inner: Fut,
    start: Instant,
}

impl<Fut> Future for Timed<Fut>
where
    Fut: Future,
{
    type Output = (Fut::Output, Duration);

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            // If the inner future is still pending, this wrapper is still pending.
            Poll::Pending => Poll::Pending,

            // If the inner future is done, measure the elapsed time and finish this wrapper future.
            Poll::Ready(v) => {
                let elapsed = this.start.elapsed();
                Poll::Ready((v, elapsed))
            },
        }
    }
}

pub trait TimedExt: Sized + Future {
    fn timed(self) -> Timed<Self> {
        Timed {
            inner: self,
            start: now(),
        }
    }
}

// All futures can use the `.timed` method defined above
impl<F: Future> TimedExt for F {}
