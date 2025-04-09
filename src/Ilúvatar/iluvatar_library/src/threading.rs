use crate::clock::now;
use crate::ring_buff::Bufferable;
use crate::transaction::TransactionId;
use iluvatar_library::ring_buff::RingBuffer;
use std::future::Future;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread::JoinHandle as OsHandle;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;
use tokio::task::JoinHandle as TokioHandle;
use tokio::time::Instant;
use tracing::{debug, error};
#[cfg(feature = "full_spans")]
use tracing::{Instrument, Span};

pub enum EventualItem<Left: Future> {
    Future(Left),
    Now(Left::Output),
}

/// return time in milliseconds to sleep for
fn sleep_time(call_ms: u64, start_t: Instant, _tid: &TransactionId) -> u64 {
    std::cmp::max(1, call_ms as i128 - start_t.elapsed().as_millis() as i128) as u64
}

/// run a function within an OS thread.
/// It will be executed every `call_ms` milliseconds.
/// This should only be used for code that runs on the live system (not simulation) *and* causes blocking that would cause issues with Tokio.
/// E.g. calling an external executable, monitoring a file, etc.
pub fn os_thread<T: Send + Sync + 'static>(
    call_ms: u64,
    tid: TransactionId,
    function: Arc<dyn Fn(&T, &TransactionId) + Send + Sync + 'static>,
) -> anyhow::Result<(OsHandle<()>, Sender<Arc<T>>)> {
    let (tx, rx) = channel::<Arc<T>>();
    let handle = std::thread::Builder::new().name(tid.clone()).spawn(move || {
        let recv_svc = match rx.recv() {
            Ok(svc) => svc,
            Err(e) => {
                error!(tid=tid, error=%e, typename=%std::any::type_name::<T>(), "OS worker thread failed to receive service from channel!");
                return;
            }
        };
        debug!(tid=tid, typename=%std::any::type_name::<T>(), "OS worker thread started");
        crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
        while crate::continuation::GLOB_CONT_CHECK.check_continue() {
            tracing::trace!(tid=tid, "Executing");
            let start = now();
            function(&recv_svc, &tid);
            let sleep_t = sleep_time(call_ms, start, &tid);
            tracing::trace!(tid=tid, "Completed");
            std::thread::sleep(Duration::from_millis(sleep_t));
        }
        crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    })?;

    Ok((handle, tx))
}

/// Compiler hack to make the borrow checker happy about us passing async closures around
pub trait Hack<'a, I: 'a, I2: 'a, R>: Fn(&'a I, &'a I2) -> Self::Fut {
    type Fut: Future<Output = R> + Send + 'a;
}

impl<'a, I: 'a, I2: 'a, R, F, Fut> Hack<'a, I, I2, R> for F
where
    F: Fn(&'a I, &'a I2) -> Fut,
    Fut: Future<Output = R> + Send + 'a,
{
    type Fut = Fut;
}

/// Start an async function inside a Tokio worker thread.
/// It will be executed every `call_ms` milliseconds.
pub fn tokio_logging_thread<F, S, L>(
    call_ms: u64,
    tid: TransactionId,
    ring_buff: Arc<RingBuffer>,
    function: F,
) -> anyhow::Result<(TokioHandle<()>, Sender<Arc<S>>)>
where
    L: Bufferable + 'static,
    S: Send + Sync + 'static,
    F: for<'a> Hack<'a, Arc<S>, TransactionId, anyhow::Result<L>> + Send + 'static,
{
    let (tx, rx) = channel();
    let td = async move {
        let service: Arc<S> = match rx.recv() {
            Ok(service) => service,
            Err(e) => {
                error!(tid=tid, error=%e, typename=%std::any::type_name::<S>(), "Tokio runtime service thread failed to receive service from channel!");
                return;
            },
        };
        crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);

        while crate::continuation::GLOB_CONT_CHECK.check_continue() {
            tracing::trace!(tid = tid, "Executing");
            let start = now();
            match function(&service, &tid).await {
                Ok(l) => ring_buff.insert(&tid, Arc::new(l)),
                Err(e) => {
                    error!(tid=tid, error=%e, typename=%std::any::type_name::<S>(), "Background logging thread error")
                },
            }
            let sleep_t = sleep_time(call_ms, start, &tid);
            tracing::trace!(tid = tid, "Completed");
            tokio::time::sleep(Duration::from_millis(sleep_t)).await;
        }
        crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    };
    #[cfg(feature = "full_spans")]
    let td = td.instrument(Span::current());
    Ok((tokio::spawn(td), tx))
}

/// Start an async function inside a Tokio worker thread.
/// It will be executed every `call_ms` milliseconds.
#[inline(always)]
pub fn tokio_thread<S, F>(call_ms: u64, tid: TransactionId, function: F) -> (TokioHandle<()>, Sender<Arc<S>>)
where
    S: Send + Sync + 'static,
    F: for<'a> Hack<'a, Arc<S>, TransactionId, ()> + Send + 'static,
{
    tokio_waiter_thread(
        call_ms,
        tid,
        function,
        None::<fn(&Arc<S>, &TransactionId) -> tokio::sync::futures::Notified<'static>>,
    )
}

pub fn tokio_waiter_thread<S, F, F2>(
    call_ms: u64,
    tid: TransactionId,
    function: F,
    waiter_function: Option<F2>,
) -> (TokioHandle<()>, Sender<Arc<S>>)
where
    S: Send + Sync + 'static,
    F: for<'a> Hack<'a, Arc<S>, TransactionId, ()> + Send + 'static,
    F2: for<'a> Hack<'a, Arc<S>, TransactionId, ()> + Send + 'static,
{
    let (tx, rx) = channel::<Arc<S>>();
    let td = async move {
        let service: Arc<S> = match rx.recv() {
            Ok(service) => service,
            Err(e) => {
                error!(tid=tid, error=%e, typename=%std::any::type_name::<S>(), "Tokio runtime service thread failed to receive service from channel!");
                return;
            },
        };
        crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);

        while crate::continuation::GLOB_CONT_CHECK.check_continue() {
            tracing::trace!(tid = tid, "Executing");
            let start = now();
            function(&service, &tid).await;
            let sleep_t = sleep_time(call_ms, start, &tid);
            tracing::trace!(tid = tid, "Completed");
            match &waiter_function {
                Some(wf) => {
                    let fut = wf(&service, &tid);
                    match tokio::time::timeout(Duration::from_millis(sleep_t), fut).await {
                        Ok(_) => (), // woken up by future activation
                        Err(_elapsed) => {
                            // check after timeout
                            debug!(
                                tid = tid,
                                "Waking up worker thread after timeout; waiter did not activate"
                            );
                        },
                    }
                },
                None => tokio::time::sleep(Duration::from_millis(sleep_t)).await,
            };
        }
        crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    };
    #[cfg(feature = "full_spans")]
    let td = td.instrument(Span::current());
    (tokio::spawn(td), tx)
}

/// Start an async function inside a Tokio worker thread.
/// It will be awakened on each notification.
pub fn tokio_notify_thread<S, T>(
    tid: TransactionId,
    notifier: Arc<Notify>,
    function: fn(Arc<S>, TransactionId) -> T,
) -> (TokioHandle<()>, Sender<Arc<S>>)
where
    T: Future<Output = ()> + Send + 'static,
    S: Send + Sync + 'static,
{
    let (tx, rx) = channel();
    let td = async move {
        let service: Arc<S> = match rx.recv() {
            Ok(cm) => cm,
            Err(_) => {
                error!(tid=tid, typename=%std::any::type_name::<S>(), "Tokio service thread failed to receive service from channel!");
                return;
            },
        };
        crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
        loop {
            tokio::select! {
              _ = crate::continuation::GLOB_NOTIFIER.notified() => break,
              _ = notifier.notified() => function(service.clone(), tid.clone()).await,
            }
        }
        crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    };
    #[cfg(feature = "full_spans")]
    let td = td.instrument(Span::current());
    let handle = tokio::spawn(td);
    (handle, tx)
}

/// Start an async function inside a Tokio worker
/// It will be executed on each sent item sent via a [Sender]
pub fn tokio_sender_thread<'a, S, T, R, F>(
    tid: TransactionId,
    function: Arc<F>,
) -> (TokioHandle<()>, Sender<Arc<S>>, UnboundedSender<T>)
where
    S: Send + Sync + 'static,
    T: Send + Sync + 'static,
    R: Future<Output = ()> + Send,
    F: Fn(Arc<S>, TransactionId, T) -> R + 'a + Sync + Send + 'static,
{
    let (service_tx, service_rx) = channel();
    let (item_tx, mut item_rx) = tokio::sync::mpsc::unbounded_channel::<T>();
    let td = async move {
        let tid = tid;
        let service: Arc<S> = match service_rx.recv() {
            Ok(cm) => cm,
            Err(_) => {
                error!(tid=tid, typename=%std::any::type_name::<S>(), "Tokio service thread failed to receive service from channel!");
                return;
            },
        };
        crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
        loop {
            tokio::select! {
              _ = crate::continuation::GLOB_NOTIFIER.notified() => break,
              item = item_rx.recv() => match item {
                  Some(item) => {
                        tracing::trace!(tid=tid, "Executing");
                        function(service.clone(), tid.clone(), item).await;
                        tracing::trace!(tid=tid, "Completed");
                    },
                  None => break,
              },
            }
        }
        crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    };
    #[cfg(feature = "full_spans")]
    let td = td.instrument(Span::current());
    let handle = tokio::spawn(td);

    (handle, service_tx, item_tx)
}
