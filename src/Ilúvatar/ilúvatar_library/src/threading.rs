use std::future::Future;
use std::sync::mpsc::{channel, Sender};
use std::thread::JoinHandle as OsHandle;
use tokio::task::JoinHandle as TokioHandle;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tracing::{debug, warn, error};
use crate::transaction::TransactionId;

/// run a function within an OS thread
/// It will be executed every [call_ms] milliseconds
pub fn os_thread<T: Send + Sync + 'static>(call_ms: u64, tid: TransactionId, function: Arc<dyn Fn(&T, &TransactionId) -> () + Send + Sync + 'static>) -> (OsHandle<()>, Sender<Arc<T>>) {
  let (tx, rx) = channel::<Arc<T>>();

  let handle = std::thread::spawn(move || {
    let recv_svc = match rx.recv() {
      Ok(svc) => svc,
      Err(e) => {
        error!(tid=%tid, error=%e, typename=%std::any::type_name::<T>(), "OW worker thread failed to receive service from channel!");
        return;
      },
    };
    debug!(tid=%tid, typename=%std::any::type_name::<T>(), "OS worker thread started");
    crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
    while crate::continuation::GLOB_CONT_CHECK.check_continue() {
      let start = SystemTime::now();
      function(&recv_svc, &tid);
      let sleep_t = match start.elapsed() {
        Ok(d) => std::cmp::max(0, call_ms - d.as_millis() as u64),
        Err(e) => {
          warn!(tid=%tid, error=%e, typename=%std::any::type_name::<T>(), "Failed to get elapsed time of OW worker thread service computation");
          call_ms
        },
      };
      std::thread::sleep(Duration::from_millis(sleep_t));
    }
    crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
  });

  (handle, tx)
}

/// Start an async function inside of a Tokio worker
/// It will be executed every [call_ms] milliseconds
pub fn tokio_thread<S, T>(call_ms: u64, tid: TransactionId, function: fn(Arc<S>, TransactionId) -> T) -> (TokioHandle<()>, Sender<Arc<S>>)
where
  T: Future<Output = ()> + Send + 'static,
  S: Send + Sync + 'static
{
  let box_function = force_boxed(function);
  let (tx, rx) = channel();
  let handle = tokio::spawn(async move {
    let service: Arc<S> = match rx.recv() {
      Ok(cm) => cm,
      Err(_) => {
        error!(tid=%tid, "Invoker service thread failed to receive service from channel!");
        return;
      },
    };
    crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
    while crate::continuation::GLOB_CONT_CHECK.check_continue() {
      box_function(service.clone(), tid.clone()).await;
      tokio::time::sleep(std::time::Duration::from_millis(call_ms)).await;
    }
    crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
  });

  (handle, tx)
}

type Incrementer<S> = Box<dyn Fn(Arc<S>, TransactionId) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>> + Send + Sync + 'static>;
fn force_boxed<S: 'static, T>(f: fn(Arc<S>, TransactionId) -> T) -> Incrementer<S>
where
    T: Future<Output = ()> + Send + 'static,
{
    Box::new(move |s, t| Box::pin(f(s,t)))
}

/// Start an async function on a new OS thread inside of a private Tokio runtime
/// It will be executed every [call_ms] milliseconds
pub fn tokio_runtime<S: Send + Sync + 'static, T>(call_ms: u64, tid: TransactionId, function: fn(Arc<S>, TransactionId) -> T) -> (OsHandle<()>, Sender<Arc<S>>)
where
  T: Future<Output = ()> + Send + 'static,
{
  let box_function = force_boxed(function);

  let (tx, rx) = channel::<Arc<S>>();
  let handle = std::thread::spawn(move || {
    let service: Arc<S> = match rx.recv() {
      Ok(service) => service,
      Err(e) => {
        error!(tid=%tid, error=%e, "Invoker service thread failed to receive service from channel!");
        return;
      },
    };

    let worker_rt = match tokio::runtime::Runtime::new() {
      Ok(rt) => rt,
      Err(e) => { 
        error!(tid=%tid, error=%e, "Tokio thread runtime failed to start");
        return ();
      },
    };
    debug!(tid=%tid, "container manager worker started");
    worker_rt.block_on(async {
      crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
      while crate::continuation::GLOB_CONT_CHECK.check_continue() {
        box_function(service.clone(), tid.clone()).await;
        tokio::time::sleep(std::time::Duration::from_millis(call_ms)).await;
      }
      crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    });
  });

  (handle, tx)
}
