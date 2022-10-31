use std::future::Future;
use std::sync::mpsc::{channel, Sender};
use std::thread::JoinHandle as OsHandle;
use tokio::task::JoinHandle as TokioHandle;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tracing::{debug, warn, error};
use crate::transaction::TransactionId;

fn sleep_time<T>(call_ms: u64, start_t: SystemTime, tid: &TransactionId) -> u64 {
  match start_t.elapsed() {
    Ok(d) => std::cmp::max(0, call_ms as i128 - d.as_millis() as i128) as u64,
    Err(e) => {
      warn!(tid=%tid, error=%e, typename=%std::any::type_name::<T>(), "Failed to get elapsed time of OW worker thread service computation");
      call_ms
    },
  }
}

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
      let sleep_t = sleep_time::<T>(call_ms, start, &tid);
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
        error!(tid=%tid, typename=%std::any::type_name::<T>(), "Tokio service thread failed to receive service from channel!");
        return;
      },
    };
    crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
    while crate::continuation::GLOB_CONT_CHECK.check_continue() {
      let start = SystemTime::now();
      box_function(service.clone(), tid.clone()).await;
      let sleep_t = sleep_time::<T>(call_ms, start, &tid);
      tokio::time::sleep(std::time::Duration::from_millis(sleep_t)).await;
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
pub fn tokio_runtime<'a, S: Send + Sync + 'static, T, T2>(call_ms: u64, tid: TransactionId, function: fn(Arc<S>, TransactionId) -> T, 
                                                  waiter_function: Option<fn(Arc<S>, TransactionId) -> T2>, num_worker_threads: Option<usize>) 
  -> (OsHandle<()>, Sender<Arc<S>>)
where
  T: Future<Output = ()> + Send + 'static,
  T2: Future<Output = ()> + Send + 'static,
{
  // let box_function = force_boxed(function);

  let (tx, rx) = channel::<Arc<S>>();
  let handle = std::thread::spawn(move || {
    let service: Arc<S> = match rx.recv() {
      Ok(service) => service,
      Err(e) => {
        error!(tid=%tid, error=%e, typename=%std::any::type_name::<T>(), "Tokio runtime service thread failed to receive service from channel!");
        return;
      },
    };

    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    match num_worker_threads {
      Some(cpus) => builder.worker_threads(cpus),
      None => &builder,
    };
    let worker_rt = match builder.build() {
      Ok(rt) => rt,
      Err(e) => { 
        error!(tid=%tid, error=%e, typename=%std::any::type_name::<T>(), "Tokio thread runtime failed to start");
        return ();
      },
    };
    debug!(tid=%tid, typename=%std::any::type_name::<T>(), "tokio runtime worker thread started");
    worker_rt.block_on(async {
      crate::continuation::GLOB_CONT_CHECK.thread_start(&tid);
      while crate::continuation::GLOB_CONT_CHECK.check_continue() {
        let start = SystemTime::now();
        function(service.clone(), tid.clone()).await;
        let sleep_t = sleep_time::<T>(call_ms, start, &tid);
        match waiter_function {
          Some(wf) => {
            // let box_wait_function = force_boxed(wf);
            wf(service.clone(), tid.clone()).await;
          },
          None => tokio::time::sleep(std::time::Duration::from_millis(sleep_t)).await,
        };
      }
      crate::continuation::GLOB_CONT_CHECK.thread_exit(&tid);
    });
  });

  (handle, tx)
}
