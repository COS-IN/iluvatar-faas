use std::{task::{Poll, Waker}, sync::Arc, time::Duration};
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;

#[derive(Debug)]
#[allow(unused)]
pub struct InvocationResult {
  pub completed: bool,
  pub result_json: String,
  pub duration: u64,
  waker: Option<Waker>,
  pub attempts: u32
}

pub type InvocationResultPtr = Arc<Mutex<InvocationResult>>;

#[derive(Debug)]
#[allow(unused)]
pub struct EnqueuedInvocation {
  pub result_ptr: InvocationResultPtr,
  pub function_name: String, 
  pub function_version: String, 
  pub json_args: String, 
  pub tid: TransactionId,
}

impl EnqueuedInvocation {
  pub fn new(function_name: String, function_version: String, json_args: String, tid: TransactionId, result: InvocationResultPtr) -> Self {
    EnqueuedInvocation {
      result_ptr: result,
      function_name,
      function_version,
      json_args,
      tid,
    }
  }
}

#[derive(Debug)]
#[allow(unused)]
pub struct QueueFuture {
  pub result: InvocationResultPtr,

}
impl QueueFuture {
  pub fn new() -> Self {
    let result = Arc::new(Mutex::new(InvocationResult {
      completed: false,
      duration: 0,
      result_json: "".to_string(),
      waker: None,
      attempts: 0,
    }));
    let q = QueueFuture {
      result: result.clone(),
    };
    // Spawn monitor thread
    std::thread::spawn(move || {
      loop {
        // TODO: another way or better sleep time?
        std::thread::sleep(Duration::from_nanos(100));
        let mut shared_state = result.lock();
        // Signal that the timer has completed and wake up the last
        // task on which the future was polled, if one exists.
        if  shared_state.completed {
          if let Some(waker) = shared_state.waker.take() {
            waker.wake();
            break;
          }
        }
      }
    });
    q
  }
}

impl std::future::Future for QueueFuture {
    type Output = InvocationResultPtr;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
      let mut result = self.result.lock();
      tracing::debug!("Polling future");
      if result.completed {
        Poll::Ready(self.result.clone())
      }
      else {
        result.waker = Some(cx.waker().clone());
        Poll::Pending
      }
    }
}