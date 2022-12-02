use std::{sync::{Arc, atomic::{AtomicU32, Ordering}}, time::Duration};
use crate::{worker_api::worker_config::{FunctionLimits, InvocationConfig}};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::TransactionId, logging::LocalTime, utils::calculate_fqdn};
use anyhow::Result;
use tracing::error;
use super::{invoker_trait::Invoker, async_tracker::AsyncHelper, invoker_structs::{EnqueuedInvocation, InvocationResultPtr, InvocationResult}};
use crate::rpc::InvokeResponse;

/// This implementation does not support [crate::worker_api::worker_config::InvocationConfig::concurrent_invokes]
pub struct QueuelessInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  clock: LocalTime,
  running_funcs: AtomicU32,
}

impl QueuelessInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    Ok(Arc::new(QueuelessInvoker {
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      clock: LocalTime::new(tid)?,
      running_funcs: AtomicU32::new(0),
    }))
  }
}

#[tonic::async_trait]
impl Invoker for QueuelessInvoker {
  fn cont_manager(&self) -> Arc<ContainerManager>  {
    self.cont_manager.clone()
  }
  fn function_config(&self) -> Arc<FunctionLimits>  {
    self.function_config.clone()
  }
  fn invocation_config(&self) -> Arc<InvocationConfig>  {
    self.invocation_config.clone()
  }
  fn timer(&self) -> &LocalTime {
    &self.clock
  }
  fn concurrency_semaphore(&self) -> Option<Arc<tokio::sync::Semaphore>> {
    None
  }
  fn running_funcs(&self) -> u32 {
    self.running_funcs.load(Ordering::Relaxed)
  }
  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<InvocationResultPtr> {
    let fqdn = calculate_fqdn(&function_name, &function_version);
    self.running_funcs.fetch_add(1, Ordering::Relaxed);
    let r = self.invoke_internal(&fqdn, &function_name, &function_version, &json_args, &tid, self.timer().now(), None).await;
    self.running_funcs.fetch_sub(1, Ordering::Relaxed);
    match r {
      Ok( (result, dur) ) => {
        let r: InvocationResultPtr = InvocationResult::boxed();
        let mut temp = r.lock();
        temp.exec_time = result.duration_sec;
        temp.result_json = result.result_string()?;
        temp.worker_result = Some(result);
        temp.duration = dur;
        Ok( r.clone() )
      },
      Err(e) => Err(e),
    }
  }

  fn async_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<String> {
    let invoke = self.enqueue_new_invocation(function_name, function_version, json_args, tid);
    self.async_functions.insert_async_invoke(invoke)
  }
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse> {
    self.async_functions.invoke_async_check(cookie, tid)
  }

  fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
    let mut result_ptr = item.result_ptr.lock();
    error!(tid=%item.tid, attempts=result_ptr.attempts, "Abandoning attempt to run invocation after attempts");
    result_ptr.duration = Duration::from_micros(0);
    result_ptr.result_json = format!("{{ \"Error\": \"{}\" }}", cause);
    result_ptr.completed = true;
    item.signal();

  }
}
