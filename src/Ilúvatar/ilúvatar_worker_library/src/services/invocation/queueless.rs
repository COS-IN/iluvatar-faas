use std::{sync::Arc, time::Duration};
use crate::{worker_api::worker_config::{FunctionLimits, InvocationConfig}};
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::{transaction::TransactionId, logging::LocalTime};
use anyhow::Result;
use super::{invoker_trait::Invoker, async_tracker::AsyncHelper};
use crate::rpc::InvokeResponse;

pub struct QueuelessInvoker {
  pub cont_manager: Arc<ContainerManager>,
  pub async_functions: AsyncHelper,
  pub function_config: Arc<FunctionLimits>,
  pub invocation_config: Arc<InvocationConfig>,
  clock: LocalTime
}

impl QueuelessInvoker {
  pub fn new(cont_manager: Arc<ContainerManager>, function_config: Arc<FunctionLimits>, invocation_config: Arc<InvocationConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    Ok(Arc::new(QueuelessInvoker {
      cont_manager,
      function_config,
      invocation_config,
      async_functions: AsyncHelper::new(),
      clock: LocalTime::new(tid)?
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

  async fn sync_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<(String, Duration)> {
    self.invoke_internal(&function_name, &function_version, &json_args, &tid, self.timer().now()).await
  }

  fn async_invocation(&self, function_name: String, function_version: String, json_args: String, tid: TransactionId) -> Result<String> {
    let invoke = self.enqueue_new_invocation(function_name, function_version, json_args, tid);
    self.async_functions.insert_async_invoke(invoke)
  }
  fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<InvokeResponse> {
    self.async_functions.invoke_async_check(cookie, tid)
  }
}