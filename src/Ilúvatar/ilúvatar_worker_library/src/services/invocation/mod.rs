use std::sync::Arc;
use anyhow::Result;
use iluvatar_library::transaction::TransactionId;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use self::{queueless::QueuelessInvoker, invoker_trait::Invoker, fcfs_invoke::FCFSInvoker, minheap_invoke::MinHeapInvoker, minheap_ed_invoke::MinHeapEDInvoker, fcfs_bypass_invoke::MinHeapBypassInvoker };
use super::containers::containermanager::ContainerManager;

pub mod invoker_structs;
pub mod invoker_trait;
pub mod queueless;
pub mod async_tracker;
pub mod fcfs_invoke;
pub mod minheap_invoke;
pub mod minheap_ed_invoke;
pub mod fcfs_bypass_invoke;

pub struct InvokerFactory {
  cont_manager: Arc<ContainerManager>, 
  function_config: Arc<FunctionLimits>, 
  invocation_config: Arc<InvocationConfig>
}

impl InvokerFactory {
  pub fn new(cont_manager: Arc<ContainerManager>,
    function_config: Arc<FunctionLimits>,
    invocation_config: Arc<InvocationConfig>) -> Self {

      InvokerFactory {
        cont_manager,
        function_config,
        invocation_config
    }
  }

  pub fn get_invoker_service(&self, tid: &TransactionId) -> Result<Arc<dyn Invoker>> {
    let r: Arc<dyn Invoker> = match self.invocation_config.queue_policy.to_lowercase().as_str() {
      "none" => {
        QueuelessInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid)?
      },
      "fcfs" => {
        FCFSInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid)?
      },
      "minheap" => {
        MinHeapInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid)?
      },
      "minheap_ed" => {
        MinHeapEDInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid)?
      },
      "fcfs_bypass" => {
        MinHeapBypassInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid)?
      }

      unknown => panic!("Unknown lifecycle backend '{}'", unknown)
    };
    Ok(r)
  }
}
