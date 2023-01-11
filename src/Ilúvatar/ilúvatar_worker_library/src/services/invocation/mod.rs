use std::sync::Arc;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::transaction::TransactionId;
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use self::cold_priority_invoke::ColdPriorityInvoker;
use self::{queueless::QueuelessInvoker, invoker_trait::Invoker, fcfs_invoke::FCFSInvoker, minheap_invoke::MinHeapInvoker};
use self::{minheap_ed_invoke::MinHeapEDInvoker, fcfs_bypass_invoke::FCFSBypassInvoker };
use self::minheap_iat_invoke::MinHeapIATInvoker;
use super::containers::containermanager::ContainerManager;

pub mod invoker_structs;
pub mod invoker_trait;
pub mod queueless;
pub mod async_tracker;
pub mod fcfs_invoke;
pub mod minheap_invoke;
pub mod minheap_ed_invoke;
pub mod minheap_iat_invoke;
pub mod fcfs_bypass_invoke;
mod cold_priority_invoke;

pub struct InvokerFactory {
  cont_manager: Arc<ContainerManager>, 
  function_config: Arc<FunctionLimits>, 
  invocation_config: Arc<InvocationConfig>,
  cmap: Arc<CharacteristicsMap>,
}

impl InvokerFactory {
  pub fn new(cont_manager: Arc<ContainerManager>,
    function_config: Arc<FunctionLimits>,
    invocation_config: Arc<InvocationConfig>,
    cmap: Arc<CharacteristicsMap>) -> Self {

      InvokerFactory {
        cont_manager,
        function_config,
        invocation_config,
        cmap,
    }
  }

  pub fn get_invoker_service(&self, tid: &TransactionId) -> Result<Arc<dyn Invoker>> {
    let r: Arc<dyn Invoker> = match self.invocation_config.queue_policy.to_lowercase().as_str() {
      "none" => {
        QueuelessInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      },
      "fcfs" => {
        FCFSInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      },
      "minheap" => {
        MinHeapInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      },
      "minheap_ed" => {
        MinHeapEDInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      },
      "minheap_iat" => {
        MinHeapIATInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      },
      "fcfs_bypass" => {
        FCFSBypassInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      },
      "cold_pri" => {
        ColdPriorityInvoker::new(self.cont_manager.clone(), self.function_config.clone(), self.invocation_config.clone(), tid, self.cmap.clone())?
      }

      unknown => panic!("Unknown lifecycle backend '{}'", unknown)
    };
    Ok(r)
  }
}
