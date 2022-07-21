use std::{sync::Arc, collections::HashMap};
use dashmap::DashMap;
use iluvatar_lib::bail_error;
use iluvatar_lib::{worker_api::worker_comm::WorkerAPIFactory, transaction::TransactionId};
use iluvatar_lib::load_balancer_api::{structs::internal::RegisteredWorker, lb_errors::MissingAsyncCookieError};
use tracing::{warn, debug};
use anyhow::Result;

#[allow(unused)]
pub struct AsyncService {
  async_invokes: Arc<DashMap<String, Arc<RegisteredWorker>>>,
  worker_fact: WorkerAPIFactory,
}

impl AsyncService {
  pub fn boxed() -> Arc<Self> {
    Arc::new(AsyncService {
      async_invokes: Arc::new(DashMap::new()),
      worker_fact: WorkerAPIFactory {},
    })
  }

  /// start tracking an async invocation on a worker
  pub fn register_async_invocation(&self, cookie: String, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
    debug!("[{}] Registering async invocation: {} and worker: {}", tid, &cookie, &worker.name);
    self.async_invokes.insert(cookie, worker);
  }

  /// Checks the worker for the status of the async invocation
  /// Returns Some(string) if it is complete, None if waiting, and an error if something went wrong
  /// Relies on informational json set by [this function](iluvatar_lib::services::invocation::invoker::InvokerService::invoke_async_check)
  pub async fn check_async_invocation(&self, cookie: String, tid: &TransactionId) -> Result<Option<String>> {
    debug!("[{}] Checking async invocation: {}", tid, &cookie);
    if let Some(worker) = self.async_invokes.get(&cookie) {
      let mut api = self.worker_fact.get_worker_api(&worker.value(), tid).await?;
      let result = api.invoke_async_check(&cookie, tid.clone()).await?;
      if result.success {
        self.async_invokes.remove(&cookie);
        return Ok(Some(result.json_result));
      } else {
        let json: HashMap<String, String> = match serde_json::from_str(&result.json_result) {
            Ok(r) => r,
            Err(e) => bail_error!("[{}] Got an error trying to deserialize async check message {}", tid, e),
        };
        match json.get("Status") {
          // if we have this key then the invocation is still running
          Some(stat) => {
            debug!("[{}] async invoke check status: {}", tid, stat);
            Ok(None)
          },
          None => match json.get("Error") {
            // if we have this key then the invocation failed for some reason
            Some(err_msg) => anyhow::bail!(err_msg.clone()),
            None => {
              // really should never get here
              bail_error!("[{}] Got an unknown json response from checking async invocation status: {}", tid, result.json_result);
            },
          }
        }
      }
    } else {
      warn!("[{}] unable to find async cookie {}", tid, cookie);
      anyhow::bail!(MissingAsyncCookieError{})
    }
  }
}
