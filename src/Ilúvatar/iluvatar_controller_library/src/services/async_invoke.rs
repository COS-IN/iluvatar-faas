use crate::server::controller_errors::MissingAsyncCookieError;
use crate::services::registration::RegisteredWorker;
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::bail_error;
use iluvatar_library::transaction::TransactionId;
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, warn};

pub struct AsyncService {
    async_invokes: Arc<DashMap<String, Arc<RegisteredWorker>>>,
    worker_fact: Arc<WorkerAPIFactory>,
}

impl AsyncService {
    pub fn boxed(worker_fact: Arc<WorkerAPIFactory>) -> Arc<Self> {
        Arc::new(AsyncService {
            async_invokes: Arc::new(DashMap::new()),
            worker_fact,
        })
    }

    /// start tracking an async invocation on a worker
    pub fn register_async_invocation(&self, cookie: String, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        debug!(tid=tid, cookie=%cookie, name=%worker.name, "Registering async invocation and worker");
        self.async_invokes.insert(cookie, worker);
    }

    /// Checks the worker for the status of the async invocation
    /// Returns Some(string) if it is complete, None if waiting, and an error if something went wrong
    /// Assumes that [this function](iluvatar_worker_library::services::invocation::async_tracker::AsyncHelper::invoke_async_check) will return a dictionary with known keys
    pub async fn check_async_invocation(&self, cookie: String, tid: &TransactionId) -> Result<Option<InvokeResponse>> {
        debug!(tid=tid, cookie=%cookie, "Checking async invocation");
        if let Some(worker) = self.async_invokes.get(&cookie) {
            let worker = worker.value();
            let mut api = self
                .worker_fact
                .get_worker_api(&worker.name, &worker.host, worker.port, tid)
                .await?;
            let result = api.invoke_async_check(&cookie, tid.clone()).await?;
            if result.success {
                self.async_invokes.remove(&cookie);
                Ok(Some(result))
            } else {
                let json: HashMap<String, String> = match serde_json::from_str(&result.json_result) {
                    Ok(r) => r,
                    Err(e) => {
                        bail_error!(tid=tid, error=%e, "Got an error trying to deserialize async check message")
                    },
                };
                match json.get("Status") {
                    // if we have this key then the invocation is still running
                    Some(stat) => {
                        debug!(tid=tid, status=%stat, "async invoke check status");
                        Ok(None)
                    },
                    None => match json.get("Error") {
                        // if we have this key then the invocation failed for some reason
                        Some(err_msg) => anyhow::bail!(err_msg.clone()),
                        None => {
                            // really should never get here
                            bail_error!(tid=tid, json=%result.json_result, "Got an unknown json response from checking async invocation status");
                        },
                    },
                }
            }
        } else {
            warn!(tid=tid,cookie=%cookie, "unable to find async cookie");
            anyhow::bail!(MissingAsyncCookieError {})
        }
    }
}
