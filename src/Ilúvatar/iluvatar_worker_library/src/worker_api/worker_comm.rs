use super::rpc::RPCWorkerAPI;
use crate::worker_api::{create_worker, iluvatar_worker::IluvatarWorkerImpl, sim_worker::SimWorkerAPI, WorkerAPI};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::utils::is_simulation;
use iluvatar_library::{bail_error, transaction::TransactionId, utils::port::Port};
use std::sync::Arc;
#[cfg(feature = "full_spans")]
use tracing::Instrument;

pub struct WorkerAPIFactory {
    /// cache of RPC connections to workers
    /// We can clone them for faster connection
    /// better than opening a new one
    rpc_apis: DashMap<String, RPCWorkerAPI>,
    sim_apis: DashMap<String, Arc<IluvatarWorkerImpl>>,
    is_simulation: bool,
}

impl WorkerAPIFactory {
    pub fn boxed() -> Arc<WorkerAPIFactory> {
        Arc::new(WorkerAPIFactory {
            rpc_apis: DashMap::new(),
            sim_apis: DashMap::new(),
            is_simulation: is_simulation(),
        })
    }
}

impl WorkerAPIFactory {
    fn try_get_rpcapi(&self, worker: &str) -> Option<RPCWorkerAPI> {
        self.rpc_apis.get(worker).map(|r| r.to_owned())
    }

    fn try_get_simapi(&self, worker: &str) -> Option<Arc<IluvatarWorkerImpl>> {
        self.sim_apis.get(worker).map(|r| r.to_owned())
    }

    /// the list of all workers the factory has cached
    pub fn get_cached_workers(&self) -> Vec<(String, Box<dyn WorkerAPI + Send>)> {
        let mut ret: Vec<(String, Box<dyn WorkerAPI + Send>)> = vec![];
        if !self.rpc_apis.is_empty() {
            self.rpc_apis
                .iter()
                .for_each(|x| ret.push((x.key().clone(), Box::new(x.value().clone()))));
        }
        if !self.sim_apis.is_empty() {
            self.sim_apis
                .iter()
                .for_each(|x| ret.push((x.key().clone(), Box::new(SimWorkerAPI::new(x.value().clone())))));
        }
        ret
    }

    /// Get the worker API that matches it's implemented communication method
    pub async fn get_worker_api(
        &self,
        worker: &str,
        host: &str,
        port: Port,
        tid: &TransactionId,
    ) -> Result<Box<dyn WorkerAPI + Send>> {
        match self.is_simulation {
            false => match self.try_get_rpcapi(worker) {
                Some(r) => Ok(Box::new(r)),
                None => {
                    let api = match RPCWorkerAPI::new(host, port, tid).await {
                        Ok(api) => api,
                        Err(e) => {
                            bail_error!(tid=tid, worker=%worker, error=%e, "Unable to create API for worker")
                        },
                    };
                    self.rpc_apis.insert(worker.to_owned(), api.clone());
                    Ok(Box::new(api))
                },
            },
            true => {
                let api = match self.try_get_simapi(worker) {
                    Some(api) => api,
                    None => match self.sim_apis.entry(worker.to_owned()) {
                        dashmap::mapref::entry::Entry::Occupied(entry) => entry.get().clone(),
                        dashmap::mapref::entry::Entry::Vacant(vacant) => {
                            let worker_config =
                                match crate::worker_api::worker_config::Configuration::boxed(Some(host), None) {
                                    Ok(w) => w,
                                    Err(e) => {
                                        anyhow::bail!("Failed to load simulation config because '{:?}'", e)
                                    },
                                };
                            #[cfg(feature = "full_spans")]
                            let api = create_worker(worker_config.clone(), tid)
                                .instrument(name_span!(worker_config.name))
                                .await?;
                            #[cfg(not(feature = "full_spans"))]
                            let api = create_worker(worker_config, tid).await?;
                            let api = Arc::new(api);
                            vacant.insert(api.clone());
                            api
                        },
                    },
                };
                Ok(Box::new(SimWorkerAPI::new(api)))
            },
        }
    }
}
