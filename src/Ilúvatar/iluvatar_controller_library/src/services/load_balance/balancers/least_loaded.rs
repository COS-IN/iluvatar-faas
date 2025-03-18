use crate::server::config::ControllerConfig;
use crate::services::load_balance::{LoadBalancerTrait, LoadMetric};
use crate::services::registration::RegisteredWorker;
use crate::{
    build_influx, prewarm, send_async_invocation, send_invocation,
    services::{controller_health::ControllerHealthService, load_reporting::LoadService},
};
use anyhow::Result;
use iluvatar_library::utils::timing::TimedExt;
use iluvatar_library::{
    bail_error, threading::tokio_thread, transaction::TransactionId, transaction::LEAST_LOADED_TID,
};
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use parking_lot::RwLock;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

#[derive(Debug, Deserialize)]
pub struct LLConfig {
    load_metric: LoadMetric,
}

#[allow(unused)]
pub struct LeastLoadedBalancer {
    workers: RwLock<HashMap<String, Arc<RegisteredWorker>>>,
    worker_fact: Arc<WorkerAPIFactory>,
    health: Arc<dyn ControllerHealthService>,
    _worker_thread: JoinHandle<()>,
    assigned_worker: RwLock<Option<Arc<RegisteredWorker>>>,
    load: Arc<LoadService>,
}

impl LeastLoadedBalancer {
    pub async fn boxed(
        health: Arc<dyn ControllerHealthService>,
        worker_fact: Arc<WorkerAPIFactory>,
        tid: &TransactionId,
        config: &ControllerConfig,
        ll_config: &LLConfig,
    ) -> Result<Arc<Self>> {
        let influx = build_influx(config, tid).await?;
        let load = crate::build_load_svc(&ll_config.load_metric, tid, &worker_fact, influx)?;
        let (handle, tx) = tokio_thread(
            ll_config.load_metric.thread_sleep_ms,
            LEAST_LOADED_TID.clone(),
            LeastLoadedBalancer::find_least_loaded,
        );

        let i = Arc::new(LeastLoadedBalancer {
            workers: RwLock::new(HashMap::new()),
            worker_fact,
            health,
            _worker_thread: handle,
            assigned_worker: RwLock::new(None),
            load,
        });
        tx.send(i.clone())?;
        Ok(i)
    }

    #[tracing::instrument(level="debug", skip(self), fields(tid=tid))]
    async fn find_least_loaded(self: &Arc<Self>, tid: &TransactionId) {
        let mut least_ld = f64::MAX;
        let mut worker = &"".to_string();
        let workers = self.workers.read();
        for worker_name in workers.keys() {
            if let Some(worker_load) = self.load.get_worker(worker_name) {
                if worker_load < least_ld {
                    worker = worker_name;
                    least_ld = worker_load;
                }
            }
        }
        match self.workers.read().get(worker) {
            Some(w) => {
                *self.assigned_worker.write() = Some(w.clone());
                info!(tid=tid, worker=%worker, "new least loaded worker");
            },
            None => {
                warn!(tid=tid, worker=%worker, "Cannot update least loaded worker because it was not registered, or no worker has been registered")
            },
        };
    }

    fn get_worker(&self, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
        let lck = self.assigned_worker.read();
        match &*lck {
            Some(w) => Ok(w.clone()),
            None => {
                bail_error!(tid = tid, "No worker has been assigned to least loaded variable yet")
            },
        }
    }
}

#[tonic::async_trait]
impl LoadBalancerTrait for LeastLoadedBalancer {
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        info!(tid=tid, worker=%worker.name, "Registering new worker in LeastLoaded load balancer");
        if self.assigned_worker.read().is_none() {
            info!(tid=tid, worker=%worker.name, "Assigning new worker in LeastLoaded load balancer");
            *self.assigned_worker.write() = Some(worker.clone());
        }
        self.workers.write().insert(worker.name.clone(), worker);
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, func, json_args), fields(tid=tid)))]
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)> {
        let worker = self.get_worker(tid)?;
        send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }

    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
        let worker = self.get_worker(tid)?;
        prewarm!(func, tid, self.worker_fact, self.health, worker)
    }

    async fn send_async_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(String, Arc<RegisteredWorker>, Duration)> {
        let worker = self.get_worker(tid)?;
        send_async_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }
}
