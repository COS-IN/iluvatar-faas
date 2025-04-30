use crate::services::controller_health::ControllerHealthService;
use crate::services::load_balance::LoadBalancerTrait;
use crate::services::registration::RegisteredWorker;
use crate::{prewarm, send_async_invocation, send_invocation};
use anyhow::Result;
use iluvatar_library::{transaction::TransactionId, utils::timing::TimedExt};
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

pub struct RoundRobinLoadBalancer {
    workers: RwLock<Vec<Arc<RegisteredWorker>>>,
    next: Mutex<usize>,
    worker_fact: Arc<WorkerAPIFactory>,
    health: Arc<dyn ControllerHealthService>,
}

impl RoundRobinLoadBalancer {
    pub fn new(health: Arc<dyn ControllerHealthService>, worker_fact: Arc<WorkerAPIFactory>) -> Self {
        RoundRobinLoadBalancer {
            workers: RwLock::new(Vec::new()),
            next: Mutex::new(0),
            worker_fact,
            health,
        }
    }

    fn get_next(&self, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
        if self.workers.read().is_empty() {
            anyhow::bail!("There are not workers available to serve the request");
        }
        let mut i = 0;
        loop {
            let mut val = self.next.lock();
            *val += 1_usize;
            if *val >= self.workers.read().len() {
                *val = 0_usize;
            }
            let worker = &self.workers.read()[*val];
            if self.health.is_healthy(worker) {
                return Ok(worker.clone());
            } else if i >= self.workers.read().len() {
                warn!(tid = tid, "Could not find a healthy worker!");
                return Ok(worker.clone());
            }
            i += 1;
        }
    }
}

#[tonic::async_trait]
impl LoadBalancerTrait for RoundRobinLoadBalancer {
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        info!(tid=tid, worker=%worker.name, "Registering new worker in RoundRobin load balancer");
        let mut workers = self.workers.write();
        workers.push(worker);
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, func, json_args), fields(tid=tid)))]
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)> {
        let worker = self.get_next(tid)?;
        send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }

    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
        let worker = self.get_next(tid)?;
        prewarm!(func, tid, self.worker_fact, self.health, worker, func.supported_compute)
    }

    async fn send_async_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(String, Arc<RegisteredWorker>, Duration)> {
        let worker = self.get_next(tid)?;
        send_async_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }
}
