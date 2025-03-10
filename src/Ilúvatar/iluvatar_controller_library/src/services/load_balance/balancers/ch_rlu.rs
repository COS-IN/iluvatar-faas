use crate::server::config::ControllerConfig;
use crate::services::load_balance::{LoadBalancerTrait, LoadMetric};
use crate::services::registration::{FunctionRegistration, RegisteredWorker};
use crate::{
    build_influx, prewarm, send_async_invocation, send_invocation,
    services::{controller_health::ControllerHealthService, load_reporting::LoadService},
};
use anyhow::Result;
use hashring::HashRing;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::utils::timing::TimedExt;
use iluvatar_library::{threading::tokio_thread, transaction::TransactionId};
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use ordered_float::OrderedFloat;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc, time::Duration};
use std::sync::atomic::{AtomicU64, Ordering};
use rand::distributions::Distribution;
use statrs::distribution::Normal;
use tokio::task::JoinHandle;
use tracing::{debug, info};
use rand::thread_rng;

pub struct AtomicF64 {
    storage: AtomicU64,
}
impl AtomicF64 {
    pub fn new(value: f64) -> Self {
        let as_u64 = value.to_bits();
        Self { storage: AtomicU64::new(as_u64) }
    }
    pub fn store(&self, value: f64, ordering: Ordering) {
        let as_u64 = value.to_bits();
        self.storage.store(as_u64, ordering)
    }
    pub fn load(&self, ordering: Ordering) -> f64 {
        let as_u64 = self.storage.load(ordering);
        f64::from_bits(as_u64)
    }
}

#[allow(unused)]
pub struct ChRluLoadedBalancer {
    workers: RwLock<Vec<Arc<RegisteredWorker>>>,
    worker_fact: Arc<WorkerAPIFactory>,
    health: Arc<dyn ControllerHealthService>,
    func_reg: Arc<FunctionRegistration>,
    _worker_thread: JoinHandle<()>,
    load: Arc<LoadService>,
    worker_cmap: WorkerCharMap,
    worker_loads: RwLock<HashMap<String, f64>>,
    popular_map: RwLock<HashSet<String>>,
    worker_ring: RwLock<HashRing<usize>>,
    popular_pct: f64,
    bounded_ceil: f64,
    sampling: AtomicF64,
}

impl ChRluLoadedBalancer {
    pub async fn boxed(
        health: Arc<dyn ControllerHealthService>,
        worker_fact: Arc<WorkerAPIFactory>,
        tid: &TransactionId,
        config: &ControllerConfig,
        load_metric: &LoadMetric,
        popular_pct: f64,
        bounded_ceil: f64,
        worker_cmap: &WorkerCharMap,
        func_reg: &Arc<FunctionRegistration>,
    ) -> Result<Arc<Self>> {
        let ch_rlu_tid: TransactionId = "CH_RLU_LB".to_string();
        let influx = build_influx(config, tid).await?;
        let load = crate::build_load_svc(load_metric, tid, &worker_fact, influx)?;
        let (handle, tx) = tokio_thread(load_metric.thread_sleep_ms, ch_rlu_tid, Self::update);

        let i = Arc::new(Self {
            workers: RwLock::new(vec![]),
            func_reg: func_reg.clone(),
            worker_fact,
            health,
            _worker_thread: handle,
            load,
            worker_cmap: worker_cmap.clone(),
            popular_map: RwLock::new(HashSet::new()),
            worker_ring: RwLock::new(Default::default()),
            worker_loads: RwLock::new(HashMap::new()),
            popular_pct,
            bounded_ceil,
            sampling: AtomicF64::new(0.0),
        });
        tx.send(i.clone())?;
        Ok(i)
    }

    #[tracing::instrument(level="debug", skip(service), fields(tid=tid))]
    async fn update(service: Arc<Self>, tid: TransactionId) {
        service.update_worker_loads(&tid);
        service.calc_popular(&tid);
    }

    fn update_worker_loads(&self, tid: &TransactionId) {
        let loads =  self.load.get_workers();
        debug!(tid=tid, loads=?loads, "latest worker loads");
        *self.worker_loads.write() =loads;
    }

    fn calc_popular(&self, tid: &TransactionId) {
        let funcs = self.func_reg.get_all_functions();
        if funcs.is_empty() {
            return;
        }
        let mut iats: Vec<(OrderedFloat<f64>, Arc<RegisteredFunction>)> = funcs
            .into_iter()
            .map(|r| (OrderedFloat(self.worker_cmap.get_avg(&r.fqdn, Chars::IAT)), r))
            .collect();
        let len = iats.len() as f64;
        let avg_iat = iats.iter().fold(0.0, |acc, v| acc + v.0.0) / len;
        let arrival_rate = 1.0 / avg_iat;
        self.sampling.store(arrival_rate / 4.0, Ordering::Relaxed);
        iats.sort_by(|a, b| a.0.cmp(&b.0));
        let take = self.popular_pct * len;
        let new_pop = HashSet::from_iter(iats.iter().take(f64::round(take) as usize).map(|a| a.1.fqdn.clone()));
        debug!(tid=tid, take=take, iats = ?iats, populars=?new_pop, "computed popularity");
        *self.popular_map.write() = new_pop;
    }

    fn get_worker(&self, func: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
        let in_map = self.popular_map.read().get(&func.fqdn).is_some();
        let default_worker = *self.worker_ring.read().get(&func.fqdn).unwrap_or(&0);
        let worker_idx = match in_map {
            false => default_worker,
            true => {
                let mut chosen_worker = default_worker;
                let mean = self.sampling.load(Ordering::Relaxed);
                let norm = Normal::new(mean, 0.1)?;
                let loads = self.worker_loads.read();
                let workers = self.workers.read();
                for i in 0..workers.len() {
                    let idx = (default_worker + i) % workers.len();
                    let load = loads.get(&workers[idx].name).unwrap_or(&0.0) + norm.sample(&mut thread_rng());
                    info!(tid=tid, pos_forwad=workers[idx].name, "trying forwarding");
                    if load <= self.bounded_ceil {
                        info!(tid=tid, forwad=workers[idx].name, "forwarding");
                        chosen_worker = idx;
                        break;
                    }
                }
                chosen_worker
            },
        };
        Ok(self.workers.read()[worker_idx].clone())
    }
}

#[tonic::async_trait]
impl LoadBalancerTrait for ChRluLoadedBalancer {
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        info!(tid=tid, worker=%worker.name, "Registering new worker in CH-RLU load balancer");
        let mut lck = self.workers.write();
        let len = lck.len();
        lck.push(worker);
        drop(lck);
        // 'virtual' nodes
        // self.worker_ring.write().add(len);
        // self.worker_ring.write().add(len);
        self.worker_ring.write().add(len);
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, func, json_args), fields(tid=tid)))]
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)> {
        let worker = self.get_worker(&func, tid)?;
        send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }

    async fn send_async_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(String, Arc<RegisteredWorker>, Duration)> {
        let worker = self.get_worker(&func, tid)?;
        send_async_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }

    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
        let worker = self.get_worker(&func, tid)?;
        prewarm!(func, tid, self.worker_fact, self.health, worker)
    }
}
