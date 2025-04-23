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
use rand::distributions::Distribution;
use rand::thread_rng;
use rcu_cell::RcuCell;
use serde::Deserialize;
use statrs::distribution::Normal;
use std::collections::HashSet;
use std::hash::Hasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tracing::{debug, info};

pub struct AtomicF64 {
    storage: AtomicU64,
}
impl AtomicF64 {
    pub fn new(value: f64) -> Self {
        let as_u64 = value.to_bits();
        Self {
            storage: AtomicU64::new(as_u64),
        }
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
#[derive(Debug, Clone)]
struct VNode {
    pub idx: usize,
    pub cores: f64,
    pub hashed: String,
}
impl VNode {
    pub fn new(idx: usize, cores: f64) -> Self {
        Self {
            idx,
            cores,
            hashed: guid_create::GUID::rand().to_string(),
        }
    }

    pub fn batch(idx: usize, cores: f64, count: usize) -> Vec<Self> {
        let mut v = vec![];
        for _ in 0..count {
            v.push(Self::new(idx, cores));
        }
        v
    }
}
impl std::hash::Hash for VNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hashed.hash(state);
    }
}

#[derive(Debug, Deserialize)]
pub struct ChRluConfig {
    /// Load metric to determine worker "overload"
    load_metric: LoadMetric,
    /// Percentage of functions to mark as "popular" for forwarding.
    popular_pct: f64,
    bounded_ceil: f64,
    #[serde(default = "default_chain")]
    chain_len: usize,
    #[serde(default = "default_vnode")]
    /// Number of vnodes per real node
    vnodes: usize,
}
fn default_chain() -> usize {
    4
}
fn default_vnode() -> usize {
    3
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
    popular_map: RcuCell<HashSet<String>>,
    worker_ring: RwLock<HashRing<VNode>>,
    chrlu_cfg: Arc<ChRluConfig>,
    arrival_rate: AtomicF64,
}

impl ChRluLoadedBalancer {
    pub async fn boxed(
        health: Arc<dyn ControllerHealthService>,
        worker_fact: Arc<WorkerAPIFactory>,
        tid: &TransactionId,
        config: &ControllerConfig,
        chrlu_cfg: &Arc<ChRluConfig>,
        worker_cmap: &WorkerCharMap,
        func_reg: &Arc<FunctionRegistration>,
    ) -> Result<Arc<Self>> {
        let ch_rlu_tid: TransactionId = "CH_RLU_LB".to_string();
        let influx = build_influx(config, tid).await?;
        let load = crate::build_load_svc(&chrlu_cfg.load_metric, tid, &worker_fact, influx)?;
        let (handle, tx) = tokio_thread(chrlu_cfg.load_metric.thread_sleep_ms, ch_rlu_tid, Self::update);

        let i = Arc::new(Self {
            workers: RwLock::new(vec![]),
            func_reg: func_reg.clone(),
            worker_fact,
            health,
            _worker_thread: handle,
            load,
            worker_cmap: worker_cmap.clone(),
            popular_map: RcuCell::new(HashSet::new()),
            worker_ring: RwLock::new(Default::default()),
            worker_loads: RwLock::new(HashMap::new()),
            chrlu_cfg: chrlu_cfg.clone(),
            arrival_rate: AtomicF64::new(0.0),
        });
        tx.send(i.clone())?;
        Ok(i)
    }

    #[tracing::instrument(level="debug", skip(self), fields(tid=tid))]
    async fn update(self: &Arc<Self>, tid: &TransactionId) {
        self.update_worker_loads(tid);
        self.calc_popular(tid);
    }

    fn update_worker_loads(&self, tid: &TransactionId) {
        let loads = self.load.get_workers();
        debug!(tid=tid, loads=?loads, "latest worker loads");
        *self.worker_loads.write() = loads;
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
        let avg_iat = iats.iter().fold(0.0, |acc, v| acc + v.0 .0) / len;
        let mut arrival_rate = 1.0 / avg_iat;
        if !arrival_rate.is_finite() {
            arrival_rate = 1.0;
        }
        self.arrival_rate.store(arrival_rate, Ordering::Relaxed);
        iats.sort_by(|a, b| a.0.cmp(&b.0));
        let take = self.chrlu_cfg.popular_pct * len;
        let new_pop = HashSet::from_iter(iats.iter().take(f64::round(take) as usize).map(|a| a.1.fqdn.clone()));
        debug!(tid=tid, take=take, iats = ?iats, populars=?new_pop, "computed popularity");
        let _old = self.popular_map.write(new_pop);
    }

    fn get_worker(&self, func: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
        let in_map = self.popular_map.read().is_some_and(|p| p.get(&func.fqdn).is_some());
        let worker_chain = match self
            .worker_ring
            .read()
            .get_with_replicas(&func.fqdn, self.chrlu_cfg.chain_len)
        {
            None => anyhow::bail!("No worker ring exists"),
            Some(w) => w,
        };
        let default_worker = worker_chain[0].idx;
        let worker = match in_map {
            false => self.workers.read()[default_worker].clone(),
            true => {
                let mut chosen_worker = None;
                let mean = self.arrival_rate.load(Ordering::Relaxed);
                let loads = self.worker_loads.read();
                let workers = self.workers.read();
                for node in worker_chain {
                    // safe if std_dev > 0.0
                    let norm = Normal::new(mean / node.cores, 0.1)?;
                    let noise = norm.sample(&mut thread_rng());
                    let load = loads.get(&workers[node.idx].name).unwrap_or(&0.0) + noise;
                    if load <= self.chrlu_cfg.bounded_ceil {
                        if default_worker != node.idx {
                            debug!(
                                tid = tid,
                                from = workers[default_worker].name,
                                to = workers[node.idx].name,
                                load = load,
                                noise = noise,
                                "forwarding"
                            );
                        }
                        let chosen = workers[node.idx].clone();
                        drop(loads);
                        drop(workers);
                        if let Some(w) = self.worker_loads.write().get_mut(&chosen.name) {
                            *w += noise;
                        };
                        chosen_worker = Some(chosen);
                        break;
                    }
                    debug!(
                        tid = tid,
                        pos_from = workers[node.idx].name,
                        load = load,
                        noise = noise,
                        "trying forwarding"
                    );
                }
                match chosen_worker {
                    None => self.workers.read()[default_worker].clone(),
                    Some(w) => w,
                }
            },
        };
        Ok(worker)
    }
}

#[tonic::async_trait]
impl LoadBalancerTrait for ChRluLoadedBalancer {
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        info!(tid=tid, worker=%worker.name, "Registering new worker in CH-RLU load balancer");
        let cores = worker.cpus as f64;
        let mut lck = self.workers.write();
        let len = lck.len();
        lck.push(worker);
        drop(lck);
        // 'virtual' nodes
        self.worker_ring
            .write()
            .batch_add(VNode::batch(len, cores, self.chrlu_cfg.vnodes));
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
