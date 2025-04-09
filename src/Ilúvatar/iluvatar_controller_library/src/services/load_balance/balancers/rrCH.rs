#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(unused)]

use crate::services::controller_health::ControllerHealthService;
use crate::services::load_balance::LoadBalancerTrait;
use crate::{prewarm, send_async_invocation, send_invocation};
use anyhow::Result;
use iluvatar_library::{transaction::TransactionId, types::Compute, utils::timing::TimedExt};
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

extern crate hashring;

use crate::services::registration::RegisteredWorker;
use hashring::HashRing;
use iluvatar_library::char_map::{Chars, Value, WorkerCharMap};
use iluvatar_worker_library::services::registration::RegisteredFunction;

/// Load balancer state for the Guardrail policy with consistent hashing
struct LBState_G_CH {
    workers: Vec<Arc<RegisteredWorker>>, // Since we keep everything stored using the worker index, this will still be needed
    workering: HashRing<i32>,            // Hash ring of just integer indexes? Which are then mapped to the workers etc?
    next: usize,
    c: f64,    // GuardRailWidth. Depends on rho. At 0.8, equal to 1.3
    g: f64,    // GuardRail parameter, G < G_min + gc^{r+1}
    Rmax: i32, // Max rank. Depends on max size. Lets assume 10 minutes, which yields log_1.3 600
    // CPU vs. GPU load? Should the LB take this into account? If GPU function, then size is different. Do we keep per-device dispatch counters?
    G_CPU: Vec<HashMap<i32, f64>>, // Per worker hashmap [rank -> dispatched traffic (based on size?)]
    G_GPU: Vec<HashMap<i32, f64>>,
}

// Some kind of trait/mixin? Envmt context {worker, device, schedgroup} + Grail_state + {RR, CH-BL, ...}

/// RoundRobin With Guardrails
pub struct CHGLoadBalancer {
    worker_fact: Arc<WorkerAPIFactory>,
    health: Arc<dyn ControllerHealthService>,
    worker_cmap: WorkerCharMap,
    // Above is all the Controller Load balancing state. Rest is the RRG policy state
    lbs: Mutex<LBState_G_CH>,
}

impl CHGLoadBalancer {
    pub fn new(
        health: Arc<dyn ControllerHealthService>,
        worker_fact: Arc<WorkerAPIFactory>,
        worker_cmap: &WorkerCharMap,
    ) -> Self {
        CHGLoadBalancer {
            worker_fact,
            health,
            worker_cmap: worker_cmap.clone(),
            lbs: Mutex::new(LBState_G_CH {
                workers: Vec::new(),
                workering: HashRing::new(),
                next: 0,
                c: 1.3,
                g: 1.1,
                Rmax: 10,
                G_CPU: Vec::new(),
                G_GPU: Vec::new(),
            }),
        }
    }

    // hmm will need a mutex again, keep a separate LBState protected by a mutex?

    fn invok_sz(&self, func: &Arc<RegisteredFunction>) -> f64 {
        let (cpu, gpu) = self.worker_cmap.get_2(
            &func.fqdn,
            Chars::CpuExecTime,
            Value::Avg,
            Chars::GpuExecTime,
            Value::Avg,
        );
        match func.supported_compute {
            Compute::CPU => cpu,
            Compute::GPU => gpu,
            _ => f64::min(cpu, gpu),
        }
        // TODO: return the size from cmap or some other source.
        // Should the controller cache this? Perhaps, yes.
        // Read from local cache, else read from one of the workers which might have this info.
        // Function characteristics service on the controller and perhaps each worker too?
        // Instead of directly invoking cmap?
        // More unified way for instant, historic, surrogate, predictive models, etc?
    }

    fn invok_rank(&self, sz: f64) -> i32 {
        1 // log_c x, where x is size. c is the guard-rail width
    }

    fn Gmin(&self, rank: i32, device: &Compute) -> f64 {
        /// From all the workers, find the one with the lowest G for the specified rank
        0.0
    }

    fn get_G(&self, rank: i32, device: &Compute, workeridx: usize) -> f64 {
        let lbsm = self.lbs.lock();
        match device {
            &Compute::GPU => *lbsm.G_GPU[workeridx].get(&rank).unwrap_or(&0.0),
            _ => *lbsm.G_CPU[workeridx].get(&rank).unwrap_or(&0.0),
        }
    }

    /// Is this function within the load GuardRails for the worker with the index?
    fn within_Grail(&self, func: Arc<RegisteredFunction>, workeridx: usize) -> bool {
        let X = self.invok_sz(&func);
        let r = self.invok_rank(X);

        let gmin = self.Gmin(r, &func.supported_compute);
        let G = self.get_G(r, &func.supported_compute, workeridx);
        let lbsm = self.lbs.lock();
        let expon = (r + 1) as f64;
        G < (gmin + (lbsm.g * f64::powf(lbsm.c, expon)))
    }

    fn get_next(&self, func: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
        let mut lbsm = self.lbs.lock();

        if lbsm.workers.is_empty() {
            anyhow::bail!("There are not workers available to serve the request");
        }

        let wi = lbsm.workering.get(&func.fqdn).unwrap_or(&0); // well always default to the 0th worker?
        let worker = &lbsm.workers[*wi as usize];
        if self.health.is_healthy(worker) {
            Ok(worker.clone())
        } else {
            warn!(tid = tid, "Could not find a healthy worker!");
            // This should be a lot more severe than a warning? How to handle this exception?
            Ok(worker.clone())
        }
    }
}

#[tonic::async_trait]
impl LoadBalancerTrait for CHGLoadBalancer {
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        info!(tid=tid, worker=%worker.name, "Registering new worker in RoundRobin load balancer");
        let mut lbsm = self.lbs.lock();
        lbsm.workers.push(worker);
        let n = lbsm.workers.len();
        lbsm.workering.add(n as i32);
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, func, json_args), fields(tid=tid)))]
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)> {
        // This is the only load balancing policy hook
        let worker = self.get_next(&func, tid)?;
        send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }

    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
        let worker = self.get_next(&func, tid)?;
        prewarm!(func, tid, self.worker_fact, self.health, worker)
    }

    async fn send_async_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(String, Arc<RegisteredWorker>, Duration)> {
        let worker = self.get_next(&func, tid)?;
        send_async_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }
}
