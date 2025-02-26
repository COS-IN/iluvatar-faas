use crate::server::structs::{RegisteredFunction, RegisteredWorker};
use crate::services::controller_health::ControllerHealthService;
use crate::services::load_balance::LoadBalancerTrait;
use crate::{prewarm, send_async_invocation, send_invocation};
use anyhow::Result;
use iluvatar_library::{transaction::TransactionId, utils::timing::TimedExt, types::Compute};
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use std::collections::HashMap ;

/// Load balancer state for the Guardrail policy with Round-Robin (next is the only RR-specific state) 
struct LBState_G_RR {
    workers: Vec<Arc<RegisteredWorker>>,
    next: usize,
    c: f64, // GuardRailWidth. Depends on rho. At 0.8, equal to 1.3
    g: f64, // GuardRail parameter, G < G_min + gc^{r+1} 
    Rmax: i32, // Max rank. Depends on max size. Lets assume 10 minutes, which yields log_1.3 600
    // CPU vs. GPU load? Should the LB take this into account? If GPU function, then size is different. Do we keep per-device dispatch counters?
    G_CPU: Vec<HashMap<i32,f64>>, // Per worker hashmap [rank -> dispatched traffic (based on size?)]
    G_GPU: Vec<HashMap<i32,f64>> 
}

// Some kind of trait/mixin? Envmt context {worker, device, schedgroup} + Grail_state + {RR, CH-BL, ...} 

/// RoundRobin With Guardrails 
pub struct RRGLoadBalancer {
    worker_fact: Arc<WorkerAPIFactory>,
    health: Arc<dyn ControllerHealthService>,
    lbs: Mutex<LBState_G_RR>,
    // Above is all the Controller Load balancing state. Rest is the RRG policy state

}

impl RRGLoadBalancer {
    pub fn new(health: Arc<dyn ControllerHealthService>, worker_fact: Arc<WorkerAPIFactory>) -> Self {
        RRGLoadBalancer {
            worker_fact: worker_fact,
            health: health,
	    lbs: Mutex::new(LBState_G_RR {
		workers: Vec::new(),
		next: 0,
		c: 1.3,
		g: 1.1,
		Rmax: 10,
		G_CPU: Vec::new(),
		G_GPU: Vec::new(),
	    })
        }
    }

    // hmm will need a mutex again, keep a separate LBState protected by a mutex? 

    fn invok_sz(&self, func: Arc<RegisteredFunction>) -> f64 {
	return 1.0 // TODO: return the size from cmap or some other source. Should the controller cache this? Perhaps, yes. Read from local cache, else read from one of the workers which might have this info. Function characteristics service on the controller and perhaps each worker too? Instead of directly invoking cmap? More unified way for instant, historic, surrogate, predictive models, etc? 
    }

    fn invok_rank(&self, sz:f64) -> i32 {
	return 1  // log_c x, where x is size. c is the guard-rail width 
    }

    fn Gmin(&self, rank:i32, device:Compute) -> f64 {
	/// From all the workers, find the one with the lowest G for the specified rank
	return 0.0 
    }

    fn get_G(&self, rank:i32, device:Compute, workeridx: usize) -> f64 {
	let lbsm = self.lbs.lock(); 
	return match device {
	    Compute::GPU => {*lbsm.G_GPU[workeridx].get(&rank).unwrap_or(&0.0)},
	    _ => {*lbsm.G_CPU[workeridx].get(&rank).unwrap_or(&0.0)}
	}
    }

    /// Is this function within the load GuardRails for the worker with the index? 
    fn within_Grail(&self, func: Arc<RegisteredFunction>, workeridx: usize) -> bool {
	let device = func.compute.clone();
	let X = self.invok_sz(func);
	let r = self.invok_rank(X);

	let gmin = self.Gmin(r, device);
	let G = self.get_G(r, device, workeridx);
	let lbsm = self.lbs.lock(); 
	let expon = (r+1) as f64 ;
	return G < (gmin + (lbsm.g * f64::powf(lbsm.c , expon))); 
    }
    
    fn get_next(&self, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
	let mut lbsm = self.lbs.lock();
	
        if lbsm.workers.len() == 0 {
            anyhow::bail!("There are not workers available to serve the request");
        }
        let mut i = 0;
        loop {
            let mut val = lbsm.next;
            val += 1_usize;
            if val >= lbsm.workers.len() {
                val = 0_usize;
            }
            let worker = &lbsm.workers[val];
            if self.health.is_healthy(worker) {
                return Ok(worker.clone());
            } else if i >= lbsm.workers.len() {
                warn!(tid=%tid, "Could not find a healthy worker!");
                return Ok(worker.clone());
            }
            i += 1;
        }
    }
}

#[tonic::async_trait]
impl LoadBalancerTrait for RRGLoadBalancer {
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
        info!(tid=%tid, worker=%worker.name, "Registering new worker in RoundRobin load balancer");
	let mut lbsm = self.lbs.lock();
        lbsm.workers.push(worker);
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, func, json_args), fields(tid=%tid)))]
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)> {
	// This is the only load balancing policy hook 
        let worker = self.get_next(tid)?;
        send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
    }

    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
        let worker = self.get_next(tid)?;
        prewarm!(func, tid, self.worker_fact, self.health, worker)
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
