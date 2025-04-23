use super::controller_health::ControllerHealthService;
use crate::server::controller_config::ControllerConfig;
use crate::services::load_balance::least_loaded::LeastLoadedBalancer;
use crate::services::registration::{FunctionRegistration, RegisteredWorker};
use anyhow::Result;
use ch_rlu::ChRluLoadedBalancer;
use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use rrCH::CHGLoadBalancer;
use rrG::RRGLoadBalancer;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

pub mod ch_rlu;
pub mod least_loaded;
pub mod round_robin;
pub mod rrCH;
pub mod rrG;

#[derive(Debug, Deserialize)]
pub struct LoadMetric {
    pub load_metric: String,
    /// Duration in milliseconds the balancer's worker thread will sleep between runs (if it has one)
    pub thread_sleep_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
/// Sub-members can be passed with defaults but `"type"` value passed decides what is used.
/// Allows re-use of variable names as between [LeastLoaded] and [CHRLU].
/// See [here](https://serde.rs/enum-representations.html#internally-tagged) for details on deserializing this
pub enum LoadBalancerAlgo {
    RoundRobin,
    LeastLoaded(crate::services::load_balance::least_loaded::LLConfig),
    RrCh,
    RrGuard,
    CHRLU(Arc<ch_rlu::ChRluConfig>),
}

#[tonic::async_trait]
pub trait LoadBalancerTrait {
    /// Add a new worker to the LB pool
    /// assumed to be unhealthy until told otherwise
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId);
    /// Send a synchronous invocation to a worker
    /// Returns the invocation result and the E2E duration of the invocation recorded by the load balancer  
    ///   Thus including both the time spent on the worker and the invocation time, plus networking
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)>;
    /// Start an async invocation on a server
    /// Return the marker cookie for it, and the worker it was launched on
    ///   And the duration of the request recorded by the load balancer  
    ///   Thus including both the time spent on the worker, plus networking
    async fn send_async_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(String, Arc<RegisteredWorker>, Duration)>;
    /// Prewarm the given function somewhere
    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration>;
}

pub type LoadBalancer = Arc<dyn LoadBalancerTrait + Send + Sync + 'static>;

pub async fn get_balancer(
    config: &ControllerConfig,
    health_svc: Arc<dyn ControllerHealthService>,
    tid: &TransactionId,
    worker_fact: Arc<WorkerAPIFactory>,
    worker_cmap: &WorkerCharMap,
    func_reg: &Arc<FunctionRegistration>,
) -> Result<LoadBalancer> {
    match &config.load_balancer.algorithm {
        LoadBalancerAlgo::RoundRobin => Ok(Arc::new(round_robin::RoundRobinLoadBalancer::new(
            health_svc,
            worker_fact,
        ))),
        LoadBalancerAlgo::LeastLoaded(metric) => {
            Ok(LeastLoadedBalancer::boxed(health_svc, worker_fact, tid, config, metric).await?)
        },
        LoadBalancerAlgo::RrCh => Ok(Arc::new(CHGLoadBalancer::new(health_svc, worker_fact, worker_cmap))),
        LoadBalancerAlgo::RrGuard => Ok(Arc::new(RRGLoadBalancer::new(health_svc, worker_fact, worker_cmap))),
        LoadBalancerAlgo::CHRLU(cfg) => {
            Ok(ChRluLoadedBalancer::boxed(health_svc, worker_fact, tid, config, cfg, worker_cmap, func_reg).await?)
        },
    }
}

#[macro_export]
macro_rules! send_invocation {
  ($func:expr, $json_args:expr, $tid:expr, $worker_fact:expr, $health:expr, $worker:expr) => {
    {
      info!(tid=$tid, fqdn=%$func.fqdn, worker=%$worker.name, "invoking function on worker");

      let mut api = $worker_fact.get_worker_api(&$worker.name, &$worker.host, $worker.port, $tid).await?;
      let (result, duration) = api.invoke($func.function_name.clone(), $func.function_version.clone(), $json_args, $tid.clone()).timed().await;
      let result = match result {
        Ok(r) => r,
        Err(e) => {
          $health.schedule_health_check($health.clone(), $worker, $tid, Some(Duration::from_secs(1)));
          anyhow::bail!(e)
        },
      };
      debug!(tid=$tid, json=%result.json_result, "invocation result");
      Ok( (result, duration) )
    }
  };
}

#[macro_export]
macro_rules! prewarm {
  ($func:expr, $tid:expr, $worker_fact:expr, $health:expr, $worker:expr) => {
    {
      info!(tid=$tid, fqdn=%$func.fqdn, worker=%$worker.name, "prewarming function on worker");
      let mut api = $worker_fact.get_worker_api(&$worker.name, &$worker.host, $worker.port, $tid).await?;
      let (result, duration) = api.prewarm($func.function_name.clone(), $func.function_version.clone(), $tid.clone(), iluvatar_library::types::Compute::CPU).timed().await;
      let result = match result {
        Ok(r) => r,
        Err(e) => {
          $health.schedule_health_check($health.clone(), $worker, $tid, Some(Duration::from_secs(1)));
          anyhow::bail!(e)
        }
      };
      debug!(tid=$tid, result=?result, "prewarm result");
      Ok(duration)
    }
  }
}

#[macro_export]
macro_rules! send_async_invocation {
  ($func:expr, $json_args:expr, $tid:expr, $worker_fact:expr, $health:expr, $worker:expr) => {
    {
      info!(tid=$tid, fqdn=%$func.fqdn, worker=%$worker.name, "invoking function async on worker");

      let mut api = $worker_fact.get_worker_api(&$worker.name, &$worker.host, $worker.port, $tid).await?;
      let (result, duration) = api.invoke_async($func.function_name.clone(), $func.function_version.clone(), $json_args, $tid.clone()).timed().await;
      let result = match result {
        Ok(r) => r,
        Err(e) => {
          $health.schedule_health_check($health.clone(), $worker, $tid, Some(Duration::from_secs(1)));
          anyhow::bail!(e)
        },
      };
      debug!(tid=$tid, result=%result, "invocation result");
      Ok( (result, $worker, duration) )
    }
  }
}
