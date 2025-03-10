extern crate anyhow;
extern crate lazy_static;

use crate::server::controller_config::ControllerConfig;
use crate::services::load_balance::LoadMetric;
use crate::services::load_reporting::LoadService;
use iluvatar_library::bail_error;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::transaction::TransactionId;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::sync::Arc;

pub mod server;
pub mod services;

pub async fn build_influx(config: &ControllerConfig, tid: &TransactionId) -> anyhow::Result<Option<Arc<InfluxClient>>> {
    match InfluxClient::new(config.influx.clone(), tid).await {
        Ok(i) => Ok(i),
        Err(e) => bail_error!(tid=tid, error=%e, "Failed to create InfluxClient"),
    }
}

pub fn build_load_svc(
    load_metric: &LoadMetric,
    tid: &TransactionId,
    worker_fact: &Arc<WorkerAPIFactory>,
    influx: Option<Arc<InfluxClient>>,
) -> anyhow::Result<Arc<LoadService>> {
    match LoadService::boxed(influx, load_metric, tid, worker_fact.clone()) {
        Ok(l) => Ok(l),
        Err(e) => bail_error!(tid=tid, error=%e, "Failed to create LoadService"),
    }
}
