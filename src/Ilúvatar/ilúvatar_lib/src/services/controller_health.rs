use std::sync::Arc;
use dashmap::DashMap;
use crate::{worker_api::worker_comm::WorkerAPIFactory, transaction::TransactionId, load_balancer_api::structs::internal::{RegisteredWorker, WorkerStatus}, ilúvatar_api::HealthStatus};
use tracing::{warn, debug, info};
use std::time::Duration;

#[allow(unused)]
pub struct HealthService {
  worker_fact: WorkerAPIFactory,
  worker_statuses: Arc<DashMap<String, WorkerStatus>>,
}

impl HealthService {
  pub fn boxed() -> Arc<Self> {
    Arc::new(HealthService {
      worker_fact: WorkerAPIFactory {},
      worker_statuses: Arc::new(DashMap::new())
    })
  }

  pub fn is_healthy(&self, worker: &Arc<RegisteredWorker>) -> bool {
    match self.worker_statuses.get(&worker.name) {
      Some(stat) => {
        stat.value() == &WorkerStatus::HEALTHY
      },
      None => false,
    }
  }

  /// returns true if the status is changed, or the worker was not seen before
  fn status_changed(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId, status: &WorkerStatus) -> bool {
    match self.worker_statuses.get(&worker.name) {
      Some(stat) => {
        info!("[{}] worker '{}' changed status to {}", tid, worker.name, tid);
        stat.value() == status
      },
      None => true,
    }
  }

  /// updates the stored status of the worker
  fn update_status(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId, status: &WorkerStatus) {
    debug!(tid=%tid, name=%worker.name, status=?status, "updating worker status");
    self.worker_statuses.insert(worker.name.clone(), status.clone());
  }

  /// get the health status for a specific worker
  /// returns [WorkerStatus::OFFLINE] or [WorkerStatus::UNHEALTHY] if an error occurs
  async fn get_worker_health(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId) -> WorkerStatus {
    let mut api = match self.worker_fact.get_worker_api(&worker, tid).await {
      Ok(api) => api,
      Err(e) => {
        warn!("[{}] couldn't connect to worker '{}' for health check {}", tid, worker.name, e);
        return WorkerStatus::OFFLINE;
      },
    };
    match api.health(tid.clone()).await {
      Ok(h) => match h {
        HealthStatus::HEALTHY => WorkerStatus::HEALTHY,
        HealthStatus::UNHEALTHY => WorkerStatus::UNHEALTHY,
      },
      Err(e) => {
        warn!("[{}] error when checking worker '{}' health {}", tid, worker.name, e);
        WorkerStatus::UNHEALTHY
      },
    }
  }

  /// check the health of a worker in the future
  /// optional to check in a specific time
  pub fn schedule_health_check(&self, svc: Arc<HealthService>, worker: Arc<RegisteredWorker>, tid: &TransactionId, in_secs: Option<Duration>) {
    debug!(tid=%tid, name=%worker.name, "scheduling future health check for worker");
    tokio::spawn(async move {
      let tid: &TransactionId = &crate::transaction::HEALTH_TID;
      let dur = match in_secs {
        Some(t) => t,
        // default check an unhealthy invoker in 30 seconds
        None => Duration::from_secs(30),
      };
      tokio::time::sleep(dur).await;

      if svc.update_worker_health(&worker, tid).await {
        svc.schedule_health_check(svc.clone(), worker, tid, in_secs);
      }
    });
  }

  /// returns true if health needs to be checked again in the future
  pub async fn update_worker_health(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId) -> bool {
    let new_status = self.get_worker_health(worker, tid).await;
    if self.status_changed(worker, tid, &new_status) {
      self.update_status(worker, tid, &new_status)
    }
    new_status != WorkerStatus::HEALTHY
  }
}
