use guid_create::GUID;

pub type TransactionId = String;

pub fn gen_tid() -> TransactionId {
  GUID::rand().to_string().to_lowercase().replace("-", "to")
}

lazy_static::lazy_static! {
  pub static ref NAMESPACE_POOL_WORKER_TID: TransactionId = "NetNsPool".to_string();
  pub static ref CTR_MGR_WORKER_TID: TransactionId = "CtrMrgWorker".to_string();
  pub static ref STARTUP_TID: TransactionId = "Startup".to_string();
  pub static ref TEST_TID: TransactionId = "TestTest".to_string();
  pub static ref INVOKER_QUEUE_WORKER_TID: TransactionId = "InvokerQueue".to_string();
  pub static ref LOAD_BALANCER_TID: TransactionId = "LoadBalancer".to_string();
  pub static ref STATUS_WORKER_TID: TransactionId = "Status".to_string();
  pub static ref HEALTH_TID: TransactionId = "HealthCheck".to_string();
  pub static ref LEAST_LOADED_TID: TransactionId = "LeastLoadedLB".to_string();
  pub static ref SIMULATION_START_TID: TransactionId = "SimulationStartup".to_string();
}
