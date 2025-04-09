use guid_create::GUID;

pub type TransactionId = String;

pub fn gen_tid() -> TransactionId {
    GUID::rand().to_string().to_lowercase().replace('-', "")
}

lazy_static::lazy_static! {
  pub static ref NAMESPACE_POOL_WORKER_TID: TransactionId = "NetNsPool".to_string();
  pub static ref STARTUP_TID: TransactionId = "Startup".to_string();
  pub static ref TEST_TID: TransactionId = "TestTest".to_string();
  pub static ref LOAD_BALANCER_TID: TransactionId = "LoadBalancer".to_string();
  pub static ref HEALTH_TID: TransactionId = "HealthCheck".to_string();
  pub static ref LEAST_LOADED_TID: TransactionId = "LeastLoadedLB".to_string();
  pub static ref SIMULATION_START_TID: TransactionId = "SimulationStartup".to_string();
  pub static ref LOAD_MONITOR_TID: TransactionId = "LoadMonitor".to_string();
  pub static ref ENERGY_MONITOR_TID: TransactionId = "EnergyMonitor".to_string();
  pub static ref WORKER_ENERGY_LOGGER_TID: TransactionId = "WorkerEnergyLogger".to_string();
  pub static ref ENERGY_LOGGER_PS_TID: TransactionId = "EnergyLoggerPS".to_string();
  pub static ref LIVE_WORKER_LOAD_TID: TransactionId = "LiveWorkerLoad".to_string();
}
