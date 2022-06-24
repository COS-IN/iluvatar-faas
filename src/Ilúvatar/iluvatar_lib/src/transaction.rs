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
}
