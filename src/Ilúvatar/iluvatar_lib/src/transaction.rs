use guid_create::GUID;

pub type TransactionId = String;

pub fn gen_tid() -> TransactionId {
  GUID::rand().to_string().to_lowercase().replace("-", "to")
}
lazy_static::lazy_static! {
  static ref NAMESPACE_POOL_WORKER_TID: TransactionId = "NetNsPool".to_string();
  static ref CTR_MGR_WORKER_TID: TransactionId = "CtrMrgWorker".to_string();
}
