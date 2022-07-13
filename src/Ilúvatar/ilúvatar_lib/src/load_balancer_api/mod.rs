pub mod lb_config;
pub use lb_config as config;
pub mod lb_structs;
pub use lb_structs as structs;

use anyhow::Result;

use crate::{transaction::TransactionId, bail_error, utils::port_utils::Port};

use self::lb_structs::RegisterWorker;

/// Send worker details to the load balancer to register 
pub async fn register_worker(name: &String, communication_method: &String, backend: &String, host: &String, port: Port, memory: i64, cpus: u32, loab_balancer_url: &String, tid: &TransactionId) -> Result<()> {
  let req = RegisterWorker {
    name: name.clone(),
    backend: backend.clone(),
    host: host.clone(),
    communication_method: communication_method.clone(),
    port,
    memory,
    cpus
  };
  let client = reqwest::Client::new();
  let _result = match client.post(format!("{}/register_worker", loab_balancer_url))
    .json(&req)
    // .body(req)
    .header("Content-Type", "application/json")
    .send()
    .await {
        Ok(r) => r,
        Err(e) =>{
          bail_error!("[{}] HTTP error when trying to register worker with the load balancer '{}'", tid, e);
        },
    };
  Ok(())
}
