use anyhow::Result;
use reqwest::StatusCode;
use crate::{transaction::TransactionId, bail_error, utils::port_utils::Port, types::CommunicationMethod};

#[allow(unused)]
#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub struct RegisterWorker {
  pub name: String,
  pub backend: String,
  pub communication_method: CommunicationMethod,
  pub host: String,
  pub port: Port,
  pub memory: i64,
  pub cpus: u32,
}

/// Send worker details to the load balancer to register 
pub async fn register_worker(name: &String, communication_method: CommunicationMethod, backend: &String, host: &String, port: Port, memory: i64, cpus: u32, loab_balancer_url: &String, tid: &TransactionId) -> Result<()> {
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
  match client.post(format!("{}/register_worker", loab_balancer_url))
    .json(&req)
    .header("Content-Type", "application/json")
    .send()
    .await {
      Ok(r) => {
        let status = r.status();
        if status == StatusCode::ACCEPTED {
          Ok(())
        } else {
          let text = r.text().await?;
          bail_error!(tid=%tid, status=?status, text=%text, "Got unexpected HTTP status when registering worker with the load balancer")
        }
      },
      Err(e) => bail_error!(tid=%tid, error=?e, "HTTP error when trying to register worker with the load balancer"),
    }
}
