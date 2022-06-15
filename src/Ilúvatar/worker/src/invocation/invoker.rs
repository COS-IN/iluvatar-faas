use std::sync::Arc;
use crate::containers::containermanager::ContainerManager;
use iluvatar_lib::{rpc::InvokeRequest, utils::calculate_fqdn};
use std::time::SystemTime;
use anyhow::Result;
use reqwest;

#[derive(Debug)]
#[allow(unused)]
pub struct InvokerService {
  pub cont_manager: Arc<ContainerManager>,
}

impl InvokerService {
    pub fn new(cont_manager: Arc<ContainerManager>) -> Self {
      InvokerService {
        cont_manager,
      }
    }

    pub async fn invoke(&self, request: &InvokeRequest) -> Result<(String, u64)> {
      let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
      match self.cont_manager.acquire_container(&fqdn) {
        Some(ctr_lock) => 
        {
          let client = reqwest::Client::new();
          let start = SystemTime::now();
          let result = client.post(&ctr_lock.container.invoke_uri)
            .body(request.json_args.to_owned())
            .header("Content-Type", "application/json")
            .send()
            .await?;
          let duration = start.elapsed()?.as_millis() as u64;
          let data = result.text().await?;
          Ok((data, duration))
        },
        None => anyhow::bail!("Unable to acquire a container for function '{}'", &fqdn),
      }
    }

    pub async fn invoke_async(&self, ) -> Result<String> {
      Ok("".to_string())
    }

    pub async fn invoke_async_check(&self, ) -> Result<String> {
      Ok("".to_string())
    }
}