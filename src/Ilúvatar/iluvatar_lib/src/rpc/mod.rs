tonic::include_proto!("iluvatar_worker");
use tonic::async_trait;
use tonic::transport::Channel;

use crate::rpc::iluvatar_worker_client::IluvatarWorkerClient;

#[allow(unused)]
pub struct RCPWorkerAPI {
  address: String, 
  port: i32,
  client: IluvatarWorkerClient<Channel>
}

impl RCPWorkerAPI {
  pub async fn new(address: String, port: i32) -> Result<RCPWorkerAPI, tonic::transport::Error> {
    let addr = format!("{}:{}", address, port);
    let client = IluvatarWorkerClient::connect(addr).await?;
    Ok(RCPWorkerAPI {
      address,
      port, 
      client
    })
  }
}

#[async_trait]
impl crate::worker_api::WorkerAPI for RCPWorkerAPI {
  async fn ping(&mut self) -> Result<String, Box<dyn std::error::Error>> {
    let request = tonic::Request::new(PingRequest {
      message: "Ping".into(),
    });
    let response = self.client.ping(request).await?;
    Ok(response.into_inner().message)
  }

  async fn invoke(&mut self, function_name: &str, version: &str) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(InvokeRequest {
      function_name: function_name.into(),
      function_version: version.into(),
    });
    let response = self.client.invoke(request).await?;
    Ok(response.into_inner().json_result)
  }

  async fn register(&mut self, function_name: &str, version: &str) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(RegisterRequest {
      function_name: function_name.into(),
      function_version: version.into(),
    });
    let response = self.client.register(request).await?;
    Ok(response.into_inner().function_json_result)
  }
  
  async fn status(&mut self) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(StatusRequest { });
    let response = self.client.status(request).await?;
    Ok(response.into_inner().json_result)
  }
}