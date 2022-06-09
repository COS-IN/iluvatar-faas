tonic::include_proto!("iluvatar_worker");
use tonic::async_trait;
use tonic::transport::Channel;
use std::error::Error;
use crate::rpc::iluvatar_worker_client::IluvatarWorkerClient;
use crate::worker_api;

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

#[derive(Debug)]
pub struct RPCError {
  message: String,
}
impl std::fmt::Display for RPCError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "{}", self.message).unwrap();
    Ok(())
  }
}
impl Error for RPCError {

}

/// An implementation of the worker API that communicates with workers via RPC
#[async_trait]
impl crate::worker_api::WorkerAPI for RCPWorkerAPI {
  async fn ping(&mut self) -> Result<String, Box<dyn std::error::Error>> {
    let request = tonic::Request::new(PingRequest {
      message: "Ping".to_string(),
    });
    let response = self.client.ping(request).await?;
    Ok(response.into_inner().message)
  }

  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<u32>) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(InvokeRequest {
      function_name: function_name,
      function_version: version,
      memory: match memory {
        Some(x) => x,
        _ => 0,
      },
      json_args: args
    });
    let response = self.client.invoke(request).await?;
    Ok(response.into_inner().json_result)
  }

  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<u32>) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(InvokeAsyncRequest {
      function_name: function_name,
      function_version: version,
      memory: match memory {
        Some(x) => x,
        _ => 0,
      },
      json_args: args
    });
    let response = self.client.invoke_async(request).await?;
    Ok(response.into_inner().lookup_cookie)
  }

  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<u32>, cpu: Option<u32>, image: Option<String>) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(PrewarmRequest {
      function_name: function_name,
      function_version: version,
      memory: match memory {
        Some(x) => x,
        _ => 0,
      },
      cpu: match cpu {
        Some(x) => x,
        _ => 0,
      },
      image_name: match image {
        Some(x) => x,
        _ => "".into(),
      },
    });
    let response = self.client.prewarm(request).await?;
    let response = response.into_inner();

    match response.success {
      true => Ok("".to_string()),
      false => Err(Box::new(RPCError { message: response.message })),
    }
  }

  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: u32, cpus: u32) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(RegisterRequest {
      function_name,
      function_version: version,
      memory,
      cpus,
      image_name
    });
    let response = self.client.register(request).await?;
    Ok(response.into_inner().function_json_result)
  }
  
  async fn status(&mut self) -> Result<String, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(StatusRequest { });
    let response = self.client.status(request).await?;
    Ok(response.into_inner().json_result)
  }

  async fn health(&mut self) -> Result<worker_api::HealthStatus, Box<(dyn std::error::Error + 'static)>> {
    let request = tonic::Request::new(HealthRequest { });
    let response = self.client.health(request).await?;
    match response.into_inner().status {
      // HealthStatus::Healthy
      0 => Ok(worker_api::HealthStatus::HEALTHY),
      // HealthStatus::Unhealthy
      1 => Ok(worker_api::HealthStatus::UNHEALTHY),
      i => Err(Box::new(RPCError {
        message: format!("Got unexpected status of {}", i)
      })),
    }
  }
}
