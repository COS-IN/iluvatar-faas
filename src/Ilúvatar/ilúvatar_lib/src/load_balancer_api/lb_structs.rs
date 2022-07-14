use serde::{Deserialize, Serialize};

use crate::utils::port_utils::Port;

pub mod json {
  use super::*;
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct Invoke {
    pub function_name: String,
    pub function_version: String,
    pub args: Vec<String>
  }
  
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct InvokeResult {
    pub json_result: String,
  }
  
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct InvokeAsyncResult {
    pub lookup_cookie: String,
  }
  
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct InvokeAsyncLookup {
    pub lookup_cookie: String,
  }
  
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct Prewarm {
    pub function_name: String,
    pub function_version: String
  }
  
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct RegisterFunction {
    pub function_name: String,
    pub function_version: String,
    pub image_name: String,
    pub memory: i64,
    pub cpus: u32,
    pub parallel_invokes: u32
  }
  
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct RegisterWorker {
    pub name: String,
    pub backend: String,
    pub communication_method: String,
    pub host: String,
    pub port: Port,
    pub memory: i64,
    pub cpus: u32,
  }
}



pub mod internal {
  use super::*;
  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct RegisteredWorker {
    pub name: String,
    pub backend: String,
    pub communication_method: String,
    pub host: String,
    pub port: Port,
    pub memory: i64,
    pub cpus: u32,
  }
  impl RegisteredWorker {
    pub fn from(req: json::RegisterWorker) -> Self {
      RegisteredWorker {
        name: req.name,
        backend: req.backend,
        communication_method: req.communication_method,
        host: req.host,
        port: req.port,
        memory: req.memory,
        cpus: req.cpus,
      }
    }
  }

  #[allow(unused)]
  #[derive(Deserialize, Serialize, Debug)]
  pub struct RegisteredFunction {
    pub function_name: String,
    pub function_version: String,
    pub image_name: String,
    pub memory: i64,
    pub cpus: u32,
    pub parallel_invokes: u32
  }

  impl RegisteredFunction {
    pub fn from(req: json::RegisterFunction) -> Self {
      RegisteredFunction {
        function_name: req.function_name,
        function_version: req.function_version,
        image_name: req.image_name,
        memory: req.memory,
        cpus: req.cpus,
        parallel_invokes: req.parallel_invokes
      }
    }
  }

  pub enum WorkerStatus {
    HEALTHY,
    UNHEALTHY,
    OFFLINE
  }
}
