use serde::{Deserialize, Serialize};

use crate::utils::port_utils::Port;

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
  pub function_version: String,
  pub memory: i64,
  pub cpu: u32,
  pub image_name: String,
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
  pub host: String,
  pub port: Port,
  pub memory: i64,
  pub cpus: u32,
}
