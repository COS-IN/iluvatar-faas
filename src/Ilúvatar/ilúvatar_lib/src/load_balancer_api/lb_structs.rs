use serde::{Deserialize, Serialize};

#[allow(unused)]
#[derive(Deserialize, Serialize, Debug)]
pub struct Invoke {
  function_name: String,
  function_version: String,
  args: Vec<String>
}

#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub struct InvokeResult {
  json_result: String,
}

#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub struct InvokeAsyncResult {
  lookup_cookie: String,
}

#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub struct InvokeAsyncLookup {
  lookup_cookie: String,
}

#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub struct Prewarm {
  function_name: String,
  function_version: String,
  memory: i64,
  cpu: u32,
  image_name: String,
}

#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub struct RegisterFunction {
  function_name: String,
  function_version: String,
  image_name: String,
  memory: i64,
  cpus: u32,
  parallel_invokes: u32
}

#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub struct RegisterWorker {
  name: String,
  backend: String,
  base_url: String,
  memory: i64,
  cpus: u32,
}
