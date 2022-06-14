use iluvatar_lib::utils::Port;

#[derive(Debug)]
#[allow(unused)]
pub struct Container {
  pub container_id: String,
  pub task: Task,
  pub port: Port,
  pub address: String,
  pub invoke_uri: String,
  pub base_uri: String,
  // TODO: reference to function somehow
}

#[derive(Debug)]
#[allow(unused)]
pub struct Task {
  pub pid: u32,
  pub container_id: Option<String>,
  pub running: bool,
}

#[derive(Debug)]
#[allow(unused)]
pub struct RegisteredFunction {
  pub function_name: String,
  pub function_version: String,
  pub image_name: String,
  pub memory: u32,
  pub cpus: u32,
  pub snapshot_base: String,
}