use crate::worker_api;

#[allow(unused)]
pub struct RCPWorkerAPI {
  address: String, 
  port: i32
}

impl RCPWorkerAPI {
  pub fn new(address: String, port: i32) -> RCPWorkerAPI {
    RCPWorkerAPI {
      address,
      port
    }
  }
}

impl worker_api::WorkerAPI for RCPWorkerAPI {
  fn ping(&self) -> Result<(), &str> {
    println!("Pong!");
    Ok(())
  }
  fn invoke(&self) -> Result<&str, &str> {
    unimplemented!();
  }
  fn register(&self) -> Result<(), &str> {
    unimplemented!();
  }
  fn status(&self) -> Result<&str, &str> {
    unimplemented!();
  }
}