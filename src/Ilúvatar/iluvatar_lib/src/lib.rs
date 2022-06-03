pub mod worker_api {
    pub trait WorkerAPI {
    fn ping(&self) -> Result<(), &str>;
    fn invoke(&self) -> Result<&str, &str>;
    fn register(&self) -> Result<(), &str>;
    fn status(&self) -> Result<&str, &str>;
  }
}

pub mod rpc;