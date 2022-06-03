pub mod worker_api {
  pub struct WorkerAPI {

  }

  pub trait Pingable {
    fn ping(&self);
  }

  impl Pingable for WorkerAPI {
    fn ping(&self) {
      println!("Pong!");
    }
  }
}
