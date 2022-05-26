extern crate worker_api;
use worker_api::worker_api::Pingable;

fn main() {
  println!("I'm a CLI!");
  let api = worker_api::worker_api::WorkerAPI { };
  api.ping();
}

pub mod cli {

}
