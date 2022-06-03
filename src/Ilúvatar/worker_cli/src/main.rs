extern crate iluvatar_lib;
use iluvatar_lib::worker_api::Pingable;

fn main() {
  println!("I'm a CLI!");
  let api = iluvatar_lib::worker_api::WorkerAPI { };
  api.ping();
}

pub mod cli {

}
