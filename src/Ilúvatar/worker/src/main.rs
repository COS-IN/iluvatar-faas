extern crate iluvatar_worker;
use iluvatar_worker::config::Configuration;

fn main() {
  let settings = Configuration::new().unwrap();
  println!("configuration = {:?}", settings);
} 

