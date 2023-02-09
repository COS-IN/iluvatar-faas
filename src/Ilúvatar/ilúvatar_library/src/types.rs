use serde::{Serialize, Deserialize};
use bitflags::bitflags;

pub type MemSizeMb = i64;

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CommunicationMethod {
  RPC,
  SIMULATION
}

bitflags! {
  #[derive(serde::Deserialize, serde::Serialize)]
  pub struct Compute: u32 {
    const CPU = 0b00000001;
    const GPU = 0b00000010;
    const FPGA = 0b00000100;
  }
  #[derive(serde::Deserialize, serde::Serialize)]
  pub struct Isolation: u32 {
    const SIMULATION = 0b00000000;
    const CONTAINERD = 0b00000001;
    const DOCKER = 0b00000010;
  }
}
