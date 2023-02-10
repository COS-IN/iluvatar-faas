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
    const CONTAINERD = 0b00000001;
    const DOCKER = 0b00000010;
    const SIMULATION = 0b00000100;
  }
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// To be used with CLI args
pub enum ComputeEnum {
  CPU,
  GPU,
}
impl Into<Compute> for Vec<ComputeEnum> {
  fn into(self) -> Compute {
    let mut r = Compute::empty();
    for i in self.iter() {
      match i {
        ComputeEnum::CPU => r |= Compute::CPU,
        ComputeEnum::GPU => r |= Compute::GPU,
      }
    }
    r
  }
}
impl Into<Compute> for u32 {
  fn into(self) -> Compute {
    Compute::from_bits_truncate(self)
  }
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// To be used with CLI args
pub enum IsolationEnum {
  CONTAINERD,
  DOCKER,
  // Simulation deliberately not included
}
impl Into<Isolation> for Vec<IsolationEnum> {
  fn into(self) -> Isolation {
    let mut r = Isolation::empty();
    for i in self.iter() {
      match i {
        IsolationEnum::CONTAINERD => r |= Isolation::CONTAINERD,
        IsolationEnum::DOCKER => r |= Isolation::DOCKER,
      }
    }
    r
  }
}
impl Into<Isolation> for u32 {
  fn into(self) -> Isolation {
    Isolation::from_bits_truncate(self)
  }
}
