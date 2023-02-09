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
// impl Into<Isolation> for IsolationEnum {
//   fn into(self) -> Isolation {
//     match self {
//       IsolationEnum::CONTAINERD => Isolation::CONTAINERD,
//       IsolationEnum::DOCKER => Isolation::DOCKER,
//     }
//   }
// }
