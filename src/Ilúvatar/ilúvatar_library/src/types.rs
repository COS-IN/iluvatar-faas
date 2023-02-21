use clap::ValueEnum;
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
    /// to only be used in testing
    const INVALID = 0b10000000000000000000000000000000;
  }
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
/// To be used with CLI args
pub enum ComputeEnum {
  CPU,
  GPU,
}
/// To turn Compute back into a string-serializable format for hashmaps
impl TryInto<ComputeEnum> for &Compute {
  fn try_into(self) -> Result<ComputeEnum, Self::Error> {
    if self.contains(Compute::CPU) {
      Ok(ComputeEnum::CPU)
    } else if self.contains(Compute::GPU) {
      Ok(ComputeEnum::GPU)
    } else {
      anyhow::bail!("Cannot convert Compute '{:?}' to enum", self)
    }
  }
  type Error = anyhow::Error;
}
impl IntoIterator for Compute {
  type Item = Compute;
  type IntoIter = std::vec::IntoIter<Self::Item>;

  /// Get a list of the individual compute components in the [Compute] bitmap
  fn into_iter(self) -> Self::IntoIter {
    let mut r = vec![];
    for supported_compute in vec![Compute::CPU, Compute::GPU, Compute::FPGA] {
      if self.contains(supported_compute){
        r.push(supported_compute);
      }
    }
    r.into_iter()
  } 
}
impl Into<Compute> for Vec<ComputeEnum> {
  fn into(self) -> Compute {
    let mut r = Compute::empty();
    for i in self.iter() {
      r |= i.into()
    }
    r
  }
}
impl Into<Compute> for &ComputeEnum {
  fn into(self) -> Compute {
    match self {
      ComputeEnum::CPU => Compute::CPU,
      ComputeEnum::GPU => Compute::GPU,
    }
  }
}
impl Into<Compute> for u32 {
  fn into(self) -> Compute {
    Compute::from_bits_truncate(self)
  }
}
impl TryFrom<&String> for Compute {
  fn try_from(value: &String) -> Result<Compute, Self::Error> {
    let mut vec = vec![];
    for slice in value.split("|") {
      vec.push(match ComputeEnum::from_str(slice, true) {
        Ok(c) => c,
        Err(e) => anyhow::bail!(e),
      });
    }
    Ok(vec.into())
  }
  type Error = anyhow::Error;
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// To be used with CLI args
pub enum IsolationEnum {
  CONTAINERD,
  DOCKER,
  // INVALID deliberately not included
}
impl Into<Isolation> for Vec<IsolationEnum> {
  fn into(self) -> Isolation {
    let mut r = Isolation::empty();
    for i in self.iter() {
      r |= i.into();
    }
    r
  }
}
impl Into<Isolation> for &IsolationEnum {
  fn into(self) -> Isolation {
    match self {
      IsolationEnum::CONTAINERD => Isolation::CONTAINERD,
      IsolationEnum::DOCKER => Isolation::DOCKER,
    }
  }
}
impl Into<Isolation> for u32 {
  fn into(self) -> Isolation {
    Isolation::from_bits_truncate(self)
  }
}
impl TryFrom<&String> for Isolation {
  fn try_from(value: &String) -> Result<Isolation, Self::Error> {
    let mut vec = vec![];
    for slice in value.split("|") {
      vec.push(match IsolationEnum::from_str(slice, true) {
        Ok(i) => i,
        Err(e) => anyhow::bail!(e),
      });
    }
    Ok(vec.into())
  }
  type Error = anyhow::Error;
}