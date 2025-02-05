use anyhow::Error;
use bitflags::bitflags;
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

/// Type to allow returning an owned object along with an error from a function
pub type ResultErrorVal<T, D, E = Error> = Result<T, (E, D)>;
/// Wrapper to make [ResultErrorVal] from a value and error
#[inline(always)]
pub fn err_val<T, D>(error: Error, value: D) -> ResultErrorVal<T, D> {
    Err((error, value))
}

pub type MemSizeMb = i64;

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// How to communicate with a worker.
/// Generally not needed to know, but live = RPC, otherwise simulation
pub enum CommunicationMethod {
    RPC = 0,
    SIMULATION = 1,
}
impl TryInto<CommunicationMethod> for u32 {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<CommunicationMethod, Self::Error> {
        match self {
            0 => Ok(CommunicationMethod::RPC),
            1 => Ok(CommunicationMethod::SIMULATION),
            _ => anyhow::bail!("Cannot parse {:?} for CommunicationMethod", self),
        }
    }
}

bitflags! {
  #[derive(serde::Deserialize, serde::Serialize,Debug,PartialEq,Copy,Clone,Eq,Hash)]
  #[serde(transparent)]
  /// The compute methods that a function supports. XXX Rename this ComputeDevice
  /// Having each one of these means it can run on each compute independently.
  /// e.g. having `CPU|GPU` will run fine in a CPU-only container, or one with an attached GPU
  pub struct Compute: u32 {
    const CPU = 0b00000001;
    const GPU = 0b00000010;
    const FPGA = 0b00000100;
  }
  #[derive(serde::Deserialize, serde::Serialize,Debug,PartialEq,Copy,Clone,Eq,Hash)]
  #[serde(transparent)]
  /// The isolation mechanism the function supports.
  /// e.g. our Docker images are OCI-compliant and can be run by Docker or Containerd, so could specify `CONTAINERD|DOCKER` or `CONTAINERD`
  pub struct Isolation: u32 {
    const CONTAINERD = 0b00000001;
    const DOCKER = 0b00000010;
    /// to only be used in testing
    const INVALID = 0b10000000000000000000000000000000;
  }
}

#[derive(
    clap::ValueEnum, std::fmt::Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash,
)]
/// To be used with CLI args and other places where it needs to be converted into a string
/// The compute methods that a function supports
/// Having each one of these means it can run on each compute independently.
/// I.E. having [ComputeEnum::cpu]|[ComputeEnum::gpu] will run fine in a CPU-only container, or one with an attached GPU
#[allow(non_camel_case_types)]
pub enum ComputeEnum {
    cpu,
    gpu,
    fpga,
}
/// To turn Compute back into a string-serializable format for hashmaps
impl TryInto<ComputeEnum> for Compute {
    fn try_into(self) -> Result<ComputeEnum, Self::Error> {
        (&self).try_into()
    }
    type Error = anyhow::Error;
}
/// To turn Compute back into a string-serializable format for hashmaps
impl TryInto<ComputeEnum> for &Compute {
    fn try_into(self) -> Result<ComputeEnum, Self::Error> {
        if self.contains(Compute::CPU) {
            Ok(ComputeEnum::cpu)
        } else if self.contains(Compute::GPU) {
            Ok(ComputeEnum::gpu)
        } else if self.contains(Compute::FPGA) {
            Ok(ComputeEnum::fpga)
        } else {
            anyhow::bail!("Cannot convert Compute '{:?}' to enum", self)
        }
    }
    type Error = anyhow::Error;
}
// impl IntoIterator for Compute {
//     type Item = Compute;
//     type IntoIter = std::vec::IntoIter<Self::Item>;
//
//     /// Get a list of the individual compute components in the [Compute] bitmap
//     fn into_iter(self) -> Self::IntoIter {
//         vec![Compute::CPU, Compute::GPU, Compute::FPGA]
//             .into_iter()
//             .filter(|x| self.contains(*x))
//             .collect::<Vec<Compute>>()
//             .into_iter()
//     }
// }
impl From<Vec<ComputeEnum>> for Compute {
    fn from(i: Vec<ComputeEnum>) -> Self {
        let mut r = Compute::empty();
        for x in i.iter() {
            r |= x.into()
        }
        r
    }
}
impl From<&ComputeEnum> for Compute {
    fn from(i: &ComputeEnum) -> Self {
        match i {
            ComputeEnum::cpu => Compute::CPU,
            ComputeEnum::gpu => Compute::GPU,
            ComputeEnum::fpga => Compute::FPGA,
        }
    }
}
impl From<u32> for Compute {
    fn from(i: u32) -> Self {
        Compute::from_bits_truncate(i)
    }
}
impl TryFrom<&String> for Compute {
    fn try_from(value: &String) -> Result<Compute, Self::Error> {
        let mut vec = vec![];
        for slice in value.split('|') {
            vec.push(match ComputeEnum::from_str(slice, true) {
                Ok(c) => c,
                Err(e) => anyhow::bail!(e),
            });
        }
        Ok(vec.into())
    }
    type Error = anyhow::Error;
}
impl std::fmt::Display for Compute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.into_iter().peekable();
        while let Some(i) = iter.next() {
            match TryInto::<ComputeEnum>::try_into(i) {
                Ok(c) => f.write_fmt(format_args!("{:?}", c))?,
                Err(e) => {
                    tracing::error!(error=%e, "Failed to format Compute");
                    return Err(std::fmt::Error);
                },
            };
            if iter.peek().is_some() {
                f.write_str("|")?;
            }
        }
        Ok(())
    }
}

#[derive(clap::ValueEnum, Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
/// To be used with CLI args and other places it needs to be converted into a string
pub enum IsolationEnum {
    CONTAINERD,
    DOCKER,
    // INVALID deliberately not included
}
impl From<Vec<IsolationEnum>> for Isolation {
    fn from(i: Vec<IsolationEnum>) -> Self {
        let mut r = Isolation::empty();
        for x in i.iter() {
            r |= x.into();
        }
        r
    }
}
impl From<&IsolationEnum> for Isolation {
    fn from(i: &IsolationEnum) -> Self {
        match i {
            IsolationEnum::CONTAINERD => Isolation::CONTAINERD,
            IsolationEnum::DOCKER => Isolation::DOCKER,
        }
    }
}
impl From<u32> for Isolation {
    fn from(i: u32) -> Self {
        Isolation::from_bits_truncate(i)
    }
}
impl TryFrom<&String> for Isolation {
    fn try_from(value: &String) -> Result<Isolation, Self::Error> {
        let mut vec = vec![];
        for slice in value.split('|') {
            vec.push(match IsolationEnum::from_str(slice, true) {
                Ok(i) => i,
                Err(e) => anyhow::bail!(e),
            });
        }
        Ok(vec.into())
    }
    type Error = anyhow::Error;
}

/// A collection of function timing data, allowing for polymorphic functions that run on several computes
pub type ResourceTimings = std::collections::HashMap<ComputeEnum, FunctionInvocationTimings>;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// A struct holding the invocation timings of a single function.
/// Broken down into several categories along warm and cold invocations.
pub struct FunctionInvocationTimings {
    /// list of warm execution duration in seconds
    pub warm_results_sec: Vec<f64>,
    /// list of warm overhead times
    pub warm_over_results_us: Vec<f64>,
    /// list of cold execution duration in seconds
    pub cold_results_sec: Vec<f64>,
    /// list of cold overhead times
    pub cold_over_results_us: Vec<f64>,
    /// warm invocation latency time, including communication time from worker
    ///   if targeting worker, recorded by benchmark
    ///   if targeting controller, recorded by controller
    pub warm_worker_duration_us: Vec<u128>,
    /// cold invocation latency time, including communication time from worker
    ///   if targeting worker, recorded by benchmark
    ///   if targeting controller, recorded by controller
    pub cold_worker_duration_us: Vec<u128>,
    /// warm invocation latency time recorded on worker
    pub warm_invoke_duration_us: Vec<u128>,
    /// cold invocation latency time recorded on worker
    pub cold_invoke_duration_us: Vec<u128>,
}
impl Default for FunctionInvocationTimings {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionInvocationTimings {
    pub fn new() -> Self {
        FunctionInvocationTimings {
            warm_results_sec: Vec::new(),
            warm_over_results_us: Vec::new(),
            cold_results_sec: Vec::new(),
            cold_over_results_us: Vec::new(),
            warm_worker_duration_us: Vec::new(),
            cold_worker_duration_us: Vec::new(),
            warm_invoke_duration_us: Vec::new(),
            cold_invoke_duration_us: Vec::new(),
        }
    }
}

#[allow(drop_bounds)]
pub trait DroppableMovableTrait: Drop + Send {}
impl DroppableMovableTrait for tokio::sync::OwnedSemaphorePermit {}
// impl DroppableMovableTrait for Option<tokio::sync::OwnedSemaphorePermit> {}
#[allow(drop_bounds, dyn_drop)]
pub type DroppableToken = Box<dyn DroppableMovableTrait>;
impl DroppableMovableTrait for Vec<DroppableToken> {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    HEALTHY,
    UNHEALTHY,
    OFFLINE,
}

#[cfg(test)]
mod types_tests {
    use super::*;

    #[test]
    fn compute_format() {
        assert_eq!("cpu|gpu", format!("{}", Compute::CPU | Compute::GPU));
        assert_eq!("cpu", format!("{}", Compute::CPU));
    }

    #[test]
    fn compute_iterable() {
        let mut has_cpu = false;
        let mut has_gpu = false;
        for c in (Compute::CPU | Compute::GPU).into_iter() {
            if c == Compute::CPU {
                has_cpu = true;
            }
            if c == Compute::CPU {
                has_gpu = true;
            }
        }
        assert!(has_cpu);
        assert!(has_gpu);
    }
}
