use anyhow::Error;
use bitflags::bitflags;
use clap::builder::PossibleValue;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;

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
    type Error = Error;

    fn try_into(self) -> Result<CommunicationMethod, Self::Error> {
        match self {
            0 => Ok(CommunicationMethod::RPC),
            1 => Ok(CommunicationMethod::SIMULATION),
            _ => anyhow::bail!("Cannot parse {:?} for CommunicationMethod", self),
        }
    }
}

#[derive(serde::Deserialize, serde::Serialize, Default, Debug, Copy, Clone)]
/// The server type running inside the container
pub enum ContainerServer {
    #[default]
    HTTP = 0,
    UnixSocket = 1,
}
impl FromStr for ContainerServer {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "http" => Ok(ContainerServer::HTTP),
            "unix-socket" => Ok(ContainerServer::UnixSocket),
            "socket" => Ok(ContainerServer::UnixSocket),
            "unix" => Ok(ContainerServer::UnixSocket),
            _ => anyhow::bail!("Cannot parse {:?} for ContainerServer", s),
        }
    }
}
impl Display for ContainerServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContainerServer::HTTP => f.write_fmt(format_args!("HTTP"))?,
            ContainerServer::UnixSocket => f.write_fmt(format_args!("UnixSocket"))?,
        }
        Ok(())
    }
}
impl TryFrom<u32> for ContainerServer {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ContainerServer::HTTP),
            1 => Ok(ContainerServer::UnixSocket),
            _ => anyhow::bail!("Cannot parse {} for ContainerServer", value),
        }
    }
}

bitflags! {
  /// The compute methods that a function supports. XXX Rename this ComputeDevice
  /// Having each one of these means it can run on each compute independently.
  /// e.g. having `CPU|GPU` will run fine in a CPU-only container, or one with an attached GPU
  #[derive(serde::Serialize, Debug,PartialEq,Copy,Clone,Eq,Hash)]
  #[serde(transparent)]
    pub struct Compute: u32 {
    const CPU = 0b00000001;
    const GPU = 0b00000010;
    const FPGA = 0b00000100;
  }

  #[derive(serde::Serialize,Debug,PartialEq,Copy,Clone,Eq,Hash)]
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

struct StrVisitor;
impl serde::de::Visitor<'_> for StrVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        write!(formatter, "a string or str")
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value)
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value.to_owned())
    }
}

impl Default for Compute {
    fn default() -> Self {
        Self::CPU
    }
}
impl clap::ValueEnum for Compute {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::CPU, Self::GPU, Self::FPGA]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(match *self {
            Self::CPU => PossibleValue::new("CPU"),
            Self::GPU => PossibleValue::new("GPU"),
            Self::FPGA => PossibleValue::new("FPGA"),
            _ => return None,
        })
    }
}
impl From<Vec<Compute>> for Compute {
    fn from(vec: Vec<Compute>) -> Self {
        vec.iter().fold(Compute::empty(), |acc, x| acc | *x)
    }
}
impl From<u32> for Compute {
    fn from(i: u32) -> Self {
        Compute::from_bits_truncate(i)
    }
}
impl TryFrom<&String> for Compute {
    type Error = Error;
    fn try_from(value: &String) -> Result<Compute, Self::Error> {
        let mut r = Compute::empty();
        for slice in value.split('|') {
            r |= match slice.to_lowercase().as_str() {
                "cpu" => Compute::CPU,
                "gpu" => Compute::GPU,
                "fpga" => Compute::FPGA,
                _ => anyhow::bail!("Cannot parse {:?} for Compute", slice),
            };
        }
        Ok(r)
    }
}
impl Display for Compute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.into_iter().peekable();
        while let Some(i) = iter.next() {
            match i {
                Compute::CPU => f.write_fmt(format_args!("CPU"))?,
                Compute::GPU => f.write_fmt(format_args!("GPU"))?,
                Compute::FPGA => f.write_fmt(format_args!("FPGA"))?,
                // won't reach as we're iterating over each flag
                _ => return Err(std::fmt::Error {}),
            };
            if iter.peek().is_some() {
                f.write_str("|")?;
            }
        }
        Ok(())
    }
}
impl<'de> serde::Deserialize<'de> for Compute {
    fn deserialize<D>(deserializer: D) -> Result<Compute, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = StrVisitor {};
        let s = deserializer.deserialize_str(v)?;
        match (&s).try_into() {
            Ok(c) => Ok(c),
            Err(e) => Err(serde::de::Error::custom(e)),
        }
    }
}

impl From<u32> for Isolation {
    fn from(i: u32) -> Self {
        Isolation::from_bits_truncate(i)
    }
}
impl TryFrom<&String> for Isolation {
    type Error = Error;
    fn try_from(value: &String) -> Result<Isolation, Self::Error> {
        let mut r = Isolation::empty();
        for slice in value.split('|') {
            r |= match slice.to_lowercase().as_str() {
                "containerd" => Isolation::CONTAINERD,
                "docker" => Isolation::DOCKER,
                "invalid" => Isolation::INVALID,
                _ => anyhow::bail!("Cannot parse {:?} for Isolation", slice),
            };
        }
        Ok(r)
    }
}
impl Default for Isolation {
    fn default() -> Self {
        Self::CONTAINERD
    }
}
impl clap::ValueEnum for Isolation {
    fn value_variants<'a>() -> &'a [Self] {
        &[Isolation::CONTAINERD, Isolation::DOCKER]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(match *self {
            Self::CONTAINERD => PossibleValue::new("CONTAINERD"),
            Self::DOCKER => PossibleValue::new("DOCKER"),
            _ => return None,
        })
    }
}
impl From<Vec<Isolation>> for Isolation {
    fn from(vec: Vec<Isolation>) -> Self {
        vec.iter().fold(Isolation::empty(), |acc, x| acc | *x)
    }
}
impl Display for Isolation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.into_iter().peekable();
        while let Some(i) = iter.next() {
            match i {
                Isolation::CONTAINERD => f.write_fmt(format_args!("CONTAINERD"))?,
                Isolation::DOCKER => f.write_fmt(format_args!("DOCKER"))?,
                // won't reach as we're iterating over each flag
                _ => return Err(std::fmt::Error {}),
            };
            if iter.peek().is_some() {
                f.write_str("|")?;
            }
        }
        Ok(())
    }
}
impl<'de> serde::Deserialize<'de> for Isolation {
    fn deserialize<D>(deserializer: D) -> Result<Isolation, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = StrVisitor {};
        let s = deserializer.deserialize_str(v)?;
        match (&s).try_into() {
            Ok(c) => Ok(c),
            Err(e) => Err(serde::de::Error::custom(e)),
        }
    }
}

/// A collection of function timing data, allowing for polymorphic functions that run on several computes
pub type ResourceTimings = std::collections::HashMap<Compute, FunctionInvocationTimings>;

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
        assert_eq!("CPU|GPU", format!("{}", Compute::CPU | Compute::GPU));
        assert_eq!("CPU", format!("{}", Compute::CPU));
    }

    #[test]
    fn isolation_format() {
        assert_eq!(
            "CONTAINERD|DOCKER",
            format!("{}", Isolation::CONTAINERD | Isolation::DOCKER)
        );
        assert_eq!("DOCKER", format!("{}", Isolation::DOCKER));
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
