pub use crate::rpc::ContainerState;
use crate::services::{containers::containermanager::ContainerManager, registration::RegisteredFunction};
use anyhow::Result;
use iluvatar_library::{
    bail_error,
    logging::timezone,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
};
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use time::{
    format_description::{self, FormatItem},
    OffsetDateTime, PrimitiveDateTime, UtcOffset,
};
use tracing::debug;

#[tonic::async_trait]
pub trait ContainerT: ToAny + std::fmt::Debug + Send + Sync {
    /// Invoke the function within the container, passing the json args to it
    async fn invoke(&self, json_args: &String, tid: &TransactionId) -> Result<(ParsedResult, Duration)>;

    /// indicate that the container as been "used" or internal datatsructures should be updated such that it has
    fn touch(&self);
    /// the unique ID for this container
    fn container_id(&self) -> &String;
    /// The time at which the container last served an invocation
    fn last_used(&self) -> SystemTime;
    /// Number of invocations this container has served
    fn invocations(&self) -> u32;
    /// Current memory usage of this container
    fn get_curr_mem_usage(&self) -> MemSizeMb;
    /// Update the memory usage of this container
    fn set_curr_mem_usage(&self, usage: MemSizeMb);
    /// the function this container serves
    fn function(&self) -> Arc<RegisteredFunction>;
    /// the fully qualified domain name of the function
    fn fqdn(&self) -> &String;
    /// true if the container is healthy
    fn is_healthy(&self) -> bool;
    /// set the container as unhealthy
    fn mark_unhealthy(&self);
    /// Get the current state of the container
    fn state(&self) -> ContainerState;
    fn set_state(&self, state: ContainerState);
    fn container_type(&self) -> Isolation;
    fn compute_type(&self) -> Compute;
    /// An optional value, returned with [Some(GPU)] if the container has extra resources
    fn device_resource(&self) -> &Option<Arc<crate::services::resources::gpu::GPU>>;
}

/// Cast a container pointer to a concrete type
/// ```let contd: &ContainerdContainer = cast::<ContainerdContainer>(&container, tid)```
pub fn cast<'a, T>(c: &'a Container, tid: &TransactionId) -> Result<&'a T>
where
    T: ContainerT + ToAny,
{
    match c.as_any().downcast_ref::<T>() {
        Some(i) => Ok(i),
        None => {
            anyhow::bail!(
                "[{}] Failed to cast ContainerT type {} to {:?}",
                tid,
                std::any::type_name::<Container>(),
                std::any::type_name::<T>()
            );
        }
    }
}

/// A trait ContainerT implementers must also implement to enable casting back to the concrete type
pub trait ToAny: 'static {
    fn as_any(&self) -> &dyn std::any::Any;
}
pub type Container = Arc<dyn ContainerT>;

#[derive(Debug, serde::Deserialize)]
pub struct ParsedResult {
    /// The result string from the user execution
    pub user_result: Option<String>,
    /// The error returned by user execution, if an error occured
    pub user_error: Option<String>,
    /// The timestamp when user code started
    pub start: String,
    /// The timestamp when user code ended
    pub end: String,
    /// If the container tracked that the invocation was run for the first time
    pub was_cold: bool,
    /// Duration in seconds for how long the execution took
    /// As recorded by the platform _inside_ the container
    pub duration_sec: f64,
}
impl ParsedResult {
    pub fn parse(from: String, tid: &TransactionId) -> Result<Self> {
        match serde_json::from_str(from.as_str()) {
            Ok(p) => Ok(p),
            Err(e) => {
                bail_error!(error=%e, tid=%tid, value=%from, "Failed to parse json from invocation return")
            }
        }
    }

    pub fn result_string(&self) -> Result<String> {
        match &self.user_result {
            Some(s) => Ok(s.clone()),
            None => match &self.user_error {
                Some(s) => Ok(s.clone()),
                None => anyhow::bail!("ParsedResult had neither a user result or error, somehow!"),
            },
        }
    }
}

/// A struct denoting that the owner has a lock on the container to invoke with
pub struct ContainerLock<'a> {
    pub container: Container,
    container_mrg: &'a ContainerManager,
    transaction_id: &'a TransactionId,
}

impl<'a> ContainerLock<'a> {
    pub fn new(container: Container, container_mrg: &'a ContainerManager, tid: &'a TransactionId) -> Self {
        ContainerLock {
            container,
            container_mrg,
            transaction_id: tid,
        }
    }

    /// Ask the internal container to invoke the function
    /// Returns
    /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
    /// [Duration]: The E2E latency between the worker and the container
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, json_args), fields(tid=%self.transaction_id), name="ContainerLock::invoke"))]
    pub async fn invoke(&self, json_args: &String) -> Result<(ParsedResult, Duration)> {
        self.container.invoke(json_args, self.transaction_id).await
    }
}

/// Automatically release the lock on the container when the lock is dropped
impl<'a> Drop for ContainerLock<'a> {
    fn drop(&mut self) {
        debug!(tid=%self.transaction_id, container_id=%self.container.container_id(), "Dropping container lock");
        self.container_mrg
            .return_container(&self.container, &self.transaction_id);
    }
}

#[derive(Debug)]
/// An container start failed because the system did not have enough memory
pub struct InsufficientMemoryError {
    pub needed: MemSizeMb,
    pub used: MemSizeMb,
    pub available: MemSizeMb,
}
impl std::fmt::Display for InsufficientMemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Not enough memory to launch container")?;
        Ok(())
    }
}
impl std::error::Error for InsufficientMemoryError {}

#[derive(Debug)]
/// An container start failed because the system did not have a free GPU
pub struct InsufficientGPUError {}
impl std::fmt::Display for InsufficientGPUError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "No GPU available to launch container")?;
        Ok(())
    }
}
impl std::error::Error for InsufficientGPUError {}

#[derive(Debug)]
/// An container start failed with a platform error
pub struct ContainerStartupError {
    pub message: String,
}
impl std::fmt::Display for ContainerStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Startup error: {}", self.message)?;
        Ok(())
    }
}
impl std::error::Error for ContainerStartupError {}

/// Parses the times returned from a container into Rust objects
pub struct ContainerTimeFormatter {
    py_tz_formatter: Vec<FormatItem<'static>>,
    py_formatter: Vec<FormatItem<'static>>,
    local_offset: UtcOffset,
}
impl ContainerTimeFormatter {
    pub fn new(tid: &TransactionId) -> Result<Self> {
        let py_tz_formatter =
            format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]:[subsecond]+[offset_hour]")?;
        let py_formatter = format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second]:[subsecond]+")?;
        let now = OffsetDateTime::now_utc();
        let tz_str = timezone(tid)?;
        let time_zone = match tzdb::tz_by_name(&tz_str) {
            Some(t) => t,
            None => anyhow::bail!("parsed local timezone string was invalid: {}", tz_str),
        };
        let tm = match time_zone.find_local_time_type(now.unix_timestamp()) {
            Ok(t) => t,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to find time zone type"),
        };
        let offset = UtcOffset::from_whole_seconds(tm.ut_offset())?;

        Ok(ContainerTimeFormatter {
            py_tz_formatter,
            py_formatter,
            local_offset: offset,
        })
    }

    /// Python format: "%Y-%m-%d %H:%M:%S:%f+%z"
    pub fn parse_python_container_time(&self, date: &str) -> Result<OffsetDateTime> {
        if date
            .chars()
            .last()
            .ok_or_else(|| anyhow::anyhow!("Passed date was empty"))?
            == '+'
        {
            Ok(PrimitiveDateTime::parse(date, &self.py_formatter)?.assume_utc())
        } else {
            Ok(OffsetDateTime::parse(date, &self.py_tz_formatter)?)
        }
    }

    /// The number of nanoseconds since the unix epoch start
    pub fn now(&self) -> OffsetDateTime {
        OffsetDateTime::now_utc().to_offset(self.local_offset)
    }
    /// The number of nanoseconds since the unix epoch start
    /// As a String
    /// Useful for simuation container
    pub fn now_str(&self) -> Result<String> {
        let time = self.now();
        self.format_time(time)
    }

    pub fn format_time(&self, time: OffsetDateTime) -> Result<String> {
        Ok(time.format(&self.py_formatter)?)
    }
}
