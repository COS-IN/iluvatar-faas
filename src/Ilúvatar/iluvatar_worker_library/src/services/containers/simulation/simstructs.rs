use crate::services::{
    containers::structs::{ContainerState, ContainerT, ContainerTimeFormatter, ParsedResult},
    registration::RegisteredFunction,
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, DroppableToken, Isolation, MemSizeMb},
};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::debug;

#[allow(unused)]
pub struct SimulatorContainer {
    pub container_id: String,
    pub fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    pub last_used: RwLock<SystemTime>,
    /// number of invocations a container has performed
    pub invocations: Mutex<u32>,
    pub state: Mutex<ContainerState>,
    current_memory: Mutex<MemSizeMb>,
    compute: Compute,
    iso: Isolation,
    device: Option<Arc<GPU>>,
    drop_on_remove: Mutex<Vec<DroppableToken>>,
}
impl SimulatorContainer {
    pub fn new(
        cid: String,
        fqdn: &str,
        reg: &Arc<RegisteredFunction>,
        state: ContainerState,
        iso: Isolation,
        compute: Compute,
        device: Option<Arc<GPU>>,
    ) -> Self {
        SimulatorContainer {
            container_id: cid,
            fqdn: fqdn.to_owned(),
            function: reg.clone(),
            last_used: RwLock::new(SystemTime::now()),
            invocations: Mutex::new(0),
            state: Mutex::new(state),
            current_memory: Mutex::new(reg.memory),
            compute,
            iso,
            device,
            drop_on_remove: Mutex::new(vec![]),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(unused)]
/// struct used to control "invocation" pattern of simulated function
pub struct SimulationInvocation {
    #[serde(deserialize_with = "deserialize_u64")]
    pub warm_dur_ms: u64,
    #[serde(deserialize_with = "deserialize_u64")]
    pub cold_dur_ms: u64,
}
struct DeserializeFromU64OrString;
impl<'de> serde::de::Visitor<'de> for DeserializeFromU64OrString {
    type Value = u64;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer or a string")
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(v)
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        v.parse::<u64>().map_err(serde::de::Error::custom)
    }
}
/// custom deserializer because the value can be a string or a direct u64
fn deserialize_u64<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    deserializer.deserialize_any(DeserializeFromU64OrString)
}

#[derive(Serialize, Deserialize)]
/// This is the output from the python functions
pub struct SimulationResult {
    pub body: Body,
}
#[derive(Serialize, Deserialize)]
pub struct Body {
    pub cold: bool,
    pub start: f64,
    pub end: f64,
    /// python runtime latency in seconds
    pub latency: f64,
}

#[tonic::async_trait]
impl ContainerT for SimulatorContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=%tid, fqdn=%self.fqdn), name="SimulatorContainer::invoke")]
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        // just sleep for a while based on data from json args
        let data = match serde_json::from_str::<SimulationInvocation>(json_args) {
            Ok(d) => d,
            Err(e) => {
                bail_error!(tid=%tid, error=%e, args=%json_args, "Unable to deserialize run time information")
            }
        };

        let (duration_us, was_cold) = match self.state() {
            ContainerState::Cold => (data.cold_dur_ms * 1000, true),
            _ => (data.warm_dur_ms * 1000, false),
        };
        *self.invocations.lock() += 1;
        let timer = ContainerTimeFormatter::new(tid)?;

        // "networking" overhead
        tokio::time::sleep(Duration::from_micros(1400)).await;

        let start = timer.now();
        let start_f64 = start.unix_timestamp_nanos() as f64 / 1_000_000_000.0;
        // 0.3 multplication from concurrency degredation on CPU
        let code_dur = Duration::from_micros(duration_us).mul_f64(1.2);
        tokio::time::sleep(code_dur).await;
        let end = timer.now();
        let end_f64 = end.unix_timestamp_nanos() as f64 / 1_000_000_000.0;
        let latency = end_f64 - start_f64;
        let ret = SimulationResult {
            body: Body {
                cold: was_cold,
                start: start_f64,
                end: end_f64,
                latency,
            },
        };
        let d = code_dur;
        let user_result = serde_json::to_string(&ret)?;
        let result = ParsedResult {
            user_result: Some(user_result),
            user_error: None,
            start: timer.format_time(start)?,
            end: timer.format_time(end)?,
            was_cold,
            duration_sec: code_dur.as_secs_f64(),
            gpu_allocation_mb: None,
        };
        Ok((result, d))
    }

    fn touch(&self) {
        let mut lock = self.last_used.write();
        *lock = SystemTime::now();
    }

    fn container_id(&self) -> &String {
        &self.container_id
    }

    fn last_used(&self) -> SystemTime {
        *self.last_used.read()
    }

    fn invocations(&self) -> u32 {
        *self.invocations.lock()
    }

    fn get_curr_mem_usage(&self) -> MemSizeMb {
        *self.current_memory.lock()
    }

    fn set_curr_mem_usage(&self, usage: MemSizeMb) {
        *self.current_memory.lock() = usage;
    }

    fn function(&self) -> Arc<RegisteredFunction> {
        self.function.clone()
    }

    fn fqdn(&self) -> &String {
        &self.fqdn
    }

    fn is_healthy(&self) -> bool {
        self.state() != ContainerState::Unhealthy
    }
    fn mark_unhealthy(&self) {
        self.set_state(ContainerState::Unhealthy);
    }
    fn state(&self) -> ContainerState {
        *self.state.lock()
    }
    fn set_state(&self, state: ContainerState) {
        *self.state.lock() = state;
    }
    fn container_type(&self) -> Isolation {
        self.iso
    }
    fn compute_type(&self) -> Compute {
        self.compute
    }
    fn device_resource(&self) -> &Option<Arc<GPU>> {
        &self.device
    }
    fn add_drop_on_remove(&self, item: DroppableToken, tid: &TransactionId) {
        debug!(tid=%tid, container_id=%self.container_id(), "Adding token to drop on remove");
        self.drop_on_remove.lock().push(item);
    }
    fn remove_drop(&self, tid: &TransactionId) {
        let mut lck = self.drop_on_remove.lock();
        let to_drop = std::mem::take(&mut *lck);
        debug!(tid=%tid, container_id=%self.container_id(), num_tokens=to_drop.len(), "Dropping tokens");
        for i in to_drop.into_iter() {
            drop(i);
        }
    }
}

impl crate::services::containers::structs::ToAny for SimulatorContainer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
