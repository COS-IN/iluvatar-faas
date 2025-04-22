use crate::services::resources::gpu::ProtectedGpuRef;
use crate::services::{
    containers::structs::{ContainerState, ContainerT, ParsedResult},
    registration::RegisteredFunction,
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::clock::{now, ContainerTimeFormatter, GlobalClock};
use iluvatar_library::types::ResultErrorVal;
use iluvatar_library::{
    bail_error, error_value,
    transaction::TransactionId,
    types::{Compute, DroppableToken, Isolation, MemSizeMb},
};
use parking_lot::{Mutex, RwLock};
use rand::{rng, seq::index::sample};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::debug;

#[allow(unused)]
#[derive(iluvatar_library::ToAny)]
pub struct SimulatorContainer {
    pub container_id: String,
    pub fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    pub last_used: RwLock<Instant>,
    /// number of invocations a container has performed
    pub invocations: Mutex<u32>,
    pub state: Mutex<ContainerState>,
    current_memory: RwLock<MemSizeMb>,
    compute: Compute,
    iso: Isolation,
    device: RwLock<Option<GPU>>,
    dev_mem_usage: RwLock<(MemSizeMb, bool)>,
    drop_on_remove: Mutex<Vec<DroppableToken>>,
    history_data: Option<Vec<f64>>,
    time_formatter: ContainerTimeFormatter,
}
impl SimulatorContainer {
    pub fn new(
        tid: &TransactionId,
        cid: String,
        fqdn: &str,
        reg: &Arc<RegisteredFunction>,
        state: ContainerState,
        iso: Isolation,
        compute: Compute,
        device: Option<GPU>,
    ) -> ResultErrorVal<Self, Option<GPU>> {
        let formatter = match ContainerTimeFormatter::new(tid) {
            Ok(f) => f,
            Err(e) => error_value!("{:?}", e, device),
        };
        Ok(SimulatorContainer {
            container_id: cid,
            fqdn: fqdn.to_owned(),
            function: reg.clone(),
            last_used: RwLock::new(now()),
            invocations: Mutex::new(0),
            state: Mutex::new(state),
            current_memory: RwLock::new(reg.memory),
            history_data: reg.historical_runtime_data_sec.get(&compute).cloned(),
            compute,
            iso,
            device: RwLock::new(device),
            dev_mem_usage: RwLock::new((0, true)),
            drop_on_remove: Mutex::new(vec![]),
            time_formatter: formatter,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
/// struct used to control "invocation" pattern of simulated function
pub struct SimInvokeData {
    #[serde(deserialize_with = "deserialize_u64")]
    pub warm_dur_ms: u64,
    #[serde(deserialize_with = "deserialize_u64")]
    pub cold_dur_ms: u64,
}
/// struct used to control "invocation" pattern of simulated function
pub type SimulationInvocation = HashMap<Compute, SimInvokeData>;

struct DeserializeFromU64OrString;
impl serde::de::Visitor<'_> for DeserializeFromU64OrString {
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
    #[tracing::instrument(skip(self, json_args), fields(tid=tid, fqdn=%self.fqdn), name="SimulatorContainer::invoke")]
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        // just sleep for a while based on data from json args
        let data = match serde_json::from_str::<SimulationInvocation>(json_args) {
            Ok(d) => d,
            Err(e) => {
                bail_error!(tid=tid, error=%e, args=%json_args, "Unable to deserialize run time information")
            },
        };

        let was_cold = self.state() == ContainerState::Cold;
        let duration_sec = match data.get(&self.compute) {
            None => anyhow::bail!(
                "No matching compute in passed simulation invocation data for container with type '{}'",
                self.compute
            ),
            Some(data) => match was_cold {
                true => data.cold_dur_ms as f64 / 1000.0 * 1.2,
                _ => match &self.history_data {
                    Some(history_data) => {
                        let idx = sample(&mut rng(), history_data.len(), 1);
                        history_data[idx.index(0)]
                    },
                    None => data.warm_dur_ms as f64 / 1000.0 * 1.2,
                },
            }, // 1.2 multiplication from concurrency degradation on CPU
        };
        *self.invocations.lock() += 1;
        let timer = ContainerTimeFormatter::new(tid)?;

        // "networking" overhead
        // TODO: HTTP vs Socket container time diff
        tokio::time::sleep(Duration::from_micros(1400)).await;

        let start = timer.now();
        let start_f64 = start.unix_timestamp_nanos() as f64 / 1_000_000_000.0;
        let code_dur = Duration::from_secs_f64(duration_sec);
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
        let user_result = serde_json::to_string(&ret)?;
        let result = ParsedResult {
            user_result: Some(user_result),
            user_error: None,
            start: timer.format_time(start)?,
            end: timer.format_time(end)?,
            was_cold,
            duration_sec: code_dur.as_secs_f64(),
            gpu_allocation_mb: 0,
        };
        Ok((result, code_dur))
    }

    fn touch(&self) {
        let mut lock = self.last_used.write();
        *lock = now();
    }

    fn container_id(&self) -> &String {
        &self.container_id
    }

    fn last_used(&self) -> Instant {
        *self.last_used.read()
    }

    fn invocations(&self) -> u32 {
        *self.invocations.lock()
    }

    fn get_curr_mem_usage(&self) -> MemSizeMb {
        *self.current_memory.read()
    }

    fn set_curr_mem_usage(&self, usage: MemSizeMb) {
        *self.current_memory.write() = usage;
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
    fn device_resource(&self) -> ProtectedGpuRef<'_> {
        self.device.read()
    }
    fn set_device_memory(&self, size: MemSizeMb) {
        let mut lck = self.dev_mem_usage.write();
        *lck = (size, lck.1);
    }
    fn device_memory(&self) -> (MemSizeMb, bool) {
        *self.dev_mem_usage.read()
    }
    fn revoke_device(&self) -> Option<GPU> {
        *self.dev_mem_usage.write() = (0, false);
        self.device.write().take()
    }
    async fn move_to_device(&self, _tid: &TransactionId) -> Result<()> {
        let mut lck = self.dev_mem_usage.write();
        *lck = (lck.0, true);
        Ok(())
    }
    async fn move_from_device(&self, _tid: &TransactionId) -> Result<()> {
        let mut lck = self.dev_mem_usage.write();
        *lck = (lck.0, false);
        Ok(())
    }
    fn add_drop_on_remove(&self, item: DroppableToken, tid: &TransactionId) {
        debug!(tid=tid, container_id=%self.container_id(), "Adding token to drop on remove");
        self.drop_on_remove.lock().push(item);
    }
    fn remove_drop(&self, tid: &TransactionId) {
        let mut lck = self.drop_on_remove.lock();
        let to_drop = std::mem::take(&mut *lck);
        debug!(tid=tid, container_id=%self.container_id(), num_tokens=to_drop.len(), "Dropping tokens");
        for i in to_drop.into_iter() {
            drop(i);
        }
    }
}

#[cfg(test)]
mod sim_struct_tests {
    use super::*;
    use iluvatar_library::transaction::gen_tid;
    use more_asserts::assert_ge;

    fn reg(data: Option<Vec<f64>>) -> Arc<RegisteredFunction> {
        let mut map = HashMap::new();
        if let Some(data) = data {
            map.insert(Compute::CPU, data);
        }
        Arc::new(RegisteredFunction {
            function_name: "fq".to_string(),
            function_version: "dn".to_string(),
            fqdn: "fqdn".to_string(),
            image_name: "none".to_string(),
            memory: 1024,
            cpus: 1,
            parallel_invokes: 1,
            historical_runtime_data_sec: map,
            ..Default::default()
        })
    }

    fn invoke_data(compute: Compute, cold: u64, warm: u64) -> Result<String> {
        Ok(serde_json::to_string(&SimulationInvocation::from([(
            compute,
            SimInvokeData {
                warm_dur_ms: warm,
                cold_dur_ms: cold,
            },
        )]))?)
    }

    #[test]
    fn no_data_empty_ecdf() {
        let reg = reg(None);
        let cont = SimulatorContainer::new(
            &gen_tid(),
            "cid".to_owned(),
            "fqdn",
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )
        .unwrap();
        assert_eq!(cont.history_data, None);
    }

    #[test]
    fn data_filled_ecdf() {
        let reg = reg(Some(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]));
        let cont = SimulatorContainer::new(
            &gen_tid(),
            "cid".to_owned(),
            "fqdn",
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )
        .unwrap();
        assert_ne!(cont.history_data, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_args_fails() {
        let reg = reg(None);
        let cont = SimulatorContainer::new(
            &gen_tid(),
            "cid".to_owned(),
            "fqdn",
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )
        .unwrap();
        cont.invoke("", &"tid".to_owned())
            .await
            .expect_err("No simulation args should error");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_no_data_uses_passed_info() {
        let reg = reg(None);
        let cold_time = 100;
        let cont = SimulatorContainer::new(
            &gen_tid(),
            "cid".to_owned(),
            "fqdn",
            &reg,
            ContainerState::Cold,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )
        .unwrap();
        let data = invoke_data(Compute::CPU, cold_time, 10).unwrap();
        let start = now();
        let (_result, _) = cont.invoke(&data, &"tid".to_owned()).await.unwrap();
        let dur = start.elapsed();
        assert_ge!(dur, Duration::from_millis(cold_time));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_no_data_uses_passed_info() {
        let reg = reg(None);
        let warm_time = 10;
        let cont = SimulatorContainer::new(
            &gen_tid(),
            "cid".to_owned(),
            "fqdn",
            &reg,
            ContainerState::Warm,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )
        .unwrap();
        let data = invoke_data(Compute::CPU, 100, warm_time).unwrap();
        let start = now();
        let (_result, _) = cont.invoke(&data, &"tid".to_owned()).await.unwrap();
        let dur = start.elapsed();
        assert_ge!(dur, Duration::from_millis(warm_time));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_data_uses_sample() {
        let reg = reg(Some(vec![1.0, 1.1, 1.2, 1.5, 2.0]));
        let cont = SimulatorContainer::new(
            &gen_tid(),
            "cid".to_owned(),
            "fqdn",
            &reg,
            ContainerState::Warm,
            Isolation::CONTAINERD,
            Compute::CPU,
            None,
        )
        .unwrap();
        let data = invoke_data(Compute::CPU, 100, 5).unwrap();
        let start = now();
        let (_result, _) = cont.invoke(&data, &"tid".to_owned()).await.unwrap();
        let dur = start.elapsed();
        assert_ge!(dur, Duration::from_secs_f64(1.0));
    }
}
