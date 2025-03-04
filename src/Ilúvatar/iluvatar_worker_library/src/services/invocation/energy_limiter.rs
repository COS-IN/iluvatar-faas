use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::energy::energy_logging::EnergyLogger;
use std::sync::Arc;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub enum PowerCapVersion {
    /// Check if power usage is under limit to allow invocation
    V0,
    /// Predict energy usage of invocation in addition to checking current power usage
    V1,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
/// Internal knobs for how the [crate::services::invocation::EnergyLimiter] works
pub struct EnergyCapConfig {
    /// Maximum power usage before pausing invocations to wait for power drop
    /// Disabled if not present or < 1.0
    pub power_cap: f64,
    /// The version to use, defaults to [PowerCapVersion::V0]
    pub power_cap_version: PowerCapVersion,
}

const POWCAP_MIN: f64 = 0.0;
pub struct EnergyLimiter {
    powcap: f64,
    version: PowerCapVersion,
    energy: Arc<EnergyLogger>,
    reading_freq_sec: f64,
}
impl EnergyLimiter {
    pub fn boxed(config: &Option<Arc<EnergyCapConfig>>, energy: Arc<EnergyLogger>) -> Result<Arc<Self>> {
        let mut powcap = 0.0;
        if let Some(c) = config {
            powcap = c.power_cap;
        }
        let mut version = PowerCapVersion::V0;
        if let Some(c) = config {
            version = c.power_cap_version.clone();
        }
        if !energy.readings_enabled() && powcap > POWCAP_MIN {
            anyhow::bail!("'power_cap set but not energy reading source available");
        }
        let reading_freq_sec = energy.get_reading_time_ms() as f64 / 1000.0;
        Ok(Arc::new(Self {
            energy,
            reading_freq_sec,
            powcap,
            version,
        }))
    }

    fn powcap_enabled(&self) -> bool {
        return self.powcap > POWCAP_MIN;
    }

    fn get_energy(&self, cmap: &WorkerCharMap, fqdn: &str, _power: f64) -> f64 {
        let exec_time = cmap.get_avg(fqdn, Chars::CpuExecTime);
        let power_2 = 2.0;
        let j = exec_time * power_2;
        // tracing::debug!("get energy exec_time({}) * power({}) = j({})", exec_time, power_2, j);
        return j;
    }

    pub fn ok_run_fn(&self, cmap: &WorkerCharMap, fname: &str) -> bool {
        if !self.powcap_enabled() {
            // tracing::debug!(fname=%fname, "power cap disabled");
            return true;
        }
        // tracing::debug!(fname=%fname, "power cap enabled");
        match self.version {
            PowerCapVersion::V0 => {
                let (_t, p) = self.energy.get_latest_reading();
                let j_predicted = p * self.reading_freq_sec;
                let j_cap = self.powcap * self.reading_freq_sec;

                tracing::debug!(fname=%fname, "power cap check j_cap({}) < j_cap({})", j_predicted, j_cap);
                return j_predicted < j_cap;
            },
            PowerCapVersion::V1 => {
                let (_t, p) = self.energy.get_latest_reading();
                let j = self.get_energy(cmap, fname, p);
                let j_predicted = p * self.reading_freq_sec;
                let j_cap = self.powcap * self.reading_freq_sec;

                tracing::debug!(fname=%fname, "power cap check j_predicted(p({}) * freq({})) + j({})  <= j_cap({})", p, self.reading_freq_sec, j, j_cap);
                return j_predicted + j <= j_cap;
            },
        }
    }

    pub fn add_pending(&self, _j: f64) {
        todo!();
    }

    pub fn sub_pending(&self, _j: f64) {
        todo!();
    }

    pub fn add_outgoing(&self, _j: f64) {
        todo!();
    }

    pub fn total_outgoing(&self) {
        todo!();
    }
}
