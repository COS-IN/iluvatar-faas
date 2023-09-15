use std::sync::Arc;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::energy::energy_logging::EnergyLogger;
use crate::worker_api::config::InvocationConfig;
use anyhow::Result;

const POWCAP_MIN: f64 = 0.0;
pub struct EnergyLimiter {
    powcap: f64,
    energy: Arc<EnergyLogger>,
    reading_freq_sec: f64,
}
impl EnergyLimiter {
    pub fn boxed(config: &Arc<InvocationConfig>, energy: Arc<EnergyLogger>) -> Result<Arc<Self>> {
        let powcap = config.power_cap.map_or(0.0, |v| v);
        if !energy.readings_enabled() && powcap > POWCAP_MIN {
            anyhow::bail!("'power_cap set but not energy reading source available");
        }
        let reading_freq_sec = energy.get_reading_time_ms() as f64 / 1000.0;
        Ok(Arc::new(Self {
            powcap, energy, reading_freq_sec
        }))
    }

    fn powcap_enabled(&self) -> bool {
        return self.powcap > POWCAP_MIN;
    }

    fn get_energy(&self, cmap: &Arc<CharacteristicsMap>, fqdn: &String, power: f64) -> f64 {
        let exec_time = cmap.get_exec_time(fqdn);
        let j = exec_time * power;
        tracing::debug!("get energy exec_time({}) * power){}) = j({})", exec_time, power, j);
        return j;
    }

    pub fn ok_run_fn(&self, cmap: &Arc<CharacteristicsMap>, fname: &String) -> bool {
        if ! self.powcap_enabled() {
            tracing::debug!(fname=%fname, "power cap disabled");
            return true;
        }
        tracing::debug!(fname=%fname, "power cap enabled");

        let (_t, p) = self.energy.get_latest_reading();
        let j = self.get_energy(cmap, fname, p);
        let j_predicted = p * self.reading_freq_sec;
        let j_cap = self.powcap * self.reading_freq_sec;

        tracing::debug!(fname=%fname, "power cap check j_predicted(p({}) * freq({})) + j({})  <= j_cap({})", p, self.reading_freq_sec, j, j_cap);
        return j_predicted + j <= j_cap;
    }

    pub fn add_pending(&self, _j:f64) {
        todo!();
    }

    pub fn sub_pending(&self, _j:f64) {
        todo!();
    }

    pub fn add_outgoing(&self, _j:f64) {
        todo!();
    }

    pub fn total_outgoing(&self) {
        todo!();
    }
}