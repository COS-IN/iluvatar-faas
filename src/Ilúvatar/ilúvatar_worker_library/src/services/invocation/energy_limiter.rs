use std::sync::Arc;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::energy::energy_logging::EnergyLogger;
use crate::worker_api::config::InvocationConfig;

pub struct EnergyLimiter {
    powcap: f64,
    energy: Arc<EnergyLogger>,
}

impl EnergyLimiter {
    pub fn boxed(config: &Arc<InvocationConfig>, energy: Arc<EnergyLogger>) -> Arc<Self> {
        Arc::new(Self {
            powcap: config.power_cap.map_or(0.0, |v| v),
            energy,
        })
    }

    fn powcap_enabled(&self) -> bool {
        return self.powcap > 1.0;
    }

    fn get_energy(&self, cmap: &Arc<CharacteristicsMap>, fqdn: &String, power: f64) -> f64 {
        let exec_time = cmap.get_exec_time(fqdn);
        let j = exec_time * power;
        return j;
    }

    pub fn ok_run_fn(&self, cmap: &Arc<CharacteristicsMap>, fname: &String) -> bool {
        if ! self.powcap_enabled() {
            return true;
        }

        let (_t, p) = self.energy.get_latest_reading();
        let j = self.get_energy(cmap, fname, p);
        let s = 10.0; //seconds
        let j_predicted = p*s;
        let j_cap = self.powcap*s;

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