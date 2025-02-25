use super::EnergyConfig;
use crate::bail_error;
use crate::clock::{get_global_clock, now, Clock};
use crate::threading::os_thread;
use crate::transaction::{TransactionId, WORKER_ENERGY_LOGGER_TID};
use anyhow::{anyhow, Result};
use parking_lot::{Mutex, RwLock};
use std::fs::{read_to_string, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, error, trace, warn};

const RAPL_PTH: &str = "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj";
const MAX_ENERGY_PTH: &str = "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj";

/// Basic interactions with (RAPL)<https://lwn.net/Articles/545745/>
pub struct RAPL {
    max_uj: u128,
}
impl RAPL {
    pub fn new() -> Result<Self> {
        Ok(RAPL {
            max_uj: RAPL::max_uj()?,
        })
    }

    pub fn record(&self) -> Result<RAPLQuery> {
        Ok(RAPLQuery {
            start: now(),
            start_uj: RAPL::get_uj()?,
        })
    }

    /// Return the elapsed time and used uj between the two queries
    ///   right must have happened before left, or it will error.
    pub fn difference(&self, left: &RAPLQuery, right: &RAPLQuery, _tid: &TransactionId) -> Result<(u128, u128)> {
        let elapsed = left.start.duration_since(right.start).as_micros();
        let uj: u128;
        if left.start_uj < right.start_uj {
            uj = left.start_uj + (self.max_uj - right.start_uj);
            debug!(
                "going around the energy horn {uj} = {} + ({} - {})",
                left.start_uj, self.max_uj, right.start_uj
            );
        } else {
            uj = left.start_uj - right.start_uj;
        }
        Ok((elapsed, uj))
    }

    fn get_uj() -> Result<u128> {
        RAPL::read_uj(RAPL_PTH)
    }
    fn max_uj() -> Result<u128> {
        RAPL::read_uj(MAX_ENERGY_PTH)
    }

    fn read_uj(pth: &str) -> Result<u128> {
        Ok(read_to_string(pth)?.strip_suffix('\n').unwrap().parse::<u128>()?)
    }
}

pub struct RAPLQuery {
    pub start: Instant,
    pub start_uj: u128,
}

const MSR_RAPL_POWER_UNIT: u64 = 0x606;
// const MSR_PKG_RAPL_POWER_LIMIT: u64 =	0x610;
const MSR_PKG_ENERGY_STATUS: u64 = 0x611;
// const MSR_PKG_PERF_STATUS: u64 = 0x613;
// const MSR_PKG_POWER_INFO: u64 =	0x614;

// /* PP0 RAPL Domain */
// const MSR_PP0_POWER_LIMIT: u64 = 0x638;
// const MSR_PP0_ENERGY_STATUS: u64 = 0x639;
// const MSR_PP0_POLICY: u64 = 0x63A;
// const MSR_PP0_PERF_STATUS: u64 = 0x63B;

// /* PP1 RAPL Domain, may reflect to uncore devices */
// const MSR_PP1_POWER_LIMIT: u64 = 0x640;
// const MSR_PP1_ENERGY_STATUS: u64 = 0x641;
// const MSR_PP1_POLICY: u64 = 0x642;

// /* DRAM RAPL Domain */
// const MSR_DRAM_POWER_LIMIT: u64 = 0x618;
// const MSR_DRAM_ENERGY_STATUS: u64 = 0x619;
// const MSR_DRAM_PERF_STATUS: u64 = 0x61B;
// const MSR_DRAM_POWER_INFO: u64 = 0x61C;

// /* PSYS RAPL Domain */
// const MSR_PLATFORM_ENERGY_STATUS: u64 =	0x64d;

// /* RAPL UNIT BITMASK */
// const POWER_UNIT_OFFSET: u64 =	0;
// const POWER_UNIT_MASK: u64 =		0x0F;

// const ENERGY_UNIT_OFFSET: u64 =	0x08;
// const ENERGY_UNIT_MASK: u64 =	0x1F00;

// const TIME_UNIT_OFFSET: u64 =	0x10;
// const TIME_UNIT_MASK: u64 =		0xF000;

const AMD_MSR_PWR_UNIT: u64 = 0xC0010299;
// const AMD_MSR_CORE_ENERGY: u64 = 0xC001029A;
const AMD_MSR_PACKAGE_ENERGY: u64 = 0xC001029B;

const AMD_TIME_UNIT_MASK: u64 = 0xF0000;
const AMD_ENERGY_UNIT_MASK: u64 = 0x1F00;
const AMD_POWER_UNIT_MASK: u64 = 0xF;

#[allow(unused)]
pub struct RaplMsr {
    open_fds: Vec<File>,
    power_units: Vec<f64>,
    cpu_energy_units: Vec<f64>,
    time_units: Vec<f64>,
    use_intel: bool,
}
impl RaplMsr {
    pub fn new(tid: &TransactionId) -> Result<Self> {
        let procs = num_cpus::get_physical();

        let mut open_fds = vec![];
        let mut power_units = vec![];
        let mut cpu_energy_units = vec![];
        let mut time_units = vec![];
        let intel = RaplMsr::use_intel(tid)?;

        for cpu in 0..procs {
            let mut file = match File::open(format!("/dev/cpu/{}/msr", cpu)) {
                Ok(f) => f,
                // This can happen if the CPU core in question has been disabled
                Err(e) => bail_error!(tid=tid, error=%e, cpu=cpu, "Failed to open MSR for cpu"),
            };
            let (pu, cpu, time) = RaplMsr::read_power_unit(cpu, &mut file, intel, tid)?;
            power_units.push(pu);
            cpu_energy_units.push(cpu);
            time_units.push(time);
            open_fds.push(file);
        }
        Ok(RaplMsr {
            open_fds,
            power_units,
            cpu_energy_units,
            time_units,
            use_intel: intel,
        })
    }

    pub fn total_uj(&mut self, tid: &TransactionId) -> u128 {
        let mut sum: u128 = 0;
        for (cpu, fd) in self.open_fds.iter_mut().enumerate() {
            let offset = match self.use_intel {
                true => MSR_PKG_ENERGY_STATUS,
                false => AMD_MSR_PACKAGE_ENERGY,
            };
            let f = RaplMsr::read_msr(cpu, fd, offset, tid) as f64;
            let adjusted = f * self.cpu_energy_units[cpu];
            trace!("RAPL uj for CPU {}: {} & {}", cpu, f, adjusted);
            sum += adjusted as u128;
        }
        debug!(sum=%sum, "RAPL total");
        sum
    }

    fn read_msr(cpu: usize, fd: &mut File, offset: u64, tid: &TransactionId) -> u64 {
        let mut buffer = [0u8; std::mem::size_of::<u64>()];
        match fd.seek(SeekFrom::Start(offset)) {
            Ok(_) => (),
            Err(e) => {
                warn!(error=%e, tid=tid, cpu=cpu, "Error repositioning MSR file pointer");
                return 0;
            },
        };
        match fd.read_exact(&mut buffer) {
            Ok(_) => (),
            Err(e) => {
                warn!(error=%e, tid=tid, cpu=cpu, "Failed to read MSR register");
                return 0;
            },
        };
        let f = u64::from_le_bytes(buffer);
        trace!(tid = tid, reading = f, offset = offset, "MSR reading");
        f
    }

    fn use_intel(tid: &TransactionId) -> Result<bool> {
        let mut file = match File::open(format!("/dev/cpu/{}/msr", 0)) {
            Ok(f) => f,
            Err(e) => {
                bail_error!(tid=tid, error=%e, cpu=0, "Failed to open msr register for CPU 0 to detect if on Intel machine")
            },
        };
        // will be 0 if the Intel MSR doesn't work
        // In that case we use AMD ones
        Ok(RaplMsr::read_msr(0, &mut file, MSR_RAPL_POWER_UNIT, tid) != 0)
    }

    /// Return the power unit, cpu energy unit, and time unit for RAPL
    fn read_power_unit(cpu: usize, fd: &mut File, use_intel: bool, tid: &TransactionId) -> Result<(f64, f64, f64)> {
        if use_intel {
            let result = RaplMsr::read_msr(cpu, fd, MSR_RAPL_POWER_UNIT, tid);
            if result == 0 {
                anyhow::bail!("An error occurred reading RAPL msr on setup");
            }
            let power_unit = 0.5_f64.powf((result & 0xf) as f64);
            let cpu_energy_unit = 0.5_f64.powf(((result >> 8) & 0x1f) as f64);
            let time_unit = 0.5_f64.powf(((result >> 16) & 0xf) as f64);
            Ok((power_unit, cpu_energy_unit, time_unit))
        } else {
            let result = RaplMsr::read_msr(cpu, fd, AMD_MSR_PWR_UNIT, tid);
            if result == 0 {
                anyhow::bail!("An error occurred reading RAPL msr on setup");
            }
            let power_unit = 0.5_f64.powf(((result & AMD_TIME_UNIT_MASK) >> 16) as f64);
            let cpu_energy_unit = 0.5_f64.powf(((result & AMD_ENERGY_UNIT_MASK) >> 8) as f64);
            let time_unit = 0.5_f64.powf((result & AMD_POWER_UNIT_MASK) as f64);
            Ok((power_unit, cpu_energy_unit, time_unit))
        }
    }
}

pub struct RaplMonitor {
    rapl: Mutex<RaplMsr>,
    _config: Arc<EnergyConfig>,
    _worker_thread: JoinHandle<()>,
    log_file: RwLock<File>,
    timer: Clock,
    latest_reading: RwLock<(i128, i128, i128)>,
}
impl RaplMonitor {
    pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
        let ms = config
            .rapl_freq_ms
            .ok_or_else(|| anyhow!("'rapl_freq_ms' cannot be 0"))?;
        let (handle, tx) = os_thread(
            ms,
            WORKER_ENERGY_LOGGER_TID.clone(),
            Arc::new(RaplMonitor::monitor_energy),
        )?;

        let mut i = RaplMsr::new(tid)?;
        let r = Arc::new(RaplMonitor {
            latest_reading: RwLock::new((0, 0, i.total_uj(tid) as i128)),
            rapl: Mutex::new(i),
            _worker_thread: handle,
            _config: config.clone(),
            timer: get_global_clock(tid)?,
            log_file: RaplMonitor::open_log_file(&config, tid)?,
        });
        r.write_text("timestamp,rapl_uj\n".to_string(), tid);
        tx.send(r.clone())?;
        Ok(r)
    }

    /// Return the latest energy reading in (timestamp_ns, Joules)
    pub fn get_latest_reading(&self) -> (i128, f64) {
        let r = *self.latest_reading.read();
        (r.0, r.1 as f64 / 1_000_000.0)
    }

    /// Reads the different energy sources and writes the current staistics out to the csv file
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self), fields(tid=tid)))]
    fn monitor_energy(&self, tid: &TransactionId) {
        let now = self.timer.now();
        let rapl_uj = self.rapl.lock().total_uj(tid) as i128;
        let mut reading = self.latest_reading.write();
        let rapl_diff = std::cmp::max(0, rapl_uj - reading.2);

        // if rapl_diff > 0 {
        *reading = (now.unix_timestamp_nanos(), rapl_diff, rapl_uj);
        // }
        drop(reading);

        let t = match self.timer.format_time(now) {
            Ok(t) => t,
            Err(e) => {
                error!(error=%e, tid=tid, "Failed to format time");
                return;
            },
        };

        let to_write = format!("{},{}\n", t, rapl_uj);
        self.write_text(to_write, tid);
    }

    fn open_log_file(config: &Arc<EnergyConfig>, tid: &TransactionId) -> Result<RwLock<File>> {
        match File::create(Path::new(&config.log_folder).join("energy-rapl.log")) {
            Ok(f) => Ok(RwLock::new(f)),
            Err(e) => {
                bail_error!(tid=tid, error=%e, "Failed to create RAPL output file")
            },
        }
    }

    fn write_text(&self, text: String, tid: &TransactionId) {
        let mut file = self.log_file.write();
        match file.write_all(text.as_bytes()) {
            Ok(_) => (),
            Err(e) => {
                error!(error=%e, tid=tid, "Failed to write csv result to RAPL file");
            },
        };
    }
}
