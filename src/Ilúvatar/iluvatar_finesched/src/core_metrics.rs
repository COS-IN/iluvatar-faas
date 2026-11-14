use std::collections::{HashMap, HashSet};
use std::fs;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use libbpf_rs::{MapCore, MapFlags, MapHandle};

use crate::bpf_intf::consts_MAX_CPU_UTIL;
use crate::CoreSchedMetrics;

pub const CORE_METRICS_MAP_PATH: &str = "/sys/fs/bpf/finesched_core_metrics";
const CPU_SYSFS_ROOT: &str = "/sys/devices/system/cpu";
const SCHED_EXT_STATE_PATH: &str = "/sys/kernel/sched_ext/state";
const PELT_LIKE_HALF_LIFE_NS: u64 = 128_000_000;
const AMD_MSR_PWR_UNIT: u64 = 0xC001_0299;
const AMD_MSR_CORE_ENERGY: u64 = 0xC001_029A;
const AMD_ENERGY_UNIT_MASK: u64 = 0x1F00;
const CORE_POWER_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

/// A single, best-effort view of metrics for one logical CPU.
///
/// `power_watts` is the physical-core power estimate, shared by SMT siblings.
/// `pelt_like_utilization_percent` is FineSched's own
/// scheduler-duty-cycle average, not the kernel's PELT value.
#[derive(Debug, Clone, PartialEq)]
pub struct CoreMetricSnapshot {
    pub cpu: u32,
    pub local_dsq_len: u64,
    pub domain_dsq_len: u64,
    pub dsq_len: u64,
    pub runnable_load: u64,
    pub utilization_percent: f64,
    pub average_utilization_percent: f64,
    pub frequency_mhz: f64,
    pub power_watts: Option<f64>,
    pub pelt_like_utilization_percent: f64,
}

/// Reads FineSched BPF metrics and Linux CPU-frequency sysfs values.
///
/// Construct this after the FineSched scheduler has started and pinned its
/// `coreMetrics` map. All methods take a logical CPU ID.
#[derive(Debug)]
pub struct CoreMetricsReader {
    metrics_map: MapHandle,
    cpu_sysfs_root: PathBuf,
    power_sampler: Option<Mutex<CorePowerSampler>>,
}

impl CoreMetricsReader {
    pub fn open() -> Result<Self> {
        let state = fs::read_to_string(SCHED_EXT_STATE_PATH)
            .with_context(|| format!("failed to read {SCHED_EXT_STATE_PATH}"))?;
        if state.trim() != "enabled" {
            bail!("FineSched metrics are unavailable while sched_ext is {}", state.trim());
        }
        Self::open_at(CORE_METRICS_MAP_PATH, CPU_SYSFS_ROOT)
    }

    fn open_at(map_path: impl AsRef<Path>, cpu_sysfs_root: impl Into<PathBuf>) -> Result<Self> {
        let map_path = map_path.as_ref();
        let metrics_map = MapHandle::from_pinned_path(map_path)
            .with_context(|| format!("failed to open FineSched core metrics map {}", map_path.display()))?;
        let cpu_sysfs_root = cpu_sysfs_root.into();
        let power_sampler = match CorePowerSampler::open(&cpu_sysfs_root) {
            Ok(sampler) => sampler.map(Mutex::new),
            Err(error) => {
                tracing::warn!(error = %error, "[finesched][power] per-core AMD energy counters unavailable");
                None
            },
        };
        Ok(Self {
            metrics_map,
            cpu_sysfs_root,
            power_sampler,
        })
    }

    /// Total queued work visible to a CPU: its local/per-CPU queues plus both
    /// shared queues belonging to its FineSched domain.
    pub fn dsq_len(&self, cpu: u32) -> Result<u64> {
        let metrics = self.bpf_metrics(cpu)?;
        Ok(metrics.local_dsq_len + metrics.domain_dsq_len)
    }

    pub fn local_dsq_len(&self, cpu: u32) -> Result<u64> {
        Ok(self.bpf_metrics(cpu)?.local_dsq_len)
    }

    /// This value is shared by all CPUs assigned to the same FineSched domain.
    pub fn domain_dsq_len(&self, cpu: u32) -> Result<u64> {
        Ok(self.bpf_metrics(cpu)?.domain_dsq_len)
    }

    /// Event-driven idle-time utilization in percent. FineSched exports this
    /// internally on a 0..1000 scale.
    pub fn utilization_percent(&self, cpu: u32) -> Result<f64> {
        Ok(Self::util_to_percent(self.bpf_metrics(cpu)?.util))
    }

    /// EWMA of `utilization_percent` using FineSched's existing 0.75/0.25 EWMA.
    pub fn average_utilization_percent(&self, cpu: u32) -> Result<f64> {
        Ok(Self::util_to_percent(self.bpf_metrics(cpu)?.avg_util))
    }

    /// Current CPU frequency reported by cpufreq, preferring the hardware
    /// reading and falling back to the governor's scaling value.
    pub fn frequency_mhz(&self, cpu: u32) -> Result<f64> {
        self.ensure_cpu_exists(cpu)?;
        let cpufreq = self.cpu_path(cpu).join("cpufreq");
        let hardware = cpufreq.join("cpuinfo_cur_freq");
        let scaling = cpufreq.join("scaling_cur_freq");

        let khz = match Self::read_u64(&hardware) {
            Ok(value) => value,
            Err(hardware_error) => Self::read_u64(&scaling).with_context(|| {
                format!("neither hardware ({hardware_error}) nor scaling frequency was readable for CPU {cpu}")
            })?,
        };
        Ok(khz as f64 / 1000.0)
    }

    /// Number of queued tasks plus one when the CPU was non-idle at the last
    /// FineSched callback. This is a runnable-load signal, not Linux loadavg.
    pub fn runnable_load(&self, cpu: u32) -> Result<u64> {
        Ok(self.bpf_metrics(cpu)?.runnable)
    }

    /// Power estimated from the AMD physical-core energy counter. SMT sibling
    /// logical CPUs map to the same physical-core reading.
    pub fn power_watts(&self, cpu: u32) -> Result<Option<f64>> {
        self.ensure_cpu_exists(cpu)?;
        let Some(power) = self.power_sampler.as_ref() else {
            return Ok(None);
        };
        let mut power = power.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        power.sample();
        Ok(power.for_cpus(&[cpu]).0.first().map(|(_, watts)| *watts))
    }

    /// Return each physical core's latest power estimate and their sum for a
    /// set of logical CPUs. SMT siblings are deduplicated. The domain sum is
    /// omitted until every represented physical core has a valid interval.
    pub fn power_for_cpus(&self, cpus: &[u32]) -> Result<(Vec<(u32, f64)>, Option<f64>)> {
        for cpu in cpus {
            self.ensure_cpu_exists(*cpu)?;
        }
        let Some(power) = self.power_sampler.as_ref() else {
            return Ok((Vec::new(), None));
        };
        let mut power = power.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        power.sample();
        let (per_core, complete) = power.for_cpus(cpus);
        let total = complete.then(|| per_core.iter().map(|(_, watts)| watts).sum());
        Ok((per_core, total))
    }

    /// Refresh physical-core power counters if the 100 ms sample interval has
    /// elapsed. Repeated CHBL decisions can use the cached values below.
    pub fn sample_power(&self) {
        if let Some(power) = self.power_sampler.as_ref() {
            power.lock().unwrap_or_else(std::sync::PoisonError::into_inner).sample();
        }
    }

    /// Return the most recently sampled power sum without triggering MSR reads.
    pub fn cached_power_for_cpus(&self, cpus: &[u32]) -> Result<Option<f64>> {
        for cpu in cpus {
            self.ensure_cpu_exists(*cpu)?;
        }
        let Some(power) = self.power_sampler.as_ref() else {
            return Ok(None);
        };
        let power = power.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let (per_core, complete) = power.for_cpus(cpus);
        Ok(complete.then(|| per_core.iter().map(|(_, watts)| watts).sum()))
    }

    /// Return FineSched's PELT-like CPU duty-cycle average in percent. The
    /// estimate uses a 128 ms half-life and projects forward from its last BPF
    /// update using the CPU's current running state.
    pub fn pelt_like_utilization_percent(&self, cpu: u32) -> Result<f64> {
        let metrics = self.bpf_metrics(cpu)?;
        Self::project_pelt_like_utilization(&metrics)
    }

    pub fn snapshot(&self, cpu: u32) -> Result<CoreMetricSnapshot> {
        let metrics = self.bpf_metrics(cpu)?;
        Ok(CoreMetricSnapshot {
            cpu,
            local_dsq_len: metrics.local_dsq_len,
            domain_dsq_len: metrics.domain_dsq_len,
            dsq_len: metrics.local_dsq_len + metrics.domain_dsq_len,
            runnable_load: metrics.runnable,
            utilization_percent: Self::util_to_percent(metrics.util),
            average_utilization_percent: Self::util_to_percent(metrics.avg_util),
            frequency_mhz: self.frequency_mhz(cpu)?,
            power_watts: self.power_watts(cpu)?,
            pelt_like_utilization_percent: Self::project_pelt_like_utilization(&metrics)?,
        })
    }

    fn bpf_metrics(&self, cpu: u32) -> Result<CoreSchedMetrics> {
        self.ensure_cpu_exists(cpu)?;
        let key = cpu.to_ne_bytes();
        let bytes = self
            .metrics_map
            .lookup(&key, MapFlags::ANY)
            .with_context(|| format!("failed to read FineSched metrics for CPU {cpu}"))?
            .with_context(|| format!("FineSched metrics do not contain CPU {cpu}"))?;

        if bytes.len() != size_of::<CoreSchedMetrics>() {
            bail!(
                "unexpected FineSched core metric size for CPU {cpu}: got {}, expected {}",
                bytes.len(),
                size_of::<CoreSchedMetrics>()
            );
        }

        // BPF map values are byte buffers and are not guaranteed to have the
        // alignment of the generated repr(C) type.
        Ok(unsafe { std::ptr::read_unaligned(bytes.as_ptr().cast::<CoreSchedMetrics>()) })
    }

    fn util_to_percent(util: u64) -> f64 {
        util as f64 * 100.0 / consts_MAX_CPU_UTIL as f64
    }

    fn project_pelt_like_utilization(metrics: &CoreSchedMetrics) -> Result<f64> {
        let now = Self::monotonic_time_ns()?;
        if metrics.pelt_like_last_update_ns == 0 || now <= metrics.pelt_like_last_update_ns {
            return Ok(Self::util_to_percent(metrics.pelt_like_util_avg));
        }

        let elapsed = now - metrics.pelt_like_last_update_ns;
        let whole_halflives = elapsed / PELT_LIKE_HALF_LIFE_NS;
        let scale = consts_MAX_CPU_UTIL as u64;
        let factor = if whole_halflives >= 10 {
            0
        } else {
            let mut factor = scale >> whole_halflives;
            let next_factor = factor >> 1;
            let remainder = elapsed % PELT_LIKE_HALF_LIFE_NS;
            factor -= ((factor - next_factor) * remainder) / PELT_LIKE_HALF_LIFE_NS;
            factor
        };
        let signal = if metrics.pelt_like_running != 0 { scale } else { 0 };
        let projected = (metrics.pelt_like_util_avg * factor + signal * (scale - factor)) / scale;
        Ok(Self::util_to_percent(projected))
    }

    fn monotonic_time_ns() -> Result<u64> {
        let mut time: libc::timespec = unsafe { std::mem::zeroed() };
        let result = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) };
        if result != 0 {
            return Err(std::io::Error::last_os_error()).context("failed to read monotonic clock");
        }
        Ok(time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64)
    }

    fn cpu_path(&self, cpu: u32) -> PathBuf {
        self.cpu_sysfs_root.join(format!("cpu{cpu}"))
    }

    fn ensure_cpu_exists(&self, cpu: u32) -> Result<()> {
        if !self.cpu_path(cpu).exists() {
            bail!(
                "logical CPU {cpu} does not exist under {}",
                self.cpu_sysfs_root.display()
            );
        }
        Ok(())
    }

    fn read_u64(path: &Path) -> Result<u64> {
        let raw = fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
        raw.trim()
            .parse::<u64>()
            .with_context(|| format!("failed to parse {} as an integer", path.display()))
    }
}

#[derive(Debug)]
struct CorePowerCounter {
    representative_cpu: u32,
    file: File,
    joules_per_tick: f64,
    previous: Option<(u32, Instant)>,
    watts: Option<f64>,
}

#[derive(Debug)]
struct CorePowerSampler {
    cpu_to_core: HashMap<u32, usize>,
    counters: Vec<CorePowerCounter>,
    last_sample: Option<Instant>,
}

impl CorePowerSampler {
    fn open(cpu_sysfs_root: &Path) -> Result<Option<Self>> {
        let cpuinfo = fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        if !cpuinfo.lines().any(|line| {
            line.split_once(':')
                .is_some_and(|(key, value)| key.trim() == "vendor_id" && value.trim() == "AuthenticAMD")
        }) {
            return Ok(None);
        }

        let mut topology: HashMap<(u32, u32), Vec<u32>> = HashMap::new();
        for entry in fs::read_dir(cpu_sysfs_root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(cpu) = name.strip_prefix("cpu").and_then(|id| id.parse::<u32>().ok()) else {
                continue;
            };
            let topology_path = entry.path().join("topology");
            let package = match Self::read_topology_id(&topology_path.join("physical_package_id")) {
                Ok(id) => id,
                Err(_) => continue,
            };
            let core = match Self::read_topology_id(&topology_path.join("core_id")) {
                Ok(id) => id,
                Err(_) => continue,
            };
            topology.entry((package, core)).or_default().push(cpu);
        }

        let mut cores: Vec<_> = topology.into_values().collect();
        for cpus in &mut cores {
            cpus.sort_unstable();
        }
        cores.sort_by_key(|cpus| cpus[0]);

        let mut counters = Vec::with_capacity(cores.len());
        let mut cpu_to_core = HashMap::new();
        for cpus in cores {
            let representative_cpu = cpus[0];
            let Ok(mut file) = File::open(format!("/dev/cpu/{representative_cpu}/msr")) else {
                continue;
            };
            let Ok(units) = Self::read_msr(&mut file, AMD_MSR_PWR_UNIT) else {
                continue;
            };
            if units == 0 || Self::read_msr(&mut file, AMD_MSR_CORE_ENERGY).is_err() {
                continue;
            }
            let energy_exp = ((units & AMD_ENERGY_UNIT_MASK) >> 8) as i32;
            let index = counters.len();
            for cpu in cpus {
                cpu_to_core.insert(cpu, index);
            }
            counters.push(CorePowerCounter {
                representative_cpu,
                file,
                joules_per_tick: 2.0_f64.powi(-energy_exp),
                previous: None,
                watts: None,
            });
        }
        if counters.is_empty() {
            tracing::warn!("[finesched][power] AMD core-energy MSRs could not be opened");
            return Ok(None);
        }
        Ok(Some(Self {
            cpu_to_core,
            counters,
            last_sample: None,
        }))
    }

    fn read_topology_id(path: &Path) -> Result<u32> {
        Ok(fs::read_to_string(path)?.trim().parse()?)
    }

    fn read_msr(file: &mut File, offset: u64) -> Result<u64> {
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = [0u8; size_of::<u64>()];
        file.read_exact(&mut buffer)?;
        Ok(u64::from_le_bytes(buffer))
    }

    fn sample(&mut self) {
        let now = Instant::now();
        if self
            .last_sample
            .is_some_and(|last| now.duration_since(last) < CORE_POWER_SAMPLE_INTERVAL)
        {
            return;
        }
        self.last_sample = Some(now);
        for counter in &mut self.counters {
            let Ok(raw) = Self::read_msr(&mut counter.file, AMD_MSR_CORE_ENERGY) else {
                continue;
            };
            let raw = raw as u32;
            if let Some((previous_raw, previous_time)) = counter.previous {
                let elapsed = now.duration_since(previous_time).as_secs_f64();
                if elapsed > 0.0 {
                    let ticks = raw.wrapping_sub(previous_raw);
                    counter.watts = Some(ticks as f64 * counter.joules_per_tick / elapsed);
                    counter.previous = Some((raw, now));
                }
            } else {
                counter.previous = Some((raw, now));
            }
        }
    }

    fn for_cpus(&self, cpus: &[u32]) -> (Vec<(u32, f64)>, bool) {
        let mut indices = HashSet::new();
        for cpu in cpus {
            if let Some(index) = self.cpu_to_core.get(cpu) {
                indices.insert(*index);
            }
        }
        let mut selected: Vec<_> = indices.into_iter().collect();
        selected.sort_unstable();
        let all_cpus_mapped = cpus.iter().all(|cpu| self.cpu_to_core.contains_key(cpu));
        let expected_core_count = selected.len();
        let per_core: Vec<(u32, f64)> = selected
            .into_iter()
            .filter_map(|index| {
                self.counters[index]
                    .watts
                    .map(|watts| (self.counters[index].representative_cpu, watts))
            })
            .collect();
        let complete = all_cpus_mapped && expected_core_count > 0 && per_core.len() == expected_core_count;
        (per_core, complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_finesched_utilization_scale_to_percent() {
        assert_eq!(CoreMetricsReader::util_to_percent(0), 0.0);
        assert_eq!(CoreMetricsReader::util_to_percent(500), 50.0);
        assert_eq!(CoreMetricsReader::util_to_percent(1000), 100.0);
    }
}
