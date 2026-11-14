use std::fs;
use std::mem::size_of;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use libbpf_rs::{MapCore, MapFlags, MapHandle};

use crate::bpf_intf::consts_MAX_CPU_UTIL;
use crate::CoreSchedMetrics;

pub const CORE_METRICS_MAP_PATH: &str = "/sys/fs/bpf/finesched_core_metrics";
const CPU_SYSFS_ROOT: &str = "/sys/devices/system/cpu";
const SCHED_EXT_STATE_PATH: &str = "/sys/kernel/sched_ext/state";

/// A single, best-effort view of metrics for one logical CPU.
///
/// `power_watts` and `pelt_utilization` are `None` until FineSched has a true
/// per-core source for them. RAPL package power is deliberately not divided
/// across CPUs because that would create a misleading scheduling signal.
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
    pub pelt_utilization: Option<f64>,
}

/// Reads FineSched BPF metrics and Linux CPU-frequency sysfs values.
///
/// Construct this after the FineSched scheduler has started and pinned its
/// `coreMetrics` map. All methods take a logical CPU ID.
#[derive(Debug)]
pub struct CoreMetricsReader {
    metrics_map: MapHandle,
    cpu_sysfs_root: PathBuf,
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
        Ok(Self {
            metrics_map,
            cpu_sysfs_root: cpu_sysfs_root.into(),
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

    /// Per-core power is not currently available. RAPL exposes package/core
    /// domains, not individual logical CPUs, so callers receive `None` rather
    /// than an invented package-power share.
    pub fn power_watts(&self, cpu: u32) -> Result<Option<f64>> {
        self.ensure_cpu_exists(cpu)?;
        Ok(None)
    }

    /// Kernel PELT rq utilization is not currently exported to FineSched.
    /// The BPF utilization methods above are available as a distinct signal.
    pub fn pelt_utilization(&self, cpu: u32) -> Result<Option<f64>> {
        self.ensure_cpu_exists(cpu)?;
        Ok(None)
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
            pelt_utilization: self.pelt_utilization(cpu)?,
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
