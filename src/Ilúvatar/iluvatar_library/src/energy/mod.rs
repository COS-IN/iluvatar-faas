mod cpu_freq;
pub mod energy_logging;
pub mod ipmi;
pub mod perf;
pub mod process_pct;
pub mod rapl;
pub mod tegrastats;

#[derive(Debug, serde::Serialize, serde::Deserialize, clap::Parser)]
#[clap(author, version, about)]
/// Energy monitoring configuring
pub struct EnergyConfig {
    /// Log RAPL energy consumption.
    /// If 0 then RAPL is disabled.
    #[clap(long, action)]
    pub rapl_freq_ms: Option<u64>,

    /// Log instantaneous power usage from IPMI.
    /// If 0 then IPMI is disabled.
    #[clap(long, action)]
    pub ipmi_freq_ms: Option<u64>,

    /// Log energy usage as monitored via `perf`.
    /// If 0 then perf is disabled.
    /// perf may not be properly killed on worker shutdown, it can be killed manually and externally.
    /// It is also hard to guarantee that perf will be removed, as the mode of the main process exiting can vary.
    /// Executing `kill -9 $(ps -ax | grep perf | awk '"'"'{print $1}'"'"' )` on the host should work.
    #[clap(long, action)]
    pub perf_freq_ms: Option<u64>,

    /// Log energy usage as monitored via `tegrastats`
    /// If 0 then tegra is disabled
    /// Currently tegra is not killed on worker shutdown, it must be killed manually and externally.
    /// It is also hard to guarantee that tegra will be removed, as the mode of the main process exiting can vary.
    /// Executing `kill -9 $(ps -ax | grep tegra | awk '"'"'{print $1}'"'"' )` on the host should work.
    #[clap(long, action)]
    pub tegra_freq_ms: Option<u64>,

    /// Log instantaneous cpu utilization of this process.
    /// If 0 then logging is disabled.
    #[clap(long, action)]
    pub process_freq_ms: Option<u64>,

    /// Time between logging CPU kernel frequencies, e.g. `/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq`.
    /// If 0 then logging is disabled.
    #[clap(long, action)]
    pub kernel_cpu_frequencies_freq_ms: Option<u64>,

    /// Time between logging CPU hardware frequencies, e.g. `/sys/devices/system/cpu/cpu0/cpufreq/cpuinfo_cur_freq`.
    /// If 0 then logging is disabled.
    #[clap(long, action)]
    pub hardware_cpu_frequencies_freq_ms: Option<u64>,

    /// File path containing the IPMI password
    #[clap(long)]
    pub ipmi_pass_file: Option<String>,

    /// IP Address for the local IPMI endpoint
    #[clap(long)]
    pub ipmi_ip_addr: Option<String>,

    /// Folder to log energy metrics to
    #[clap(long)]
    pub log_folder: String,
}

impl EnergyConfig {
    /// True if Some and non-zero
    fn enabled(item: &Option<u64>) -> bool {
        match item {
            None | Some(0) => false,
            Some(_) => true,
        }
    }
    pub fn perf_enabled(&self) -> bool {
        Self::enabled(&self.perf_freq_ms)
    }
    pub fn tegra_enabled(&self) -> bool {
        Self::enabled(&self.tegra_freq_ms)
    }
    pub fn rapl_enabled(&self) -> bool {
        Self::enabled(&self.rapl_freq_ms)
    }
    pub fn ipmi_enabled(&self) -> bool {
        Self::enabled(&self.ipmi_freq_ms)
    }
    pub fn process_enabled(&self) -> bool {
        Self::enabled(&self.process_freq_ms)
    }
    pub fn cpu_freqs_enabled(&self) -> bool {
        Self::enabled(&self.kernel_cpu_frequencies_freq_ms) || Self::enabled(&self.hardware_cpu_frequencies_freq_ms)
    }
}
