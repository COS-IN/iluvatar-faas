pub mod status_service;
pub use status_service::StatusService;

use crate::types::MemSizeMb;

#[derive(Debug)]
/// Load metrics recorded by the local machine
pub struct WorkerStatus {
  /// length of the invoker queue
  pub queue_len: i64,
  /// amount of memory used by containers
  pub used_mem: MemSizeMb,
  /// amount of memory usable by containers, used and unused
  pub total_mem: MemSizeMb,
  /// cpu-time non-kernel: https://www.man7.org/linux/man-pages/man8/vmstat.8.html
  pub cpu_us: i64,
  /// cpu-time kernel: https://www.man7.org/linux/man-pages/man8/vmstat.8.html
  pub cpu_sy: i64,
  /// cpu-time idle: https://www.man7.org/linux/man-pages/man8/vmstat.8.html
  pub cpu_id: i64,
  /// cpu-time waiting: https://www.man7.org/linux/man-pages/man8/vmstat.8.html
  pub cpu_wa: i64,
  /// one minute load average from https://www.man7.org/linux/man-pages/man1/uptime.1.html
  /// NOT NORMALIZED by number of cores
  pub load_avg_1minute: f64,
  /// The number of (logical) CPU cores on the system
  /// used to normalize the load average value
  pub num_system_cores: u32
}