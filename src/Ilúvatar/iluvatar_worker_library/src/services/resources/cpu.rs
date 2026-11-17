use super::fineloadbalancing::BuildFineLoadBalancing;
use super::fineloadbalancing::FineLoadBalancing;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::queueing::InvokerCpuQueuePolicy;
use crate::services::invocation::Invoker;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::StatusConfig;
use crate::worker_api::worker_config::FineLoadBalancingConfig;
use crate::worker_api::worker_config::SchedExtMigrationMethod;
use crate::worker_api::worker_config::{CPUResourceConfig, CpuConcurrencyLoadSource, FineschedDomainLoadAggregation};
use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use dashmap::DashSet;
use iluvatar_finesched::SchedGroupID;
use iluvatar_library::char_map::Chars;
use iluvatar_library::char_map::Value;
use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::clock::now;
use iluvatar_library::ring_buff::{RingBuffer, Wireable};
use iluvatar_library::threading::{is_simulation, tokio_thread};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::DroppableMovableTrait;
use iluvatar_library::{bail_error, threading};
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fs::read_to_string;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
  pub static ref CPU_CONCUR_WORKER_TID: TransactionId = "CPUConcurrencyMonitor".to_string();
}

type Callback = Arc<dyn Fn() + Send + Sync>;
const SCHED_EXT_POLICY: libc::c_int = 7;
static NEXT_FINE_SCHED_INVOCATION_ID: AtomicU64 = AtomicU64::new(1);

pub struct DomainCpuPermit {
    _cpu_permit: OwnedSemaphorePermit,
    callback: Callback,
}

impl DomainCpuPermit {
    pub fn new(cpu_permit: OwnedSemaphorePermit, callback: Callback) -> Self {
        DomainCpuPermit {
            _cpu_permit: cpu_permit,
            callback,
        }
    }
}

impl Drop for DomainCpuPermit {
    fn drop(&mut self) {
        (*self.callback)();
    }
}

impl DroppableMovableTrait for DomainCpuPermit {}

/// An invoker that scales concurrency based on a configured load signal.
/// Prioritizes based on container availability.
/// Increases concurrency by 1 every [crate::worker_api::worker_config::ComputeResourceConfig::concurrency_update_check_ms].
/// If system load is above [crate::worker_api::worker_config::ComputeResourceConfig::max_load], then the concurrency is reduced by half the distance to [crate::worker_api::worker_config::ComputeResourceConfig::max_oversubscribe] rounded up.
pub struct CpuResourceTracker {
    _load_thread: Option<tokio::task::JoinHandle<()>>,
    concurrency_semaphore: Option<Arc<Semaphore>>,
    max_concur: u32,
    min_concur: u32,
    current_concur: Mutex<u32>,
    max_load: Option<f64>,
    concurrency_load_source: CpuConcurrencyLoadSource,
    finesched_domain_load_aggregation: FineschedDomainLoadAggregation,
    pub cores: f64,
    load_avg: LoadAvg,
    fineloadbalancing: Option<FineLoadBalancing>,
    sched_ext_cgroups: DashSet<String>,
}

impl CpuResourceTracker {
    pub fn new(
        config: &Arc<CPUResourceConfig>,
        load_avg: LoadAvg,
        tid: &TransactionId,
        cmap: WorkerCharMap,
        fineloadbalancing_config: Option<Arc<FineLoadBalancingConfig>>,
    ) -> Result<Arc<Self>> {
        Self::new_with_core_utilization(
            config,
            load_avg,
            tid,
            cmap,
            fineloadbalancing_config,
            CpuCoreUtilization::new(0),
        )
    }

    pub fn new_with_core_utilization(
        config: &Arc<CPUResourceConfig>,
        load_avg: LoadAvg,
        tid: &TransactionId,
        cmap: WorkerCharMap,
        fineloadbalancing_config: Option<Arc<FineLoadBalancingConfig>>,
        core_utilization: Arc<CpuCoreUtilization>,
    ) -> Result<Arc<Self>> {
        let mut max_concur = 0;
        let available_cores = match config.count {
            0 => num_cpus::get(),
            n => n as usize,
        };
        let sem = match config.count {
            0 => None,
            p => Some(Arc::new(Semaphore::new(p as usize))),
        };

        let (load_handle, load_tx) = match config.concurrency_update_check_ms {
            Some(0) => (None, None),
            Some(check_dur) => {
                let max_load = config
                    .max_load
                    .ok_or_else(|| anyhow::anyhow!("max_load was not present in InvocationConfig"))?;
                if max_load == 0.0 {
                    anyhow::bail!("Cannot have a 'max_load' of 0");
                }
                max_concur = config
                    .max_oversubscribe
                    .ok_or_else(|| anyhow::anyhow!("max_oversubscribe was not present in InvocationConfig"))?;
                if max_concur == 0 {
                    anyhow::bail!("Cannot have a 'max_concurrency' of 0");
                }

                let (h, tx) = tokio_thread(check_dur, CPU_CONCUR_WORKER_TID.clone(), Self::monitor_load);
                (Some(h), Some(tx))
            },
            None => (None, None),
        };

        let mut fineloadbalancing = None;
        if let Some(ref fineloadbalancing_config) = fineloadbalancing_config {
            fineloadbalancing = Some(FineLoadBalancing::build_arc_with_cpu_utilization(
                fineloadbalancing_config.clone(),
                cmap,
                core_utilization.clone(),
            ));
        }

        let svc = Arc::new(CpuResourceTracker {
            concurrency_semaphore: sem,
            min_concur: config.count,
            current_concur: Mutex::new(config.count),
            max_load: config.max_load,
            concurrency_load_source: config.concurrency_load_source,
            finesched_domain_load_aggregation: config.finesched_domain_load_aggregation,
            max_concur,
            _load_thread: load_handle,
            cores: available_cores as f64,
            load_avg,
            fineloadbalancing,
            sched_ext_cgroups: DashSet::new(),
        });
        if let Some(load_tx) = load_tx {
            load_tx.send(svc.clone())?;
        }
        debug!(tid = tid, "Created CPUResourceMananger");
        Ok(svc)
    }

    /// Return a permit for the function to run on its registered number of cores.
    /// If the semaphore is [None], then no permits are being tracked.
    pub fn try_acquire_cores(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
    ) -> Result<Option<OwnedSemaphorePermit>, tokio::sync::TryAcquireError> {
        debug!(cpu_sem =? self.concurrency_semaphore, "CPUResourceMananger");
        if let Some(sem) = &self.concurrency_semaphore {
            if self.assign_domain_to_function_request(tid, reg.clone()).is_ok() {
                return match sem.clone().try_acquire_many_owned(reg.cpus) {
                    Ok(p) => Ok(Some(p)),
                    Err(e) => {
                        self.release_domain_for_request(tid, reg.clone());
                        Err(e)
                    },
                };
            }
        }
        Ok(None)
    }

    /// Blocks until an owned permit can be acquired for the function to run on its registered number of cores.
    /// If the semaphore is [None], then no permits are being tracked.
    pub async fn acquire_cores(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
    ) -> Result<Option<DomainCpuPermit>, tokio::sync::AcquireError> {
        debug!(cpu_sem =? self.concurrency_semaphore, "CPUResourceMananger");
        if let Some(sem) = &self.concurrency_semaphore {
            if self.assign_domain_to_function_request(tid, reg.clone()).is_ok() {
                return match sem.clone().acquire_many_owned(reg.cpus).await {
                    Ok(p) => {
                        let release_domain_callback = self.build_release_domain_callback(tid, reg.clone());
                        Ok(Some(DomainCpuPermit::new(p, release_domain_callback)))
                    },
                    Err(e) => {
                        self.release_domain_for_request(tid, reg.clone());
                        Err(e)
                    },
                };
            } else {
                self.await_domain_release_or_timeout().await;
            }
        }
        Ok(None)
    }

    pub fn available_cores(&self) -> usize {
        if let Some(sem) = &self.concurrency_semaphore {
            return sem.available_permits();
        }
        self.cores as usize
    }

    pub fn fine_scheduling_enabled(&self) -> bool {
        self.fineloadbalancing.is_some()
    }

    pub fn set_cpu_queue_preview(&self, queue: Weak<dyn InvokerCpuQueuePolicy>) {
        if let Some(fineloadbalancing) = &self.fineloadbalancing {
            fineloadbalancing.set_cpu_queue_preview(queue);
        }
    }

    pub fn set_container_manager(&self, manager: Weak<ContainerManager>) {
        if let Some(fineloadbalancing) = &self.fineloadbalancing {
            fineloadbalancing.set_container_manager(manager);
        }
    }

    fn cgroup_threads_file(cgroup_id: &str) -> Result<PathBuf> {
        let candidates = [
            PathBuf::from("/sys/fs/cgroup/scx.slice")
                .join(cgroup_id)
                .join("cgroup.threads"),
            PathBuf::from("/sys/fs/cgroup").join(cgroup_id).join("cgroup.threads"),
            PathBuf::from("/sys/fs/cgroup/system.slice")
                .join(cgroup_id)
                .join("cgroup.threads"),
        ];

        candidates
            .into_iter()
            .find(|path| path.is_file())
            .ok_or_else(|| anyhow::anyhow!("Unable to locate cgroup.threads for cgroup '{}'", cgroup_id))
    }

    fn set_cgroup_sched_ext(&self, cgroup_id: &str) -> Result<()> {
        if is_simulation() || self.sched_ext_cgroups.contains(cgroup_id) {
            return Ok(());
        }

        let threads_file = Self::cgroup_threads_file(cgroup_id)?;
        let threads =
            read_to_string(&threads_file).with_context(|| format!("Failed to read {}", threads_file.display()))?;
        let sched_param = libc::sched_param { sched_priority: 0 };
        let mut switched = 0;

        for thread_id in threads.lines() {
            let thread_id = thread_id
                .parse::<libc::pid_t>()
                .with_context(|| format!("Invalid thread ID '{}' in {}", thread_id, threads_file.display()))?;
            let result = unsafe { libc::sched_setscheduler(thread_id, SCHED_EXT_POLICY, &sched_param) };
            if result != 0 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() == Some(libc::ESRCH) {
                    continue;
                }
                return Err(error).with_context(|| format!("Failed to move thread {} to SCHED_EXT", thread_id));
            }
            switched += 1;
        }

        if switched == 0 {
            bail!("No threads found in cgroup '{}'", cgroup_id);
        }

        self.sched_ext_cgroups.insert(cgroup_id.to_string());
        debug!(cgroup_id=%cgroup_id, threads=%switched, "[finesched] moved cgroup threads to SCHED_EXT");
        Ok(())
    }

    fn aggregate_finesched_domain_load(
        aggregation: FineschedDomainLoadAggregation,
        snapshot: &[(SchedGroupID, Option<f64>)],
    ) -> Option<f64> {
        // Require a complete finite snapshot so missing domains cannot look like
        // zero load and bias the controller toward increasing concurrency.
        let mut loads = snapshot
            .iter()
            .map(|(_, load)| load.as_ref().copied())
            .collect::<Option<Vec<_>>>()?;
        if loads.is_empty() || loads.iter().any(|load| !load.is_finite()) {
            return None;
        }

        match aggregation {
            FineschedDomainLoadAggregation::Mean => Some(loads.iter().sum::<f64>() / loads.len() as f64),
            FineschedDomainLoadAggregation::Median => {
                loads.sort_by(f64::total_cmp);
                let middle = loads.len() / 2;
                if loads.len() % 2 == 0 {
                    Some((loads[middle - 1] + loads[middle]) / 2.0)
                } else {
                    Some(loads[middle])
                }
            },
            FineschedDomainLoadAggregation::Min => loads.into_iter().reduce(f64::min),
            FineschedDomainLoadAggregation::Max => loads.into_iter().reduce(f64::max),
        }
    }

    fn normalized_concurrency_load(&self) -> (f64, &'static str) {
        let system_load = *self.load_avg.read() / self.cores;
        if self.concurrency_load_source == CpuConcurrencyLoadSource::System {
            return (system_load, "system");
        }

        let domain_load = self.fineloadbalancing.as_ref().and_then(|fineloadbalancing| {
            match fineloadbalancing.domain_pelt_like_utilization_snapshot() {
                Ok(snapshot) => {
                    Self::aggregate_finesched_domain_load(self.finesched_domain_load_aggregation, &snapshot)
                },
                Err(error) => {
                    debug!(error=%error, "[cpu-concurrency] FineSched domain PELT snapshot failed");
                    None
                },
            }
        });

        if let Some(domain_load_percent) = domain_load {
            return (domain_load_percent / 100.0, "finesched_domains");
        }

        debug!(
            aggregation=?self.finesched_domain_load_aggregation,
            finesched_enabled=self.fineloadbalancing.is_some(),
            "[cpu-concurrency] FineSched domain load unavailable; falling back to system load"
        );
        (system_load, "system_fallback")
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self), fields(tid=tid)))]
    async fn monitor_load(self: &Arc<Self>, tid: &TransactionId) {
        let (norm_load, load_source) = self.normalized_concurrency_load();
        let current = *self.current_concur.lock();
        if norm_load > self.max_load.unwrap() {
            let change = current - self.min_concur;
            let change = f64::ceil(change as f64 / 2.0) as u32;
            if change > 0 {
                if let Some(sem) = &self.concurrency_semaphore {
                    match sem.acquire_many(change).await {
                        Ok(s) => {
                            s.forget();
                            *self.current_concur.lock() = u32::max(self.min_concur, current - change);
                        },
                        Err(e) => {
                            error!(tid=tid, error=%e, "Failed to acquire concurrency semaphore")
                        },
                    };
                }
            }
        } else if current < self.max_concur {
            if let Some(sem) = &self.concurrency_semaphore {
                sem.add_permits(1);
                *self.current_concur.lock() += 1;
            }
        }
        debug!(
            tid = tid,
            concurrency = *self.current_concur.lock(),
            load = norm_load,
            load_source,
            "Current concurrency"
        );
    }

    pub fn assign_domain_to_function_request(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) -> Result<()> {
        let fqdn = reg.fqdn.as_str();
        if let Some(fineloadbalancing) = self.fineloadbalancing.as_ref() {
            let _lock = fineloadbalancing.domain_operation_lock.lock().unwrap();
            let stats = fineloadbalancing.stats.clone();
            match stats.tid_map.get(tid) {
                Some(_entry) => return Ok(()),
                _ => {},
            };

            let lbpolicy = &fineloadbalancing.lbpolicy;
            let domain_id = lbpolicy.assign_domain_to_function_request(tid, reg.clone());
            if let Some(domain_id) = domain_id {
                // Capture stats for the domain assignment.
                debug!( tid=%tid, fqdn=%fqdn, domain_id=%domain_id, "[finesched] inserting to tid map");
                stats.tid_map.insert(tid.clone(), domain_id);

                let scheduled_invocations = &stats.domain_map.get_or_create(&domain_id).scheduled_invocations;
                scheduled_invocations.fetch_add(1, Ordering::Relaxed);

                debug!( tid=%tid, fqdn=%fqdn, domain_id=%domain_id, "[finesched] domain assigned to function request");
                drop(_lock);
                fineloadbalancing.maybe_log_core_metrics(tid, fqdn, domain_id);
                return Ok(());
            }
            debug!( tid=%tid, fqdn=%fqdn, domain_id=%"None", "[finesched] domain not assigned to function request");
        } else {
            debug!( tid=%tid, fqdn=%fqdn, domain_id=%"no_finesched", "[finesched] domain not assigned to function request");
            return Ok(());
        }

        bail!("no domain assigned")
    }

    pub fn cgroup_assigned_to_function(
        &self,
        cgroup_id: &str,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Result<()> {
        let fqdn = reg.fqdn.as_str();
        if let Some(fineloadbalancing) = self.fineloadbalancing.as_ref() {
            let _lock = fineloadbalancing.domain_operation_lock.lock().unwrap();
            let stats = fineloadbalancing.stats.clone();
            let domain_id = match stats.tid_map.get(tid) {
                Some(entry) => *entry,
                None => {
                    error!(tid=%tid, "[finesched] no domain found for tid in tid_stats map");
                    bail!("no fine-scheduling domain found for transaction '{}'", tid);
                },
            };

            // Update cgroup characteristics map shared with the scheduler.
            // BPF task storage survives when a warm container reuses its Linux
            // task. Use a fresh generation so SRPT can reset CPU runtime at
            // each invocation boundary.
            let invocation_id = NEXT_FINE_SCHED_INVOCATION_ID.fetch_add(1, Ordering::Relaxed);
            let warm_dur = fineloadbalancing.cmap.get(fqdn, Chars::CpuWarmTime, Value::Avg);
            let dur = if warm_dur.is_finite() && warm_dur > 0.0 {
                warm_dur
            } else {
                fineloadbalancing.cmap.get(fqdn, Chars::CpuExecTime, Value::Avg)
            };
            let dur_ms = (dur * 1000.0) as u64;
            let dur_ns = (dur.max(0.0) * 1_000_000_000.0) as u64;

            // Keep the container-level EEVDF estimate in wallclock units.
            // FineSched learns CPU/wall parallelism in BPF, so the worker must
            // not convert this estimate to aggregate core time.
            if let Err(err) = iluvatar_finesched::update_cgroup_wall_estimate(cgroup_id, dur_ns, invocation_id) {
                warn!(tid=%tid, fqdn=%fqdn, cgroup_id=%cgroup_id, error=%err,
                      "[finesched] unable to update container wallclock estimate");
            }

            // TODO: lookup arrival time and push
            let sched_domains = &fineloadbalancing.preallocated_domains;
            sched_domains.update_cgroup_chrs(domain_id, invocation_id, dur_ms, 0, cgroup_id);
            if fineloadbalancing.config.sched_ext_migration == SchedExtMigrationMethod::Syscall {
                self.set_cgroup_sched_ext(cgroup_id)?;
            }

            debug!( tid=%tid, fqdn=%fqdn, cgroup_id=%cgroup_id, invocation_id=invocation_id, domain_id=%domain_id, "[finesched] domain assigned to function cgroup");
        }
        Ok(())
    }

    pub fn cgroup_released_for_function(&self, cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let fqdn = reg.fqdn.as_str();
        if let Some(fineloadbalancing) = &self.fineloadbalancing {
            let _lock = fineloadbalancing.domain_operation_lock.lock().unwrap();
            let lbpolicy = &fineloadbalancing.lbpolicy;
            let stats = fineloadbalancing.stats.clone();
            let domain_id = match stats.tid_map.get(tid) {
                Some(entry) => *entry,
                None => {
                    error!(tid=%tid, "[finesched] no domain found for tid in tid_stats map");
                    return;
                },
            };

            lbpolicy.invoke_is_complete(cgroup_id, tid, reg.clone());
            stats.tid_map.remove(tid);

            let scheduled_invocations = &stats.domain_map.get_or_create(&domain_id).scheduled_invocations;
            scheduled_invocations.fetch_sub(1, Ordering::Relaxed);

            fineloadbalancing.domain_release_signal.notify_waiters();
            debug!( tid=%tid, fqdn=%fqdn, cgroup_id=%cgroup_id, domain_id=%domain_id, "[finesched] domain released for function cgroup");
        }
    }

    pub fn release_domain_for_request(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let callback = self.build_release_domain_callback(tid, reg);
        (*callback)();
    }

    pub fn build_release_domain_callback(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) -> Callback {
        if let Some(fineloadbalancing) = &self.fineloadbalancing {
            let fineloadbalancing = fineloadbalancing.clone();
            let tid = tid.clone();
            let domain_release_signal = fineloadbalancing.domain_release_signal.clone();

            let callback = move || {
                let fqdn = reg.fqdn.as_str();
                let _lock = fineloadbalancing.domain_operation_lock.lock().unwrap();
                let lbpolicy = &fineloadbalancing.lbpolicy;
                let stats = fineloadbalancing.stats.clone();
                let domain_id = match stats.tid_map.get(&tid) {
                    Some(entry) => *entry,
                    // Normal invocation completion releases the cgroup/domain before
                    // the RAII CPU permit is dropped.  The permit callback is also
                    // used as failure-path cleanup, so it must be idempotent.
                    None => return,
                };

                lbpolicy.release_domain(&tid, reg.clone());
                stats.tid_map.remove(&tid);

                let scheduled_invocations = &stats.domain_map.get_or_create(&domain_id).scheduled_invocations;
                scheduled_invocations.fetch_sub(1, Ordering::Relaxed);

                domain_release_signal.notify_waiters();
                debug!( tid=%tid, fqdn=%fqdn, domain_id=%domain_id, pending_invocations=%scheduled_invocations.load(Ordering::Relaxed), "[finesched] domain released for function request");
            };
            Arc::new(callback)
        } else {
            Arc::new(|| {})
        }
    }

    async fn await_domain_release_or_timeout(&self) {
        if let Some(fineloadbalancing) = &self.fineloadbalancing {
            let fineloadbalancing = fineloadbalancing.clone();
            let domain_release_signal = fineloadbalancing.domain_release_signal.clone();

            let sleep_t = 200;
            let _ = tokio::time::timeout(Duration::from_millis(sleep_t), domain_release_signal.notified_owned()).await;
        }
    }
}

/// A type to share the most recently computed system load average.
/// Should only have one per system.
pub type LoadAvg = Arc<RwLock<f64>>;
pub fn build_load_avg_signal() -> LoadAvg {
    Arc::new(RwLock::new(0.0))
}

pub fn get_cpu_mon(
    config: &Arc<StatusConfig>,
    ring_buff: &Arc<RingBuffer>,
    num_cpus: u32,
    signal: LoadAvg,
    invoker: Arc<dyn Invoker>,
    tid: &TransactionId,
) -> Result<CpuMonitor> {
    get_cpu_mon_with_core_utilization(
        config,
        ring_buff,
        num_cpus,
        signal,
        invoker,
        tid,
        CpuCoreUtilization::new(num_cpus as usize),
    )
}

pub fn get_cpu_mon_with_core_utilization(
    config: &Arc<StatusConfig>,
    ring_buff: &Arc<RingBuffer>,
    num_cpus: u32,
    signal: LoadAvg,
    invoker: Arc<dyn Invoker>,
    tid: &TransactionId,
    core_utilization: Arc<CpuCoreUtilization>,
) -> Result<CpuMonitor> {
    CpuMonitorWrap::boxed(config, ring_buff, num_cpus, signal, invoker, tid, core_utilization)
}

/// Latest `/proc/stat` busy percentage for each logical CPU, indexed by CPU ID.
/// Entries remain unavailable until two samples have been collected.
#[derive(Default)]
pub struct CpuCoreUtilization {
    values: RwLock<Vec<Option<f64>>>,
}

impl CpuCoreUtilization {
    pub fn new(cpu_count: usize) -> Arc<Self> {
        Arc::new(Self {
            values: RwLock::new(vec![None; cpu_count]),
        })
    }

    fn update(&self, values: &BTreeMap<u32, f64>) {
        let mut current = self.values.write();
        current.fill(None);
        for (cpu, utilization) in values {
            if *cpu as usize >= current.len() {
                current.resize(*cpu as usize + 1, None);
            }
            if let Some(slot) = current.get_mut(*cpu as usize) {
                *slot = Some(*utilization);
            }
        }
    }

    pub fn snapshot(&self) -> Vec<Option<f64>> {
        self.values.read().clone()
    }
}

#[derive(serde::Serialize, iluvatar_library::ToAny)]
pub struct CpuUtil {
    /// [cpu-time non-kernel](https://linux.die.net/man/1/mpstat)
    pub cpu_us: f64,
    /// [cpu-time kernel, hypervisor, servicing, interrupts](https://linux.die.net/man/1/mpstat)
    pub cpu_sy: f64,
    /// [cpu-time idle](https://linux.die.net/man/1/mpstat)
    pub cpu_id: f64,
    /// [cpu-time waiting](https://linux.die.net/man/1/mpstat)
    pub cpu_wa: f64,
    /// [one minute load average](https://www.man7.org/linux/man-pages/man1/uptime.1.html)
    /// NOT NORMALIZED by number of cores
    pub load_avg_1minute: f64,
    /// The number of (logical) CPU cores on the system
    /// used to normalize the load average value
    pub num_system_cores: u32,
}
impl Wireable for CpuUtil {}
impl std::fmt::Display for CpuUtil {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match serde_json::to_string::<CpuUtil>(self) {
            Ok(s) => s,
            Err(_e) => return Err(std::fmt::Error {}),
        };
        write!(f, "{}", s)
    }
}

pub const CPU_MON_TID: &str = "CPU_MON_BG";
struct CpuMonitorWrap {
    mon: CpuMonitor,
    num_cpus: u32,
}
impl CpuMonitorWrap {
    pub fn boxed(
        config: &Arc<StatusConfig>,
        ring_buff: &Arc<RingBuffer>,
        num_cpus: u32,
        signal: LoadAvg,
        invoker: Arc<dyn Invoker>,
        tid: &TransactionId,
        core_utilization: Arc<CpuCoreUtilization>,
    ) -> Result<CpuMonitor> {
        let (_handle, sender) = threading::tokio_logging_thread(
            config.report_freq_ms,
            CPU_MON_TID.to_owned(),
            ring_buff.clone(),
            Self::log,
        )?;
        let r = Arc::new(Self {
            mon: match is_simulation() {
                true => CpuSimMonitor::new(num_cpus, signal, invoker, tid),
                false => CpuHardwareMonitor::new(signal, tid, core_utilization)?,
            },
            num_cpus,
        });
        sender.send(r.clone())?;
        Ok(r)
    }

    async fn log(self: &Arc<Self>, tid: &TransactionId) -> Result<CpuUtil> {
        let (computed_util, minute_load_avg) = self.mon.cpu_util(tid)?;
        let r = CpuUtil {
            cpu_us: computed_util.cpu_user + computed_util.cpu_nice,
            cpu_sy: computed_util.cpu_system
                + computed_util.cpu_irq
                + computed_util.cpu_softirq
                + computed_util.cpu_steal
                + computed_util.cpu_guest
                + computed_util.cpu_guest_nice,
            cpu_id: computed_util.cpu_idle,
            cpu_wa: computed_util.cpu_iowait,
            load_avg_1minute: minute_load_avg,
            num_system_cores: self.num_cpus,
        };
        info!(tid=tid, cpu_util=%r, "CPU utilization");
        Ok(r)
    }
}
impl CpuMonitorTrait for CpuMonitorWrap {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        self.mon.cpu_util(tid)
    }
}

pub type CpuMonitor = Arc<dyn CpuMonitorTrait + Send + Sync>;
pub trait CpuMonitorTrait {
    /// Returns a tuple of two items:
    /// 1) Breakdown in CPU utilization, since previous query.
    ///     More frequent querying results in more accurate utilization.
    ///     _NOT_ an instantaneous CPU utilization.
    /// 2) System [load average](https://www.man7.org/linux/man-pages/man5/proc_loadavg.5.html) over 1 minute.
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)>;
}

struct CpuHardwareMonitor {
    last_instant_usage: Mutex<CPUUtilInstant>,
    signal: LoadAvg,
    core_utilization: Arc<CpuCoreUtilization>,
}
impl CpuHardwareMonitor {
    pub fn new(signal: LoadAvg, tid: &TransactionId, core_utilization: Arc<CpuCoreUtilization>) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            last_instant_usage: Mutex::new(CPUUtilInstant::get(tid)?),
            signal,
            core_utilization,
        }))
    }
}
impl CpuMonitorTrait for CpuHardwareMonitor {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        debug!(tid = tid, "Computing system utilization");
        let now = CPUUtilInstant::get(tid)?;
        let mut last = self.last_instant_usage.lock();
        let diff = &now - &*last;
        *last = now;
        self.core_utilization.update(&diff.per_cpu_busy_percent);

        let buff = match read_to_string("/proc/loadavg") {
            Ok(f) => f,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to read /proc/loadavg"),
        };
        let lines: Vec<&str> = buff.split(' ').filter(|str| !str.is_empty()).collect();
        let min = lines[0];
        let load_avg = match min.parse::<f64>() {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid = tid, "error parsing float from uptime {}: {}", min, e);
            },
        };
        *self.signal.write() = load_avg;

        Ok((diff, load_avg))
    }
}

struct CpuSimMonitor {
    num_cpus: u32,
    signal: LoadAvg,
    invoker: Arc<dyn Invoker>,
}
impl CpuSimMonitor {
    fn new(num_cpus: u32, signal: LoadAvg, invoker: Arc<dyn Invoker>, _tid: &TransactionId) -> Arc<Self> {
        Arc::new(Self {
            num_cpus,
            signal,
            invoker,
        })
    }
}
impl CpuMonitorTrait for CpuSimMonitor {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        debug!(tid = tid, "Computing system utilization");
        // additional 0.5 to account for system work
        let running = self.invoker.running_funcs() as f64 + 0.5;
        let mut lck = self.signal.write();
        let load_avg = (running * 0.5) + (*lck * (1.0 - 0.5));
        *lck = load_avg;
        Ok((
            CPUUtilPcts {
                read_diff: Duration::from_micros(0),
                cpu_user: load_avg / self.num_cpus as f64,
                cpu_nice: 0.0,
                cpu_system: 0.5,
                cpu_idle: 0.0,
                cpu_iowait: 0.0,
                cpu_irq: 0.0,
                cpu_softirq: 0.0,
                cpu_steal: 0.0,
                cpu_guest: 0.0,
                cpu_guest_nice: 0.0,
                per_cpu_busy_percent: BTreeMap::new(),
            },
            load_avg,
        ))
    }
}

/// The CPU usage metrics reported by /proc/stat
pub struct CPUUtilInstant {
    read_time: Instant,
    cpu_user: f64,
    cpu_nice: f64,
    cpu_system: f64,
    cpu_idle: f64,
    cpu_iowait: f64,
    cpu_irq: f64,
    cpu_softirq: f64,
    cpu_steal: f64,
    cpu_guest: f64,
    cpu_guest_nice: f64,
    per_cpu: BTreeMap<u32, [f64; 10]>,
}
impl Default for CPUUtilInstant {
    fn default() -> Self {
        Self {
            read_time: now(),
            cpu_user: 0.0,
            cpu_nice: 0.0,
            cpu_system: 0.0,
            cpu_idle: 0.0,
            cpu_iowait: 0.0,
            cpu_irq: 0.0,
            cpu_softirq: 0.0,
            cpu_steal: 0.0,
            cpu_guest: 0.0,
            cpu_guest_nice: 0.0,
            per_cpu: BTreeMap::new(),
        }
    }
}
impl CPUUtilInstant {
    pub fn get(tid: &TransactionId) -> Result<Self> {
        let contents = Self::read()?;
        Self::parse(contents, tid)
    }
    fn read() -> Result<String> {
        std::fs::read_to_string("/proc/stat").context("failed to read /proc/stat")
    }
    fn parse(contents: String, tid: &TransactionId) -> Result<Self> {
        let mut aggregate = None;
        let mut per_cpu = BTreeMap::new();
        for line in contents.lines() {
            let mut fields = line.split_whitespace();
            let Some(label) = fields.next() else { continue };
            let cpu_id = label.strip_prefix("cpu").and_then(|id| id.parse::<u32>().ok());
            if label != "cpu" && cpu_id.is_none() {
                continue;
            }
            let values = fields
                .map(str::parse::<f64>)
                .collect::<std::result::Result<Vec<_>, _>>()
                .with_context(|| format!("failed to parse /proc/stat CPU row for {label} (transaction {tid})"))?;
            if label == "cpu" {
                aggregate = Some(values);
            } else if let Some(cpu_id) = cpu_id {
                if values.len() < 4 {
                    continue;
                }
                let mut counters = [0.0; 10];
                for (index, value) in values.into_iter().take(10).enumerate() {
                    counters[index] = value;
                }
                per_cpu.insert(cpu_id, counters);
            }
        }
        let values = aggregate.ok_or_else(|| anyhow::anyhow!("Unable to find matching 'cpu ' line in /proc/stat"))?;
        if values.len() < 10 {
            anyhow::bail!("/proc/stat aggregate CPU row was unexpectedly short");
        }
        Ok(Self {
            read_time: now(),
            cpu_user: values[0],
            cpu_nice: values[1],
            cpu_system: values[2],
            cpu_idle: values[3],
            cpu_iowait: values[4],
            cpu_irq: values[5],
            cpu_softirq: values[6],
            cpu_steal: values[7],
            cpu_guest: values[8],
            cpu_guest_nice: values[9],
            per_cpu,
        })
    }
}

pub struct CPUUtilPcts {
    pub read_diff: std::time::Duration,
    pub cpu_user: f64,
    pub cpu_nice: f64,
    pub cpu_system: f64,
    pub cpu_idle: f64,
    pub cpu_iowait: f64,
    pub cpu_irq: f64,
    pub cpu_softirq: f64,
    pub cpu_steal: f64,
    pub cpu_guest: f64,
    pub cpu_guest_nice: f64,
    pub per_cpu_busy_percent: BTreeMap<u32, f64>,
}
impl std::ops::Sub for &CPUUtilInstant {
    type Output = CPUUtilPcts;

    fn sub(self, rhs: Self) -> Self::Output {
        let dur = self.read_time - rhs.read_time;
        let user_diff = self.cpu_user - rhs.cpu_user;
        let nice_diff = self.cpu_nice - rhs.cpu_nice;
        let system_diff = self.cpu_system - rhs.cpu_system;
        let idle_diff = self.cpu_idle - rhs.cpu_idle;
        let iowait_diff = self.cpu_iowait - rhs.cpu_iowait;
        let irq_diff = self.cpu_irq - rhs.cpu_irq;
        let softirq_diff = self.cpu_softirq - rhs.cpu_softirq;
        let steal_diff = self.cpu_steal - rhs.cpu_steal;
        let guest_diff = self.cpu_guest - rhs.cpu_guest;
        let guest_nice_diff = self.cpu_guest_nice - rhs.cpu_guest_nice;
        let tot = user_diff
            + nice_diff
            + system_diff
            + idle_diff
            + iowait_diff
            + irq_diff
            + softirq_diff
            + steal_diff
            + guest_diff
            + guest_nice_diff;
        let percentage = |value: f64| if tot > 0.0 { value / tot * 100.0 } else { 0.0 };
        CPUUtilPcts {
            read_diff: dur,
            cpu_user: percentage(user_diff),
            cpu_nice: percentage(nice_diff),
            cpu_system: percentage(system_diff),
            cpu_idle: percentage(idle_diff),
            cpu_iowait: percentage(iowait_diff),
            cpu_irq: percentage(irq_diff),
            cpu_softirq: percentage(softirq_diff),
            cpu_steal: percentage(steal_diff),
            cpu_guest: percentage(guest_diff),
            cpu_guest_nice: percentage(guest_nice_diff),
            per_cpu_busy_percent: self
                .per_cpu
                .iter()
                .filter_map(|(cpu, current)| {
                    let previous = rhs.per_cpu.get(cpu)?;
                    // guest/guest_nice are included in user/nice in /proc/stat.
                    let total: f64 = current
                        .iter()
                        .take(8)
                        .zip(previous.iter().take(8))
                        .map(|(new, old)| new - old)
                        .sum();
                    if total <= 0.0 {
                        return None;
                    }
                    let idle = (current[3] - previous[3]) + (current[4] - previous[4]);
                    Some((*cpu, ((total - idle) / total * 100.0).clamp(0.0, 100.0)))
                })
                .collect(),
        }
    }
}
