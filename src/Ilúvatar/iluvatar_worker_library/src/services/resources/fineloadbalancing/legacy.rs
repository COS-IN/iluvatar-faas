use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::ContainerState;
use crate::services::invocation::queueing::InvokerCpuQueuePolicy;
use crate::services::registration::RegisteredFunction;
use crate::services::resources::arc_map::ArcMap;
use crate::services::resources::arc_vec::ArcVec;
use crate::services::resources::cpu::CpuCoreUtilization;

use crate::worker_api::worker_config::FineLoadBalancingConfig;
use anyhow::bail;
use anyhow::Result;
use iluvatar_finesched::load_bpf_scheduler_async;
use iluvatar_finesched::read_scx_available_cpus;
use iluvatar_finesched::CoreMetricsReader;
use iluvatar_finesched::PreAllocatedGroups;
use iluvatar_finesched::SchedGroupID;
use iluvatar_finesched::SharedMapsDummy;
use iluvatar_finesched::SharedMapsRef;
use iluvatar_finesched::SharedMapsSafe;
use iluvatar_library::char_map::Chars;
use iluvatar_library::char_map::Value;
use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::thread;
use std::time::Duration;
use tokio::sync::Notify;
use tracing::debug;
use tracing::error;
use tracing::warn;

pub trait LoadBalancingPolicyTrait {
    fn assign_domain_to_function_request(
        &self,
        _tid: &TransactionId,
        _reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID>;
    fn invoke_is_complete(&self, _cgroup_id: &str, _tid: &TransactionId, _reg: Arc<RegisteredFunction>) {}
    fn release_domain(&self, _tid: &TransactionId, _reg: Arc<RegisteredFunction>) {}

    /// Move a policy reservation after generic burst spillover picks another
    /// domain. Policies with no per-domain reservation can accept the move.
    fn move_reservation(&self, _reg: &RegisteredFunction, _from: SchedGroupID, _to: SchedGroupID) -> bool {
        true
    }
}

fn move_domain_reservation(
    fineloadbalancing: &FineLoadBalancingWeak,
    reg: &RegisteredFunction,
    from: SchedGroupID,
    to: SchedGroupID,
) -> bool {
    let Some(fineloadbalancing) = fineloadbalancing.upgrade() else {
        return false;
    };
    let domains = fineloadbalancing.system_domains.immutable_clone();
    let (Some(source), Some(target)) = (domains.get(from as usize), domains.get(to as usize)) else {
        return false;
    };
    // Reserve first so a failed capacity check leaves the original assignment intact.
    if target.can_serve_and_acquire_append(&reg.fqdn, reg.cpus).is_err() {
        return false;
    }
    source.return_cpus_and_release_func(&reg.fqdn, reg.cpus);
    true
}
type LoadBalancingPolicy = Box<dyn LoadBalancingPolicyTrait + Sync + Send>;

#[derive(Default)]
pub struct DomainStats {
    pub scheduled_invocations: AtomicU32,
}

#[derive(Default)]
pub struct FineLoadBalancingStatsStruct {
    pub tid_map: ArcMap<TransactionId, SchedGroupID>,
    pub domain_map: ArcMap<SchedGroupID, DomainStats>,
}
pub type FineLoadBalancingStats = Arc<FineLoadBalancingStatsStruct>;

pub struct FineLoadBalancingStruct {
    pub config: Arc<FineLoadBalancingConfig>,
    pub cmap: WorkerCharMap,

    pub preallocated_domains: Arc<PreAllocatedGroups>,
    pub system_domains: ArcVec<Domain>,
    pub stats: FineLoadBalancingStats,
    cold_start_counts: Mutex<HashMap<(SchedGroupID, String), u32>>,
    pub lbpolicy: LoadBalancingPolicy,
    pub cpu_core_utilization: Arc<CpuCoreUtilization>,

    pub core_metrics_reader: Option<CoreMetricsReader>,
    core_metrics_debug_counter: AtomicU64,
    workload_phase_debug_counter: AtomicU64,

    pub domain_operation_lock: Mutex<bool>,
    pub domain_release_signal: Arc<Notify>,
    pub(super) cpu_queue_preview: Mutex<Option<Weak<dyn InvokerCpuQueuePolicy>>>,
    container_manager: Mutex<Option<Weak<ContainerManager>>>,
}
pub type FineLoadBalancing = Arc<FineLoadBalancingStruct>;
pub type FineLoadBalancingWeak = Weak<FineLoadBalancingStruct>;

pub trait BuildFineLoadBalancing {
    fn build_arc(config: Arc<FineLoadBalancingConfig>, cmap: WorkerCharMap) -> FineLoadBalancing;
    fn build_arc_with_cpu_utilization(
        config: Arc<FineLoadBalancingConfig>,
        cmap: WorkerCharMap,
        cpu_core_utilization: Arc<CpuCoreUtilization>,
    ) -> FineLoadBalancing;
}

impl BuildFineLoadBalancing for FineLoadBalancing {
    fn build_arc(config: Arc<FineLoadBalancingConfig>, cmap: WorkerCharMap) -> FineLoadBalancing {
        let cpu_count = config
            .preallocated_groups
            .groups
            .iter()
            .flat_map(|group| group.cores.iter().copied())
            .max()
            .map(|cpu| cpu as usize + 1)
            .unwrap_or_else(num_cpus::get);
        Self::build_arc_with_cpu_utilization(config, cmap, CpuCoreUtilization::new(cpu_count))
    }

    fn build_arc_with_cpu_utilization(
        config: Arc<FineLoadBalancingConfig>,
        cmap: WorkerCharMap,
        cpu_core_utilization: Arc<CpuCoreUtilization>,
    ) -> FineLoadBalancing {
        let available_cpus =
            read_scx_available_cpus().unwrap_or_else(|error| panic!("failed to read sched_ext CPU set: {error:#}"));
        let mut final_groups = if config.preallocated_groups.groups.is_empty() {
            config
                .preallocated_groups
                .generate_from_cpus(&available_cpus)
                .unwrap_or_else(|error| panic!("failed to generate FineSched CPU domains: {error:#}"))
        } else {
            config.preallocated_groups.clone()
        };
        final_groups = final_groups
            .constrain_to_cpus(&available_cpus)
            .unwrap_or_else(|error| panic!("invalid FineSched CPU domain map: {error:#}"));
        let domain_map: Vec<(usize, Vec<u32>)> = final_groups
            .groups
            .iter()
            .enumerate()
            .map(|(domain, group)| (domain, group.cores.clone()))
            .collect();
        tracing::info!(
            available_cpus = ?available_cpus,
            domains = ?domain_map,
            "[finesched] effective CPU domain map"
        );
        let mut normalized_config = (*config).clone();
        normalized_config.preallocated_groups = final_groups;
        let config = Arc::new(normalized_config);

        let scx_scheduler_sharedmaps: SharedMapsRef;
        if config.testing == 0 {
            scx_scheduler_sharedmaps = Arc::new(SharedMapsSafe::new());
        } else {
            scx_scheduler_sharedmaps = Arc::new(SharedMapsDummy::new());
        }

        let preallocated_domains = Arc::new(PreAllocatedGroups::new(
            scx_scheduler_sharedmaps.clone(),
            config.preallocated_groups.clone(),
        ));

        if config.testing == 0 {
            // TODO: Blocks forever if scheduler fails to load. Update logic
            // to error.
            load_bpf_scheduler_async(
                config.bpf_verbose,
                config.sched_ext_migration == crate::worker_api::worker_config::SchedExtMigrationMethod::Kfunc,
                config.task_ip_monitoring_enabled(),
                config.srpt_trace_enabled,
                config.wc_metrics_enabled,
            );
        }

        let core_metrics_reader = if config.testing != 0 {
            None
        } else {
            match open_core_metrics_reader_with_retry() {
                Ok(reader) => Some(reader),
                Err(error) => {
                    warn!(
                        error = %error,
                        "[finesched][core-metrics] metrics unavailable because the reader could not be opened"
                    );
                    None
                },
            }
        };

        let domains_config = &config.preallocated_groups.groups;
        let domain_count = domains_config.len();
        let system_domains = ArcVec::<Domain>::new();
        for domain_id in 0..domain_count {
            let assigned_cores = domains_config[domain_id].cores.len() as u32;
            let domain = Domain::new(domain_id, assigned_cores);
            system_domains.push(domain);
        }

        Arc::new_cyclic(move |fineloadbalancing_weak| {
            let lbpolicy_name = config.dispatchpolicy.to_lowercase();
            let lbpolicy: Option<LoadBalancingPolicy> = match lbpolicy_name.as_str() {
                "guardrailsnocpubound" => Some(Box::new(Guardrails::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                ))),
                "guardrails" => Some(Box::new(GuardrailsPickOnSystemDomains::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    GuardrailsPickOnSystemDomainsValueType::DURATION,
                ))),
                "guardrails_iat" => Some(Box::new(GuardrailsPickOnSystemDomains::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    GuardrailsPickOnSystemDomainsValueType::IAT,
                ))),
                "guardrails_cpuutil" => Some(Box::new(GuardrailsPickOnSystemDomains::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    GuardrailsPickOnSystemDomainsValueType::CPUTIL,
                ))),
                "guardrails_energy" => Some(Box::new(GuardrailsPickOnSystemDomains::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    GuardrailsPickOnSystemDomainsValueType::ENERGY,
                ))),
                "guardrails_iatenergy" => Some(Box::new(GuardrailsPickOnSystemDomains::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    GuardrailsPickOnSystemDomainsValueType::IATENERGY,
                ))),
                "guardrails_iatcpuutil" => Some(Box::new(GuardrailsPickOnSystemDomains::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    GuardrailsPickOnSystemDomainsValueType::IATCPUUTIL,
                ))),
                "iat_consistent_hashing" => Some(Box::new(IATConsistentHashing::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    system_domains.clone(),
                ))),
                "consistent_hashing" => Some(Box::new(ConsistentHashing::new(
                    fineloadbalancing_weak.clone(),
                    FuncDomainMapper::new(system_domains.clone()),
                ))),
                "chbl" => Some(Box::new(super::CHBL::new(
                    fineloadbalancing_weak.clone(),
                    FuncDomainMapper::new(system_domains.clone()),
                    config.clone(),
                ))),
                "power_of_two" | "power_of_two_rand" => {
                    Some(Box::new(super::PowerOfTwo::new(fineloadbalancing_weak.clone())))
                },
                "power_of_two_det" => Some(Box::new(super::PowerOfTwo::new_deterministic(
                    fineloadbalancing_weak.clone(),
                ))),
                "least_loaded" => Some(Box::new(super::LeastLoaded::new(fineloadbalancing_weak.clone()))),
                "hash" => Some(Box::new(super::Hash::new(fineloadbalancing_weak.clone()))),
                "power_of_two_peek" => Some(Box::new(super::PowerOfTwoPeek::new(fineloadbalancing_weak.clone()))),
                "power_of_two_peek_det" => Some(Box::new(super::PowerOfTwoPeek::new_deterministic(
                    fineloadbalancing_weak.clone(),
                ))),
                "consistent_hashing_guardrailspick" => Some(Box::new(ConsistentHashingGuardrailsPick::new(
                    fineloadbalancing_weak.clone(),
                    config.clone(),
                    system_domains.clone(),
                ))),
                "consistent_hashing_iatrebalance" => Some(Box::new(ConsistentHashingIATRebalance::new(
                    fineloadbalancing_weak.clone(),
                    system_domains.clone(),
                ))),
                "consistent_hashing_cpuutilrebalance" => Some(Box::new(ConsistentHashingCPUUtilizationRebalance::new(
                    fineloadbalancing_weak.clone(),
                    system_domains.clone(),
                ))),
                "consistent_hashing_iatcpuutilrebalance" => {
                    Some(Box::new(ConsistentHashingIATCPUUtilizationRebalance::new(
                        fineloadbalancing_weak.clone(),
                        system_domains.clone(),
                    )))
                },
                "consistent_hashing_iatenergyrebalance" => Some(Box::new(ConsistentHashingIATEnergyRebalance::new(
                    fineloadbalancing_weak.clone(),
                    system_domains.clone(),
                ))),

                "domain_zero" | "posted_price" | "congestion_pricing" => {
                    Some(Box::new(DomainZero::new(fineloadbalancing_weak.clone())))
                },
                _ => None,
            };
            let lbpolicy = lbpolicy.unwrap();

            FineLoadBalancingStruct {
                config: config.clone(),
                cmap,

                preallocated_domains,
                system_domains,
                stats: Default::default(),
                cold_start_counts: Mutex::new(HashMap::new()),
                lbpolicy,
                cpu_core_utilization,

                core_metrics_reader,
                core_metrics_debug_counter: AtomicU64::new(0),
                workload_phase_debug_counter: AtomicU64::new(0),

                domain_operation_lock: Mutex::new(false),
                domain_release_signal: Arc::new(Notify::new()),
                cpu_queue_preview: Mutex::new(None),
                container_manager: Mutex::new(None),
            }
        })
    }
}

/// sched_ext can take a short time to report `enabled` after FineSched attaches.
/// Retry opening the reader during that transition; keep startup bounded if the
/// scheduler or its pinned map never becomes available.
fn open_core_metrics_reader_with_retry() -> Result<CoreMetricsReader> {
    const ATTEMPTS: usize = 50;
    const RETRY_DELAY: Duration = Duration::from_millis(100);

    let mut last_error = None;
    for attempt in 0..ATTEMPTS {
        match CoreMetricsReader::open() {
            Ok(reader) => return Ok(reader),
            Err(error) => {
                // Retry the observed sched_ext state transition, but don't add
                // startup latency for unrelated failures such as permissions.
                if !format!("{error:#}").contains("sched_ext is disabled") {
                    return Err(error);
                }
                last_error = Some(error);
            },
        }

        if attempt + 1 < ATTEMPTS {
            thread::sleep(RETRY_DELAY);
        }
    }

    Err(last_error.expect("at least one core metrics open attempt"))
}

impl FineLoadBalancingStruct {
    /// Sample the queue's coarse CPU workload phase at the configured rate.
    pub(crate) fn maybe_log_cpu_workload_phase(&self, tid: &TransactionId, fqdn: &str, policy: &str) {
        let sample_every = self.config.core_metrics_debug_every;
        if sample_every == 0 {
            return;
        }

        let assignment_ordinal = self.workload_phase_debug_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if assignment_ordinal % sample_every != 0 {
            return;
        }

        let phase = self.cpu_workload_phase();
        debug!(
            tid = %tid,
            fqdn,
            policy,
            phase = ?phase,
            assignment_ordinal,
            sample_every,
            "[finesched][workload-phase] sampled CPU workload phase"
        );
    }

    pub fn try_reserve_likely_cold(
        self: &Arc<Self>,
        domain_id: SchedGroupID,
        fqdn: &str,
        limit: u32,
    ) -> Option<LikelyColdPermit> {
        let key = (domain_id, fqdn.to_owned());
        let mut counts = self.cold_start_counts.lock().unwrap();
        let count = counts.entry(key.clone()).or_default();
        if *count >= limit {
            return None;
        }
        *count += 1;
        Some(LikelyColdPermit { owner: self.clone(), key })
    }

    /// Attach a read-only handle to the CPU invocation queue for lookahead policies.
    pub fn set_cpu_queue_preview(&self, queue: Weak<dyn InvokerCpuQueuePolicy>) {
        *self.cpu_queue_preview.lock().unwrap() = Some(queue);
    }

    /// Count function names among the next `limit` queued CPU invocations.
    pub fn queued_fqdn_counts(&self, limit: usize) -> HashMap<String, usize> {
        let queue = self.cpu_queue_preview.lock().unwrap().as_ref().and_then(Weak::upgrade);
        queue.map_or_else(HashMap::new, |queue| queue.preview_fqdn_counts(limit))
    }

    /// Snapshot the number of queued invocations and their summed estimated
    /// execution time. Both values are best-effort reads of the live queue.
    pub fn cpu_queue_snapshot(&self) -> Option<super::CpuQueueSnapshot> {
        let queue = self
            .cpu_queue_preview
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)?;
        Some(super::CpuQueueSnapshot {
            queued_invocations: queue.queue_len(),
            estimated_work_secs: queue.est_queue_time(),
        })
    }

    /// Peek at the next CPU invocation's function name and requested cores.
    pub fn peek_next_cpu_invocation(&self) -> Option<(String, u32)> {
        let queue = self
            .cpu_queue_preview
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)?;
        let item = queue.peek_queue()?;
        Some((item.registration.fqdn.clone(), item.registration.cpus))
    }

    /// Best-effort snapshot of the best idle CPU container state for an FQDN.
    /// `None` means the manager is unavailable; `Cold` means no idle container
    /// was visible at the time of this check, not that a cold start is certain.
    pub fn idle_cpu_container_state(&self, fqdn: &str) -> Option<ContainerState> {
        let manager = self
            .container_manager
            .lock()
            .unwrap()
            .as_ref()
            .and_then(Weak::upgrade)?;
        Some(manager.container_available(fqdn, Compute::CPU))
    }

    /// Attach a weak reference so FineSched policies can query container state.
    pub fn set_container_manager(&self, manager: Weak<ContainerManager>) {
        *self.container_manager.lock().unwrap() = Some(manager);
    }

    /// Mean `/proc/stat` busy percentage across all logical CPUs in a domain.
    /// Returns `None` until every CPU in that domain has a valid sample.
    pub fn domain_cpu_utilization_percent(&self, domain_id: SchedGroupID) -> Option<f64> {
        let group = self.preallocated_domains.get_schedgroup(domain_id)?;
        if group.cores.is_empty() {
            return None;
        }
        let samples = self.cpu_core_utilization.snapshot();
        let mut total = 0.0;
        for cpu in &group.cores {
            total += samples.get(*cpu as usize).copied().flatten()?;
        }
        Some(total / group.cores.len() as f64)
    }

    /// Mean FineSched PELT-like duty-cycle utilization across a domain's
    /// logical CPUs. Returns `None` when BPF metrics or any CPU sample is not
    /// available.
    pub fn domain_pelt_like_utilization_percent(&self, domain_id: SchedGroupID) -> Result<Option<f64>> {
        let Some(reader) = self.core_metrics_reader.as_ref() else {
            return Ok(None);
        };
        let Some(group) = self.preallocated_domains.get_schedgroup(domain_id) else {
            return Ok(None);
        };
        if group.cores.is_empty() {
            return Ok(None);
        }

        let mut total = 0.0;
        for cpu in &group.cores {
            total += reader.pelt_like_utilization_percent(*cpu)?;
        }
        Ok(Some(total / group.cores.len() as f64))
    }

    /// Sum FineSched runnable load across all logical CPUs in a domain.
    pub fn domain_runnable_load(&self, domain_id: SchedGroupID) -> Option<f64> {
        let reader = self.core_metrics_reader.as_ref()?;
        let group = self.preallocated_domains.get_schedgroup(domain_id)?;
        if group.cores.is_empty() {
            return None;
        }
        let mut total = 0u64;
        for cpu in &group.cores {
            total = total.saturating_add(reader.runnable_load(*cpu).ok()?);
        }
        Some(total as f64)
    }

    /// Snapshot FineSched PELT-like utilization for each configured domain.
    /// The vector preserves domain IDs and unavailable samples; callers can
    /// decide how to aggregate the values without confusing missing data with 0%.
    pub fn domain_pelt_like_utilization_snapshot(&self) -> Result<Vec<(SchedGroupID, Option<f64>)>> {
        self.system_domains
            .immutable_clone()
            .iter()
            .map(|domain| {
                Ok((
                    domain.schedgroup_id(),
                    self.domain_pelt_like_utilization_percent(domain.schedgroup_id())?,
                ))
            })
            .collect()
    }

    pub fn domain_pelt_and_cold_snapshot(&self) -> Result<Vec<super::DomainPeltAndColdSnapshot>> {
        let cold = self.cold_start_counts.lock().unwrap();
        self.system_domains
            .immutable_clone()
            .iter()
            .map(|domain| {
                let domain_id = domain.schedgroup_id();
                let likely_cold_by_fqdn = cold
                    .iter()
                    .filter(|((id, _), _)| *id == domain_id)
                    .map(|((_, fqdn), count)| (fqdn.clone(), *count))
                    .collect();
                Ok(super::DomainPeltAndColdSnapshot {
                    domain_id,
                    pelt_utilization_percent: self.domain_pelt_like_utilization_percent(domain_id)?,
                    likely_cold_by_fqdn,
                })
            })
            .collect()
    }

    /// Sum the most recently sampled physical-core power values in a domain.
    pub fn domain_power_watts(&self, domain_id: SchedGroupID) -> Option<f64> {
        let reader = self.core_metrics_reader.as_ref()?;
        let group = self.preallocated_domains.get_schedgroup(domain_id)?;
        reader.cached_power_for_cpus(&group.cores).ok().flatten()
    }

    /// Emit a low-rate diagnostic for a core in the assigned domain. Samples
    /// rotate through the domain's CPUs to avoid a random-number generator in
    /// the invocation path while still covering every core over time.
    pub(crate) fn maybe_log_core_metrics(&self, tid: &TransactionId, fqdn: &str, domain_id: SchedGroupID) {
        let sample_every = self.config.core_metrics_debug_every;
        if sample_every == 0 {
            return;
        }

        let assignment_ordinal = self.core_metrics_debug_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if assignment_ordinal % sample_every != 0 {
            return;
        }

        let domain_scheduled_invocations = self.stats.domain_map.get(&domain_id)
            .map(|stats| stats.scheduled_invocations.load(Ordering::Relaxed))
            .unwrap_or_default();
        let domain_used_cores = self.system_domains.immutable_clone().get(domain_id as usize)
            .map(|domain| domain.used_cores())
            .unwrap_or_default();
        let cpu_queue_snapshot = self.cpu_queue_snapshot();
        let cpu_queue_queued_invocations = cpu_queue_snapshot.map(|snapshot| snapshot.queued_invocations);
        let cpu_queue_estimated_work_secs = cpu_queue_snapshot.map(|snapshot| snapshot.estimated_work_secs);

        let Some(reader) = self.core_metrics_reader.as_ref() else {
            return;
        };
        let Some(group) = self.preallocated_domains.get_schedgroup(domain_id) else {
            warn!(
                tid = %tid,
                fqdn,
                domain_id,
                assignment_ordinal,
                "[finesched][core-metrics] assigned domain does not exist"
            );
            return;
        };
        if group.cores.is_empty() {
            warn!(
                tid = %tid,
                fqdn,
                domain_id,
                assignment_ordinal,
                "[finesched][core-metrics] assigned domain has no CPUs"
            );
            return;
        }

        let sample_ordinal = assignment_ordinal / sample_every;
        let core_index = (sample_ordinal.saturating_sub(1) as usize) % group.cores.len();
        let cpu = group.cores[core_index];
        let (domain_core_power_watts, domain_power_watts) = match reader.power_for_cpus(&group.cores) {
            Ok(power) => power,
            Err(error) => {
                warn!(
                    tid = %tid,
                    fqdn,
                    domain_id,
                    error = %error,
                    "[finesched][power] failed to sample domain power"
                );
                (Vec::new(), None)
            },
        };
        match reader.snapshot(cpu) {
            Ok(snapshot) => debug!(
                tid = %tid,
                fqdn,
                assignment_ordinal,
                sample_every,
                domain_id,
                cpu = snapshot.cpu,
                domain_scheduled_invocations,
                domain_used_cores,
                cpu_queue_queued_invocations = ?cpu_queue_queued_invocations,
                cpu_queue_estimated_work_secs = ?cpu_queue_estimated_work_secs,
                local_dsq_len = snapshot.local_dsq_len,
                domain_dsq_len = snapshot.domain_dsq_len,
                dsq_len = snapshot.dsq_len,
                runnable_load = snapshot.runnable_load,
                utilization_percent = snapshot.utilization_percent,
                average_utilization_percent = snapshot.average_utilization_percent,
                domain_proc_stat_utilization_percent = self.domain_cpu_utilization_percent(domain_id),
                frequency_mhz = snapshot.frequency_mhz,
                power_watts = ?snapshot.power_watts,
                domain_core_power_watts = ?domain_core_power_watts,
                domain_power_watts = ?domain_power_watts,
                pelt_like_utilization_percent = snapshot.pelt_like_utilization_percent,
                domain_pelt_like_utilization_percent = ?self.domain_pelt_like_utilization_percent(domain_id),
                idle_while_loaded_ns = snapshot.idle_while_loaded_ns,
                idle_with_queued_events = snapshot.idle_with_queued_events,
                dispatch_calls = snapshot.dispatch_calls,
                dispatch_misses = snapshot.dispatch_misses,
                steal_attempts = snapshot.steal_attempts,
                steal_successes = snapshot.steal_successes,
                enq_latency_hist = ?snapshot.enq_latency_hist,
                "[finesched][core-metrics] sampled assigned domain"
            ),
            Err(error) => warn!(
                tid = %tid,
                fqdn,
                domain_id,
                cpu,
                assignment_ordinal,
                error = %error,
                "[finesched][core-metrics] failed to sample assigned domain"
            ),
        }
    }
}

pub struct LikelyColdPermit {
    owner: FineLoadBalancing,
    key: (SchedGroupID, String),
}

impl Drop for LikelyColdPermit {
    fn drop(&mut self) {
        let mut counts = self.owner.cold_start_counts.lock().unwrap();
        if let Some(count) = counts.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&self.key);
            }
        }
    }
}

////////////////////////////////////
/// Always assign Domain 0
pub struct DomainZero {
    fineloadbalancing: FineLoadBalancingWeak,
}

impl DomainZero {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak) -> Self {
        DomainZero { fineloadbalancing }
    }
}

impl LoadBalancingPolicyTrait for DomainZero {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        let fqdn = reg.fqdn.as_str();
        let stats = self.fineloadbalancing.upgrade().unwrap().stats.clone();
        let scheduled_invocations = &stats.domain_map.get_or_create(&0).scheduled_invocations;

        debug!( tid=%tid, fqdn=%fqdn, lbpolicy=%"domain_zero", scheduled_invocations=%scheduled_invocations.load(Ordering::Relaxed), "[finesched] assign_domain_to_function_request");

        return Some(0);
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let fqdn = reg.fqdn.as_str();
        let stats = self.fineloadbalancing.upgrade().unwrap().stats.clone();
        let scheduled_invocations = &stats.domain_map.get_or_create(&0).scheduled_invocations;

        debug!( tid=%tid, fqdn=%fqdn, lbpolicy=%"domain_zero", scheduled_invocations=%scheduled_invocations.load(Ordering::Relaxed), "[finesched] assign_domain_to_function_request");
    }
}

////////////////////////////////////
/// Guardrails

type GRRankID = u32;

#[derive(Default)]
pub struct GRRankStats {
    sched_domain_counters: Mutex<Vec<u32>>,
}

pub struct Guardrails {
    fineloadbalancing: FineLoadBalancingWeak,

    tightness: u32,  // g
    log_base_c: u32, // c, execution time cutoff in ms > 2

    rank_stats_map: ArcMap<GRRankID, GRRankStats>,
}

impl Guardrails {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, config: Arc<FineLoadBalancingConfig>) -> Self {
        Guardrails {
            fineloadbalancing,

            tightness: config.guardrails_tightness,
            log_base_c: config.guardrails_log_base_c,
            rank_stats_map: ArcMap::new(),
        }
    }

    fn execution_duration_to_rank(&self, dur_ms: u32) -> u32 {
        if dur_ms == 0 {
            return 0;
        }

        dur_ms.ilog2() / self.log_base_c.ilog2()
    }

    fn expand_counters(&self, sched_domain_counters: &mut Vec<u32>, max_domain_id: SchedGroupID) {
        while sched_domain_counters.len() < max_domain_id as usize {
            sched_domain_counters.push(0);
        }
    }

    fn get_rank_stats(&self, rank: GRRankID) -> Arc<GRRankStats> {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();

        match self.rank_stats_map.get(&rank) {
            Some(stats) => stats,
            None => {
                for lrank in self.rank_stats_map.len()..(rank as usize + 1) {
                    let lrank = lrank as GRRankID;

                    let rank_stats = self.rank_stats_map.get_or_create(&lrank);
                    let mut sched_domain_counters = rank_stats.sched_domain_counters.lock().unwrap();
                    self.expand_counters(
                        &mut sched_domain_counters,
                        fineloadbalancing.preallocated_domains.total_groups() as SchedGroupID,
                    );
                }

                self.rank_stats_map.get(&rank).unwrap()
            },
        }
    }

    fn set_of_safe_domains(&self, dur_ms: u32, domain_set: &Vec<SchedGroupID>) -> Vec<SchedGroupID> {
        let rank = self.execution_duration_to_rank(dur_ms);
        let rank_stats = self.get_rank_stats(rank);
        let sched_domain_counters = rank_stats.sched_domain_counters.lock().unwrap();
        let select_counters: Vec<u32> = domain_set
            .iter()
            .map(|domain_id: &SchedGroupID| sched_domain_counters[*domain_id as usize])
            .collect();

        let rank_min_counter = *select_counters.iter().min().unwrap();

        let is_safe_domain = move |arg: &(usize, &u32)| {
            let (_index, domain_counter) = arg;
            **domain_counter + dur_ms <= rank_min_counter + self.tightness * self.log_base_c.pow(rank + 1)
        };

        let to_domain_id = |arg: (usize, &u32)| {
            let (index, _domain_counter) = arg;
            domain_set[index] as SchedGroupID
        };

        select_counters
            .iter()
            .enumerate()
            .filter(is_safe_domain)
            .map(to_domain_id)
            .collect()
    }

    pub fn guardrails_pick(&self, dur_ms: u32, domain_set: &Vec<SchedGroupID>) -> SchedGroupID {
        let safe_domains = self.set_of_safe_domains(dur_ms, domain_set);
        assert!(safe_domains.len() != 0);

        let domain_id: SchedGroupID = safe_domains[0];

        let rank = self.execution_duration_to_rank(dur_ms);
        let rank_stats = self.get_rank_stats(rank);
        let mut sched_domain_counters = rank_stats.sched_domain_counters.lock().unwrap();
        sched_domain_counters[domain_id as usize] += dur_ms;

        debug!( execution_dur_ms=%dur_ms, rank=%rank, domain_id=%domain_id, safe_domains=?safe_domains, sched_domain_counters=?sched_domain_counters, "[finesched][guardrails] guardrails pick");

        domain_id
    }

    fn reset_domain(&self, domain_id: SchedGroupID) {
        let total_ranks = self.rank_stats_map.len();
        for rank in 0..total_ranks {
            let rank = rank as GRRankID;
            let rank_stats = self.get_rank_stats(rank);

            let mut sched_domain_counters = rank_stats.sched_domain_counters.lock().unwrap();

            sched_domain_counters[domain_id as usize] = *sched_domain_counters.iter().min().unwrap();
        }
    }

    pub fn return_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let fqdn = reg.fqdn.as_str();
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let stats = fineloadbalancing.stats.clone();

        let domain_id = match stats.tid_map.get(tid) {
            Some(entry) => *entry,
            None => {
                error!(tid=%tid, "[finesched] no domain found for tid in tid_stats map");
                return;
            },
        };

        let scheduled_invocations = &stats.domain_map.get_or_create(&domain_id).scheduled_invocations;
        if scheduled_invocations.load(Ordering::Relaxed) == 1 {
            debug!( tid=%tid, fqdn=%fqdn, domain_id=%domain_id, "[finesched][guardrails] domain reset");
            self.reset_domain(domain_id);
        }
    }
}

impl LoadBalancingPolicyTrait for Guardrails {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        let fqdn = reg.fqdn.as_str();
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let system_domains = fineloadbalancing.system_domains.immutable_clone();
        let domain_set: Vec<SchedGroupID> = system_domains.iter().map(|domain| domain.schedgroup_id()).collect();

        let execution_dur_ms = (fineloadbalancing.cmap.get(fqdn, Chars::CpuWarmTime, Value::Avg) * 1000.0) as u32;
        let domain_id = self.guardrails_pick(execution_dur_ms, &domain_set);

        debug!( tid=%tid, fqdn=%fqdn, domain_id=%domain_id, "[finesched][guardrails] picked domain");

        Some(domain_id)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.return_domain(tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.return_domain(tid, reg.clone());
    }
}

// Consistent hashing
pub type DomainId = usize;

#[derive(Debug)]
struct DomainMutableData {
    used_cores: u32,
    assigned_to_funcs: Vec<String>,
}

impl DomainMutableData {
    // XXX 1411 does not allow overcommit at all!?
    pub fn acquire_cores(&mut self, requested_cores: u32, assigned_cores: u32) -> bool {
        // XXX 1411: Why this double mutation instead of temp var?
        self.used_cores += requested_cores;
        if self.used_cores <= assigned_cores {
            debug!( tag=%"cpu_release_issue", lbpolicy=%"consistent_hashing", used_cores=%self.used_cores, "[finesched] domain acquired");
            return true;
        }

        self.used_cores -= requested_cores;
        return false;
    }

    pub fn acquire_cores_oc(&mut self, requested_cores: u32) -> bool {
        self.used_cores += requested_cores;
        return true;
    }

    // overcommit ok
}

#[derive(Debug)]
pub struct DomainStruct {
    id: DomainId,
    pub assigned_cores: u32,
    mut_data: Mutex<DomainMutableData>,
}

impl DomainStruct {
    pub fn new(id: DomainId, assigned_cores: u32) -> Self {
        Self {
            id,
            assigned_cores,
            mut_data: Mutex::new(DomainMutableData {
                used_cores: 0,
                assigned_to_funcs: vec![],
            }),
        }
    }

    pub fn schedgroup_id(&self) -> SchedGroupID {
        self.id as SchedGroupID
    }

    pub fn domain_id(&self) -> DomainId {
        self.id as DomainId
    }

    pub fn available(&self) -> bool {
        self.mut_data.lock().unwrap().used_cores < self.assigned_cores
    }

    pub fn used_cores(&self) -> u32 {
        self.mut_data.lock().unwrap().used_cores
    }

    /// Atomically acquire `requested_cores` on this domain only if `used_cores + requested_cores <= assigned_cores`.
    pub fn try_acquire_cores(&self, requested_cores: u32) -> bool {
        let mut data = self.mut_data.lock().unwrap();
        if data.used_cores.saturating_add(requested_cores) <= self.assigned_cores {
            data.used_cores += requested_cores;
            true
        } else {
            false
        }
    }

    pub fn assigned_funcs(&self) -> Vec<String> {
        self.mut_data.lock().unwrap().assigned_to_funcs.clone()
    }

    pub fn acquire_cores_oc(&self, requested_cores: u32) {
        let mut data = self.mut_data.lock().unwrap();
        data.used_cores += requested_cores;
        // not pushing func name to the domain..either make this a hashmap with counts or something else ... perhaps along with workload context. or in characteristics map?
    }

    // XXX 1411: Should be giving tokens instead of functions "acquiring" domains.
    pub fn can_serve_and_acquire_empty_or_assigned(&self, func_name: &String, cpus: u32) -> Result<()> {
        let mut d = self.mut_data.lock().unwrap();

        if d.assigned_to_funcs.len() == 0 {
            if d.acquire_cores(cpus, self.assigned_cores) {
                d.assigned_to_funcs.push(func_name.clone());
                return Ok(());
            }
        }
        // XXX 1411: surely a more compact way in Rust!?
        if let Some(_index) = d
            .assigned_to_funcs
            .iter()
            .position(|assigned_func| assigned_func == func_name)
        {
            if d.acquire_cores(cpus, self.assigned_cores) {
                return Ok(());
            }
        }

        bail!("domain not acquired")
    }

    pub fn can_serve_and_acquire_append(&self, func_name: &String, cpus: u32) -> Result<()> {
        let mut d = self.mut_data.lock().unwrap();

        if d.acquire_cores(cpus, self.assigned_cores) {
            d.assigned_to_funcs.push(func_name.clone());
            return Ok(());
        }

        bail!("domain not acquired")
    }

    pub fn return_cpus_and_release(&self, cpus: u32) {
        let mut d = self.mut_data.lock().unwrap();

        d.used_cores = d.used_cores.saturating_sub(cpus);
        debug!( tag=%"cpu_release_issue", lbpolicy=%"consistent_hashing", domain_id=%self.id, used_cores=%d.used_cores, "[finesched] domain released");
        if d.used_cores == 0 {
            d.assigned_to_funcs.clear();
            debug!( lbpolicy=%"consistent_hashing", domain_id=%self.id, "[finesched] domain released");
        }
    }

    pub fn return_cpus_and_release_func(&self, func_name: &str, cpus: u32) {
        let mut d = self.mut_data.lock().unwrap();
        d.used_cores -= cpus;
        if let Some(index) = d.assigned_to_funcs.iter().position(|assigned| assigned == func_name) {
            d.assigned_to_funcs.remove(index);
        }
        if d.used_cores == 0 {
            d.assigned_to_funcs.clear();
        }
    }
}

impl PartialEq for DomainStruct {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

pub(super) type Domain = DomainStruct;

pub(super) fn dump_domains(domains: &Vec<Arc<Domain>>) -> String {
    let mut dump = "".to_string();
    for domain in domains.iter() {
        dump += format!("(id: {}, used_cores: {})", domain.domain_id(), domain.used_cores()).as_str();
    }
    dump
}

pub(super) fn dump_domain(domain: &Domain) -> String {
    let mut dump = "".to_string();
    dump += format!("(id: {}, used_cores: {}", domain.domain_id(), domain.used_cores()).as_str();
    dump += ", assigned_to_funcs: {";
    for func in domain.assigned_funcs() {
        dump += format!("{},", func).as_str();
    }
    dump += "}";
    dump
}

pub trait DomainMapperTrait {
    fn get(&self, reg: Arc<RegisteredFunction>) -> Vec<Arc<Domain>>;
    fn expand_preferred_set(&self, reg: Arc<RegisteredFunction>) -> Vec<Arc<Domain>>;
    // XXX 1411 what does this do?
    fn rebalance_domains(&self, rebalance_logic: RebalanceClosure);
}

pub(super) type DomainMapper = Arc<dyn DomainMapperTrait + Sync + Send>;

pub(super) type RebalanceClosure<'a> =
    &'a (dyn Fn(
        ArcMap<String, ArcVec<Domain>>,
        ArcMap<DomainId, ArcVec<String>>,
    ) -> (ArcMap<String, ArcVec<Domain>>, ArcMap<DomainId, ArcVec<String>>));

struct FuncDomainMapper {
    domains: ArcVec<Domain>,

    func_domains_map: ArcMap<String, ArcVec<Domain>>,
    domains_func_map: ArcMap<DomainId, ArcVec<String>>,
}

impl FuncDomainMapper {
    pub fn new(domains: ArcVec<Domain>) -> DomainMapper {
        Arc::new(Self {
            domains,

            func_domains_map: ArcMap::new(),
            domains_func_map: ArcMap::new(),
        })
    }

    // XXX 1411: What is this doing with shared_counts etc!? Infinite loop risk etc?!
    fn pick_next_domain(&self, starting_id: DomainId, excluding_set: Vec<Arc<Domain>>) -> Arc<Domain> {
        let domains = self.domains.immutable_clone();

        let total_domains = domains.len();
        let not_in_excluding_set = |domain_id| {
            !excluding_set
                .iter()
                .any(|excluded_domain| excluded_domain.domain_id() == domain_id)
        };
        let next_domain_id = |domain_id| (domain_id + 1) % total_domains;
        let mut domain_id = next_domain_id(starting_id);
        let mut shared_count = 0;

        loop {
            let assigned_to_funcs = self.domains_func_map.get_or_create(&domain_id);

            if assigned_to_funcs.immutable_clone().len() < shared_count && not_in_excluding_set(domain_id) {
                return domains[domain_id].clone();
            }

            if domain_id == starting_id {
                shared_count += 1;
            }
            domain_id = next_domain_id(domain_id);
        }
    }
}

impl DomainMapperTrait for FuncDomainMapper {
    fn get(&self, reg: Arc<RegisteredFunction>) -> Vec<Arc<Domain>> {
        let func_name = &reg.fqdn;
        if self.func_domains_map.get(func_name).is_some() {
            return self.func_domains_map.get(func_name).unwrap().immutable_clone();
        }

        self.expand_preferred_set(reg)
    }

    fn expand_preferred_set(&self, reg: Arc<RegisteredFunction>) -> Vec<Arc<Domain>> {
        let func_name = &reg.fqdn;
        let domain_set: Arc<ArcVec<Domain>> = self.func_domains_map.get_or_create(func_name);
        let domain_id = domain_set
            .immutable_clone()
            .iter()
            .map(|domain| domain.domain_id())
            .last()
            .unwrap_or(0);

        let domain_set_clone = domain_set.immutable_clone();
        if domain_set_clone.len() < self.domains.immutable_clone().len() {
            let domain = self.pick_next_domain(domain_id, domain_set_clone);
            self.domains_func_map
                .get_or_create(&domain.domain_id())
                .push(func_name.clone());
            domain_set.push_arc(domain);
        }

        domain_set.immutable_clone()
    }

    fn rebalance_domains(&self, rebalance_logic: RebalanceClosure) {
        let (func_domains_map, domains_func_map) =
            rebalance_logic(self.func_domains_map.clone(), self.domains_func_map.clone());

        self.func_domains_map.clear();
        for (key, value) in func_domains_map.immutable_clone().iter() {
            self.func_domains_map.insert_arc(key.clone(), value.clone());
        }

        self.domains_func_map.clear();
        for (key, value) in domains_func_map.immutable_clone().iter() {
            self.domains_func_map.insert_arc(key.clone(), value.clone());
        }
    }
}

type BucketID = usize;
struct IATDomainMapper {
    fineloadbalancing: FineLoadBalancingWeak,
    domains: ArcVec<Domain>,

    bucket_size: usize,
    bucket_domains_map: ArcMap<BucketID, ArcVec<Domain>>,
    domains_bucket_map: ArcMap<DomainId, ArcVec<BucketID>>,
}

impl IATDomainMapper {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, domains: ArcVec<Domain>, bucket_size: usize) -> DomainMapper {
        Arc::new(Self {
            fineloadbalancing,
            domains,

            bucket_size,
            bucket_domains_map: ArcMap::new(),
            domains_bucket_map: ArcMap::new(),
        })
    }

    fn pick_next_domain(&self, starting_id: DomainId, excluding_set: Vec<Arc<Domain>>) -> Arc<Domain> {
        let domains = self.domains.immutable_clone();

        let total_domains = domains.len();
        let not_in_excluding_set = |domain_id| {
            !excluding_set
                .iter()
                .any(|excluded_domain| excluded_domain.domain_id() == domain_id)
        };
        let next_domain_id = |domain_id| (domain_id + 1) % total_domains;
        let mut domain_id = next_domain_id(starting_id);
        let mut shared_count = 0;

        loop {
            let assigned_to_buckets = self.domains_bucket_map.get_or_create(&domain_id);

            if assigned_to_buckets.immutable_clone().len() < shared_count && not_in_excluding_set(domain_id) {
                return domains[domain_id].clone();
            }

            if domain_id == starting_id {
                shared_count += 1;
            }
            domain_id = next_domain_id(domain_id);
        }
    }

    fn bucket(&self, func_name: &String) -> BucketID {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let iat_ms = fineloadbalancing.cmap.get(func_name, Chars::IAT, Value::Avg) * 1000.0;
        let bucket_size = self.bucket_size as f64;

        (iat_ms / bucket_size) as BucketID
    }
}

impl DomainMapperTrait for IATDomainMapper {
    fn get(&self, reg: Arc<RegisteredFunction>) -> Vec<Arc<Domain>> {
        let func_name = &reg.fqdn;
        let bucket = self.bucket(func_name);

        if self.bucket_domains_map.get(&bucket).is_some() {
            return self.bucket_domains_map.get(&bucket).unwrap().immutable_clone();
        }

        self.expand_preferred_set(reg)
    }

    fn expand_preferred_set(&self, reg: Arc<RegisteredFunction>) -> Vec<Arc<Domain>> {
        let func_name = &reg.fqdn;
        let bucket = self.bucket(func_name);
        let system_domain_count = self.domains.immutable_clone().len();

        let domain_set: Arc<ArcVec<Domain>> = self.bucket_domains_map.get_or_create(&bucket);
        let domain_id = domain_set
            .immutable_clone()
            .iter()
            .map(|domain| domain.domain_id())
            .last()
            .unwrap_or(system_domain_count - 1);

        let domain_set_clone = domain_set.immutable_clone();
        if domain_set_clone.len() < system_domain_count {
            let domain = self.pick_next_domain(domain_id, domain_set_clone);
            self.domains_bucket_map.get_or_create(&domain.domain_id()).push(bucket);
            domain_set.push_arc(domain);
        }

        domain_set.immutable_clone()
    }

    fn rebalance_domains(&self, _rebalance_logic: RebalanceClosure) {}
}

//////

pub struct ConsistentHashing {
    fineloadbalancing: FineLoadBalancingWeak,

    domain_mapper: DomainMapper,
}

pub trait SelectDomain {
    fn pick_domain_from_set(&self, reg: Arc<RegisteredFunction>, domains: &Vec<Arc<Domain>>) -> Option<Arc<Domain>>;
}

impl SelectDomain for ConsistentHashing {
    /// Pick first acquirable domain from the set.
    fn pick_domain_from_set(&self, reg: Arc<RegisteredFunction>, domains: &Vec<Arc<Domain>>) -> Option<Arc<Domain>> {
        let func_name = &reg.fqdn;
        let requested_cores = reg.cpus;
        let acquire_domain = |domain: &Arc<Domain>| {
            if domain.can_serve_and_acquire_append(func_name, requested_cores).is_ok() {
                debug!( lbpolicy=%"consistent_hashing", domain_assigned=%dump_domain(&domain), "[finesched] assign_domain_to_function_request");
                return Some(domain.clone());
            }
            None
        };

        let least_loaded_domain = domains
            .iter()
            .map(|domain| domain.used_cores())
            .enumerate()
            .map(|(i, used_cores)| (used_cores, i))
            .min()
            .unwrap();
        let least_loaded_domain = &domains[least_loaded_domain.1];
        if let Some(domain) = acquire_domain(least_loaded_domain) {
            return Some(domain);
        }

        for domain in domains.iter() {
            if let Some(domain) = acquire_domain(domain) {
                return Some(domain);
            }
        }
        None
    }
}

impl ConsistentHashing {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, domain_mapper: DomainMapper) -> Self {
        ConsistentHashing {
            fineloadbalancing,
            domain_mapper,
        }
    }

    pub fn rebalance_domains(&self, rebalance_domains: RebalanceClosure) {
        self.domain_mapper.rebalance_domains(rebalance_domains)
    }

    pub fn pick_domain(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
        domains_selection: &dyn SelectDomain,
    ) -> Option<SchedGroupID> {
        let func_name = &reg.fqdn;

        let preferred_domains = self.domain_mapper.get(reg.clone());
        debug!( tid=%tid, lbpolicy=%"consistent_hashing", fqdn=%func_name, preferred_domains=%dump_domains(&preferred_domains), "[finesched] assign_domain_to_function_request");

        let mut domain;
        domain = domains_selection.pick_domain_from_set(reg.clone(), &preferred_domains);
        if domain.is_some() {
            debug!( tid=%tid, lbpolicy=%"consistent_hashing", fqdn=%func_name, domain_assigned=%dump_domain(&domain.as_ref().unwrap()), "[finesched] assign_domain_to_function_request");
            return Some(domain.unwrap().schedgroup_id());
        }

        let preferred_domains = self.domain_mapper.expand_preferred_set(reg.clone());
        domain = domains_selection.pick_domain_from_set(reg.clone(), &preferred_domains);
        if domain.is_some() {
            debug!( tid=%tid, lbpolicy=%"consistent_hashing", fqdn=%func_name, domain_assigned=%dump_domain(&domain.as_ref().unwrap()), "[finesched] assign_domain_to_function_request");
            return Some(domain.unwrap().schedgroup_id());
        }

        None
    }

    // release resource after func completion..
    pub fn return_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let stats = fineloadbalancing.stats.clone();
        let domain_id = *stats.tid_map.get(tid).unwrap() as DomainId;
        let domain = &fineloadbalancing.system_domains.immutable_clone()[domain_id];
        domain.return_cpus_and_release(reg.cpus);
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashing {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.pick_domain(tid, reg.clone(), self)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.return_domain(tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.return_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.fineloadbalancing, reg, from, to)
    }
}

////////

// Consistent hashing with Guardrails pick
pub struct ConsistentHashingGuardrailsPick {
    fineloadbalancing: FineLoadBalancingWeak,

    consistent_hashing: ConsistentHashing,
    guardrails: Guardrails,
}

impl ConsistentHashingGuardrailsPick {
    pub fn new(
        fineloadbalancing: FineLoadBalancingWeak,
        config: Arc<FineLoadBalancingConfig>,
        domains: ArcVec<Domain>,
    ) -> Self {
        Self {
            fineloadbalancing: fineloadbalancing.clone(),

            consistent_hashing: ConsistentHashing::new(
                fineloadbalancing.clone(),
                FuncDomainMapper::new(domains.clone()),
            ),
            guardrails: Guardrails::new(fineloadbalancing.clone(), config.clone()),
        }
    }
}

impl SelectDomain for ConsistentHashingGuardrailsPick {
    /// Pick a fair acquirable domain from the set using Guardrails.
    fn pick_domain_from_set(&self, reg: Arc<RegisteredFunction>, domains: &Vec<Arc<Domain>>) -> Option<Arc<Domain>> {
        let func_name = &reg.fqdn;
        let requested_cores = reg.cpus;
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();

        let domain_set: Vec<SchedGroupID> = domains.iter().map(|domain| domain.schedgroup_id()).collect();
        let sys_domains = fineloadbalancing.system_domains.immutable_clone();

        let dur_ms = (fineloadbalancing.cmap.get(func_name, Chars::CpuExecTime, Value::Avg) * 1000.0) as u32;
        let domain_id = self.guardrails.guardrails_pick(dur_ms, &domain_set);
        let domain = &sys_domains[domain_id as usize];
        if domain.can_serve_and_acquire_append(func_name, requested_cores).is_ok() {
            return Some(domain.clone());
        }

        None
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashingGuardrailsPick {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.consistent_hashing.pick_domain(tid, reg.clone(), self)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistent_hashing.return_domain(tid, reg.clone());
        self.guardrails.return_domain(tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistent_hashing.return_domain(tid, reg.clone());
        self.guardrails.return_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.fineloadbalancing, reg, from, to)
    }
}

struct IATConsistentHashing {
    consistent_hashing: ConsistentHashing,
}

impl IATConsistentHashing {
    pub fn new(
        fineloadbalancing: FineLoadBalancingWeak,
        config: Arc<FineLoadBalancingConfig>,
        domains: ArcVec<Domain>,
    ) -> Self {
        let iat_to_domain_mapper =
            IATDomainMapper::new(fineloadbalancing.clone(), domains.clone(), config.iat_bucket_size);

        Self {
            consistent_hashing: ConsistentHashing::new(fineloadbalancing.clone(), iat_to_domain_mapper),
        }
    }
}

impl LoadBalancingPolicyTrait for IATConsistentHashing {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.consistent_hashing
            .pick_domain(tid, reg.clone(), &self.consistent_hashing)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistent_hashing.return_domain(tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistent_hashing.return_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.consistent_hashing.fineloadbalancing, reg, from, to)
    }
}

// Consistent hashing with Custom rebalance
type CustomRebalanceValue = Arc<dyn Fn(&String) -> u32 + Sync + Send>;

struct ConsistentHashingCustomRebalance {
    fineloadbalancing: FineLoadBalancingWeak,

    rebalance_since_request: AtomicU32,
    rebalance_since_request_limit: AtomicU32,

    rebalance_value: CustomRebalanceValue,
    consistent_hashing: ConsistentHashing,
}

impl ConsistentHashingCustomRebalance {
    pub fn new(
        fineloadbalancing: FineLoadBalancingWeak,
        domains: ArcVec<Domain>,
        rebalance_value: CustomRebalanceValue,
    ) -> Self {
        let domain_count = domains.immutable_clone().len() as u32;

        Self {
            fineloadbalancing: fineloadbalancing.clone(),

            rebalance_since_request: AtomicU32::new(0),
            rebalance_since_request_limit: AtomicU32::new(2 * domain_count),

            rebalance_value,
            consistent_hashing: ConsistentHashing::new(
                fineloadbalancing.clone(),
                FuncDomainMapper::new(domains.clone()),
            ),
        }
    }

    fn rebalance_domains(&self) {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let system_domains = fineloadbalancing.system_domains.immutable_clone();
        let total_domains = system_domains.iter().count();

        let custom_rebalance = |func_domains_map: ArcMap<String, ArcVec<Domain>>,
                                _domains_func_map: ArcMap<DomainId, ArcVec<String>>|
         -> (ArcMap<String, ArcVec<Domain>>, ArcMap<DomainId, ArcVec<String>>) {
            let func_domains_map = func_domains_map.immutable_clone();
            let balanced_func_domains_map: ArcMap<String, ArcVec<Domain>> = ArcMap::new();
            let balanced_domains_func_map: ArcMap<DomainId, ArcVec<String>> = ArcMap::new();

            let mut ascending_funcs: Vec<String> = vec![];
            let index_to_domain_id = |index: usize| index % total_domains;

            let mut funcs_values = vec![];
            for (func, _domains) in func_domains_map.iter() {
                let value = (*self.rebalance_value)(func);
                funcs_values.push((value, func.clone()));
            }
            funcs_values.sort();
            if funcs_values.len() > total_domains {
                funcs_values[0..total_domains]
                    .iter()
                    .for_each(|(_value, func)| ascending_funcs.push(func.clone()));
                funcs_values[total_domains..]
                    .iter()
                    .rev()
                    .for_each(|(_value, func)| ascending_funcs.push(func.clone()));
            } else {
                funcs_values
                    .iter()
                    .for_each(|(_value, func)| ascending_funcs.push(func.clone()));
            }

            for (i, func) in ascending_funcs.iter().enumerate() {
                let domain_id = index_to_domain_id(i);
                balanced_domains_func_map.get_or_create(&domain_id).push(func.clone());
                balanced_func_domains_map
                    .get_or_create(&func)
                    .push_arc(system_domains[domain_id].clone());
            }

            (balanced_func_domains_map, balanced_domains_func_map)
        };
        self.consistent_hashing.rebalance_domains(&custom_rebalance);
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashingCustomRebalance {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        if self.rebalance_since_request.fetch_add(1, Ordering::Relaxed)
            >= self.rebalance_since_request_limit.load(Ordering::Relaxed)
        {
            self.rebalance_domains();
            self.rebalance_since_request.store(0, Ordering::Relaxed);
        }
        self.consistent_hashing
            .pick_domain(tid, reg.clone(), &self.consistent_hashing)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistent_hashing.return_domain(tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistent_hashing.return_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.fineloadbalancing, reg, from, to)
    }
}

// Consistent hashing with IAT rebalance
pub struct ConsistentHashingIATRebalance {
    consistenthashing_customrebalance: ConsistentHashingCustomRebalance,
}

impl ConsistentHashingIATRebalance {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, domains: ArcVec<Domain>) -> Self {
        let fineloadbalancing_weak = fineloadbalancing.clone();
        let iat_for_func = move |func: &String| -> u32 {
            let fineloadbalancing = fineloadbalancing_weak.upgrade().unwrap();
            let cmap = fineloadbalancing.cmap.clone();

            (cmap.get(func, Chars::IAT, Value::Avg) * 1000.0) as u32
        };

        Self {
            consistenthashing_customrebalance: ConsistentHashingCustomRebalance::new(
                fineloadbalancing.clone(),
                domains,
                Arc::new(iat_for_func),
            ),
        }
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashingIATRebalance {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.consistenthashing_customrebalance
            .assign_domain_to_function_request(tid, reg.clone())
    }

    fn invoke_is_complete(&self, cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance
            .invoke_is_complete(cgroup_id, tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance.release_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.consistenthashing_customrebalance.fineloadbalancing, reg, from, to)
    }
}

// Consistent hashing with CPU Utilization rebalance
pub struct ConsistentHashingCPUUtilizationRebalance {
    consistenthashing_customrebalance: ConsistentHashingCustomRebalance,
}

impl ConsistentHashingCPUUtilizationRebalance {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, domains: ArcVec<Domain>) -> Self {
        let fineloadbalancing_weak = fineloadbalancing.clone();
        let cpuutil_for_func = move |func: &String| -> u32 {
            let fineloadbalancing = fineloadbalancing_weak.upgrade().unwrap();
            let cmap = fineloadbalancing.cmap.clone();

            (cmap.get(func, Chars::CPUtil, Value::Avg) * 1000.0) as u32
        };

        Self {
            consistenthashing_customrebalance: ConsistentHashingCustomRebalance::new(
                fineloadbalancing.clone(),
                domains,
                Arc::new(cpuutil_for_func),
            ),
        }
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashingCPUUtilizationRebalance {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.consistenthashing_customrebalance
            .assign_domain_to_function_request(tid, reg.clone())
    }

    fn invoke_is_complete(&self, cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance
            .invoke_is_complete(cgroup_id, tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance.release_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.consistenthashing_customrebalance.fineloadbalancing, reg, from, to)
    }
}

// Consistent hashing with IAT, CPU Utilization hybrid rebalance
pub struct ConsistentHashingIATCPUUtilizationRebalance {
    consistenthashing_customrebalance: ConsistentHashingCustomRebalance,
}

impl ConsistentHashingIATCPUUtilizationRebalance {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, domains: ArcVec<Domain>) -> Self {
        let fineloadbalancing_weak = fineloadbalancing.clone();
        let cpuutil_for_func = move |func: &String| -> u32 {
            let fineloadbalancing = fineloadbalancing_weak.upgrade().unwrap();
            let cmap = fineloadbalancing.cmap.clone();

            let iat = cmap.get(func, Chars::IAT, Value::Avg) * 1000.0;
            let cpu_util = cmap.get(func, Chars::CPUtil, Value::Avg);

            ((iat + 1000.0) / cpu_util) as u32
        };

        Self {
            consistenthashing_customrebalance: ConsistentHashingCustomRebalance::new(
                fineloadbalancing.clone(),
                domains,
                Arc::new(cpuutil_for_func),
            ),
        }
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashingIATCPUUtilizationRebalance {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.consistenthashing_customrebalance
            .assign_domain_to_function_request(tid, reg.clone())
    }

    fn invoke_is_complete(&self, cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance
            .invoke_is_complete(cgroup_id, tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance.release_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.consistenthashing_customrebalance.fineloadbalancing, reg, from, to)
    }
}

// Consistent hashing with IAT, Energy hybrid rebalance
pub struct ConsistentHashingIATEnergyRebalance {
    consistenthashing_customrebalance: ConsistentHashingCustomRebalance,
}

impl ConsistentHashingIATEnergyRebalance {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak, domains: ArcVec<Domain>) -> Self {
        let fineloadbalancing_weak = fineloadbalancing.clone();
        let iatenergy_for_func = move |func: &String| -> u32 {
            let fineloadbalancing = fineloadbalancing_weak.upgrade().unwrap();
            let cmap = fineloadbalancing.cmap.clone();

            let iat = cmap.get(func, Chars::IAT, Value::Avg) * 1000.0;
            let dur = cmap.get(func, Chars::CpuWarmTime, Value::Avg) * 1000.0;
            let cpu_util = cmap.get(func, Chars::CPUtil, Value::Avg);

            let energy = cpu_util * dur;

            (iat + energy) as u32
        };

        Self {
            consistenthashing_customrebalance: ConsistentHashingCustomRebalance::new(
                fineloadbalancing.clone(),
                domains,
                Arc::new(iatenergy_for_func),
            ),
        }
    }
}

impl LoadBalancingPolicyTrait for ConsistentHashingIATEnergyRebalance {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.consistenthashing_customrebalance
            .assign_domain_to_function_request(tid, reg.clone())
    }

    fn invoke_is_complete(&self, cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance
            .invoke_is_complete(cgroup_id, tid, reg.clone());
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.consistenthashing_customrebalance.release_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.consistenthashing_customrebalance.fineloadbalancing, reg, from, to)
    }
}

// Guardrails pick on system domains
pub enum GuardrailsPickOnSystemDomainsValueType {
    DURATION,
    IAT,
    CPUTIL,
    ENERGY,
    IATENERGY,
    IATCPUUTIL,
}

pub struct GuardrailsPickOnSystemDomains {
    fineloadbalancing: FineLoadBalancingWeak,

    value_type: GuardrailsPickOnSystemDomainsValueType,
    guardrails: Guardrails,
}

impl GuardrailsPickOnSystemDomains {
    pub fn new(
        fineloadbalancing: FineLoadBalancingWeak,
        config: Arc<FineLoadBalancingConfig>,
        value_type: GuardrailsPickOnSystemDomainsValueType,
    ) -> Self {
        Self {
            fineloadbalancing: fineloadbalancing.clone(),

            value_type,
            guardrails: Guardrails::new(fineloadbalancing.clone(), config.clone()),
        }
    }

    fn value(&self, func_name: &String) -> u32 {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let cmap = fineloadbalancing.cmap.clone();

        match self.value_type {
            GuardrailsPickOnSystemDomainsValueType::DURATION => {
                (cmap.get(func_name, Chars::CpuWarmTime, Value::Avg) * 1000.0) as u32
            },
            GuardrailsPickOnSystemDomainsValueType::IAT => {
                (cmap.get(func_name, Chars::IAT, Value::Avg) * 1000.0) as u32
            },
            GuardrailsPickOnSystemDomainsValueType::CPUTIL => {
                (cmap.get(func_name, Chars::CPUtil, Value::Avg) * 1000.0) as u32
            },
            GuardrailsPickOnSystemDomainsValueType::ENERGY => {
                let dur = cmap.get(func_name, Chars::CpuWarmTime, Value::Avg) * 1000.0;
                let cpu_util = cmap.get(func_name, Chars::CPUtil, Value::Avg);

                (cpu_util * dur) as u32
            },
            GuardrailsPickOnSystemDomainsValueType::IATENERGY => {
                let iat = cmap.get(func_name, Chars::IAT, Value::Avg) * 1000.0;
                let dur = cmap.get(func_name, Chars::CpuWarmTime, Value::Avg) * 1000.0;
                let cpu_util = cmap.get(func_name, Chars::CPUtil, Value::Avg);

                let energy = cpu_util * dur;

                (iat + energy) as u32
            },
            GuardrailsPickOnSystemDomainsValueType::IATCPUUTIL => {
                let iat = cmap.get(func_name, Chars::IAT, Value::Avg) * 1000.0;
                let cpu_util = cmap.get(func_name, Chars::CPUtil, Value::Avg);

                ((iat + 1000.0) / cpu_util) as u32
            },
        }
    }
}

impl LoadBalancingPolicyTrait for GuardrailsPickOnSystemDomains {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        let func_name = &reg.fqdn;
        let requested_cores = reg.cpus;
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let system_domains = fineloadbalancing.system_domains.immutable_clone();
        let any_domain_available = |domains: &Vec<Arc<Domain>>| domains.iter().any(|domain| domain.available());

        let domain_set: Vec<SchedGroupID> = system_domains.iter().map(|domain| domain.schedgroup_id()).collect();

        let value = self.value(func_name);

        while any_domain_available(&system_domains) {
            let domain_id = self.guardrails.guardrails_pick(value, &domain_set);
            let domain = &system_domains[domain_id as usize];
            if domain.can_serve_and_acquire_append(func_name, requested_cores).is_ok() {
                debug!( tid=%tid, lbpolicy=%"guardrails_onsystemdomains", fqdn=%func_name, value=%value, domain_assigned=%dump_domain(&domain.as_ref()), "[finesched] assign_domain_to_function_request");
                return Some(domain_id);
            }
        }

        None
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.release_domain(tid, reg);
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let stats = fineloadbalancing.stats.clone();
        let domain_id = *stats.tid_map.get(tid).unwrap() as DomainId;
        let domain = &fineloadbalancing.system_domains.immutable_clone()[domain_id];
        domain.return_cpus_and_release(reg.cpus);

        self.guardrails.return_domain(tid, reg.clone());
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        move_domain_reservation(&self.fineloadbalancing, reg, from, to)
    }
}

#[cfg(test)]
mod fineloadbalancing_tests {
    use super::*;

    #[iluvatar_library::sim_test]
    fn domain_vector_assignment() {
        let domain_id: DomainId = 0;
        let func_name = "test_func".to_string();
        let cpus: u32 = 1;

        let domains: ArcVec<Domain> = ArcVec::<Domain>::new();
        let domain = Domain::new(domain_id, cpus);
        domains.push(domain);

        let domains_ref0 = domains.immutable_clone();
        let domain = domains_ref0[domain_id].clone();
        if !domain.can_serve_and_acquire_empty_or_assigned(&func_name, cpus).is_ok() {
            assert!(false);
        }

        let domains_ref1 = domains.immutable_clone();
        let domain = domains_ref1[domain_id].clone();
        assert_eq!(domain.assigned_funcs()[0], func_name);
    }
}

#[cfg(test)]
mod fineloadbalancing_func_domain_mapper_tests {
    use super::*;

    fn build_func_domain_mapper(domain_count: usize) -> DomainMapper {
        let domains = ArcVec::<Domain>::new();
        for domain_id in 0..domain_count {
            let assigned_cores = 1;
            let domain = Domain::new(domain_id, assigned_cores);
            domains.push(domain);
        }

        FuncDomainMapper::new(domains)
    }

    #[iluvatar_library::sim_test]
    fn get_funcs_under_domain_count() {
        let mapper = build_func_domain_mapper(/*domain_count=*/ 3);

        let funcs = vec!["f0", "f1", "f2"];
        let funcs: Vec<Arc<RegisteredFunction>> = funcs
            .iter()
            .map(|func_name| {
                Arc::new(RegisteredFunction {
                    fqdn: func_name.to_string(),
                    ..Default::default()
                })
            })
            .collect();
        let domains = vec![1, 2, 0];

        for (func, domain_id) in funcs.iter().zip(domains) {
            let domain_set = mapper.get(func.clone());
            assert!(domain_set.len() == 1);
            assert_eq!(domain_set[0].domain_id(), domain_id);
        }
    }

    #[iluvatar_library::sim_test]
    fn get_funcs_over_domain_count() {
        let mapper = build_func_domain_mapper(/*domain_count=*/ 3);

        let funcs = vec!["f0", "f1", "f2", "f3", "f4"];
        let funcs: Vec<Arc<RegisteredFunction>> = funcs
            .iter()
            .map(|func_name| {
                Arc::new(RegisteredFunction {
                    fqdn: func_name.to_string(),
                    ..Default::default()
                })
            })
            .collect();
        let domains = vec![1, 2, 0, 1, 2];

        for (func, domain_id) in funcs.iter().zip(domains) {
            let domain_set = mapper.get(func.clone());

            assert!(domain_set.len() == 1);
            assert_eq!(domain_set[0].domain_id(), domain_id);
        }
    }

    #[iluvatar_library::sim_test]
    fn expand_preferred_set_single_func() {
        let mapper = build_func_domain_mapper(/*domain_count=*/ 3);

        let func = Arc::new(RegisteredFunction {
            fqdn: "f0".to_string(),
            ..Default::default()
        });
        let domain_set = mapper.expand_preferred_set(func.clone());
        assert!(domain_set.len() == 1);
        assert_eq!(domain_set[0].domain_id(), 1);

        let domain_set = mapper.expand_preferred_set(func.clone());
        assert!(domain_set.len() == 2);
        assert_eq!(domain_set[1].domain_id(), 2);

        let domain_set = mapper.expand_preferred_set(func.clone());
        assert!(domain_set.len() == 3);
        assert_eq!(domain_set[2].domain_id(), 0);

        let domain_set = mapper.expand_preferred_set(func.clone());
        assert!(domain_set.len() == 3);
    }

    #[iluvatar_library::sim_test]
    fn expand_preferred_set_multi_func() {
        let mapper = build_func_domain_mapper(/*domain_count=*/ 3);

        let func0 = Arc::new(RegisteredFunction {
            fqdn: "f0".to_string(),
            ..Default::default()
        });
        let domain_set = mapper.expand_preferred_set(func0.clone());
        assert!(domain_set.len() == 1);
        assert_eq!(domain_set[0].domain_id(), 1);

        let func1 = Arc::new(RegisteredFunction {
            fqdn: "f1".to_string(),
            ..Default::default()
        });
        let domain_set = mapper.expand_preferred_set(func1.clone());
        assert!(domain_set.len() == 1);
        assert_eq!(domain_set[0].domain_id(), 2);

        let domain_set = mapper.expand_preferred_set(func1.clone());
        assert!(domain_set.len() == 2);
        assert_eq!(domain_set[1].domain_id(), 0);

        let func2 = Arc::new(RegisteredFunction {
            fqdn: "f2".to_string(),
            ..Default::default()
        });
        let domain_set = mapper.expand_preferred_set(func2.clone());
        assert!(domain_set.len() == 1);
        assert_eq!(domain_set[0].domain_id(), 1);

        let domain_set = mapper.expand_preferred_set(func1.clone());
        assert!(domain_set.len() == 3);
        assert_eq!(domain_set[2].domain_id(), 1);
    }
}
