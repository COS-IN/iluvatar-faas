//! Fine grained load balancing.
//!
//! The existing scheduler and policy implementations are kept in `legacy` so
//! current configuration names and callers continue to work. New policies
//! should be implemented in `policies` and then added to the legacy policy
//! factory until policy construction is moved out of that module.

mod legacy;
pub mod policies;

pub use legacy::*;

use crate::services::containers::structs::ContainerState;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::worker_config::{ChblLoadType, FineLoadBalancingConfig};
use iluvatar_finesched::SchedGroupID;
use iluvatar_library::char_map::{Chars, Value};
use iluvatar_library::transaction::TransactionId;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

/// Aggregate, best-effort view of the queued CPU work.
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuQueueSnapshot {
    pub queued_invocations: usize,
    pub estimated_work_secs: f64,
}

#[derive(Debug, Clone)]
pub struct DomainPeltAndColdSnapshot {
    pub domain_id: SchedGroupID,
    pub pelt_utilization_percent: Option<f64>,
    pub likely_cold_by_fqdn: HashMap<String, u32>,
}

/// Coarse classification of the current CPU invocation-queue phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CpuWorkloadPhase {
    Low,
    High,
    Bursty,
}

const LARGE_QUEUE_ITEMS_PER_CORE: usize = 2;
const LARGE_QUEUE_WORK_SECS_PER_CORE: f64 = 2.0;
const BURSTY_TOP_FUNCTIONS: usize = 3;
const BURSTY_FUNCTION_SHARE: f64 = 0.7;

impl FineLoadBalancingStruct {
    /// Classify queued CPU work as low, high, or bursty.
    ///
    /// A backlog is large when either queued items or estimated queued work is
    /// at least twice the CPU-core count. A large backlog is bursty when its
    /// three most common FQDNs account for at least 70% of the queued items.
    /// Missing queue data is treated as low load.
    pub fn cpu_workload_phase(&self) -> CpuWorkloadPhase {
        let Some(snapshot) = self.cpu_queue_snapshot() else {
            return CpuWorkloadPhase::Low;
        };
        let function_counts = self.queued_fqdn_counts(snapshot.queued_invocations);
        let cpu_cores = self
            .config
            .preallocated_groups
            .groups
            .iter()
            .map(|group| group.cores.len())
            .sum::<usize>()
            .max(1);
        classify_cpu_workload_phase(snapshot, &function_counts, cpu_cores)
    }
}

/// Classify a queue snapshot using CPU-core-relative backlog and FQDN skew.
pub fn classify_cpu_workload_phase(
    snapshot: CpuQueueSnapshot,
    function_counts: &HashMap<String, usize>,
    cpu_cores: usize,
) -> CpuWorkloadPhase {
    let cpu_cores = cpu_cores.max(1);
    let large_backlog = snapshot.queued_invocations >= LARGE_QUEUE_ITEMS_PER_CORE * cpu_cores
        || snapshot.estimated_work_secs >= LARGE_QUEUE_WORK_SECS_PER_CORE * cpu_cores as f64;
    if !large_backlog {
        return CpuWorkloadPhase::Low;
    }

    let total_count = function_counts.values().sum::<usize>();
    if total_count == 0 {
        return CpuWorkloadPhase::High;
    }
    let mut counts = function_counts.values().copied().collect::<Vec<_>>();
    counts.sort_unstable_by(|a, b| b.cmp(a));
    let top_functions_count = counts.iter().take(BURSTY_TOP_FUNCTIONS).sum::<usize>();
    let dominant_share = top_functions_count as f64 / total_count as f64;
    if dominant_share >= BURSTY_FUNCTION_SHARE {
        CpuWorkloadPhase::Bursty
    } else {
        CpuWorkloadPhase::High
    }
}

/// Choose between two domains using lower PELT-like utilization. The random
/// variant samples a fresh pair per invocation; the deterministic variant
/// hashes the function FQDN to a stable pair. If metrics are unavailable, the
/// random variant falls back to a random candidate and the deterministic one
/// chooses its first candidate.
#[derive(Clone, Copy)]
enum PowerOfTwoCandidates {
    Random,
    DeterministicFqdn,
}

pub struct PowerOfTwo {
    fineloadbalancing: FineLoadBalancingWeak,
    candidates: PowerOfTwoCandidates,
}

impl PowerOfTwo {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak) -> Self {
        Self {
            fineloadbalancing,
            candidates: PowerOfTwoCandidates::Random,
        }
    }

    pub fn new_deterministic(fineloadbalancing: FineLoadBalancingWeak) -> Self {
        Self {
            fineloadbalancing,
            candidates: PowerOfTwoCandidates::DeterministicFqdn,
        }
    }

    fn assign_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) -> Option<SchedGroupID> {
        let fineloadbalancing = self.fineloadbalancing.upgrade()?;
        let deterministic_key = match self.candidates {
            PowerOfTwoCandidates::Random => None,
            PowerOfTwoCandidates::DeterministicFqdn => Some(reg.fqdn.clone()),
        };
        let policy = match self.candidates {
            PowerOfTwoCandidates::Random => "power_of_two_rand",
            PowerOfTwoCandidates::DeterministicFqdn => "power_of_two_det",
        };
        select_power_of_two_domain(
            &fineloadbalancing,
            tid,
            reg,
            deterministic_key.as_deref(),
            None,
            None,
            None,
            false,
            policy,
        )
    }
}

impl LoadBalancingPolicyTrait for PowerOfTwo {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.assign_domain(tid, reg)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        return_power_of_two_domain(&self.fineloadbalancing, tid, reg);
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        return_power_of_two_domain(&self.fineloadbalancing, tid, reg);
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        let Some(fineloadbalancing) = self.fineloadbalancing.upgrade() else {
            return false;
        };
        let domains = fineloadbalancing.system_domains.immutable_clone();
        let (Some(source), Some(target)) = (domains.get(from as usize), domains.get(to as usize)) else {
            return false;
        };
        source.return_cpus_and_release(reg.cpus);
        target.acquire_cores_oc(reg.cpus);
        true
    }
}

/// Power of two choices that uses the next queued invocation's estimated CPU
/// work as a packing hint.
pub struct PowerOfTwoPeek {
    fineloadbalancing: FineLoadBalancingWeak,
    candidates: PowerOfTwoCandidates,
}

impl PowerOfTwoPeek {
    pub fn new(fineloadbalancing: FineLoadBalancingWeak) -> Self {
        Self {
            fineloadbalancing,
            candidates: PowerOfTwoCandidates::Random,
        }
    }

    pub fn new_deterministic(fineloadbalancing: FineLoadBalancingWeak) -> Self {
        Self {
            fineloadbalancing,
            candidates: PowerOfTwoCandidates::DeterministicFqdn,
        }
    }
}

impl LoadBalancingPolicyTrait for PowerOfTwoPeek {
    /// Choose between two random domains using current load and the next queued
    /// invocation. Estimate each invocation's work as requested CPUs multiplied
    /// by the average duration for its currently visible idle-container state
    /// (warm, prewarm, or cold). Normally choose the lower-loaded domain. If
    /// the next invocation's estimated work exceeds the current invocation's,
    /// choose the more-loaded domain to pack the current invocation tightly
    /// and leave the lighter domain open for the larger request. The current
    /// invocation has already left the queue, so the peek is the work
    /// immediately after it. If either estimate is unavailable, use the normal
    /// lower-load choice. Ties or unavailable domain-load metrics fall back to
    /// a random sampled domain.
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        let fineloadbalancing = self.fineloadbalancing.upgrade()?;
        let next_invocation = fineloadbalancing.peek_next_cpu_invocation();
        let current_work = anticipated_cpu_work(&fineloadbalancing, &reg.fqdn, reg.cpus, "current");
        let next_work = next_invocation
            .as_ref()
            .and_then(|(fqdn, cpus)| anticipated_cpu_work(&fineloadbalancing, fqdn, *cpus, "next"));
        let pack_tightly = next_work
            .zip(current_work)
            .is_some_and(|(next, current)| next > current);
        let deterministic_key = match self.candidates {
            PowerOfTwoCandidates::Random => None,
            PowerOfTwoCandidates::DeterministicFqdn => Some(reg.fqdn.clone()),
        };
        let policy = match self.candidates {
            PowerOfTwoCandidates::Random => "power_of_two_peek",
            PowerOfTwoCandidates::DeterministicFqdn => "power_of_two_peek_det",
        };
        select_power_of_two_domain(
            &fineloadbalancing,
            tid,
            reg,
            deterministic_key.as_deref(),
            next_invocation,
            current_work,
            next_work,
            pack_tightly,
            policy,
        )
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        return_power_of_two_domain(&self.fineloadbalancing, tid, reg);
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        return_power_of_two_domain(&self.fineloadbalancing, tid, reg);
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        let Some(fineloadbalancing) = self.fineloadbalancing.upgrade() else {
            return false;
        };
        let domains = fineloadbalancing.system_domains.immutable_clone();
        let (Some(source), Some(target)) = (domains.get(from as usize), domains.get(to as usize)) else {
            return false;
        };
        source.return_cpus_and_release(reg.cpus);
        target.acquire_cores_oc(reg.cpus);
        true
    }
}

/// Estimate the queued invocation's CPU demand from its CPU request and the
/// average duration for the currently visible container state.
fn anticipated_cpu_work(
    fineloadbalancing: &FineLoadBalancingStruct,
    fqdn: &str,
    requested_cpus: u32,
    queue_position: &str,
) -> Option<f64> {
    let state = fineloadbalancing.idle_cpu_container_state(fqdn)?;
    let (duration_characteristic, cold_start_estimate) = match state {
        ContainerState::Warm => (Chars::CpuWarmTime, false),
        ContainerState::Prewarm => (Chars::CpuPreWarmTime, false),
        _ => (Chars::CpuColdTime, true),
    };
    let duration = fineloadbalancing.cmap.get(fqdn, duration_characteristic, Value::Avg);
    let work = duration * requested_cpus as f64;
    if cold_start_estimate {
        debug!(
            fqdn,
            queue_position,
            requested_cpus,
            estimated_duration_secs = duration,
            estimated_cpu_work = work,
            "[finesched] cold start anticipated for PowerOfTwoPeek"
        );
    }
    (work.is_finite() && work > 0.0).then_some(work)
}

fn select_power_of_two_domain(
    fineloadbalancing: &FineLoadBalancingStruct,
    tid: &TransactionId,
    reg: Arc<RegisteredFunction>,
    deterministic_key: Option<&str>,
    next_invocation: Option<(String, u32)>,
    current_work: Option<f64>,
    next_work: Option<f64>,
    pack_tightly: bool,
    policy: &str,
) -> Option<SchedGroupID> {
    let domains = fineloadbalancing.system_domains.immutable_clone();
    if domains.is_empty() {
        return None;
    }

    let mut rng = rand::rng();
    let (first_index, second_index) = if let Some(key) = deterministic_key {
        deterministic_domain_choices(key, domains.len())
    } else {
        let first_index = rng.random_range(0..domains.len());
        let second_index = if domains.len() > 1 {
            let other_index = rng.random_range(0..domains.len() - 1);
            Some(if other_index >= first_index {
                other_index + 1
            } else {
                other_index
            })
        } else {
            None
        };
        (first_index, second_index)
    };

    let first = &domains[first_index];
    let first_util = fineloadbalancing
        .domain_pelt_like_utilization_percent(first.schedgroup_id())
        .ok()
        .flatten();
    let (base_selected, second_id, second_util) = if let Some(second_index) = second_index {
        let second = &domains[second_index];
        let second_util = fineloadbalancing
            .domain_pelt_like_utilization_percent(second.schedgroup_id())
            .ok()
            .flatten();
        let selected = match (first_util, second_util, pack_tightly) {
            (Some(a), Some(b), false) if a < b => first,
            (Some(a), Some(b), false) if b < a => second,
            (Some(a), Some(b), true) if a > b => first,
            (Some(a), Some(b), true) if b > a => second,
            (Some(_), Some(_), _) => {
                deterministic_key.map_or_else(|| if rng.random_bool(0.5) { first } else { second }, |_| first)
            },
            (Some(_), None, false) => first,
            (None, Some(_), false) => second,
            _ => deterministic_key.map_or_else(|| if rng.random_bool(0.5) { first } else { second }, |_| first),
        };
        (selected, Some(second.schedgroup_id()), second_util)
    } else {
        (first, None, None)
    };

    let selected = base_selected;

    selected.acquire_cores_oc(reg.cpus);
    debug!(
        tid=%tid,
        fqdn=%reg.fqdn,
        next_invocation_fqdn=?next_invocation.as_ref().map(|(fqdn, _)| fqdn),
        next_invocation_cpus=?next_invocation.as_ref().map(|(_, cpus)| cpus),
        current_estimated_cpu_work=?current_work,
        next_estimated_cpu_work=?next_work,
        packing_for_next_invocation=pack_tightly,
        first_domain=%first.schedgroup_id(),
        first_pelt_utilization=?first_util,
        second_domain=?second_id,
        second_pelt_utilization=?second_util,
        selected_domain=%selected.schedgroup_id(),
        candidate_selection=if deterministic_key.is_some() { "fqdn_hash" } else { "random" },
        lbpolicy=%policy,
        "[finesched] selected domain from two choices"
    );
    Some(selected.schedgroup_id())
}

/// Map a stable key to two distinct domain candidates. The two FNV-1a seeds
/// provide independent candidate positions; a collision is advanced by one.
fn deterministic_domain_choices(key: &str, domain_count: usize) -> (usize, Option<usize>) {
    if domain_count <= 1 {
        return (0, None);
    }

    fn fnv1a(key: &str, seed: u64) -> u64 {
        key.as_bytes().iter().fold(seed, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
    }

    let count = domain_count as u64;
    let first = (fnv1a(key, 0xcbf2_9ce4_8422_2325) % count) as usize;
    let mut second = (fnv1a(key, 0x8422_2325_cbf2_9ce4) % count) as usize;
    if second == first {
        second = (second + 1) % domain_count;
    }
    (first, Some(second))
}

fn return_power_of_two_domain(
    fineloadbalancing: &FineLoadBalancingWeak,
    tid: &TransactionId,
    reg: Arc<RegisteredFunction>,
) {
    let Some(fineloadbalancing) = fineloadbalancing.upgrade() else {
        return;
    };
    let Some(domain_id) = fineloadbalancing.stats.tid_map.get(tid).map(|entry| *entry) else {
        return;
    };
    if let Some(domain) = fineloadbalancing
        .system_domains
        .immutable_clone()
        .get(domain_id as usize)
    {
        domain.return_cpus_and_release(reg.cpus);
    }
}

/// Consistent hashing with load threshold based domain selection.
pub struct CHBL {
    fineloadbalancing: FineLoadBalancingWeak,
    domain_mapper: legacy::DomainMapper,
    chbl_load_type: ChblLoadType,
    chbl_thresh: f32,
    chbl_cpu_tdp_watts: f64,
    total_domain_cores: u64,
}

impl legacy::SelectDomain for CHBL {
    fn pick_domain_from_set(
        &self,
        reg: Arc<RegisteredFunction>,
        domains: &Vec<Arc<legacy::Domain>>,
    ) -> Option<Arc<legacy::Domain>> {
        if domains.is_empty() {
            return None;
        }
        let fineloadbalancing = self.fineloadbalancing.upgrade()?;
        if self.chbl_load_type == ChblLoadType::Power {
            if let Some(reader) = fineloadbalancing.core_metrics_reader.as_ref() {
                reader.sample_power();
            }
        }

        let hash = reg
            .fqdn
            .as_bytes()
            .iter()
            .fold(0usize, |hash, byte| hash.wrapping_mul(31).wrapping_add(*byte as usize));
        let home = hash % domains.len();
        // Probe the FQDN's home domain first, then spill over around the ring.
        for offset in 0..domains.len() {
            let index = (home + offset) % domains.len();
            let domain = &domains[index];
            let is_home = index == home;
            let (load_value, load_maximum) = match self.chbl_load_type {
                ChblLoadType::UsedCores => (
                    Some(domain.used_cores().saturating_add(reg.cpus) as f64),
                    Some(domain.assigned_cores as f64),
                ),
                ChblLoadType::ProcStat => (
                    fineloadbalancing.domain_cpu_utilization_percent(domain.schedgroup_id()),
                    Some(100.0),
                ),
                ChblLoadType::Pelt => (
                    fineloadbalancing
                        .domain_pelt_like_utilization_percent(domain.schedgroup_id())
                        .ok()
                        .flatten(),
                    Some(100.0),
                ),
                ChblLoadType::Power => {
                    let maximum = (self.total_domain_cores > 0).then(|| {
                        self.chbl_cpu_tdp_watts * domain.assigned_cores as f64 / self.total_domain_cores as f64
                    });
                    (fineloadbalancing.domain_power_watts(domain.schedgroup_id()), maximum)
                },
            };
            let normalized_load = load_value
                .zip(load_maximum)
                .and_then(|(value, maximum)| (maximum > 0.0).then_some(value / maximum));
            let threshold = self.chbl_thresh as f64;
            if normalized_load.is_some_and(|load| load < threshold) || is_home {
                domain.acquire_cores_oc(reg.cpus);
                debug!(
                    lbpolicy=%"chbl",
                    load_type=?self.chbl_load_type,
                    domain_load_value=?load_value,
                    domain_load_maximum=?load_maximum,
                    normalized_domain_load=?normalized_load,
                    load_threshold=threshold,
                    domain_assigned=%legacy::dump_domain(domain),
                    "[finesched] assign_domain_to_function_request"
                );
                return Some(domain.clone());
            }
        }
        None
    }
}

impl CHBL {
    pub fn new(
        fineloadbalancing: FineLoadBalancingWeak,
        domain_mapper: legacy::DomainMapper,
        config: Arc<FineLoadBalancingConfig>,
    ) -> Self {
        Self {
            fineloadbalancing,
            chbl_thresh: config.chbl_thresh,
            chbl_load_type: config.chbl_load_type,
            chbl_cpu_tdp_watts: config.chbl_cpu_tdp_watts,
            total_domain_cores: config
                .preallocated_groups
                .groups
                .iter()
                .map(|group| group.cores.len() as u64)
                .sum(),
            domain_mapper,
        }
    }

    pub fn rebalance_domains(&self, rebalance_domains: legacy::RebalanceClosure<'_>) {
        legacy::DomainMapperTrait::rebalance_domains(&*self.domain_mapper, rebalance_domains)
    }

    pub fn pick_domain(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
        domains_selection: &dyn legacy::SelectDomain,
    ) -> Option<SchedGroupID> {
        let preferred_domains = self.domain_mapper.get(reg.clone());
        debug!(tid=%tid, lbpolicy=%"consistent_hashing", fqdn=%reg.fqdn, preferred_domains=%legacy::dump_domains(&preferred_domains), "[finesched] assign_domain_to_function_request");
        if let Some(domain) = domains_selection.pick_domain_from_set(reg.clone(), &preferred_domains) {
            debug!(tid=%tid, lbpolicy=%"consistent_hashing", fqdn=%reg.fqdn, domain_assigned=%legacy::dump_domain(&domain), "[finesched] assign_domain_to_function_request");
            return Some(domain.schedgroup_id());
        }
        let expanded = self.domain_mapper.expand_preferred_set(reg.clone());
        let domain = domains_selection.pick_domain_from_set(reg, &expanded)?;
        debug!(tid=%tid, lbpolicy=%"consistent_hashing", domain_assigned=%legacy::dump_domain(&domain), "[finesched] assign_domain_to_function_request");
        Some(domain.schedgroup_id())
    }

    fn return_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        let fineloadbalancing = self.fineloadbalancing.upgrade().unwrap();
        let domain_id = *fineloadbalancing.stats.tid_map.get(tid).unwrap() as legacy::DomainId;
        let domain = &fineloadbalancing.system_domains.immutable_clone()[domain_id];
        domain.return_cpus_and_release(reg.cpus);
    }
}

impl LoadBalancingPolicyTrait for CHBL {
    fn assign_domain_to_function_request(
        &self,
        tid: &TransactionId,
        reg: Arc<RegisteredFunction>,
    ) -> Option<SchedGroupID> {
        self.pick_domain(tid, reg.clone(), self)
    }

    fn invoke_is_complete(&self, _cgroup_id: &str, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.return_domain(tid, reg);
    }

    fn release_domain(&self, tid: &TransactionId, reg: Arc<RegisteredFunction>) {
        self.return_domain(tid, reg);
    }

    fn move_reservation(&self, reg: &RegisteredFunction, from: SchedGroupID, to: SchedGroupID) -> bool {
        let Some(fineloadbalancing) = self.fineloadbalancing.upgrade() else {
            return false;
        };
        let domains = fineloadbalancing.system_domains.immutable_clone();
        let (Some(source), Some(target)) = (domains.get(from as usize), domains.get(to as usize)) else {
            return false;
        };
        source.return_cpus_and_release(reg.cpus);
        target.acquire_cores_oc(reg.cpus);
        true
    }
}
