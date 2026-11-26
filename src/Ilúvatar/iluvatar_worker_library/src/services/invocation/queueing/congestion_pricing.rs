//! Online Self-Calibrating Pigouvian Congestion Pricing admission control.
//!
//! Evaluates incoming CPU invocation requests against domain-level Pigouvian tolls
//! derived from FineSched PELT domain telemetry and self-calibrating supply curves.

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy};
use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::ContainerState;
use crate::services::resources::fineloadbalancing::FineLoadBalancingWeak;
use crate::worker_api::worker_config::CongestionPricingConfig;
use anyhow::Result;
use iluvatar_finesched::SchedGroupID;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use ordered_float::OrderedFloat;
use parking_lot::Mutex;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tracing::{debug, info, trace};

const DEFAULT_S_I: f64 = 10.0;       // ms
const DEFAULT_T_BOOT: f64 = 250.0;   // ms
const DEFAULT_ETA: f64 = 0.8;
const DEFAULT_CORES_PER_DOMAIN: u32 = 4;
const DEFAULT_MAX_QUEUE_PER_DOMAIN: usize = 4096;
/// Maximum short-job lead window (ms) in virtual-deadline ordering so shorter
/// jobs step ahead of concurrent arrivals without ever starving large jobs or
/// bunching 24 identical large jobs into a cold-start stampede.
const MAX_SCHEDULING_SLACK_MS: f64 = 300.0;
/// Maximum consecutive warm express-lane pops while normal ready work is waiting.
const MAX_EXPRESS_STREAK: u32 = 8;
/// Scale-invariant soft-affinity utilization hysteresis margin.
/// `C_mig = SOFT_AFFINITY_RHO_MARGIN * s_i`, so any function (tiny `hello` or large
/// `pyaes`) stays on its warm domain while `rho_warm - rho_alt <= 0.20` (~1 core on a
/// 6-core domain) and spills once an alternate domain is meaningfully quieter (>= 2 cores).
const SOFT_AFFINITY_RHO_MARGIN: f64 = 0.20;

fn scheduling_slack_ms(s_i: f64) -> f64 {
    (0.5 * s_i.max(0.0)).min(MAX_SCHEDULING_SLACK_MS)
}

fn soft_affinity_migration_cost(s_i: f64) -> f64 {
    SOFT_AFFINITY_RHO_MARGIN * s_i.max(0.0)
}

fn placement_ranking_cost(
    rho_d: f64,
    s_i: f64,
    overload_toll: f64,
    c_mig: f64,
    is_warm_domain: bool,
) -> f64 {
    let migration_cost = if is_warm_domain { 0.0 } else { c_mig };
    rho_d * s_i + overload_toll.max(0.0) + migration_cost
}

/// Build a Power-of-Two candidate pair `[primary, alt]` from `pool`, picking one
/// alternate domain `alt != primary` using `random_offset`. Prevents synchronized
/// herding across all domains while keeping `primary` (warm/home) in every comparison.
fn select_po2_candidates(primary: usize, pool: &[usize], random_offset: usize) -> Vec<usize> {
    let mut others: Vec<usize> = pool
        .iter()
        .copied()
        .filter(|&d| d != primary)
        .collect();
    others.sort_unstable();
    others.dedup();
    if others.is_empty() {
        return vec![primary];
    }
    let alt = others[random_offset % others.len()];
    vec![primary, alt]
}

#[derive(Clone)]
struct WaitEntry {
    deadline_ms: OrderedFloat<f64>,
    weight: f64,
    seq: u64,
    enqueue_ms: f64,
    item: Arc<EnqueuedInvocation>,
    is_cold: bool,
    coalesced: bool,
}

impl PartialEq for WaitEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline_ms == other.deadline_ms && self.seq == other.seq
    }
}

impl Eq for WaitEntry {}

impl PartialOrd for WaitEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for WaitEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .deadline_ms
            .cmp(&self.deadline_ms)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[derive(Clone)]
struct ReadyEntry {
    deadline_ms: OrderedFloat<f64>,
    seq: u64,
    ready_at: Instant,
    prior_wait_ms: f64,
    item: Arc<EnqueuedInvocation>,
}

impl PartialEq for ReadyEntry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline_ms == other.deadline_ms && self.seq == other.seq
    }
}

impl Eq for ReadyEntry {}

impl PartialOrd for ReadyEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReadyEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .deadline_ms
            .cmp(&self.deadline_ms)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[derive(Clone, Debug)]
struct ActiveAdmission {
    domain_id: usize,
    fqdn: String,
    added_weight: f64,
    reserved_cold_boot: bool,
    reserved_booting_fqdn: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ColdWaveState {
    WarmIdle,
    Pioneer,
    Follower,
}

fn cold_wave_state(idle_available: usize, total_containers: usize, booting: u32) -> ColdWaveState {
    if idle_available > 0 {
        ColdWaveState::WarmIdle
    } else if total_containers == 0 && booting == 0 {
        ColdWaveState::Pioneer
    } else {
        ColdWaveState::Follower
    }
}

fn warm_idle_candidate_domains(warm_ready_domains: &[usize], fallback_domains: &[usize]) -> Vec<usize> {
    if warm_ready_domains.is_empty() {
        return fallback_domains.to_vec();
    }
    let mut ordered = Vec::with_capacity(fallback_domains.len());
    for &d in warm_ready_domains {
        if !ordered.contains(&d) {
            ordered.push(d);
        }
    }
    for &d in fallback_domains {
        if !ordered.contains(&d) {
            ordered.push(d);
        }
    }
    ordered
}

fn pioneer_creation_credit(w_eff: f64, t_boot_ms: f64, queued_for_fqdn: usize, cores_per_domain: u32) -> f64 {
    let wave_size = (queued_for_fqdn + 1).min(cores_per_domain as usize) as f64;
    w_eff * t_boot_ms * wave_size
}

fn scaleout_creation_credit(w_eff: f64, t_boot_ms: f64, queued_for_fqdn: usize, cores_per_domain: u32) -> f64 {
    let wave_size = queued_for_fqdn.min(cores_per_domain as usize).max(1) as f64;
    w_eff * t_boot_ms * wave_size
}

struct DomainState {
    domain_id: SchedGroupID,
    active_weight_sum: f64, // W_act,d
    queue_weight_sum: f64,  // W_q,d
    cold_booting: u32,      // k_cold,d
    expected_finish_ms: BinaryHeap<Reverse<OrderedFloat<f64>>>,
    wait_queue: BinaryHeap<WaitEntry>,
    coalesced_wait_queue: BinaryHeap<WaitEntry>,
}

impl DomainState {
    fn new(domain_id: SchedGroupID) -> Self {
        Self {
            domain_id,
            active_weight_sum: 0.0,
            queue_weight_sum: 0.0,
            cold_booting: 0,
            expected_finish_ms: BinaryHeap::new(),
            wait_queue: BinaryHeap::new(),
            coalesced_wait_queue: BinaryHeap::new(),
        }
    }

    fn prune_finished(&mut self, now_ms: f64) {
        while let Some(&Reverse(finish_time)) = self.expected_finish_ms.peek() {
            if finish_time.into_inner() <= now_ms {
                self.expected_finish_ms.pop();
            } else {
                break;
            }
        }
    }

    fn r_min(&mut self, now_ms: f64, rho_d: f64) -> f64 {
        if rho_d < 1.0 {
            return 0.0;
        }
        self.prune_finished(now_ms);
        if let Some(&Reverse(earliest)) = self.expected_finish_ms.peek() {
            (earliest.into_inner() - now_ms).max(0.25)
        } else {
            0.5
        }
    }
}

#[derive(Clone, Copy)]
struct FnEwma {
    s_i: f64,    // de-stretched warm exec time ms
    t_boot: f64, // cold boot duration ms
    eta_i: f64,  // CPU duty cycle
}

#[derive(Clone, Copy)]
struct WelfareBreakdown {
    rho: f64,
    r_min: f64,
    sigma: f64,
    delta_sigma: f64,
    benefit: f64,
    warm_toll: f64,
    cold_toll: f64,
    queue_backlog_toll: f64,
    creation_credit: f64,
    io_overlap_credit: f64,
    total_toll: f64,
    net_gain: f64,
}

fn best_net_gain_candidate(candidates: &[(usize, WelfareBreakdown)]) -> Option<(usize, f64)> {
    let mut best = None;
    for (domain, price) in candidates {
        if best.map_or(true, |(_, best_net_gain)| price.net_gain > best_net_gain) {
            best = Some((*domain, price.net_gain));
        }
    }
    best
}

fn should_use_work_conservation_override(wave_state: ColdWaveState, max_net: f64) -> bool {
    wave_state != ColdWaveState::WarmIdle && max_net <= 0.0
}

fn should_admit_invocation(
    wave_state: ColdWaveState,
    max_net: f64,
    force_coalesce: bool,
) -> bool {
    !force_coalesce && (wave_state == ColdWaveState::WarmIdle || max_net > 0.0)
}

struct CoordinatorState {
    domains: Vec<DomainState>,
    fn_state: HashMap<String, FnEwma>,
    /// Cold starts admitted or queued as wave pioneers/scale-out replicas.
    booting_by_fqdn: HashMap<String, u32>,
    cold_wave_domain: HashMap<String, usize>,
    express_ready_queue: VecDeque<ReadyEntry>,
    express_streak: u32,
    ready_queue: BinaryHeap<ReadyEntry>,
    next_seq: u64,
    active_admissions: HashMap<TransactionId, ActiveAdmission>,
    invocation_starts: HashMap<TransactionId, Instant>,
    pricing_queue_ms: HashMap<TransactionId, f64>,
    timing_samples: HashMap<&'static str, Vec<CongestionPricingTimingSample>>,
    theta_warm: f64,
    theta_cold: f64,
    phi_aff: f64,
    start_instant: Instant,
}

/// Component timings for one successfully completed congestion-pricing invocation, in ms.
#[derive(Clone, Copy, Debug)]
pub struct CongestionPricingTimingSample {
    pub pricing_queue_ms: f64,
    pub container_acquire_ms: f64,
    pub exec_ms: f64,
    pub post_overhead_ms: f64,
}

fn function_class(fqdn: &str) -> &'static str {
    let fqdn = fqdn.to_ascii_lowercase();
    if fqdn.contains("graph-bfs") {
        "graph-bfs"
    } else if fqdn.contains("gzip") {
        "gzip"
    } else if fqdn.contains("pyaes") {
        "pyaes"
    } else if fqdn.contains("hello") {
        "hello"
    } else {
        "other"
    }
}

fn summarize_metric(
    samples: &[CongestionPricingTimingSample],
    value: impl Fn(&CongestionPricingTimingSample) -> f64,
) -> (f64, f64, f64) {
    let mut values: Vec<f64> = samples.iter().map(value).collect();
    if values.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    values.sort_by(f64::total_cmp);
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let percentile = |p: f64| {
        let position = (values.len() - 1) as f64 * p;
        let lower = position.floor() as usize;
        let upper = position.ceil() as usize;
        values[lower] + (values[upper] - values[lower]) * (position - lower as f64)
    };
    (percentile(0.50), mean, percentile(0.95))
}

impl CoordinatorState {
    fn now_ms(&self) -> f64 {
        self.start_instant.elapsed().as_secs_f64() * 1000.0
    }

    fn alloc_seq(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        seq
    }

    fn has_ready_work(&self) -> bool {
        !self.express_ready_queue.is_empty() || !self.ready_queue.is_empty()
    }

    fn should_take_express(&self) -> bool {
        !self.express_ready_queue.is_empty()
            && (self.ready_queue.is_empty() || self.express_streak < MAX_EXPRESS_STREAK)
    }

    fn push_ready(
        &mut self,
        item: Arc<EnqueuedInvocation>,
        deadline_ms: f64,
        prior_wait_ms: f64,
        express: bool,
        admission: ActiveAdmission,
    ) {
        item.set_preferred_domain(Some(admission.domain_id as SchedGroupID));
        self.active_admissions.insert(item.tid.clone(), admission);
        let seq = self.alloc_seq();
        let entry = ReadyEntry {
            deadline_ms: OrderedFloat(deadline_ms),
            seq,
            ready_at: Instant::now(),
            prior_wait_ms,
            item,
        };
        if express {
            self.express_ready_queue.push_back(entry);
        } else {
            self.ready_queue.push(entry);
        }
    }

    fn active_count_for_fqdn(&self, fqdn: &str) -> usize {
        self.active_admissions
            .values()
            .filter(|adm| adm.fqdn == fqdn)
            .count()
    }
}

pub struct CongestionPricingCoordinator {
    state: Mutex<CoordinatorState>,
    fineloadbalancing: FineLoadBalancingWeak,
    container_manager: Mutex<Option<Weak<ContainerManager>>>,
    cmap: WorkerCharMap,
    beta: f64,
    rho_knee: f64,
    cores_per_domain: u32,
    max_queue_per_domain: usize,
    freeze_calibration: bool,
    cold_wave_coalescing: bool,
    num_domains: usize,
}

impl CongestionPricingCoordinator {
    pub fn new(
        config: Option<&CongestionPricingConfig>,
        fineloadbalancing: FineLoadBalancingWeak,
        cmap: WorkerCharMap,
        cont_manager: Option<Arc<ContainerManager>>,
    ) -> Arc<Self> {
        let (num_domains, detected_cores) = if let Some(flb) = fineloadbalancing.upgrade() {
            let domains = flb.system_domains.immutable_clone();
            let n = domains.len().max(1);
            let c = domains.first().map(|d| d.assigned_cores).unwrap_or(DEFAULT_CORES_PER_DOMAIN);
            (n, c)
        } else {
            (1, DEFAULT_CORES_PER_DOMAIN)
        };

        let beta = config.and_then(|c| c.welfare_beta).unwrap_or(1.0);
        let rho_knee = config.and_then(|c| c.rho_knee).unwrap_or(1.05).max(0.0);
        let cold_wave_coalescing = config.and_then(|c| c.cold_wave_coalescing).unwrap_or(true);
        let cores_per_domain = config.and_then(|c| c.cores_per_domain).unwrap_or(detected_cores).max(1);
        let max_queue_per_domain = config.and_then(|c| c.max_queue_per_domain).unwrap_or(DEFAULT_MAX_QUEUE_PER_DOMAIN);

        let (theta_warm, theta_cold, phi_aff, freeze_calibration) = if let Some(cal) = config.and_then(|c| c.pricing_calibration.as_ref()) {
            (
                cal.theta_warm.unwrap_or(1.0).clamp(0.25, 3.0),
                cal.theta_cold.unwrap_or(1.5).clamp(0.5, 5.0),
                cal.phi_aff.unwrap_or(0.05),
                cal.freeze_calibration.unwrap_or(false),
            )
        } else {
            (1.0, 1.5, 0.05, false)
        };

        let mut domains = Vec::with_capacity(num_domains);
        for d in 0..num_domains {
            domains.push(DomainState::new(d as SchedGroupID));
        }

        let cont_weak = cont_manager.map(|cm| Arc::downgrade(&cm));

        Arc::new(Self {
            state: Mutex::new(CoordinatorState {
                domains,
                fn_state: HashMap::new(),
                booting_by_fqdn: HashMap::new(),
                cold_wave_domain: HashMap::new(),
                express_ready_queue: VecDeque::new(),
                express_streak: 0,
                ready_queue: BinaryHeap::new(),
                next_seq: 0,
                active_admissions: HashMap::new(),
                invocation_starts: HashMap::new(),
                pricing_queue_ms: HashMap::new(),
                timing_samples: HashMap::new(),
                theta_warm,
                theta_cold,
                phi_aff,
                start_instant: Instant::now(),
            }),
            fineloadbalancing,
            container_manager: Mutex::new(cont_weak),
            cmap,
            beta,
            rho_knee,
            cores_per_domain,
            max_queue_per_domain,
            freeze_calibration,
            cold_wave_coalescing,
            num_domains,
        })
    }

    pub fn set_container_manager(&self, manager: Weak<ContainerManager>) {
        *self.container_manager.lock() = Some(manager);
    }

    fn fn_estimates(&self, state: &CoordinatorState, fqdn: &str) -> FnEwma {
        if let Some(entry) = state.fn_state.get(fqdn) {
            *entry
        } else {
            let cmap_warm = self.cmap.get_avg(fqdn, Chars::CpuWarmTime);
            let cmap_cold = self.cmap.get_avg(fqdn, Chars::CpuColdTime);
            FnEwma {
                s_i: if cmap_warm > 0.0 { cmap_warm } else { DEFAULT_S_I },
                t_boot: if cmap_cold > 0.0 { cmap_cold } else { DEFAULT_T_BOOT },
                eta_i: DEFAULT_ETA,
            }
        }
    }

    /// Take the start time and time spent in the coordinator's user-space queue.
    pub fn take_invocation_timing(&self, tid: &TransactionId) -> (f64, Instant) {
        let mut state = self.state.lock();
        let started_at = state
            .invocation_starts
            .remove(tid)
            .unwrap_or_else(Instant::now);
        let pricing_queue_ms = state.pricing_queue_ms.remove(tid).unwrap_or(0.0);
        (pricing_queue_ms, started_at)
    }

    pub fn record_invocation_timing(
        &self,
        tid: &TransactionId,
        fqdn: &str,
        sample: CongestionPricingTimingSample,
    ) {
        let class = function_class(fqdn);
        debug!(
            tid = %tid,
            function_class = class,
            pricing_queue_ms = sample.pricing_queue_ms,
            container_acquire_ms = sample.container_acquire_ms,
            exec_ms = sample.exec_ms,
            post_overhead_ms = sample.post_overhead_ms,
            "[congestion_pricing] invocation timing"
        );
        self.state.lock().timing_samples.entry(class).or_default().push(sample);
    }

    fn get_container_state(&self, fqdn: &str) -> ContainerState {
        if let Some(cm) = self.container_manager.lock().as_ref().and_then(Weak::upgrade) {
            cm.container_available(fqdn, Compute::CPU)
        } else if let Some(flb) = self.fineloadbalancing.upgrade() {
            flb.idle_cpu_container_state(fqdn).unwrap_or(ContainerState::Cold)
        } else {
            ContainerState::Cold
        }
    }

    fn decrement_booting_fqdn(state: &mut CoordinatorState, fqdn: &str) -> u32 {
        let mut remaining = 0;
        if let Some(booting) = state.booting_by_fqdn.get_mut(fqdn) {
            *booting = booting.saturating_sub(1);
            remaining = *booting;
            if *booting == 0 {
                state.booting_by_fqdn.remove(fqdn);
            }
        }
        remaining
    }

    fn release_active_admission_locked(
        &self,
        state: &mut CoordinatorState,
        tid: Option<&TransactionId>,
        fallback_domain: usize,
        fqdn: &str,
        is_cold_fallback: bool,
    ) -> (usize, bool) {
        if let Some(tid) = tid {
            if let Some(adm) = state.active_admissions.remove(tid) {
                if let Some(dom) = state.domains.get_mut(adm.domain_id) {
                    dom.active_weight_sum = (dom.active_weight_sum - adm.added_weight).max(0.0);
                    if adm.reserved_cold_boot {
                        dom.cold_booting = dom.cold_booting.saturating_sub(1);
                    }
                }
                if adm.reserved_booting_fqdn {
                    Self::decrement_booting_fqdn(state, &adm.fqdn);
                }
                return (adm.domain_id, adm.reserved_cold_boot);
            }
        }
        let s_i = self.fn_estimates(state, fqdn).s_i;
        let is_express_warm = !is_cold_fallback && s_i < 2.0;
        if let Some(dom) = state.domains.get_mut(fallback_domain) {
            if !is_express_warm {
                dom.active_weight_sum = (dom.active_weight_sum - s_i.powf(-self.beta)).max(0.0);
            }
            if is_cold_fallback {
                dom.cold_booting = dom.cold_booting.saturating_sub(1);
            }
        }
        (fallback_domain, is_cold_fallback)
    }

    /// Mark the in-flight cold wave as container-ready. The pioneer is still
    /// using the container, so followers stay queued until it returns to idle.
    pub fn on_cold_boot_finished(&self, fqdn: &str, domain_id: usize) {
        let mut state = self.state.lock();
        if domain_id < state.domains.len() {
            state.cold_wave_domain.insert(fqdn.to_string(), domain_id);
        }
        Self::decrement_booting_fqdn(&mut state, fqdn);
        if let Some(adm) = state
            .active_admissions
            .values_mut()
            .find(|adm| adm.fqdn == fqdn && adm.reserved_booting_fqdn)
        {
            adm.reserved_booting_fqdn = false;
        }
    }

    fn promote_next_pioneer_locked(&self, state: &mut CoordinatorState, fqdn: &str, preferred_domain: usize) -> bool {
        let preferred = state.cold_wave_domain.get(fqdn).copied().unwrap_or(preferred_domain);
        let mut order = Vec::with_capacity(state.domains.len());
        if preferred < state.domains.len() {
            order.push(preferred);
        }
        order.extend((0..state.domains.len()).filter(|domain| *domain != preferred));

        let mut next_pioneer = None;
        for domain_idx in order {
            let domain = &mut state.domains[domain_idx];
            let mut retained = BinaryHeap::new();
            while let Some(entry) = domain.coalesced_wait_queue.pop() {
                if next_pioneer.is_none() && entry.item.registration.fqdn == fqdn {
                    next_pioneer = Some((domain_idx, entry));
                } else {
                    retained.push(entry);
                }
            }
            domain.coalesced_wait_queue = retained;
            if next_pioneer.is_some() {
                break;
            }
        }

        let Some((domain_idx, mut entry)) = next_pioneer else {
            return false;
        };
        let now_ms = state.now_ms();
        let fn_entry = self.fn_estimates(state, fqdn);
        let w_base = fn_entry.s_i.powf(-self.beta);
        let wait_ms = (now_ms - entry.enqueue_ms).max(0.0);
        let deadline_ms = entry.enqueue_ms + scheduling_slack_ms(fn_entry.s_i);
        {
            let domain = &mut state.domains[domain_idx];
            domain.queue_weight_sum = (domain.queue_weight_sum - entry.weight).max(0.0);
            domain.active_weight_sum += w_base;
            domain.cold_booting += 1;
            domain.expected_finish_ms.push(Reverse(OrderedFloat(now_ms + fn_entry.t_boot)));
        }
        entry.is_cold = true;
        entry.coalesced = false;
        entry.item.likely_cold.store(true, std::sync::atomic::Ordering::Relaxed);
        *state.booting_by_fqdn.entry(fqdn.to_string()).or_default() += 1;
        state.cold_wave_domain.insert(fqdn.to_string(), domain_idx);
        state.push_ready(
            entry.item,
            deadline_ms,
            wait_ms,
            false,
            ActiveAdmission {
                domain_id: domain_idx,
                fqdn: fqdn.to_string(),
                added_weight: w_base,
                reserved_cold_boot: true,
                reserved_booting_fqdn: true,
            },
        );
        true
    }

    /// Release one coalesced follower as the next pioneer if the current boot
    /// failed. Keeps the wave moving without dispatching duplicate cold starts.
    pub fn on_cold_boot_failed(&self, fqdn: &str, failed_domain: usize) {
        let mut state = self.state.lock();
        let reserved_domain = state.cold_wave_domain.get(fqdn).copied().unwrap_or(failed_domain);
        let remaining_boots = Self::decrement_booting_fqdn(&mut state, fqdn);
        let s_i = self.fn_estimates(&state, fqdn).s_i;
        if let Some(domain) = state.domains.get_mut(reserved_domain) {
            domain.cold_booting = domain.cold_booting.saturating_sub(1);
            domain.active_weight_sum = (domain.active_weight_sum - s_i.powf(-self.beta)).max(0.0);
        }
        if self.cold_wave_coalescing && remaining_boots == 0 {
            self.promote_next_pioneer_locked(&mut state, fqdn, reserved_domain);
        }
        self.drain_wait_queues_locked(&mut state, reserved_domain);
    }

    /// Undo the scheduling reservation for a cold invocation that failed before
    /// it entered execution.
    pub fn on_cold_invocation_aborted(&self, fqdn: &str) {
        let mut state = self.state.lock();
        let domain_id = state.cold_wave_domain.get(fqdn).copied().unwrap_or(0);
        Self::decrement_booting_fqdn(&mut state, fqdn);
        let s_i = self.fn_estimates(&state, fqdn).s_i;
        if let Some(domain) = state.domains.get_mut(domain_id) {
            domain.cold_booting = domain.cold_booting.saturating_sub(1);
            domain.active_weight_sum = (domain.active_weight_sum - s_i.powf(-self.beta)).max(0.0);
        }
        self.drain_wait_queues_locked(&mut state, domain_id);
    }

    /// Clean up exact per-TID reservation when an admitted invocation aborts before completion.
    pub fn on_invocation_aborted(&self, tid: &TransactionId, fqdn: &str, acquire_failed: bool) {
        let mut state = self.state.lock();
        state.invocation_starts.remove(tid);
        state.pricing_queue_ms.remove(tid);
        let (released_dom, was_cold_boot) =
            self.release_active_admission_locked(&mut state, Some(tid), 0, fqdn, false);

        if self.cold_wave_coalescing && acquire_failed {
            let remaining_boots = state.booting_by_fqdn.get(fqdn).copied().unwrap_or(0);
            let total_warm = self
                .container_manager
                .lock()
                .as_ref()
                .and_then(Weak::upgrade)
                .map(|cm| cm.cpu_container_wave_counts(fqdn).2)
                .unwrap_or(0);
            if remaining_boots == 0 && (was_cold_boot || (total_warm == 0 && state.active_count_for_fqdn(fqdn) == 0)) {
                self.promote_next_pioneer_locked(&mut state, fqdn, released_dom);
            }
        }
        self.drain_wait_queues_locked(&mut state, released_dom);
    }

    /// Promote coalesced followers after a container returns to the idle pool.
    /// If the last container was removed (e.g. unhealthy) leaving 0 warm and 0
    /// booting replicas, promote a single follower as the replacement pioneer.
    pub fn on_warm_container_available(&self, fqdn: &str, preferred_domain: usize) {
        if !self.cold_wave_coalescing {
            return;
        }
        let Some(manager) = self.container_manager.lock().as_ref().and_then(Weak::upgrade) else {
            return;
        };
        let (warm, prewarm, total_warm) = manager.cpu_container_wave_counts(fqdn);
        let idle = warm + prewarm;
        let mut state = self.state.lock();
        if idle > 0 {
            Self::promote_coalesced_followers_with_beta(
                &mut state,
                fqdn,
                preferred_domain,
                idle,
                self.beta,
            );
        } else if total_warm == 0
            && state.booting_by_fqdn.get(fqdn).copied().unwrap_or(0) == 0
            && state.active_count_for_fqdn(fqdn) == 0
        {
            self.promote_next_pioneer_locked(&mut state, fqdn, preferred_domain);
        }
    }

    #[cfg(test)]
    fn promote_coalesced_followers(
        state: &mut CoordinatorState,
        fqdn: &str,
        preferred_domain: usize,
        available: usize,
    ) {
        Self::promote_coalesced_followers_with_beta(state, fqdn, preferred_domain, available, 1.0);
    }

    fn promote_coalesced_followers_with_beta(
        state: &mut CoordinatorState,
        fqdn: &str,
        preferred_domain: usize,
        mut available: usize,
        beta: f64,
    ) {
        if available == 0 {
            return;
        }

        let now_ms = state.now_ms();
        let s_i = state.fn_state.get(fqdn).map(|f| f.s_i).unwrap_or(DEFAULT_S_I);
        let is_express = s_i < 2.0;
        let w_base = s_i.powf(-beta);

        let num_domains = state.domains.len();
        let mut domain_order = Vec::with_capacity(num_domains);
        if preferred_domain < num_domains {
            domain_order.push(preferred_domain);
        }
        domain_order.extend((0..num_domains).filter(|d| *d != preferred_domain));

        let mut ready = Vec::new();
        for domain_idx in domain_order {
            let domain = &mut state.domains[domain_idx];
            let mut retained = Vec::new();
            while let Some(entry) = domain.coalesced_wait_queue.pop() {
                if available > 0 && entry.coalesced && entry.item.registration.fqdn == fqdn {
                    domain.queue_weight_sum = (domain.queue_weight_sum - entry.weight).max(0.0);
                    entry.item.likely_cold.store(false, std::sync::atomic::Ordering::Relaxed);
                    let wait_ms = (now_ms - entry.enqueue_ms).max(0.0);
                    let deadline_ms = entry.enqueue_ms + scheduling_slack_ms(s_i);
                    let target_dom = if preferred_domain < num_domains {
                        preferred_domain
                    } else {
                        domain_idx
                    };
                    ready.push((entry.item, deadline_ms, wait_ms, target_dom));
                    available -= 1;
                } else {
                    retained.push(entry);
                }
            }
            for entry in retained {
                domain.coalesced_wait_queue.push(entry);
            }
            if available == 0 {
                break;
            }
        }
        for (item, deadline_ms, wait_ms, target_dom) in ready {
            let added_weight = if is_express { 0.0 } else { w_base };
            if !is_express {
                if let Some(dom) = state.domains.get_mut(target_dom) {
                    dom.active_weight_sum += added_weight;
                    dom.expected_finish_ms.push(Reverse(OrderedFloat(now_ms + s_i)));
                }
            }
            state.push_ready(
                item,
                deadline_ms,
                wait_ms,
                is_express,
                ActiveAdmission {
                    domain_id: target_dom,
                    fqdn: fqdn.to_string(),
                    added_weight,
                    reserved_cold_boot: false,
                    reserved_booting_fqdn: false,
                },
            );
        }
    }

    /// Snapshot domains with an idle warm container.
    fn idle_warm_domains(&self, fqdn: &str) -> Vec<usize> {
        let Some(cm) = self.container_manager.lock().as_ref().and_then(Weak::upgrade) else {
            return Vec::new();
        };
        let Some(flb) = self.fineloadbalancing.upgrade() else {
            return Vec::new();
        };
        let cgroups = cm.idle_cpu_warm_cgroups(fqdn);
        if cgroups.is_empty() {
            return Vec::new();
        }
        let mut domains: Vec<usize> = cgroups
            .iter()
            .filter_map(|cgroup| flb.preallocated_domains.cgroup_domain(cgroup))
            .map(|domain| domain as usize)
            .filter(|domain| *domain < self.num_domains)
            .collect();
        if domains.len() != cgroups.len() {
            domains.extend(0..self.num_domains);
        }
        domains.sort_unstable();
        domains.dedup();
        domains
    }

    fn read_rho(&self, domain_id: SchedGroupID, state: &CoordinatorState) -> f64 {
        if let Some(flb) = self.fineloadbalancing.upgrade() {
            // TODO: incorporate in-flight core reservations cleanly without distorting PELT utilization.
            if let Ok(Some(pelt_pct)) = flb.domain_pelt_like_utilization_percent(domain_id) {
                let pelt_ratio = (pelt_pct / 100.0).max(0.0);
                if pelt_pct >= 99.0 {
                    if let Some(reader) = flb.core_metrics_reader.as_ref() {
                        if let Some(group) = flb.preallocated_domains.get_schedgroup(domain_id) {
                            let mut dsq_sum: u64 = 0;
                            for cpu in &group.cores {
                                if let Ok(dsq) = reader.domain_dsq_len(*cpu) {
                                    dsq_sum = dsq;
                                    break;
                                }
                            }
                            if dsq_sum > 0 {
                                return 1.0 + (dsq_sum as f64) / (self.cores_per_domain as f64);
                            }
                        }
                    }
                }
                return pelt_ratio;
            }
        }
        if let Some(dom) = state.domains.get(domain_id as usize) {
            (dom.active_weight_sum / (self.cores_per_domain as f64)).max(0.0)
        } else {
            0.0
        }
    }

    fn sigma_warm(&self, rho: f64, theta_warm: f64) -> f64 {
        if rho <= self.rho_knee {
            1.0
        } else {
            1.0 + theta_warm * (rho - self.rho_knee)
        }
    }

    fn delta_sigma_warm(&self, rho: f64, eta: f64, c: f64, theta_warm: f64) -> f64 {
        let delta_rho = eta / c;
        self.sigma_warm(rho + delta_rho, theta_warm) - self.sigma_warm(rho, theta_warm)
    }

    fn delta_mu_warm(rho: f64, eta: f64, c: f64) -> f64 {
        if rho < 1.0 && eta < 0.8 {
            ((1.0 - rho).min(1.0 - eta) / c).max(0.0)
        } else {
            0.0
        }
    }

    fn delta_sigma_cold(k_cold: u32, c: f64, theta_cold: f64) -> f64 {
        theta_cold * ((k_cold + 1) as f64) / c
    }

    fn drain_wait_queues_locked(&self, state: &mut CoordinatorState, start_domain: usize) {
        let now_ms = state.now_ms();
        let theta_warm_val = state.theta_warm;
        let c_per_dom = self.cores_per_domain as f64;
        let n_doms = state.domains.len();
        if n_doms == 0 {
            return;
        }

        let mut order = Vec::with_capacity(n_doms);
        if start_domain < n_doms {
            order.push(start_domain);
        }
        order.extend((0..n_doms).filter(|d| *d != start_domain));

        let mut drained_items = Vec::new();
        for dom_idx in order {
            loop {
                let Some(top_clone) = state.domains[dom_idx].wait_queue.peek().cloned() else {
                    break;
                };
                if top_clone.coalesced {
                    break;
                }
                let fqdn = top_clone.item.registration.fqdn.clone();
                let fn_est = self.fn_estimates(state, &fqdn);
                let top_s_i = fn_est.s_i;
                let top_t_boot = fn_est.t_boot;
                let top_eta_i = fn_est.eta_i;

                let wait_ms = (now_ms - top_clone.enqueue_ms).max(0.0);
                let w_base_top = top_s_i.powf(-self.beta);
                let w_eff_top = w_base_top * (1.0 + wait_ms / (2.0 * top_s_i.max(0.1)));
                let is_express = !top_clone.is_cold && top_s_i < 2.0;

                let dom = &mut state.domains[dom_idx];
                dom.prune_finished(now_ms);
                let rho_d = (dom.active_weight_sum / c_per_dom).max(0.0);
                let r_min_d = dom.r_min(now_ms, rho_d);
                let delta_sigma = self.delta_sigma_warm(rho_d, top_eta_i, c_per_dom, theta_warm_val);
                let sigma = self.sigma_warm(rho_d, theta_warm_val);

                let benefit = w_eff_top * ((r_min_d * sigma).max(0.01) - top_s_i * delta_sigma);
                let toll = delta_sigma * dom.active_weight_sum * top_s_i;

                if is_express || benefit - toll > 0.0 || rho_d < 1.0 {
                    let popped = dom.wait_queue.pop().unwrap();
                    dom.queue_weight_sum = (dom.queue_weight_sum - popped.weight).max(0.0);
                    let added_weight = if is_express { 0.0 } else { w_base_top };
                    let reserved_cold_boot = !is_express && popped.is_cold;
                    if !is_express {
                        dom.active_weight_sum += added_weight;
                        if reserved_cold_boot {
                            dom.cold_booting += 1;
                        }
                        let expected_dur = if popped.is_cold { top_t_boot } else { top_s_i };
                        dom.expected_finish_ms.push(Reverse(OrderedFloat(now_ms + expected_dur)));
                    }
                    let deadline_ms = popped.enqueue_ms + scheduling_slack_ms(top_s_i);
                    drained_items.push((
                        popped.item,
                        deadline_ms,
                        wait_ms,
                        is_express,
                        ActiveAdmission {
                            domain_id: dom_idx,
                            fqdn,
                            added_weight,
                            reserved_cold_boot,
                            reserved_booting_fqdn: popped.is_cold,
                        },
                    ));
                } else {
                    break;
                }
            }
        }

        for (item, deadline_ms, wait_ms, express, adm) in drained_items {
            state.push_ready(item, deadline_ms, wait_ms, express, adm);
        }

        // Work-conservation anti-deadlock fallback: if zero invocations are active or ready
        // anywhere in the coordinator, never leave non-coalesced wait_queue work stranded.
        if !state.has_ready_work() && state.active_admissions.is_empty() {
            let mut best_dom = None;
            let mut best_deadline = OrderedFloat(f64::INFINITY);
            for (idx, d) in state.domains.iter().enumerate() {
                if let Some(top) = d.wait_queue.peek() {
                    if !top.coalesced && top.deadline_ms < best_deadline {
                        best_deadline = top.deadline_ms;
                        best_dom = Some(idx);
                    }
                }
            }
            if let Some(dom_idx) = best_dom {
                let popped = state.domains[dom_idx].wait_queue.pop().unwrap();
                let fqdn = popped.item.registration.fqdn.clone();
                let fn_est = self.fn_estimates(state, &fqdn);
                let wait_ms = (now_ms - popped.enqueue_ms).max(0.0);
                let w_base = fn_est.s_i.powf(-self.beta);
                let deadline_ms = popped.enqueue_ms + scheduling_slack_ms(fn_est.s_i);
                let is_express = !popped.is_cold && fn_est.s_i < 2.0;
                let added_weight = if is_express { 0.0 } else { w_base };
                let reserved_cold_boot = !is_express && popped.is_cold;
                {
                    let dom = &mut state.domains[dom_idx];
                    dom.queue_weight_sum = (dom.queue_weight_sum - popped.weight).max(0.0);
                    if !is_express {
                        dom.active_weight_sum += added_weight;
                        if reserved_cold_boot {
                            dom.cold_booting += 1;
                        }
                        let expected_dur = if popped.is_cold { fn_est.t_boot } else { fn_est.s_i };
                        dom.expected_finish_ms.push(Reverse(OrderedFloat(now_ms + expected_dur)));
                    }
                }
                state.push_ready(
                    popped.item,
                    deadline_ms,
                    wait_ms,
                    is_express,
                    ActiveAdmission {
                        domain_id: dom_idx,
                        fqdn,
                        added_weight,
                        reserved_cold_boot,
                        reserved_booting_fqdn: popped.is_cold,
                    },
                );
            }
        }
    }

    pub fn on_completion(
        &self,
        domain_id: usize,
        fqdn: &str,
        wall_exec_ms: f64,
        is_cold: bool,
        rho_obs: f64,
        cpu_exec_ms: Option<f64>,
    ) {
        self.on_completion_inner(None, domain_id, fqdn, wall_exec_ms, is_cold, rho_obs, cpu_exec_ms);
    }

    pub fn on_invocation_completed(
        &self,
        tid: &TransactionId,
        domain_id: usize,
        fqdn: &str,
        wall_exec_ms: f64,
        is_cold: bool,
        rho_obs: f64,
        cpu_exec_ms: Option<f64>,
    ) {
        self.on_completion_inner(Some(tid), domain_id, fqdn, wall_exec_ms, is_cold, rho_obs, cpu_exec_ms);
    }

    fn on_completion_inner(
        &self,
        tid: Option<&TransactionId>,
        domain_id: usize,
        fqdn: &str,
        wall_exec_ms: f64,
        is_cold: bool,
        rho_obs: f64,
        cpu_exec_ms: Option<f64>,
    ) {
        let mut state = self.state.lock();
        let now_ms = state.now_ms();
        let warm_sample_ms = cpu_exec_ms.unwrap_or(wall_exec_ms).max(0.05);

        // 1. Online calibration update
        if !self.freeze_calibration {
            if !is_cold && rho_obs > self.rho_knee {
                let s_prior = self.fn_estimates(&state, fqdn).s_i;
                let s_obs = (warm_sample_ms / s_prior.max(0.05)).clamp(1.0, 4.0);
                let theta_obs = (s_obs - 1.0) / (rho_obs - self.rho_knee);
                let alpha = 0.02;
                state.theta_warm = ((1.0 - alpha) * state.theta_warm + alpha * theta_obs).clamp(0.25, 3.0);
            }
            if is_cold {
                if let Some(dom) = state.domains.get(domain_id) {
                    if dom.cold_booting >= 2 {
                        let alpha = 0.05;
                        let t_prior = self.fn_estimates(&state, fqdn).t_boot;
                        let s_obs = (wall_exec_ms / t_prior.max(1.0)).clamp(1.0, 5.0);
                        let theta_obs = s_obs;
                        state.theta_cold = ((1.0 - alpha) * state.theta_cold + alpha * theta_obs).clamp(0.5, 5.0);
                    }
                }
            }
        }

        // 2. Release exact admission reservation BEFORE mutating fn_state so weight accounting never drifts
        let (released_dom, _) =
            self.release_active_admission_locked(&mut state, tid, domain_id, fqdn, is_cold);
        if let Some(dom) = state.domains.get_mut(released_dom) {
            dom.prune_finished(now_ms);
        }

        // 3. Per-function EWMA updates (use pure function execution duration when available so sub-2ms
        // functions like `hello` are not pushed out of the express lane by HTTP RPC overhead).
        let mut fn_entry = self.fn_estimates(&state, fqdn);
        if !is_cold {
            if rho_obs <= 1.0 {
                fn_entry.s_i = (0.9 * fn_entry.s_i + 0.1 * warm_sample_ms).max(0.05);
            } else {
                let stretch = self.sigma_warm(rho_obs, state.theta_warm);
                let s_norm = warm_sample_ms / stretch;
                fn_entry.s_i = (0.9 * fn_entry.s_i + 0.1 * s_norm).max(0.05);
            }
        } else {
            fn_entry.t_boot = (0.9 * fn_entry.t_boot + 0.1 * wall_exec_ms).max(1.0);
        }

        if let Some(cpu_ms) = cpu_exec_ms {
            let realized_eta = (cpu_ms / wall_exec_ms.max(0.01)).clamp(0.05, 1.0);
            fn_entry.eta_i = 0.9 * fn_entry.eta_i + 0.1 * realized_eta;
        }
        state.fn_state.insert(fqdn.to_string(), fn_entry);

        // 4. Drain eligible wait_queue items across domains
        self.drain_wait_queues_locked(&mut state, released_dom);
    }

    fn compute_welfare_gain(
        &self,
        domain_id: usize,
        home_domain: usize,
        price_as_cold: bool,
        s_i: f64,
        t_boot: f64,
        eta_i: f64,
        w_eff: f64,
        creation_credit: f64,
        rho_d: f64,
        state: &mut CoordinatorState,
        now_ms: f64,
    ) -> WelfareBreakdown {
        let c_per_dom = self.cores_per_domain as f64;
        let theta_warm = state.theta_warm;
        let theta_cold = state.theta_cold;
        let phi_aff = state.phi_aff;
        let dom = &mut state.domains[domain_id];
        let r_min_d = dom.r_min(now_ms, rho_d);

        let sigma = self.sigma_warm(rho_d, theta_warm);
        let delta_sigma = self.delta_sigma_warm(rho_d, eta_i, c_per_dom, theta_warm);
        let delta_mu = Self::delta_mu_warm(rho_d, eta_i, c_per_dom);
        let home_bonus = if domain_id == home_domain {
            phi_aff * s_i * (1.0_f64 - rho_d).max(0.0)
        } else {
            0.0
        };

        let benefit = w_eff * ((r_min_d * sigma).max(0.01) - s_i * delta_sigma + home_bonus);

        let warm_toll = delta_sigma * dom.active_weight_sum * s_i;
        let cold_toll = if price_as_cold {
            let delta_cold = Self::delta_sigma_cold(dom.cold_booting, c_per_dom, theta_cold);
            delta_cold * dom.active_weight_sum * t_boot
        } else {
            0.0
        };
        let eff_q_weight = dom.queue_weight_sum.min(c_per_dom);
        let queue_backlog_toll = s_i.min(r_min_d) * delta_sigma * eff_q_weight;

        let io_overlap_credit = delta_mu * s_i * eff_q_weight;
        let gamma_d = creation_credit + io_overlap_credit;

        let toll = warm_toll + cold_toll + queue_backlog_toll - gamma_d;
        WelfareBreakdown {
            rho: rho_d,
            r_min: r_min_d,
            sigma,
            delta_sigma,
            benefit,
            warm_toll,
            cold_toll,
            queue_backlog_toll,
            creation_credit,
            io_overlap_credit,
            total_toll: toll,
            net_gain: benefit - toll,
        }
    }

    fn route_or_enqueue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        let invocation_started_at = Instant::now();
        let fqdn = &item.registration.fqdn;
        let cstate = self.get_container_state(fqdn);
        let warm_domains = self.idle_warm_domains(fqdn);

        let mut hash_val: u64 = 0;
        for b in fqdn.bytes() {
            hash_val = hash_val.wrapping_mul(31).wrapping_add(b as u64);
        }
        let home_domain = (hash_val as usize) % self.num_domains.max(1);

        let mut state = self.state.lock();
        let now_ms = state.now_ms();
        let spill_domain = if self.num_domains > 1 {
            (home_domain + 1 + ((state.next_seq as usize) % (self.num_domains - 1))) % self.num_domains
        } else {
            home_domain
        };

        let queued_for_fqdn = state.express_ready_queue.iter()
            .chain(state.ready_queue.iter())
            .filter(|queued| queued.item.registration.fqdn == *fqdn)
            .count()
            + state.domains.iter().map(|domain| domain.wait_queue.iter()
                .chain(domain.coalesced_wait_queue.iter())
                .filter(|queued| queued.item.registration.fqdn == *fqdn).count()).sum::<usize>();
        let (idle_warm_count, idle_prewarm_count, total_warm) = self
            .container_manager
            .lock()
            .as_ref()
            .and_then(Weak::upgrade)
            .map(|manager| manager.cpu_container_wave_counts(fqdn))
            .unwrap_or_else(|| match cstate {
                ContainerState::Warm => (1, 0, 1),
                ContainerState::Prewarm => (0, 1, 1),
                _ => (0, 0, 0),
            });
        let idle_available = idle_warm_count + idle_prewarm_count;
        let booting = state.booting_by_fqdn.get(fqdn).copied().unwrap_or(0);
        let total_booting_global: u32 = state.booting_by_fqdn.values().sum();
        let wave_state = cold_wave_state(idle_available, total_warm, booting);
        let n_doms = self.num_domains.max(1);
        let home_rotated_domains: Vec<usize> = (0..n_doms)
            .map(|offset| (home_domain + offset) % n_doms)
            .collect();
        let mut warm_candidates = warm_domains.clone();
        if warm_candidates.is_empty() && idle_available > 0 {
            warm_candidates.extend(home_rotated_domains.iter().copied());
        }

        let fn_est = self.fn_estimates(&state, fqdn);
        let (s_i, t_boot, eta_i) = (fn_est.s_i, fn_est.t_boot, fn_est.eta_i);
        let deadline_ms = now_ms + scheduling_slack_ms(s_i);

        let w_base = s_i.powf(-self.beta);
        let w_eff = w_base;

        let underloaded_domains: Vec<(usize, f64)> = home_rotated_domains
            .iter()
            .copied()
            .map(|domain| (domain, self.read_rho(domain as SchedGroupID, &state)))
            .filter(|(_, rho)| *rho < 0.85)
            .collect();
        let replica_count = (total_warm + booting as usize).max(1);
        let max_replicas = (self.cores_per_domain as usize * n_doms).max(2);
        let max_global_boots = (self.cores_per_domain * 2).max(4);
        // Scale out smoothly 1 replica at a time per FQDN when coalescing is enabled
        // and at least one full wave per existing replica is already queued.
        let scale_out = wave_state == ColdWaveState::Follower
            && if self.cold_wave_coalescing {
                booting == 0
                    && total_booting_global < max_global_boots
                    && replica_count < max_replicas
                    && queued_for_fqdn >= replica_count
                    && !underloaded_domains.is_empty()
            } else {
                total_warm < (self.cores_per_domain as usize) && booting < self.cores_per_domain
            };
        let likely_cold = wave_state == ColdWaveState::Pioneer || scale_out;
        let force_coalesce = self.cold_wave_coalescing
            && wave_state == ColdWaveState::Follower
            && !scale_out
            && idle_available == 0;
        let creation_credit = match wave_state {
            ColdWaveState::Pioneer => pioneer_creation_credit(w_eff, t_boot, queued_for_fqdn, self.cores_per_domain),
            ColdWaveState::Follower if scale_out => scaleout_creation_credit(w_eff, t_boot, queued_for_fqdn, self.cores_per_domain),
            _ => 0.0,
        };
        let is_cold = likely_cold;
        item.likely_cold.store(likely_cold, std::sync::atomic::Ordering::Relaxed);
        if likely_cold {
            debug!(
                tid = %item.tid,
                fqdn = %fqdn,
                wave_state = ?wave_state,
                queued_for_fqdn,
                idle_warm_count,
                idle_prewarm_count,
                total_warm,
                booting,
                creation_credit,
                "[congestion_pricing] invocation predicted likely cold"
            );
        }

        // Warm Express-Lane Bypass: sub-2.0ms warm tasks finish within a scheduler tick
        // and bypass user-space congestion gating without distorting r_min or active_weight_sum.
        if !likely_cold && !force_coalesce && s_i < 2.0 {
            let primary = if !warm_domains.is_empty() {
                warm_domains
                    .iter()
                    .copied()
                    .min_by(|a, b| {
                        let ra = self.read_rho(*a as SchedGroupID, &state);
                        let rb = self.read_rho(*b as SchedGroupID, &state);
                        ra.total_cmp(&rb)
                    })
                    .unwrap_or(home_domain)
            } else {
                home_domain
            };
            let po2_candidates = select_po2_candidates(
                primary,
                &home_rotated_domains,
                rand::rng().random_range(0..n_doms),
            );
            let c_mig = soft_affinity_migration_cost(s_i);
            let best_dom = po2_candidates
                .into_iter()
                .min_by(|a, b| {
                    let ra = self.read_rho(*a as SchedGroupID, &state);
                    let rb = self.read_rho(*b as SchedGroupID, &state);
                    let ca = placement_ranking_cost(ra, s_i, 0.0, c_mig, warm_domains.contains(a));
                    let cb = placement_ranking_cost(rb, s_i, 0.0, c_mig, warm_domains.contains(b));
                    (OrderedFloat(ca), OrderedFloat(ra)).cmp(&(OrderedFloat(cb), OrderedFloat(rb)))
                })
                .unwrap_or(primary);

            trace!(
                tid = %item.tid,
                fqdn = %fqdn,
                s_i,
                best_dom,
                likely_cold,
                queued_for_fqdn,
                idle_warm_count,
                idle_prewarm_count,
                "[congestion_pricing] express-lane bypass admission"
            );

            state.invocation_starts.entry(item.tid.clone()).or_insert(invocation_started_at);
            state.push_ready(
                item.clone(),
                deadline_ms,
                0.0,
                idle_available > 0,
                ActiveAdmission {
                    domain_id: best_dom,
                    fqdn: fqdn.clone(),
                    added_weight: 0.0,
                    reserved_cold_boot: false,
                    reserved_booting_fqdn: false,
                },
            );
            return Ok(());
        }

        let domain_tie_key = |domain: usize, rho: f64, st: &CoordinatorState| {
            let active_w = st.domains.get(domain).map(|d| d.active_weight_sum).unwrap_or(0.0);
            let dist = (domain + n_doms - home_domain) % n_doms;
            (OrderedFloat(rho), OrderedFloat(active_w), dist)
        };

        let candidates: Vec<usize> = match wave_state {
            // Power-of-Two soft-affinity sampling: compare the best warm (or home)
            // domain against one random alternate domain to prevent burst herding.
            ColdWaveState::WarmIdle => {
                let primary = if !warm_domains.is_empty() {
                    warm_domains
                        .iter()
                        .copied()
                        .min_by(|a, b| {
                            let ra = self.read_rho(*a as SchedGroupID, &state);
                            let rb = self.read_rho(*b as SchedGroupID, &state);
                            ra.total_cmp(&rb)
                        })
                        .unwrap_or(home_domain)
                } else {
                    home_domain
                };
                select_po2_candidates(
                    primary,
                    &home_rotated_domains,
                    rand::rng().random_range(0..n_doms),
                )
            },
            ColdWaveState::Pioneer => {
                let no_cold_boots: Vec<usize> = home_rotated_domains.iter().copied()
                    .filter(|domain| state.domains[*domain].cold_booting == 0)
                    .collect();
                let pool = if no_cold_boots.is_empty() { home_rotated_domains.clone() } else { no_cold_boots };
                let best = pool.into_iter().min_by(|a, b| {
                    let rho_a = self.read_rho(*a as SchedGroupID, &state);
                    let rho_b = self.read_rho(*b as SchedGroupID, &state);
                    domain_tie_key(*a, rho_a, &state).cmp(&domain_tie_key(*b, rho_b, &state))
                }).unwrap_or(home_domain);
                vec![best]
            },
            ColdWaveState::Follower if scale_out => {
                let pool: Vec<(usize, f64)> = if underloaded_domains.is_empty() {
                    home_rotated_domains.iter().copied()
                        .map(|d| (d, self.read_rho(d as SchedGroupID, &state)))
                        .collect()
                } else {
                    underloaded_domains.clone()
                };
                let best = pool.into_iter()
                    .min_by(|(da, ra), (db, rb)| {
                        domain_tie_key(*da, *ra, &state).cmp(&domain_tie_key(*db, *rb, &state))
                    })
                    .map(|(domain, _)| domain)
                    .unwrap_or(home_domain);
                vec![best]
            },
            ColdWaveState::Follower => {
                let preferred = if self.cold_wave_coalescing {
                    state.cold_wave_domain.get(fqdn).copied()
                } else {
                    None
                };
                if let Some(pref) = preferred {
                    vec![pref]
                } else {
                    home_rotated_domains.clone()
                }
            },
        };
        let mut price_candidates = Vec::with_capacity(candidates.len());
        for candidate in candidates.iter().copied() {
            let rho = self.read_rho(candidate as SchedGroupID, &state);
            let price = self.compute_welfare_gain(
                candidate, home_domain, likely_cold, s_i, t_boot, eta_i, w_eff,
                creation_credit, rho, &mut state, now_ms,
            );
            price_candidates.push((candidate, price));
        }
        let (chosen_domain, mut max_net) = if wave_state == ColdWaveState::WarmIdle {
            let c_mig = soft_affinity_migration_cost(s_i);
            let best_warm = price_candidates
                .iter()
                .enumerate()
                .min_by(|(idx_a, (da, pa)), (idx_b, (db, pb))| {
                    let overload_a = pa.warm_toll + pa.cold_toll + pa.queue_backlog_toll;
                    let overload_b = pb.warm_toll + pb.cold_toll + pb.queue_backlog_toll;
                    let cost_a = placement_ranking_cost(
                        pa.rho,
                        s_i,
                        overload_a,
                        c_mig,
                        warm_domains.contains(da),
                    );
                    let cost_b = placement_ranking_cost(
                        pb.rho,
                        s_i,
                        overload_b,
                        c_mig,
                        warm_domains.contains(db),
                    );
                    let wa = state.domains.get(*da).map(|d| d.active_weight_sum).unwrap_or(0.0);
                    let wb = state.domains.get(*db).map(|d| d.active_weight_sum).unwrap_or(0.0);
                    (OrderedFloat(cost_a), OrderedFloat(pa.rho), OrderedFloat(wa), *idx_a).cmp(&(
                        OrderedFloat(cost_b),
                        OrderedFloat(pb.rho),
                        OrderedFloat(wb),
                        *idx_b,
                    ))
                })
                .map(|(_, (d, p))| (*d, p.net_gain));
            best_warm.unwrap_or((home_domain, f64::NEG_INFINITY))
        } else {
            best_net_gain_candidate(&price_candidates)
                .unwrap_or((home_domain, f64::NEG_INFINITY))
        };

        let mut final_domain = chosen_domain;
        let mut work_conservation_override = false;
        // Warm-ready capacity has already incurred its memory cost. Keep its
        // marginal price for domain ranking, but never use it to hold a ready
        // WarmIdle invocation in the coordinator queue.
        // Cold starts and scale-out still use the work-conservation fallback.
        if should_use_work_conservation_override(wave_state, max_net) {
            let fallback_candidates = candidates.clone();
            for d in fallback_candidates {
                let rho = self.read_rho(d as SchedGroupID, &state);
                if rho < 1.0 {
                    final_domain = d;
                    max_net = 1.0;
                    work_conservation_override = true;
                    break;
                }
            }
        }

        let admitted = should_admit_invocation(wave_state, max_net, force_coalesce);
        for (candidate, price) in price_candidates {
            debug!(
                decision_time_ms = now_ms,
                tid = %item.tid,
                fqdn = %fqdn,
                function_class = function_class(fqdn),
                candidate_domain = candidate,
                chosen_domain,
                final_domain,
                selected = candidate == final_domain,
                admitted,
                force_coalesce,
                work_conservation_override,
                likely_cold,
                wave_state = ?wave_state,
                estimated_runtime_ms = s_i,
                rho = price.rho,
                r_min_ms = price.r_min,
                sigma_warm = price.sigma,
                delta_sigma_warm = price.delta_sigma,
                benefit = price.benefit,
                warm_toll = price.warm_toll,
                cold_toll = price.cold_toll,
                queue_backlog_toll = price.queue_backlog_toll,
                creation_credit = price.creation_credit,
                io_overlap_credit = price.io_overlap_credit,
                total_marginal_toll = price.total_toll,
                net_gain = price.net_gain,
                welfare_beta = self.beta,
                theta_warm = state.theta_warm,
                theta_cold = state.theta_cold,
                rho_knee = self.rho_knee,
                queued_for_fqdn,
                idle_warm_count,
                idle_prewarm_count,
                "[congestion_pricing] price candidate"
            );
        }

        trace!(
            tid = %item.tid,
            fqdn = %fqdn,
            home_domain,
            spill_domain,
            final_domain,
            max_net,
            is_cold,
            likely_cold,
            queued_for_fqdn,
            idle_warm_count,
            idle_prewarm_count,
            "[congestion_pricing] routing decision"
        );

        if admitted {
            if likely_cold {
                *state.booting_by_fqdn.entry(fqdn.to_string()).or_default() += 1;
                state.cold_wave_domain.insert(fqdn.to_string(), final_domain);
            }
            {
                let dom = &mut state.domains[final_domain];
                dom.active_weight_sum += w_base;
                if is_cold {
                    dom.cold_booting += 1;
                }
                let expected_dur = if is_cold { t_boot } else { s_i };
                dom.expected_finish_ms.push(Reverse(OrderedFloat(now_ms + expected_dur)));
            }
            state.invocation_starts.entry(item.tid.clone()).or_insert(invocation_started_at);
            state.push_ready(
                item.clone(),
                deadline_ms,
                0.0,
                false,
                ActiveAdmission {
                    domain_id: final_domain,
                    fqdn: fqdn.clone(),
                    added_weight: w_base,
                    reserved_cold_boot: is_cold,
                    reserved_booting_fqdn: likely_cold,
                },
            );
        } else {
            let queued_in_domain = state.domains[final_domain].wait_queue.len()
                + state.domains[final_domain].coalesced_wait_queue.len();
            let queue_limit = if force_coalesce {
                self.max_queue_per_domain.saturating_mul(2)
            } else {
                self.max_queue_per_domain
            };
            if queued_in_domain >= queue_limit {
                anyhow::bail!(
                    "Congestion pricing: domain {} queue full ({}/{})",
                    final_domain,
                    queued_in_domain,
                    queue_limit
                );
            }
            if likely_cold {
                *state.booting_by_fqdn.entry(fqdn.to_string()).or_default() += 1;
                state.cold_wave_domain.insert(fqdn.to_string(), final_domain);
            }
            item.set_preferred_domain(Some(final_domain as SchedGroupID));
            let seq = state.alloc_seq();
            let dom = &mut state.domains[final_domain];
            dom.queue_weight_sum += w_base;
            let entry = WaitEntry {
                deadline_ms: OrderedFloat(deadline_ms),
                weight: w_base,
                seq,
                enqueue_ms: now_ms,
                item: item.clone(),
                is_cold,
                coalesced: force_coalesce,
            };
            if force_coalesce {
                dom.coalesced_wait_queue.push(entry);
            } else {
                dom.wait_queue.push(entry);
            }
            state.invocation_starts.entry(item.tid.clone()).or_insert(invocation_started_at);
        }

        Ok(())
    }

    /// Re-evaluate domain placement at dispatch time using live domain load and
    /// warm-container locality with Power-of-Two sampling, transferring any active
    /// reservation cleanly.
    fn refresh_dispatch_domain_locked(
        &self,
        state: &mut CoordinatorState,
        item: &Arc<EnqueuedInvocation>,
        warm_domains: &[usize],
    ) {
        let n_doms = state.domains.len();
        if n_doms <= 1 {
            return;
        }
        let random_offset = rand::rng().random_range(0..n_doms);
        self.refresh_dispatch_domain_with_offset_locked(state, item, warm_domains, random_offset);
    }

    fn refresh_dispatch_domain_with_offset_locked(
        &self,
        state: &mut CoordinatorState,
        item: &Arc<EnqueuedInvocation>,
        warm_domains: &[usize],
        random_offset: usize,
    ) {
        let n_doms = state.domains.len();
        if n_doms <= 1 {
            return;
        }
        let Some((current_dom, added_weight, reserved_cold_boot)) = state
            .active_admissions
            .get(&item.tid)
            .map(|adm| (adm.domain_id, adm.added_weight, adm.reserved_cold_boot))
        else {
            return;
        };

        let fqdn = &item.registration.fqdn;
        let mut hash_val: u64 = 0;
        for b in fqdn.bytes() {
            hash_val = hash_val.wrapping_mul(31).wrapping_add(b as u64);
        }
        let home_domain = (hash_val as usize) % n_doms;
        let home_rotated: Vec<usize> = (0..n_doms).map(|off| (home_domain + off) % n_doms).collect();
        let is_cold = item.likely_cold.load(std::sync::atomic::Ordering::Relaxed);
        let fn_est = self.fn_estimates(state, fqdn);
        let (s_i, t_boot, eta_i) = (fn_est.s_i, fn_est.t_boot, fn_est.eta_i);
        let w_eff = s_i.powf(-self.beta);
        let now_ms = state.now_ms();
        let aff_domain = if !is_cold && !warm_domains.is_empty() {
            warm_domains
                .iter()
                .copied()
                .min_by(|a, b| {
                    let ra = self.read_rho(*a as SchedGroupID, state);
                    let rb = self.read_rho(*b as SchedGroupID, state);
                    ra.total_cmp(&rb)
                })
                .unwrap_or(warm_domains[0])
        } else {
            home_domain
        };

        let candidates: Vec<usize> = if !is_cold {
            select_po2_candidates(aff_domain, &home_rotated, random_offset)
        } else {
            let no_other_cold: Vec<usize> = home_rotated
                .iter()
                .copied()
                .filter(|&d| {
                    let boots = state.domains[d].cold_booting;
                    boots == 0 || (d == current_dom && reserved_cold_boot && boots == 1)
                })
                .collect();
            let pool = if no_other_cold.is_empty() {
                home_rotated
            } else {
                no_other_cold
            };
            let primary = if pool.contains(&current_dom) {
                current_dom
            } else {
                pool[0]
            };
            select_po2_candidates(primary, &pool, random_offset)
        };

        let mut best_dom = current_dom;
        let mut best_key: Option<(OrderedFloat<f64>, OrderedFloat<f64>, OrderedFloat<f64>, usize)> = None;
        let c_mig = soft_affinity_migration_cost(s_i);
        for d in candidates {
            let rho_d = self.read_rho(d as SchedGroupID, state);
            let aff_target = if !is_cold && warm_domains.contains(&d) { d } else { aff_domain };
            let price = self.compute_welfare_gain(
                d, aff_target, is_cold, s_i, t_boot, eta_i, w_eff, 0.0, rho_d, state, now_ms,
            );
            let overload_toll = price.warm_toll + price.cold_toll + price.queue_backlog_toll;
            let is_warm_dom = is_cold || warm_domains.contains(&d);
            let cost = placement_ranking_cost(rho_d, s_i, overload_toll, c_mig, is_warm_dom);
            let eff_w = (state.domains[d].active_weight_sum
                - if d == current_dom { added_weight } else { 0.0 })
            .max(0.0);
            let dist = (d + n_doms - home_domain) % n_doms;
            let key = (
                OrderedFloat(cost),
                OrderedFloat(rho_d),
                OrderedFloat(eff_w),
                dist,
            );
            if best_key.map_or(true, |cur| key < cur) {
                best_key = Some(key);
                best_dom = d;
            }
        }

        if best_dom != current_dom {
            if added_weight > 0.0 {
                if let Some(old_dom) = state.domains.get_mut(current_dom) {
                    old_dom.active_weight_sum = (old_dom.active_weight_sum - added_weight).max(0.0);
                }
                if let Some(new_dom) = state.domains.get_mut(best_dom) {
                    new_dom.active_weight_sum += added_weight;
                }
            }
            if reserved_cold_boot {
                if let Some(old_dom) = state.domains.get_mut(current_dom) {
                    old_dom.cold_booting = old_dom.cold_booting.saturating_sub(1);
                }
                if let Some(new_dom) = state.domains.get_mut(best_dom) {
                    new_dom.cold_booting += 1;
                }
                state.cold_wave_domain.insert(fqdn.clone(), best_dom);
            }
            if let Some(adm) = state.active_admissions.get_mut(&item.tid) {
                adm.domain_id = best_dom;
            }
        }
        item.set_preferred_domain(Some(best_dom as SchedGroupID));
    }
}

impl Drop for CongestionPricingCoordinator {
    fn drop(&mut self) {
        let state = self.state.lock();
        for class in ["hello", "graph-bfs", "gzip", "pyaes", "other"] {
            let Some(samples) = state.timing_samples.get(class) else {
                continue;
            };
            if samples.is_empty() {
                continue;
            }
            let pricing = summarize_metric(samples, |s| s.pricing_queue_ms);
            let acquire = summarize_metric(samples, |s| s.container_acquire_ms);
            let exec = summarize_metric(samples, |s| s.exec_ms);
            let post = summarize_metric(samples, |s| s.post_overhead_ms);
            info!(
                function_class = class,
                successful_invocations = samples.len(),
                pricing_queue_p50_ms = pricing.0,
                pricing_queue_mean_ms = pricing.1,
                pricing_queue_p95_ms = pricing.2,
                container_acquire_p50_ms = acquire.0,
                container_acquire_mean_ms = acquire.1,
                container_acquire_p95_ms = acquire.2,
                exec_p50_ms = exec.0,
                exec_mean_ms = exec.1,
                exec_p95_ms = exec.2,
                post_overhead_p50_ms = post.0,
                post_overhead_mean_ms = post.1,
                post_overhead_p95_ms = post.2,
                "[congestion_pricing] invocation timing summary"
            );
        }
    }
}

impl InvokerCpuQueuePolicy for CongestionPricingCoordinator {
    fn queue_len(&self) -> usize {
        let state = self.state.lock();
        let waiting: usize = state.domains.iter()
            .map(|d| d.wait_queue.len() + d.coalesced_wait_queue.len()).sum();
        waiting + state.express_ready_queue.len() + state.ready_queue.len()
    }

    fn est_queue_time(&self) -> f64 {
        let state = self.state.lock();
        let total_w_q: f64 = state.domains.iter().map(|d| d.queue_weight_sum).sum();
        let avg_s_i = if !state.fn_state.is_empty() {
            let sum: f64 = state.fn_state.values().map(|f| f.s_i).sum();
            sum / (state.fn_state.len() as f64)
        } else {
            DEFAULT_S_I
        };
        ((total_w_q + state.ready_queue.len() as f64) * avg_s_i) / 1000.0
    }

    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let mut state = self.state.lock();
        if !state.has_ready_work() && state.active_admissions.is_empty() {
            self.drain_wait_queues_locked(&mut state, 0);
        }
        if state.should_take_express() {
            state.express_ready_queue.front().map(|entry| entry.item.clone())
        } else {
            state.ready_queue.peek().map(|entry| entry.item.clone())
        }
    }

    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        let mut state = self.state.lock();
        if !state.has_ready_work() && state.active_admissions.is_empty() {
            self.drain_wait_queues_locked(&mut state, 0);
        }
        let entry = if state.should_take_express() {
            state.express_streak = state.express_streak.saturating_add(1);
            state
                .express_ready_queue
                .pop_front()
                .expect("express_ready_queue verified non-empty")
        } else if let Some(entry) = state.ready_queue.pop() {
            state.express_streak = 0;
            entry
        } else {
            state
                .express_ready_queue
                .pop_front()
                .expect("pop_queue called without an admitted invocation")
        };
        let total_wait_ms = entry.prior_wait_ms + entry.ready_at.elapsed().as_secs_f64() * 1000.0;
        state.pricing_queue_ms.insert(entry.item.tid.clone(), total_wait_ms);
        drop(state);

        let warm_domains = self.idle_warm_domains(&entry.item.registration.fqdn);
        let mut state = self.state.lock();
        self.refresh_dispatch_domain_locked(&mut state, &entry.item, &warm_domains);
        entry.item
    }

    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
        self.route_or_enqueue(item)
    }

    fn preview_fqdn_counts(&self, limit: usize) -> HashMap<String, usize> {
        let state = self.state.lock();
        let mut counts = HashMap::new();
        if limit == 0 {
            return counts;
        }
        let mut remaining = limit;
        for entry in state.express_ready_queue.iter() {
            if remaining == 0 {
                break;
            }
            *counts.entry(entry.item.registration.fqdn.clone()).or_insert(0) += 1;
            remaining -= 1;
        }
        let mut ready = state.ready_queue.clone();
        while remaining > 0 {
            let Some(entry) = ready.pop() else {
                break;
            };
            *counts.entry(entry.item.registration.fqdn.clone()).or_insert(0) += 1;
            remaining -= 1;
        }
        if remaining > 0 {
            let mut all_entries: Vec<&WaitEntry> = Vec::new();
            for d in &state.domains {
                for entry in &d.wait_queue {
                    all_entries.push(entry);
                }
                for entry in &d.coalesced_wait_queue {
                    all_entries.push(entry);
                }
            }
            all_entries.sort_by(|a, b| b.cmp(a));
            for entry in all_entries.into_iter().take(remaining) {
                *counts.entry(entry.item.registration.fqdn.clone()).or_insert(0) += 1;
            }
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::invocation::queueing::EnqueuedInvocation;
    use crate::services::registration::RegisteredFunction;
    use crate::services::resources::fineloadbalancing::FineLoadBalancingWeak;
    use crate::worker_api::worker_config::{CongestionPricingCalibration, CongestionPricingConfig};
    use iluvatar_library::char_map::worker_char_map;
    use time::OffsetDateTime;

    fn invocation_for(fqdn: &str, tid: &str) -> Arc<EnqueuedInvocation> {
        let registration = Arc::new(RegisteredFunction {
            fqdn: fqdn.to_string(),
            cpus: 1,
            memory: 64,
            ..Default::default()
        });
        Arc::new(EnqueuedInvocation::new(
            registration,
            "{}".to_string(),
            tid.to_string(),
            OffsetDateTime::now_utc(),
            10.0,
            0.0,
        ))
    }

    fn invocation(tid: &str) -> Arc<EnqueuedInvocation> {
        invocation_for("wave.function", tid)
    }

    fn coordinator(config: Option<&CongestionPricingConfig>) -> Arc<CongestionPricingCoordinator> {
        CongestionPricingCoordinator::new(
            config,
            FineLoadBalancingWeak::new(),
            worker_char_map(),
            None,
        )
    }

    #[test]
    fn pioneer_receives_wave_creation_credit() {
        assert_eq!(cold_wave_state(0, 0, 0), ColdWaveState::Pioneer);
        assert_eq!(pioneer_creation_credit(0.5, 200.0, 2, 4), 300.0);
        assert_eq!(pioneer_creation_credit(0.5, 200.0, 8, 4), 400.0);

        let coordinator = coordinator(None);
        let pioneer = invocation("pioneer");
        coordinator.add_item_to_queue(&pioneer, None).unwrap();
        assert!(pioneer.likely_cold.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(pioneer.preferred_domain(), Some(0));
        assert_eq!(coordinator.state.lock().booting_by_fqdn.get("wave.function"), Some(&1));
    }

    #[test]
    fn warm_idle_candidates_prefer_ready_domains_without_excluding_spillover() {
        let all_domains = vec![0, 1, 2, 3];
        let warm_ready_domains = vec![1, 3];
        assert_eq!(
            warm_idle_candidate_domains(&warm_ready_domains, &all_domains),
            vec![1, 3, 0, 2]
        );
        assert_eq!(warm_idle_candidate_domains(&[], &all_domains), all_domains);
    }

    #[test]
    fn warm_idle_ranks_all_domains_and_spills_under_soft_affinity() {
        assert_eq!(select_po2_candidates(1, &[0, 1, 2, 3], 0), vec![1, 0]);
        assert_eq!(select_po2_candidates(1, &[0, 1, 2, 3], 1), vec![1, 2]);
        assert_eq!(select_po2_candidates(1, &[0, 1, 2, 3], 2), vec![1, 3]);

        // Scale-invariant soft-affinity hysteresis works identically for tiny (hello) and large (pyaes) functions.
        for s_i in [0.05, 50.0, 300.0] {
            let c_mig = soft_affinity_migration_cost(s_i);
            // Warm domain at rho=0.35 vs alternate at rho=0.20 (diff 0.15 <= 0.20) -> warm wins.
            assert!(
                placement_ranking_cost(0.35, s_i, 0.0, c_mig, true)
                    < placement_ranking_cost(0.20, s_i, 0.0, c_mig, false)
            );
            // Warm domain at rho=0.60 vs alternate at rho=0.20 (diff 0.40 > 0.20) -> alternate wins.
            assert!(
                placement_ranking_cost(0.20, s_i, 0.0, c_mig, false)
                    < placement_ranking_cost(0.60, s_i, 0.0, c_mig, true)
            );
        }

        let cmap = worker_char_map();
        cmap.update("503.graph-bfs-0.0.1", Chars::CpuWarmTime, 50.0);
        let coordinator = CongestionPricingCoordinator::new(
            None,
            FineLoadBalancingWeak::new(),
            cmap,
            None,
        );
        {
            let mut state = coordinator.state.lock();
            state.domains = (0..4).map(|d| DomainState::new(d as SchedGroupID)).collect();
        }

        // Warm domain 1 vs alternate domain 0 (offset=0): rho_1 - rho_0 = 0.15 <= 0.20 -> keeps domain 1.
        let item_keep = invocation_for("503.graph-bfs-0.0.1", "tid-aff-keep");
        coordinator.add_item_to_queue(&item_keep, None).unwrap();
        item_keep.likely_cold.store(false, std::sync::atomic::Ordering::Relaxed);
        {
            let mut state = coordinator.state.lock();
            state.domains[0].active_weight_sum = 0.0; // rho = 0.00
            state.domains[1].active_weight_sum = 0.6; // rho = 0.15 (diff 0.15 <= 0.20)
            state.domains[2].active_weight_sum = 2.0;
            state.domains[3].active_weight_sum = 2.0;
            coordinator.refresh_dispatch_domain_with_offset_locked(&mut state, &item_keep, &[1], 0);
        }
        assert_eq!(item_keep.preferred_domain(), Some(1));

        // Warm domain 1 vs alternate domain 2 (offset=1): rho_1 - rho_2 = 0.50 > 0.20 -> spills to idle domain 2.
        let item_spill = invocation_for("503.graph-bfs-1-0.0.1", "tid-aff-spill");
        coordinator.cmap.update("503.graph-bfs-1-0.0.1", Chars::CpuWarmTime, 50.0);
        coordinator.add_item_to_queue(&item_spill, None).unwrap();
        item_spill.likely_cold.store(false, std::sync::atomic::Ordering::Relaxed);
        {
            let mut state = coordinator.state.lock();
            state.domains[0].active_weight_sum = 2.0;
            state.domains[1].active_weight_sum = 2.0; // rho = 0.50
            state.domains[2].active_weight_sum = 0.0; // rho = 0.00 (diff 0.50 > 0.20)
            state.domains[3].active_weight_sum = 2.0;
            coordinator.refresh_dispatch_domain_with_offset_locked(&mut state, &item_spill, &[1], 1);
        }
        assert_eq!(item_spill.preferred_domain(), Some(2));
    }

    #[test]
    fn warm_idle_with_negative_net_gain_admits_without_override() {
        let config = CongestionPricingConfig {
            welfare_beta: Some(0.5),
            rho_knee: Some(0.8),
            pricing_calibration: Some(CongestionPricingCalibration {
                freeze_calibration: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        let coordinator = coordinator(Some(&config));
        let wave_state = cold_wave_state(1, 1, 0);
        assert_eq!(wave_state, ColdWaveState::WarmIdle);

        let s_i = 10.0;
        let rho = 1.35;
        let mut state = coordinator.state.lock();
        state.domains[0].active_weight_sum = rho * coordinator.cores_per_domain as f64;
        let now_ms = state.now_ms();
        let price = coordinator.compute_welfare_gain(
            0,
            0,
            false,
            s_i,
            DEFAULT_T_BOOT,
            DEFAULT_ETA,
            s_i.powf(-coordinator.beta),
            0.0,
            rho,
            &mut state,
            now_ms,
        );

        assert!(price.delta_sigma > 0.0);
        assert!(price.warm_toll > price.benefit);
        assert!(price.net_gain <= 0.0);
        assert!(!should_use_work_conservation_override(wave_state, price.net_gain));
        assert!(should_admit_invocation(wave_state, price.net_gain, false));
    }

    #[test]
    fn followers_coalesce_and_wake_against_returned_warm_capacity() {
        let coordinator = coordinator(None);
        let pioneer = invocation("pioneer");
        coordinator.add_item_to_queue(&pioneer, None).unwrap();
        coordinator.pop_queue();

        let mut followers = Vec::new();
        for n in 2..=4 {
            let follower = invocation(&format!("follower-{n}"));
            coordinator.add_item_to_queue(&follower, None).unwrap();
            assert!(!follower.likely_cold.load(std::sync::atomic::Ordering::Relaxed));
            followers.push(follower);
        }

        {
            let state = coordinator.state.lock();
            assert_eq!(state.booting_by_fqdn.get("wave.function"), Some(&1));
            assert_eq!(state.domains.iter().map(|d| d.coalesced_wait_queue.len()).sum::<usize>(), 3);
            assert!(state.domains.iter().flat_map(|d| d.coalesced_wait_queue.iter()).all(|entry| entry.coalesced && !entry.is_cold));
        }

        coordinator.on_cold_boot_finished("wave.function", 0);
        {
            let state = coordinator.state.lock();
            assert_eq!(state.booting_by_fqdn.get("wave.function"), None);
            assert!(followers.iter().all(|f| !f.likely_cold.load(std::sync::atomic::Ordering::Relaxed)));
        }
        assert!(coordinator.peek_queue().is_none(), "coalesced followers must remain hidden from dispatch");

        // Simulate the new warm container returning to idle; one idle container
        // wakes exactly one follower, preventing another cold start.
        let mut state = coordinator.state.lock();
        CongestionPricingCoordinator::promote_coalesced_followers(&mut state, "wave.function", 0, 1);
        assert_eq!(state.ready_queue.len(), 1);
        assert_eq!(state.domains.iter().map(|d| d.coalesced_wait_queue.len()).sum::<usize>(), 2);
    }

    #[test]
    fn ready_queue_prioritizes_short_welfare_tasks_and_preserves_express_lane() {
        let cmap = worker_char_map();
        cmap.update("pyaes-0.0.1", Chars::CpuWarmTime, 400.0);
        cmap.update("hello-0.0.1", Chars::CpuWarmTime, 0.05);
        let coordinator = CongestionPricingCoordinator::new(
            None,
            FineLoadBalancingWeak::new(),
            cmap,
            None,
        );

        let long_a = invocation_for("pyaes-0.0.1", "long-a");
        let long_b = invocation_for("pyaes-0.0.1", "long-b");
        let short = invocation_for("hello-0.0.1", "short-1");
        coordinator.add_item_to_queue(&long_a, None).unwrap();
        // Mark pioneer boot complete so second pyaes doesn't coalesce; instead use distinct warm-seeded s_i:
        coordinator.on_cold_boot_finished("pyaes-0.0.1", 0);
        // Enqueue a second distinct long function so both sit in ready_queue with short
        let long_other = invocation_for("pyaes-1-0.0.1", "long-other");
        coordinator.cmap.update("pyaes-1-0.0.1", Chars::CpuWarmTime, 380.0);
        coordinator.add_item_to_queue(&long_other, None).unwrap();
        coordinator.add_item_to_queue(&short, None).unwrap();
        let _ = long_b;

        assert_eq!(coordinator.pop_queue().tid, "short-1");
        assert_eq!(coordinator.pop_queue().tid, "long-other");
        assert_eq!(coordinator.pop_queue().tid, "long-a");

        // Verify pure cpu_exec_ms keeps `hello` inside the <2.0ms express lane despite 5ms HTTP RPC wall time.
        coordinator.on_invocation_completed(&"short-1".to_string(), 0, "hello-0.0.1", 5.2, false, 0.5, Some(0.003));
        let hello_s_i = coordinator.state.lock().fn_state.get("hello-0.0.1").unwrap().s_i;
        assert!(hello_s_i < 2.0, "hello s_i={hello_s_i} should stay < 2.0ms");
    }

    #[test]
    fn exact_tid_reservation_prevents_weight_drift_and_abort_leaks() {
        let coordinator = coordinator(None);
        let p = invocation("p1");
        coordinator.add_item_to_queue(&p, None).unwrap();
        let _ = coordinator.pop_queue();
        assert!(coordinator.state.lock().domains[0].active_weight_sum > 0.0);

        // Even when s_i changes substantially on completion, active_weight_sum returns to 0.0.
        coordinator.on_invocation_completed(&"p1".to_string(), 0, "wave.function", 500.0, true, 0.9, Some(450.0));
        let state = coordinator.state.lock();
        assert!(state.domains[0].active_weight_sum.abs() < 1e-12);
        assert_eq!(state.domains[0].cold_booting, 0);
        assert!(state.active_admissions.is_empty());
    }

    #[test]
    fn rho_knee_delays_warm_stretch_and_marginal_toll() {
        let config = CongestionPricingConfig {
            rho_knee: Some(1.05),
            pricing_calibration: Some(CongestionPricingCalibration {
                freeze_calibration: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        };
        let coordinator = coordinator(Some(&config));
        assert_eq!(coordinator.sigma_warm(1.04, 1.0), 1.0);
        assert_eq!(coordinator.sigma_warm(1.05, 1.0), 1.0);
        assert!(coordinator.sigma_warm(1.06, 1.0) > 1.0);
        assert_eq!(coordinator.delta_sigma_warm(1.02, 0.08, 4.0, 1.0), 0.0);
        assert!(coordinator.delta_sigma_warm(1.04, 0.08, 4.0, 1.0) > 0.0);
    }

    #[test]
    fn virtual_deadline_prevents_long_job_starvation_and_lifo_inversion() {
        let cmap = worker_char_map();
        cmap.update("pyaes-0.0.1", Chars::CpuWarmTime, 560.0);
        cmap.update("503.graph-bfs-0.0.1", Chars::CpuWarmTime, 60.0);
        let cfg = CongestionPricingConfig {
            cold_wave_coalescing: Some(false),
            ..Default::default()
        };
        let coordinator = CongestionPricingCoordinator::new(
            Some(&cfg),
            FineLoadBalancingWeak::new(),
            cmap,
            None,
        );

        let pyaes_1 = invocation_for("pyaes-0.0.1", "pyaes-1");
        let pyaes_2 = invocation_for("pyaes-0.0.1", "pyaes-2");
        let bfs_concurrent = invocation_for("503.graph-bfs-0.0.1", "bfs-concurrent");
        coordinator.add_item_to_queue(&pyaes_1, None).unwrap();
        coordinator.add_item_to_queue(&pyaes_2, None).unwrap();
        coordinator.add_item_to_queue(&bfs_concurrent, None).unwrap();

        // Simulate 350ms passing before a later graph-bfs arrives: aged pyaes_1/pyaes_2
        // (slack capped at 280ms) must dispatch ahead of bfs_late (350ms + 30ms = 380ms).
        std::thread::sleep(std::time::Duration::from_millis(350));
        let bfs_late = invocation_for("503.graph-bfs-0.0.1", "bfs-late");
        coordinator.add_item_to_queue(&bfs_late, None).unwrap();

        assert_eq!(coordinator.pop_queue().tid, "bfs-concurrent");
        assert_eq!(coordinator.pop_queue().tid, "pyaes-1");
        assert_eq!(coordinator.pop_queue().tid, "pyaes-2");
        assert_eq!(coordinator.pop_queue().tid, "bfs-late");
    }

    #[test]
    fn pop_queue_refreshes_preferred_domain_and_transfers_weight() {
        let cmap = worker_char_map();
        cmap.update("503.graph-bfs-0.0.1", Chars::CpuWarmTime, 50.0);
        let coordinator = CongestionPricingCoordinator::new(
            None,
            FineLoadBalancingWeak::new(),
            cmap,
            None,
        );
        // Simulate 4 domains without needing live eBPF FineLoadBalancing.
        {
            let mut state = coordinator.state.lock();
            state.domains = (0..4).map(|d| DomainState::new(d as SchedGroupID)).collect();
        }

        let item = invocation_for("503.graph-bfs-0.0.1", "tid-refresh");
        coordinator.add_item_to_queue(&item, None).unwrap();
        let initial_dom = item.preferred_domain().unwrap() as usize;
        let added_w = coordinator
            .state
            .lock()
            .active_admissions
            .get(&item.tid)
            .unwrap()
            .added_weight;
        assert!(added_w > 0.0);

        // Make initial_dom heavily loaded and all other domains idle before pop_queue
        // so any Po2 alternate deterministically moves off initial_dom.
        {
            let mut state = coordinator.state.lock();
            state.domains[initial_dom].active_weight_sum += 10.0;
        }

        let popped = coordinator.pop_queue();
        let new_dom = popped.preferred_domain().unwrap() as usize;
        assert_ne!(new_dom, initial_dom);
        let state = coordinator.state.lock();
        assert_eq!(state.active_admissions.get(&popped.tid).unwrap().domain_id, new_dom);
        assert!((state.domains[initial_dom].active_weight_sum - 10.0).abs() < 1e-9);
        assert!((state.domains[new_dom].active_weight_sum - added_w).abs() < 1e-9);
    }
}
