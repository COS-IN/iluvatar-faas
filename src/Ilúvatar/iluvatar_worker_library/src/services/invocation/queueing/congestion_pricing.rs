//! Clean-room Late-Binding Social Marginal Cost (SMC) Posted-Price Coordinator (`posted_price`).
//!
//! Strictly separates CPU congestion pricing into three modular stages:
//! - Stage A: Pluggable [`DomainLoadEstimator`] + [`UnitFootprintEstimator`]
//! - Stage B: Pure narrow-waist unit pricer [`domain_cpu_price`]
//! - Stage C: Unified SMC shopper ([`invocation_price`] & [`PostedPriceCoordinator::rank_domains_for_dispatch`])

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy};
use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::ContainerState;
use crate::services::resources::fineloadbalancing::FineLoadBalancingWeak;
use crate::worker_api::worker_config::{LoadEstimatorKind, PostedPriceConfig};
use anyhow::Result;
use iluvatar_finesched::SchedGroupID;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::transaction::TransactionId;
use ordered_float::OrderedFloat;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Instant;
use tracing::{debug, info};

pub type DomainId = usize;

/// Minimum floor for estimated warm service time `s_i` (ms) to keep `w_0 = s_i^(-beta)` finite.
pub const MIN_S_I_MS: f64 = 0.05;
/// Default warm service duration `s_i` (ms) before benchmark or runtime observations arrive.
pub const DEFAULT_S_I_MS: f64 = 10.0;
/// Default isolated cold boot duration `T_boot` (ms).
pub const DEFAULT_T_BOOT_MS: f64 = 250.0;
/// Fraction of baseline boot cost charged for cross-domain warm container migration (`0.01`).
pub const MIGRATION_COST_BOOT_FRACTION: f64 = 0.01;
/// Default warm cross-domain cache migration cost `C_mig` (`2.5 ms`).
pub const DEFAULT_C_MIG_MS: f64 = DEFAULT_T_BOOT_MS * MIGRATION_COST_BOOT_FRACTION;
/// Domain congestion stretch multiplier applied to `C_boot(d)` above isolated `T_boot`.
pub const BOOT_CONGESTION_FACTOR: f64 = 0.10;
/// Warm functions shorter than this threshold (ms) use the lightweight express-lane placement helper.
pub const EXPRESS_LANE_MAX_S_I_MS: f64 = 2.0;

const DEFAULT_BETA: f64 = 0.5;
const DEFAULT_CORES_PER_DOMAIN: u32 = 4;
const DEFAULT_MAX_QUEUE_LEN: usize = 16_384;
const PELT_REFRESH_INTERVAL_NS: u64 = 2_000_000; // 2 ms

/// Default starvation deadband (seconds) before queue aging activates (`w_eff == w_0` below this).
pub const DEFAULT_AGING_GRACE_SEC: f64 = 1.5;
/// Default multiplicative queue aging rate per second beyond `aging_grace_sec`.
pub const DEFAULT_AGING_ALPHA: f64 = 1.0;

/// Minimum per-function CPU footprint in core-units for L1/L2 estimators.
pub const MIN_FOOTPRINT_CORES: f64 = 0.05;
/// Default active cold-boot footprint in core-units for L1/L2 estimators.
pub const DEFAULT_U_BOOT_CORES: f64 = 1.0;
/// Default EWMA smoothing factor for `u_hat[fqdn]` under low/moderate contention.
pub const DEFAULT_UTIL_EWMA_ALPHA: f64 = 0.2;
/// Default contention ceiling (`domain_rho < 0.65 && pelt_util < 0.65`) for downward `u_hat` updates.
pub const DEFAULT_UTIL_LOW_CONTENTION_RHO: f64 = 0.65;
/// Default kernel PELT half-life (ms) modeled by the L2 observer.
pub const DEFAULT_PELT_HALF_LIFE_MS: f64 = 32.0;
/// Default L2 observer gain `k_p` on predicted PELT state `p_hat`.
pub const DEFAULT_OBSERVER_KP: f64 = 0.5;
/// Default L2 observer gain `k_b` on residual load bias `b_hat`.
pub const DEFAULT_OBSERVER_KB: f64 = 0.2;
/// Default maximum time step (ms) clamped in L2 `advance_time`.
pub const MAX_OBSERVER_DT_MS: f64 = 500.0;

/// Smooth linear slope of unit self-slowdown `sigma_d(rho)` from `rho = 0.0` (no dead zone).
pub const SIGMA_LINEAR_SLOPE: f64 = 0.12;
/// Superlinear quartic congestion coefficient of `sigma_d(rho)` (`sigma(1.0) = 1.0`).
pub const SIGMA_SUPERLINEAR_COEFF: f64 = 0.88;

// ============================================================================
// Stage A: Pluggable DomainLoadEstimator Trait + L0 / L1 / L2 Estimators
// ============================================================================

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DomainLoadEstimate {
    pub effective_rho: f64,
    pub active_weight: f64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DomainEstimatorDiag {
    /// Instantaneous footprint utilization `L_l1[d] / C_d` prior to L2 bias correction.
    pub l1_rho: f64,
    /// L2 observer residual load bias `b_hat[d]` in core-units (`0.0` on L0/L1).
    pub bias_cores: f64,
    /// L2 observer predicted PELT load `p_hat[d]` in core-units (`0.0` on L0/L1).
    pub pred_pelt_cores: f64,
}

pub trait DomainLoadEstimator: Send + Sync {
    /// Record an invocation dispatch on `domain` and return the exact `footprint_used` (in cores)
    /// added to the domain's running load so `on_complete` subtracts the identical float.
    fn on_dispatch(&mut self, domain: DomainId, fqdn: &str, weight: f64) -> f64;
    /// Record completion on `domain`, subtracting the exact `footprint_used` returned by `on_dispatch`.
    fn on_complete(&mut self, domain: DomainId, fqdn: &str, weight: f64, footprint_used: f64);
    fn on_boot_start(&mut self, domain: DomainId, fqdn: &str);
    fn on_boot_end(&mut self, domain: DomainId, fqdn: &str);
    /// Record a cold-boot start paired with an already-dispatched invocation whose `pending_footprint`
    /// has not yet entered the domain cgroup (`cgroup_assigned_to_function` runs post-boot).
    fn on_boot_start_with_footprint(&mut self, domain: DomainId, fqdn: &str, _pending_footprint: f64) {
        self.on_boot_start(domain, fqdn);
    }
    /// Complete a cold boot paired with `pending_footprint`, transitioning it into cgroup execution.
    fn on_boot_end_with_footprint(&mut self, domain: DomainId, fqdn: &str, _pending_footprint: f64) {
        self.on_boot_end(domain, fqdn);
    }
    /// Ingest a domain PELT sample (`pelt_util` normalized to `[0.0, 1.0]` per domain).
    fn on_pelt_sample(&mut self, domain: DomainId, pelt_util: f64, pelt_runnable: f64);
    /// Feed realized per-invocation CPU core utilization `cpu_util_cores = cpu_time_us / wall_time_us`
    /// (clamped to `[0.05, cores_per_domain]`). Default no-op on L0.
    fn on_util_sample(&mut self, _domain: DomainId, _fqdn: &str, _cpu_util_cores: f64, _domain_rho: f64) {}
    fn predict(&self, domain: DomainId) -> DomainLoadEstimate;

    /// Diagnostic breakdown (`l1_rho`, `bias_cores`, `pred_pelt_cores`) for dispatch telemetry.
    fn diagnostics(&self, domain: DomainId) -> DomainEstimatorDiag {
        DomainEstimatorDiag {
            l1_rho: self.predict(domain).effective_rho,
            bias_cores: 0.0,
            pred_pelt_cores: 0.0,
        }
    }

    fn estimator_name(&self) -> &'static str {
        "custom"
    }

    /// Optional latest `(pelt_util, pelt_runnable)` for dispatch telemetry.
    fn latest_pelt(&self, _domain: DomainId) -> (Option<f64>, Option<f64>) {
        (None, None)
    }

    /// Optional total active invocations + active cold boots across all domains.
    fn total_active_activity(&self) -> (u32, u32) {
        (0, 0)
    }

    /// Optional per-domain `(in_flight_count, active_boot_count)` for lifecycle inspection/tests.
    fn domain_counts(&self, _domain: DomainId) -> (u32, u32) {
        (0, 0)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct UnitDomainSlot {
    in_flight_count: u32,
    active_boot_count: u32,
    active_weight: f64,
    kernel_active_weight: Option<f64>,
    pelt_util: Option<f64>,
    pelt_runnable: Option<f64>,
}

/// L0 footprint estimator (`u_f = 1.0`, `u_boot = 1.0`, `b_d = 0.0`).
#[derive(Debug, Clone)]
pub struct UnitFootprintEstimator {
    slots: Vec<UnitDomainSlot>,
    domain_cores: Vec<f64>,
    booting_by_fqdn: HashMap<String, u32>,
}

impl UnitFootprintEstimator {
    pub fn new(num_domains: usize, cores_per_domain: u32) -> Self {
        let n = num_domains.max(1);
        Self::new_heterogeneous(vec![cores_per_domain.max(1); n])
    }

    pub fn new_heterogeneous(cores_by_domain: Vec<u32>) -> Self {
        let domain_cores: Vec<f64> = if cores_by_domain.is_empty() {
            vec![DEFAULT_CORES_PER_DOMAIN as f64]
        } else {
            cores_by_domain.into_iter().map(|c| (c.max(1)) as f64).collect()
        };
        Self {
            slots: vec![UnitDomainSlot::default(); domain_cores.len()],
            domain_cores,
            booting_by_fqdn: HashMap::new(),
        }
    }

    fn cores_for(&self, domain: DomainId) -> f64 {
        self.domain_cores
            .get(domain)
            .copied()
            .or_else(|| self.domain_cores.first().copied())
            .unwrap_or(DEFAULT_CORES_PER_DOMAIN as f64)
    }

    pub fn set_kernel_active_weight(&mut self, domain: DomainId, kernel_w_d: Option<f64>) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.kernel_active_weight = kernel_w_d.filter(|w| w.is_finite() && *w > 0.0);
        }
    }

    pub fn active_boots_for_fqdn(&self, fqdn: &str) -> u32 {
        self.booting_by_fqdn.get(fqdn).copied().unwrap_or(0)
    }
}

impl DomainLoadEstimator for UnitFootprintEstimator {
    fn on_dispatch(&mut self, domain: DomainId, _fqdn: &str, weight: f64) -> f64 {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.in_flight_count = slot.in_flight_count.saturating_add(1);
            if weight.is_finite() && weight > 0.0 {
                slot.active_weight += weight;
            }
        }
        1.0
    }

    fn on_complete(&mut self, domain: DomainId, _fqdn: &str, weight: f64, _footprint_used: f64) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.in_flight_count = slot.in_flight_count.saturating_sub(1);
            if slot.in_flight_count == 0 {
                slot.active_weight = 0.0;
            } else if weight.is_finite() && weight > 0.0 {
                slot.active_weight = (slot.active_weight - weight).max(0.0);
            }
        }
    }

    fn on_boot_start(&mut self, domain: DomainId, fqdn: &str) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.active_boot_count = slot.active_boot_count.saturating_add(1);
        }
        *self.booting_by_fqdn.entry(fqdn.to_string()).or_insert(0) += 1;
    }

    fn on_boot_end(&mut self, domain: DomainId, fqdn: &str) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.active_boot_count = slot.active_boot_count.saturating_sub(1);
        }
        if let Some(cnt) = self.booting_by_fqdn.get_mut(fqdn) {
            *cnt = cnt.saturating_sub(1);
            if *cnt == 0 {
                self.booting_by_fqdn.remove(fqdn);
            }
        }
    }

    fn on_pelt_sample(&mut self, domain: DomainId, pelt_util: f64, pelt_runnable: f64) {
        if let Some(slot) = self.slots.get_mut(domain) {
            if pelt_util.is_finite() {
                slot.pelt_util = Some(pelt_util.max(0.0));
            }
            if pelt_runnable.is_finite() {
                slot.pelt_runnable = Some(pelt_runnable.max(0.0));
            }
        }
    }

    fn predict(&self, domain: DomainId) -> DomainLoadEstimate {
        let Some(slot) = self.slots.get(domain) else {
            return DomainLoadEstimate::default();
        };
        let effective_rho =
            ((slot.in_flight_count as f64) + (slot.active_boot_count as f64)) / self.cores_for(domain);
        let active_weight = slot
            .kernel_active_weight
            .filter(|w| *w > 0.0)
            .unwrap_or(slot.active_weight)
            .max(0.0);
        DomainLoadEstimate {
            effective_rho,
            active_weight,
        }
    }

    fn estimator_name(&self) -> &'static str {
        "unit"
    }

    fn latest_pelt(&self, domain: DomainId) -> (Option<f64>, Option<f64>) {
        self.slots
            .get(domain)
            .map(|s| (s.pelt_util, s.pelt_runnable))
            .unwrap_or((None, None))
    }

    fn total_active_activity(&self) -> (u32, u32) {
        let in_flight = self.slots.iter().map(|s| s.in_flight_count).sum();
        let boots = self.slots.iter().map(|s| s.active_boot_count).sum();
        (in_flight, boots)
    }

    fn domain_counts(&self, domain: DomainId) -> (u32, u32) {
        self.slots
            .get(domain)
            .map(|s| (s.in_flight_count, s.active_boot_count))
            .unwrap_or((0, 0))
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct UtilDomainSlot {
    in_flight_count: u32,
    active_boot_count: u32,
    sum_active_footprints: f64,
    pending_boot_footprints: f64,
    active_weight: f64,
    pelt_util: Option<f64>,
    pelt_runnable: Option<f64>,
}

/// L1 utilization-weighted footprint estimator (`u_hat[fqdn]` with contention-shrinkage guard).
#[derive(Debug, Clone)]
pub struct UtilWeightedEstimator {
    slots: Vec<UtilDomainSlot>,
    u_hat: HashMap<String, f64>,
    domain_cores: Vec<f64>,
    max_domain_cores: f64,
    u_boot: f64,
    ewma_alpha: f64,
    low_contention_rho: f64,
    booting_by_fqdn: HashMap<String, u32>,
}

impl UtilWeightedEstimator {
    pub fn new(
        num_domains: usize,
        cores_per_domain: u32,
        u_boot: f64,
        ewma_alpha: f64,
        low_contention_rho: f64,
    ) -> Self {
        Self::new_heterogeneous(
            vec![cores_per_domain.max(1); num_domains.max(1)],
            u_boot,
            ewma_alpha,
            low_contention_rho,
        )
    }

    pub fn new_heterogeneous(
        cores_by_domain: Vec<u32>,
        u_boot: f64,
        ewma_alpha: f64,
        low_contention_rho: f64,
    ) -> Self {
        let domain_cores: Vec<f64> = if cores_by_domain.is_empty() {
            vec![DEFAULT_CORES_PER_DOMAIN as f64]
        } else {
            cores_by_domain.into_iter().map(|c| (c.max(1)) as f64).collect()
        };
        let max_c = domain_cores
            .iter()
            .copied()
            .fold(1.0_f64, f64::max);
        Self {
            slots: vec![UtilDomainSlot::default(); domain_cores.len()],
            u_hat: HashMap::new(),
            domain_cores,
            max_domain_cores: max_c,
            u_boot: if u_boot.is_finite() {
                u_boot.clamp(MIN_FOOTPRINT_CORES, max_c)
            } else {
                DEFAULT_U_BOOT_CORES.min(max_c)
            },
            ewma_alpha: if ewma_alpha.is_finite() {
                ewma_alpha.clamp(0.001, 1.0)
            } else {
                DEFAULT_UTIL_EWMA_ALPHA
            },
            low_contention_rho: if low_contention_rho.is_finite() {
                low_contention_rho.max(0.0)
            } else {
                DEFAULT_UTIL_LOW_CONTENTION_RHO
            },
            booting_by_fqdn: HashMap::new(),
        }
    }

    pub fn from_config(
        num_domains: usize,
        cores_per_domain: u32,
        config: Option<&PostedPriceConfig>,
    ) -> Self {
        Self::from_config_heterogeneous(vec![cores_per_domain.max(1); num_domains.max(1)], config)
    }

    pub fn from_config_heterogeneous(
        cores_by_domain: Vec<u32>,
        config: Option<&PostedPriceConfig>,
    ) -> Self {
        let u_boot = config
            .and_then(|c| c.u_boot)
            .unwrap_or(DEFAULT_U_BOOT_CORES);
        let ewma_alpha = config
            .and_then(|c| c.util_ewma_alpha)
            .unwrap_or(DEFAULT_UTIL_EWMA_ALPHA);
        let low_contention_rho = config
            .and_then(|c| c.util_low_contention_rho)
            .unwrap_or(DEFAULT_UTIL_LOW_CONTENTION_RHO);
        Self::new_heterogeneous(cores_by_domain, u_boot, ewma_alpha, low_contention_rho)
    }

    pub fn cores_per_domain(&self) -> f64 {
        self.max_domain_cores
    }

    pub fn domain_cores(&self, domain: DomainId) -> f64 {
        self.domain_cores
            .get(domain)
            .copied()
            .or_else(|| self.domain_cores.first().copied())
            .unwrap_or(self.max_domain_cores)
    }

    /// Raw EWMA CPU utilization `u_hat[fqdn]` in `[0.05, max_domain_cores]` (defaults to `1.0`).
    pub fn raw_u_hat(&self, fqdn: &str) -> f64 {
        self.u_hat
            .get(fqdn)
            .copied()
            .unwrap_or(1.0)
            .clamp(MIN_FOOTPRINT_CORES, self.max_domain_cores)
    }

    /// Effective L1 core footprint `max(1.0, u_hat[fqdn])` (capped at `max_domain_cores`):
    /// preserves a `1.0`-core slot floor for single-core functions while scaling up for `u_hat > 1.0`.
    pub fn estimated_footprint(&self, fqdn: &str) -> f64 {
        self.raw_u_hat(fqdn).max(1.0).min(self.max_domain_cores)
    }

    /// Footprints of containers **currently executing inside `domain`'s cgroup** (excludes
    /// pre-cgroup cold-starting invocations whose containers are still being created by Docker).
    pub fn cgroup_load_cores(&self, domain: DomainId) -> f64 {
        self.slots
            .get(domain)
            .map(|s| (s.sum_active_footprints - s.pending_boot_footprints).max(0.0))
            .unwrap_or(0.0)
    }

    /// Instantaneous L1 load `L_l1[d] = cgroup_load_cores[d] + active_boots[d] * u_boot` in cores.
    /// Avoids double-counting a cold invocation as both `u_hat` and `u_boot` while booting.
    pub fn l1_load_cores(&self, domain: DomainId) -> f64 {
        self.slots
            .get(domain)
            .map(|s| {
                let cgrp = (s.sum_active_footprints - s.pending_boot_footprints).max(0.0);
                (cgrp + (s.active_boot_count as f64) * self.u_boot).max(0.0)
            })
            .unwrap_or(0.0)
    }
}

impl DomainLoadEstimator for UtilWeightedEstimator {
    fn on_dispatch(&mut self, domain: DomainId, fqdn: &str, weight: f64) -> f64 {
        let footprint = self.estimated_footprint(fqdn);
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.in_flight_count = slot.in_flight_count.saturating_add(1);
            slot.sum_active_footprints += footprint;
            if weight.is_finite() && weight > 0.0 {
                slot.active_weight += weight;
            }
        }
        footprint
    }

    fn on_complete(&mut self, domain: DomainId, _fqdn: &str, weight: f64, footprint_used: f64) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.in_flight_count = slot.in_flight_count.saturating_sub(1);
            if slot.in_flight_count == 0 {
                slot.sum_active_footprints = 0.0;
                slot.active_weight = 0.0;
            } else {
                if footprint_used.is_finite() && footprint_used > 0.0 {
                    slot.sum_active_footprints = (slot.sum_active_footprints - footprint_used).max(0.0);
                }
                if weight.is_finite() && weight > 0.0 {
                    slot.active_weight = (slot.active_weight - weight).max(0.0);
                }
            }
        }
    }

    fn on_boot_start(&mut self, domain: DomainId, fqdn: &str) {
        self.on_boot_start_with_footprint(domain, fqdn, 0.0);
    }

    fn on_boot_end(&mut self, domain: DomainId, fqdn: &str) {
        self.on_boot_end_with_footprint(domain, fqdn, 0.0);
    }

    fn on_boot_start_with_footprint(&mut self, domain: DomainId, fqdn: &str, pending_footprint: f64) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.active_boot_count = slot.active_boot_count.saturating_add(1);
            if pending_footprint.is_finite() && pending_footprint > 0.0 {
                slot.pending_boot_footprints += pending_footprint;
            }
        }
        *self.booting_by_fqdn.entry(fqdn.to_string()).or_insert(0) += 1;
    }

    fn on_boot_end_with_footprint(&mut self, domain: DomainId, fqdn: &str, pending_footprint: f64) {
        if let Some(slot) = self.slots.get_mut(domain) {
            slot.active_boot_count = slot.active_boot_count.saturating_sub(1);
            if slot.active_boot_count == 0 {
                slot.pending_boot_footprints = 0.0;
            } else if pending_footprint.is_finite() && pending_footprint > 0.0 {
                slot.pending_boot_footprints = (slot.pending_boot_footprints - pending_footprint).max(0.0);
            }
        }
        if let Some(cnt) = self.booting_by_fqdn.get_mut(fqdn) {
            *cnt = cnt.saturating_sub(1);
            if *cnt == 0 {
                self.booting_by_fqdn.remove(fqdn);
            }
        }
    }

    fn on_pelt_sample(&mut self, domain: DomainId, pelt_util: f64, pelt_runnable: f64) {
        if let Some(slot) = self.slots.get_mut(domain) {
            if pelt_util.is_finite() {
                slot.pelt_util = Some(pelt_util.max(0.0));
            }
            if pelt_runnable.is_finite() {
                slot.pelt_runnable = Some(pelt_runnable.max(0.0));
            }
        }
    }

    fn on_util_sample(&mut self, domain: DomainId, fqdn: &str, cpu_util_cores: f64, domain_rho: f64) {
        if !cpu_util_cores.is_finite() || !domain_rho.is_finite() {
            return;
        }
        let dom_c = self.domain_cores(domain);
        let sample = cpu_util_cores.clamp(MIN_FOOTPRINT_CORES, dom_c);
        let pelt_low = self
            .slots
            .get(domain)
            .and_then(|s| s.pelt_util)
            .map_or(true, |p| p < self.low_contention_rho);
        let low_contention = domain_rho < self.low_contention_rho && pelt_low;

        if low_contention {
            if let Some(cur) = self.u_hat.get_mut(fqdn) {
                *cur = ((1.0 - self.ewma_alpha) * (*cur) + self.ewma_alpha * sample)
                    .clamp(MIN_FOOTPRINT_CORES, self.max_domain_cores);
            } else {
                self.u_hat.insert(fqdn.to_string(), sample);
            }
        } else {
            // Contention-shrinkage guard: skip downward updates when contended;
            // allow upward updates only if `sample > cur`.
            let cur = self.raw_u_hat(fqdn);
            if sample > cur {
                let next = ((1.0 - self.ewma_alpha) * cur + self.ewma_alpha * sample)
                    .clamp(MIN_FOOTPRINT_CORES, self.max_domain_cores);
                self.u_hat.insert(fqdn.to_string(), next);
            }
        }
    }

    fn predict(&self, domain: DomainId) -> DomainLoadEstimate {
        let Some(slot) = self.slots.get(domain) else {
            return DomainLoadEstimate::default();
        };
        let effective_rho = self.l1_load_cores(domain) / self.domain_cores(domain);
        DomainLoadEstimate {
            effective_rho,
            active_weight: slot.active_weight.max(0.0),
        }
    }

    fn estimator_name(&self) -> &'static str {
        "util_weighted"
    }

    fn latest_pelt(&self, domain: DomainId) -> (Option<f64>, Option<f64>) {
        self.slots
            .get(domain)
            .map(|s| (s.pelt_util, s.pelt_runnable))
            .unwrap_or((None, None))
    }

    fn total_active_activity(&self) -> (u32, u32) {
        let in_flight = self.slots.iter().map(|s| s.in_flight_count).sum();
        let boots = self.slots.iter().map(|s| s.active_boot_count).sum();
        (in_flight, boots)
    }

    fn domain_counts(&self, domain: DomainId) -> (u32, u32) {
        self.slots
            .get(domain)
            .map(|s| (s.in_flight_count, s.active_boot_count))
            .unwrap_or((0, 0))
    }
}

#[derive(Clone, Copy, Debug)]
struct ObserverDomainSlot {
    /// Predicted PELT load in core-units.
    p_hat: f64,
    /// Estimated residual load bias in core-units.
    b_hat: f64,
    /// Timestamp of last `advance_time` predict step.
    last_ts: Instant,
}

/// L2 2-state steady-state Kalman / Luenberger PELT bias observer composed over L1 (`UtilWeightedEstimator`).
///
/// Note on units: `refresh_pelt_if_due` passes `pelt_util` normalized to `[0.0, 1.0]` per domain,
/// so `on_pelt_sample` converts to core-units via `y_d = cores_per_domain * pelt_util`.
#[derive(Debug, Clone)]
pub struct PeltBiasObserverEstimator {
    l1: UtilWeightedEstimator,
    obs_slots: Vec<ObserverDomainSlot>,
    pelt_half_life_ms: f64,
    k_p: f64,
    k_b: f64,
    bias_clamp_cores: f64,
}

impl PeltBiasObserverEstimator {
    #[allow(clippy::too_many_arguments)]
    pub fn new_at(
        num_domains: usize,
        cores_per_domain: u32,
        u_boot: f64,
        ewma_alpha: f64,
        low_contention_rho: f64,
        pelt_half_life_ms: f64,
        k_p: f64,
        k_b: f64,
        bias_clamp_cores: f64,
        start_now: Instant,
    ) -> Self {
        let n = num_domains.max(1);
        let l1 = UtilWeightedEstimator::new(
            n,
            cores_per_domain,
            u_boot,
            ewma_alpha,
            low_contention_rho,
        );
        let c = l1.cores_per_domain();
        Self {
            l1,
            obs_slots: vec![
                ObserverDomainSlot {
                    p_hat: 0.0,
                    b_hat: 0.0,
                    last_ts: start_now,
                };
                n
            ],
            pelt_half_life_ms: if pelt_half_life_ms.is_finite() && pelt_half_life_ms > 0.0 {
                pelt_half_life_ms
            } else {
                DEFAULT_PELT_HALF_LIFE_MS
            },
            k_p: if k_p.is_finite() {
                k_p.clamp(0.0, 2.0)
            } else {
                DEFAULT_OBSERVER_KP
            },
            k_b: if k_b.is_finite() {
                k_b.clamp(0.0, 2.0)
            } else {
                DEFAULT_OBSERVER_KB
            },
            bias_clamp_cores: if bias_clamp_cores.is_finite() && bias_clamp_cores >= 0.0 {
                bias_clamp_cores
            } else {
                0.5 * c
            },
        }
    }

    pub fn from_config_heterogeneous(
        cores_by_domain: Vec<u32>,
        config: Option<&PostedPriceConfig>,
    ) -> Self {
        let start_now = Instant::now();
        let max_c = cores_by_domain
            .iter()
            .copied()
            .max()
            .unwrap_or(DEFAULT_CORES_PER_DOMAIN)
            .max(1) as f64;
        let u_boot = config
            .and_then(|cfg| cfg.u_boot)
            .unwrap_or(DEFAULT_U_BOOT_CORES);
        let ewma_alpha = config
            .and_then(|cfg| cfg.util_ewma_alpha)
            .unwrap_or(DEFAULT_UTIL_EWMA_ALPHA);
        let low_contention_rho = config
            .and_then(|cfg| cfg.util_low_contention_rho)
            .unwrap_or(DEFAULT_UTIL_LOW_CONTENTION_RHO);
        let pelt_half_life_ms = config
            .and_then(|cfg| cfg.pelt_half_life_ms)
            .unwrap_or(DEFAULT_PELT_HALF_LIFE_MS);
        let k_p = config
            .and_then(|cfg| cfg.observer_kp)
            .unwrap_or(DEFAULT_OBSERVER_KP);
        let k_b = config
            .and_then(|cfg| cfg.observer_kb)
            .unwrap_or(DEFAULT_OBSERVER_KB);
        let bias_clamp_cores = config
            .and_then(|cfg| cfg.observer_bias_clamp_cores)
            .unwrap_or(0.5 * max_c);
        let l1 = UtilWeightedEstimator::new_heterogeneous(
            cores_by_domain,
            u_boot,
            ewma_alpha,
            low_contention_rho,
        );
        let n = l1.slots.len().max(1);
        Self {
            l1,
            obs_slots: vec![
                ObserverDomainSlot {
                    p_hat: 0.0,
                    b_hat: 0.0,
                    last_ts: start_now,
                };
                n
            ],
            pelt_half_life_ms: if pelt_half_life_ms.is_finite() && pelt_half_life_ms > 0.0 {
                pelt_half_life_ms
            } else {
                DEFAULT_PELT_HALF_LIFE_MS
            },
            k_p: if k_p.is_finite() {
                k_p.clamp(0.0, 2.0)
            } else {
                DEFAULT_OBSERVER_KP
            },
            k_b: if k_b.is_finite() {
                k_b.clamp(0.0, 2.0)
            } else {
                DEFAULT_OBSERVER_KB
            },
            bias_clamp_cores: if bias_clamp_cores.is_finite() && bias_clamp_cores >= 0.0 {
                bias_clamp_cores
            } else {
                0.5 * max_c
            },
        }
    }

    pub fn from_config(
        num_domains: usize,
        cores_per_domain: u32,
        config: Option<&PostedPriceConfig>,
    ) -> Self {
        Self::from_config_at(num_domains, cores_per_domain, config, Instant::now())
    }

    pub fn from_config_at(
        num_domains: usize,
        cores_per_domain: u32,
        config: Option<&PostedPriceConfig>,
        start_now: Instant,
    ) -> Self {
        let c = (cores_per_domain.max(1)) as f64;
        let u_boot = config
            .and_then(|cfg| cfg.u_boot)
            .unwrap_or(DEFAULT_U_BOOT_CORES);
        let ewma_alpha = config
            .and_then(|cfg| cfg.util_ewma_alpha)
            .unwrap_or(DEFAULT_UTIL_EWMA_ALPHA);
        let low_contention_rho = config
            .and_then(|cfg| cfg.util_low_contention_rho)
            .unwrap_or(DEFAULT_UTIL_LOW_CONTENTION_RHO);
        let pelt_half_life_ms = config
            .and_then(|cfg| cfg.pelt_half_life_ms)
            .unwrap_or(DEFAULT_PELT_HALF_LIFE_MS);
        let k_p = config
            .and_then(|cfg| cfg.observer_kp)
            .unwrap_or(DEFAULT_OBSERVER_KP);
        let k_b = config
            .and_then(|cfg| cfg.observer_kb)
            .unwrap_or(DEFAULT_OBSERVER_KB);
        let bias_clamp_cores = config
            .and_then(|cfg| cfg.observer_bias_clamp_cores)
            .unwrap_or(0.5 * c);
        Self::new_at(
            num_domains,
            cores_per_domain,
            u_boot,
            ewma_alpha,
            low_contention_rho,
            pelt_half_life_ms,
            k_p,
            k_b,
            bias_clamp_cores,
            start_now,
        )
    }

    /// Predict step using the previous cgroup-resident `cgroup_load_cores[d]` BEFORE mutating `l1`.
    /// Pre-cgroup Docker cold boots run in the host cgroup until `boot_end` / `cgroup_assigned_to_function`,
    /// so excluding them from `p_hat` prevents false negative `b_hat` spikes during cold starts
    /// while `predict(d)` still includes `active_boots * u_boot` in `L_hat_d`.
    pub fn advance_time(&mut self, domain: DomainId, now: Instant) {
        let l_cgrp_prev = self.l1.cgroup_load_cores(domain);
        let Some(obs) = self.obs_slots.get_mut(domain) else {
            return;
        };
        let dt_ms = (now.saturating_duration_since(obs.last_ts).as_secs_f64() * 1000.0)
            .clamp(0.0, MAX_OBSERVER_DT_MS);
        if dt_ms > 0.0 {
            let gamma = 2.0_f64.powf(-dt_ms / self.pelt_half_life_ms);
            let input_load = (l_cgrp_prev + obs.b_hat).max(0.0);
            obs.p_hat = gamma * obs.p_hat + (1.0 - gamma) * input_load;
        }
        obs.last_ts = now;
    }

    pub fn on_dispatch_at(&mut self, domain: DomainId, fqdn: &str, weight: f64, now: Instant) -> f64 {
        self.advance_time(domain, now);
        self.l1.on_dispatch(domain, fqdn, weight)
    }

    pub fn on_complete_at(
        &mut self,
        domain: DomainId,
        fqdn: &str,
        weight: f64,
        footprint_used: f64,
        now: Instant,
    ) {
        self.advance_time(domain, now);
        self.l1.on_complete(domain, fqdn, weight, footprint_used);
    }

    pub fn on_boot_start_at(&mut self, domain: DomainId, fqdn: &str, now: Instant) {
        self.on_boot_start_with_footprint_at(domain, fqdn, 0.0, now);
    }

    pub fn on_boot_end_at(&mut self, domain: DomainId, fqdn: &str, now: Instant) {
        self.on_boot_end_with_footprint_at(domain, fqdn, 0.0, now);
    }

    pub fn on_boot_start_with_footprint_at(
        &mut self,
        domain: DomainId,
        fqdn: &str,
        pending_footprint: f64,
        now: Instant,
    ) {
        self.advance_time(domain, now);
        self.l1.on_boot_start_with_footprint(domain, fqdn, pending_footprint);
    }

    pub fn on_boot_end_with_footprint_at(
        &mut self,
        domain: DomainId,
        fqdn: &str,
        pending_footprint: f64,
        now: Instant,
    ) {
        self.advance_time(domain, now);
        self.l1.on_boot_end_with_footprint(domain, fqdn, pending_footprint);
    }

    pub fn on_pelt_sample_at(
        &mut self,
        domain: DomainId,
        pelt_util: f64,
        pelt_runnable: f64,
        now: Instant,
    ) {
        self.advance_time(domain, now);
        self.l1.on_pelt_sample(domain, pelt_util, pelt_runnable);
        if !pelt_util.is_finite() {
            return;
        }
        let c = self.l1.domain_cores(domain);
        // `pelt_util` is normalized to `[0.0, 1.0]` per domain -> convert to core-units `y_d`.
        let y_d = c * pelt_util.max(0.0);
        let l_cgrp_now = self.l1.cgroup_load_cores(domain);
        let (_, active_boots) = self.l1.domain_counts(domain);
        if let Some(obs) = self.obs_slots.get_mut(domain) {
            let e_d = y_d - obs.p_hat;
            obs.p_hat = (obs.p_hat + self.k_p * e_d).clamp(0.0, 2.0 * c);
            if !(active_boots > 0 && e_d < 0.0) {
                let clamp_c = self.bias_clamp_cores.min(c);
                obs.b_hat = (obs.b_hat + self.k_b * e_d).clamp(-clamp_c, clamp_c);
            }
            if l_cgrp_now == 0.0 && y_d < 0.05 {
                obs.b_hat *= 0.9;
            }
        }
    }

    pub fn bias_cores(&self, domain: DomainId) -> f64 {
        self.obs_slots.get(domain).map(|o| o.b_hat).unwrap_or(0.0)
    }

    pub fn pred_pelt_cores(&self, domain: DomainId) -> f64 {
        self.obs_slots.get(domain).map(|o| o.p_hat).unwrap_or(0.0)
    }

    pub fn l1_load_cores(&self, domain: DomainId) -> f64 {
        self.l1.l1_load_cores(domain)
    }

    pub fn l_hat_cores(&self, domain: DomainId) -> f64 {
        (self.l1.l1_load_cores(domain) + self.bias_cores(domain)).max(0.0)
    }

    pub fn estimated_footprint(&self, fqdn: &str) -> f64 {
        self.l1.estimated_footprint(fqdn)
    }
}

impl DomainLoadEstimator for PeltBiasObserverEstimator {
    fn on_dispatch(&mut self, domain: DomainId, fqdn: &str, weight: f64) -> f64 {
        self.on_dispatch_at(domain, fqdn, weight, Instant::now())
    }

    fn on_complete(&mut self, domain: DomainId, fqdn: &str, weight: f64, footprint_used: f64) {
        self.on_complete_at(domain, fqdn, weight, footprint_used, Instant::now());
    }

    fn on_boot_start(&mut self, domain: DomainId, fqdn: &str) {
        self.on_boot_start_at(domain, fqdn, Instant::now());
    }

    fn on_boot_end(&mut self, domain: DomainId, fqdn: &str) {
        self.on_boot_end_at(domain, fqdn, Instant::now());
    }

    fn on_boot_start_with_footprint(&mut self, domain: DomainId, fqdn: &str, pending_footprint: f64) {
        self.on_boot_start_with_footprint_at(domain, fqdn, pending_footprint, Instant::now());
    }

    fn on_boot_end_with_footprint(&mut self, domain: DomainId, fqdn: &str, pending_footprint: f64) {
        self.on_boot_end_with_footprint_at(domain, fqdn, pending_footprint, Instant::now());
    }

    fn on_pelt_sample(&mut self, domain: DomainId, pelt_util: f64, pelt_runnable: f64) {
        self.on_pelt_sample_at(domain, pelt_util, pelt_runnable, Instant::now());
    }

    fn on_util_sample(&mut self, domain: DomainId, fqdn: &str, cpu_util_cores: f64, domain_rho: f64) {
        self.l1.on_util_sample(domain, fqdn, cpu_util_cores, domain_rho);
    }

    fn predict(&self, domain: DomainId) -> DomainLoadEstimate {
        let l1_est = self.l1.predict(domain);
        let effective_rho = self.l_hat_cores(domain) / self.l1.domain_cores(domain);
        DomainLoadEstimate {
            effective_rho,
            active_weight: l1_est.active_weight,
        }
    }

    fn diagnostics(&self, domain: DomainId) -> DomainEstimatorDiag {
        let c = self.l1.domain_cores(domain);
        DomainEstimatorDiag {
            l1_rho: self.l1.l1_load_cores(domain) / c,
            bias_cores: self.bias_cores(domain),
            pred_pelt_cores: self.pred_pelt_cores(domain),
        }
    }

    fn estimator_name(&self) -> &'static str {
        "pelt_observer"
    }

    fn latest_pelt(&self, domain: DomainId) -> (Option<f64>, Option<f64>) {
        self.l1.latest_pelt(domain)
    }

    fn total_active_activity(&self) -> (u32, u32) {
        self.l1.total_active_activity()
    }

    fn domain_counts(&self, domain: DomainId) -> (u32, u32) {
        self.l1.domain_counts(domain)
    }
}

// ============================================================================
// Stage B: Pure Narrow-Waist Unit Pricer (`domain_cpu_price`)
// ============================================================================

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct DomainUnitPrice {
    /// Unit self-slowdown factor (>= 0.0, strictly increasing in `effective_rho`).
    pub sigma_d: f64,
    /// Marginal co-runner externality rate per unit CPU time (`weight` units).
    pub p_d: f64,
}

/// Pure narrow-waist CPU pricer with zero Ilúvatar state.
///
/// - `sigma_d(rho) = 0.12 * rho + 0.88 * rho^4`: strictly increasing from `0.0` at `rho = 0.0`
///   with no flat zero dead zone, reaching `1.0` at `rho = 1.0` and rising superlinearly above `1.0`.
/// - `phi(rho) = (1 / C_d) * d(sigma_d)/d(rho) = (0.12 + 3.52 * rho^3) / C_d`: marginal slowdown
///   rate inflicted per co-runner unit time, rising superlinearly as `effective_rho -> 1.0+`.
/// - `p_d = active_weight * phi(rho)`.
pub fn domain_cpu_price(est: DomainLoadEstimate, cores_per_domain: f64) -> DomainUnitPrice {
    let rho = if est.effective_rho.is_finite() {
        est.effective_rho.max(0.0)
    } else {
        0.0
    };
    let c = if cores_per_domain.is_finite() {
        cores_per_domain.max(1.0)
    } else {
        1.0
    };
    let active_w = if est.active_weight.is_finite() {
        est.active_weight.max(0.0)
    } else {
        0.0
    };

    let rho2 = rho * rho;
    let rho3 = rho2 * rho;
    let rho4 = rho2 * rho2;

    let sigma_d = SIGMA_LINEAR_SLOPE * rho + SIGMA_SUPERLINEAR_COEFF * rho4;
    let phi_rho = (SIGMA_LINEAR_SLOPE + 4.0 * SIGMA_SUPERLINEAR_COEFF * rho3) / c;
    let p_d = active_w * phi_rho;

    DomainUnitPrice { sigma_d, p_d }
}

// ============================================================================
// Stage C: Unified SMC Shopper (`invocation_price` & `rank_domains_for_dispatch`)
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum InvocationStateKind {
    /// Idle warm (or unplaced prewarm) container available on domain `d` (`state_cost = 0.0`).
    WarmLocal,
    /// Idle warm container exists only on some other domain `d' != d` (`state_cost = w_0 * c_mig_ms`).
    WarmRemote { c_mig_ms: f64 },
    /// Running on `d` requires a cold boot (`state_cost = w_0 * (c_boot_ms - gamma_create_ms)`).
    ColdBoot { c_boot_ms: f64, gamma_create_ms: f64 },
}

impl InvocationStateKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WarmLocal => "WarmLocal",
            Self::WarmRemote { .. } => "WarmRemote",
            Self::ColdBoot { .. } => "ColdBoot",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct InvocationDomainPrice {
    pub domain: DomainId,
    pub l1_rho: f64,
    pub bias_cores: f64,
    pub pred_pelt_cores: f64,
    pub effective_rho: f64,
    pub active_weight: f64,
    pub pelt_util: Option<f64>,
    pub pelt_runnable: Option<f64>,
    pub sigma_d: f64,
    pub p_d: f64,
    pub w_0: f64,
    pub s_i_ms: f64,
    pub benefit_b_i: f64,
    pub private_cost: f64,
    pub externality_cost: f64,
    pub state_cost: f64,
    pub total_price: f64,
    pub toll_to_benefit: f64,
    pub state_kind: InvocationStateKind,
}

/// Evaluate the unified SMC welfare price `P_i(d)` on candidate domain `d`.
/// All cost and benefit terms share identical `weight * time` (`ms^(1 - beta)`) welfare units.
pub fn invocation_price(
    domain: DomainId,
    s_i_ms: f64,
    beta: f64,
    est: DomainLoadEstimate,
    pelt: (Option<f64>, Option<f64>),
    cores_per_domain: f64,
    state_kind: InvocationStateKind,
) -> InvocationDomainPrice {
    let s_i = s_i_ms.max(MIN_S_I_MS);
    let w_0 = s_i.powf(-beta);
    let benefit_b_i = w_0 * s_i;

    let unit = domain_cpu_price(est, cores_per_domain);
    let private_cost = w_0 * unit.sigma_d * s_i;
    let externality_cost = unit.p_d * s_i;
    let state_cost = match state_kind {
        InvocationStateKind::WarmLocal => 0.0,
        InvocationStateKind::WarmRemote { c_mig_ms } => w_0 * c_mig_ms.max(0.0),
        InvocationStateKind::ColdBoot {
            c_boot_ms,
            gamma_create_ms,
        } => w_0 * (c_boot_ms - gamma_create_ms).max(0.0),
    };
    let total_price = private_cost + externality_cost + state_cost;
    let toll_to_benefit = if benefit_b_i > 0.0 {
        total_price / benefit_b_i
    } else {
        0.0
    };

    InvocationDomainPrice {
        domain,
        l1_rho: est.effective_rho,
        bias_cores: 0.0,
        pred_pelt_cores: 0.0,
        effective_rho: est.effective_rho,
        active_weight: est.active_weight,
        pelt_util: pelt.0,
        pelt_runnable: pelt.1,
        sigma_d: unit.sigma_d,
        p_d: unit.p_d,
        w_0,
        s_i_ms: s_i,
        benefit_b_i,
        private_cost,
        externality_cost,
        state_cost,
        total_price,
        toll_to_benefit,
        state_kind,
    }
}

#[derive(Clone, Debug)]
pub struct DispatchPriceSnapshot {
    pub candidates: Vec<InvocationDomainPrice>,
    pub ranked_first: DomainId,
    pub s_i_ms: f64,
    pub w_0: f64,
    pub benefit_b_i: f64,
    pub is_cold: bool,
    pub cold_admitted: bool,
}

#[derive(Clone, Copy, Debug)]
struct FnStats {
    s_i_ms: f64,
    t_boot_ms: f64,
}

/// Component timings for one successfully completed invocation, in ms.
#[derive(Clone, Copy, Debug)]
pub struct CongestionPricingTimingSample {
    pub pricing_queue_ms: f64,
    pub container_acquire_ms: f64,
    pub exec_ms: f64,
    pub post_overhead_ms: f64,
}

#[derive(Clone)]
struct QueuedEntry {
    seq: u64,
    enqueued_at: Instant,
    item: Arc<EnqueuedInvocation>,
}

struct PricingQueueState {
    by_fqdn: HashMap<String, VecDeque<QueuedEntry>>,
    total_len: usize,
    next_seq: u64,
}

impl PricingQueueState {
    fn new() -> Self {
        Self {
            by_fqdn: HashMap::new(),
            total_len: 0,
            next_seq: 0,
        }
    }

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq = self.next_seq.wrapping_add(1);
        s
    }
}

struct TelemetryState {
    invocation_starts: HashMap<TransactionId, Instant>,
    pricing_queue_ms: HashMap<TransactionId, f64>,
    timing_samples: HashMap<&'static str, Vec<CongestionPricingTimingSample>>,
}

fn function_class(fqdn: &str) -> &'static str {
    let lower = fqdn.to_ascii_lowercase();
    if lower.contains("graph-bfs") {
        "graph-bfs"
    } else if lower.contains("gzip") {
        "gzip"
    } else if lower.contains("pyaes") {
        "pyaes"
    } else if lower.contains("hello") {
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

#[derive(Clone, Copy, Debug)]
struct FqdnContainerSnapshot {
    has_idle: bool,
    has_unplaced_idle: bool,
    total_containers: usize,
}

/// RAII guard for `on_dispatch` -> `on_complete` 1:1 domain attribution and exact footprint symmetry.
pub struct PostedPriceDispatchGuard {
    coordinator: Arc<PostedPriceCoordinator>,
    domain: DomainId,
    fqdn: String,
    weight: f64,
    footprint_used: f64,
    s_i_ms: f64,
    completed: bool,
}

impl PostedPriceDispatchGuard {
    pub fn actual_domain(&self) -> DomainId {
        self.domain
    }

    pub fn weight(&self) -> f64 {
        self.weight
    }

    pub fn footprint_used(&self) -> f64 {
        self.footprint_used
    }

    pub fn s_i_ms(&self) -> f64 {
        self.s_i_ms
    }

    pub fn complete_now(&mut self) {
        if !self.completed {
            self.completed = true;
            self.coordinator.on_complete_with_footprint(
                self.domain,
                &self.fqdn,
                self.weight,
                self.footprint_used,
            );
        }
    }
}

impl Drop for PostedPriceDispatchGuard {
    fn drop(&mut self) {
        self.complete_now();
    }
}

/// RAII guard for `on_boot_start` -> `on_boot_end` 1:1 cold-boot tracking.
pub struct PostedPriceBootGuard {
    coordinator: Arc<PostedPriceCoordinator>,
    domain: DomainId,
    fqdn: String,
    pending_footprint: f64,
    ended: bool,
}

impl PostedPriceBootGuard {
    pub fn finish_now(&mut self) {
        if !self.ended {
            self.ended = true;
            self.coordinator
                .on_boot_end_with_footprint(self.domain, &self.fqdn, self.pending_footprint);
        }
    }
}

impl Drop for PostedPriceBootGuard {
    fn drop(&mut self) {
        self.finish_now();
    }
}

/// Bundle of dispatch attribution + lifecycle guards passed into worker execution.
pub struct PostedPriceInvocationLifecycle {
    pub actual_domain: DomainId,
    pub s_i_ms: f64,
    pub w_0: f64,
    pub dispatch_rho: f64,
    pub dispatch_guard: PostedPriceDispatchGuard,
    pub boot_guard: Option<PostedPriceBootGuard>,
}

pub struct PostedPriceCoordinator {
    estimator: RwLock<Box<dyn DomainLoadEstimator>>,
    fn_state: RwLock<HashMap<String, FnStats>>,
    booting_by_fqdn: RwLock<HashMap<String, u32>>,
    queue: Mutex<PricingQueueState>,
    telemetry: Mutex<TelemetryState>,
    fineloadbalancing: FineLoadBalancingWeak,
    container_manager: RwLock<Option<Weak<ContainerManager>>>,
    cmap: WorkerCharMap,
    beta: f64,
    c_mig_ms: f64,
    aging_grace_sec: f64,
    aging_alpha: f64,
    cores_per_domain: u32,
    domain_assigned_cores: Vec<u32>,
    max_queue_len: usize,
    num_domains: usize,
    rr_counter: AtomicU64,
    last_pelt_poll_ns: AtomicU64,
    start_instant: Instant,
}

pub type CongestionPricingCoordinator = PostedPriceCoordinator;

impl PostedPriceCoordinator {
    pub fn new(
        config: Option<&PostedPriceConfig>,
        fineloadbalancing: FineLoadBalancingWeak,
        cmap: WorkerCharMap,
        cont_manager: Option<Arc<ContainerManager>>,
    ) -> Arc<Self> {
        Self::new_with_fallback_cores(
            config,
            fineloadbalancing,
            cmap,
            cont_manager,
            DEFAULT_CORES_PER_DOMAIN,
        )
    }

    pub fn new_with_fallback_cores(
        config: Option<&PostedPriceConfig>,
        fineloadbalancing: FineLoadBalancingWeak,
        cmap: WorkerCharMap,
        cont_manager: Option<Arc<ContainerManager>>,
        fallback_total_cores: u32,
    ) -> Arc<Self> {
        let detected_domain_cores: Vec<u32> = if let Some(flb) = fineloadbalancing.upgrade() {
            let domains = flb.system_domains.immutable_clone();
            if domains.is_empty() {
                vec![fallback_total_cores.max(1)]
            } else {
                domains.iter().map(|d| d.assigned_cores.max(1)).collect()
            }
        } else {
            vec![fallback_total_cores.max(1)]
        };
        let num_domains = detected_domain_cores.len().max(1);
        let domain_cores: Vec<u32> = if let Some(override_c) = config.and_then(|c| c.cores_per_domain) {
            vec![override_c.max(1); num_domains]
        } else {
            detected_domain_cores
        };
        let estimator: Box<dyn DomainLoadEstimator> = match config
            .and_then(|c| c.load_estimator)
            .unwrap_or_default()
        {
            LoadEstimatorKind::Unit => Box::new(UnitFootprintEstimator::new_heterogeneous(domain_cores.clone())),
            LoadEstimatorKind::UtilWeighted => {
                Box::new(UtilWeightedEstimator::from_config_heterogeneous(domain_cores.clone(), config))
            },
            LoadEstimatorKind::PeltObserver => {
                Box::new(PeltBiasObserverEstimator::from_config_heterogeneous(domain_cores.clone(), config))
            },
        };
        Self::with_domain_cores_and_estimator(
            config,
            fineloadbalancing,
            cmap,
            cont_manager,
            domain_cores,
            estimator,
        )
    }

    pub fn with_estimator(
        config: Option<&PostedPriceConfig>,
        fineloadbalancing: FineLoadBalancingWeak,
        cmap: WorkerCharMap,
        cont_manager: Option<Arc<ContainerManager>>,
        num_domains: usize,
        estimator: Box<dyn DomainLoadEstimator>,
    ) -> Arc<Self> {
        let n = num_domains.max(1);
        let detected_domain_cores: Vec<u32> = if let Some(override_c) = config.and_then(|c| c.cores_per_domain) {
            vec![override_c.max(1); n]
        } else if let Some(flb) = fineloadbalancing.upgrade() {
            let domains = flb.system_domains.immutable_clone();
            if domains.len() == n {
                domains.iter().map(|d| d.assigned_cores.max(1)).collect()
            } else {
                let first_c = domains
                    .first()
                    .map(|d| d.assigned_cores.max(1))
                    .unwrap_or(DEFAULT_CORES_PER_DOMAIN);
                vec![first_c; n]
            }
        } else {
            vec![DEFAULT_CORES_PER_DOMAIN; n]
        };
        Self::with_domain_cores_and_estimator(
            config,
            fineloadbalancing,
            cmap,
            cont_manager,
            detected_domain_cores,
            estimator,
        )
    }

    pub fn with_domain_cores_and_estimator(
        config: Option<&PostedPriceConfig>,
        fineloadbalancing: FineLoadBalancingWeak,
        cmap: WorkerCharMap,
        cont_manager: Option<Arc<ContainerManager>>,
        domain_assigned_cores: Vec<u32>,
        estimator: Box<dyn DomainLoadEstimator>,
    ) -> Arc<Self> {
        let domain_assigned_cores: Vec<u32> = if domain_assigned_cores.is_empty() {
            vec![config
                .and_then(|c| c.cores_per_domain)
                .unwrap_or(DEFAULT_CORES_PER_DOMAIN)
                .max(1)]
        } else {
            domain_assigned_cores.into_iter().map(|c| c.max(1)).collect()
        };
        let num_domains = domain_assigned_cores.len();
        let cores_per_domain = domain_assigned_cores
            .iter()
            .copied()
            .max()
            .unwrap_or(DEFAULT_CORES_PER_DOMAIN)
            .max(1);

        let beta = config.and_then(|c| c.welfare_beta).unwrap_or(DEFAULT_BETA);
        let max_queue_len = config
            .and_then(|c| c.max_queue_len)
            .unwrap_or(DEFAULT_MAX_QUEUE_LEN)
            .max(1);
        let c_mig_ms = config
            .and_then(|c| c.c_mig_ms)
            .unwrap_or(DEFAULT_C_MIG_MS)
            .max(0.0);
        let aging_grace_sec = config
            .and_then(|c| c.aging_grace_sec)
            .unwrap_or(DEFAULT_AGING_GRACE_SEC)
            .max(0.0);
        let aging_alpha = config
            .and_then(|c| c.aging_alpha)
            .unwrap_or(DEFAULT_AGING_ALPHA)
            .max(0.0);

        Arc::new(Self {
            estimator: RwLock::new(estimator),
            fn_state: RwLock::new(HashMap::new()),
            booting_by_fqdn: RwLock::new(HashMap::new()),
            queue: Mutex::new(PricingQueueState::new()),
            telemetry: Mutex::new(TelemetryState {
                invocation_starts: HashMap::new(),
                pricing_queue_ms: HashMap::new(),
                timing_samples: HashMap::new(),
            }),
            fineloadbalancing,
            container_manager: RwLock::new(cont_manager.map(|cm| Arc::downgrade(&cm))),
            cmap,
            beta,
            c_mig_ms,
            aging_grace_sec,
            aging_alpha,
            cores_per_domain,
            domain_assigned_cores,
            max_queue_len,
            num_domains,
            rr_counter: AtomicU64::new(0),
            last_pelt_poll_ns: AtomicU64::new(0),
            start_instant: Instant::now(),
        })
    }

    pub fn set_container_manager(&self, manager: Weak<ContainerManager>) {
        *self.container_manager.write() = Some(manager);
    }

    pub fn num_domains(&self) -> usize {
        self.num_domains
    }

    pub fn cores_per_domain(&self) -> u32 {
        self.cores_per_domain
    }

    pub fn domain_cores(&self, domain: DomainId) -> u32 {
        self.domain_assigned_cores
            .get(domain)
            .copied()
            .unwrap_or(self.cores_per_domain)
    }

    pub fn beta(&self) -> f64 {
        self.beta
    }

    pub fn c_mig_ms(&self) -> f64 {
        self.c_mig_ms
    }

    fn now_ms(&self) -> f64 {
        self.start_instant.elapsed().as_secs_f64() * 1000.0
    }

    fn fn_stats(&self, fqdn: &str) -> FnStats {
        if let Some(stats) = self.fn_state.read().get(fqdn).copied() {
            return stats;
        }
        // Note: WorkerCharMap stores CpuWarmTime / CpuExecTime / CpuColdTime in SECONDS.
        let cmap_warm_ms = self.cmap.get_avg(fqdn, Chars::CpuWarmTime) * 1000.0;
        let cmap_exec_ms = self.cmap.get_avg(fqdn, Chars::CpuExecTime) * 1000.0;
        let cmap_cold_ms = self.cmap.get_avg(fqdn, Chars::CpuColdTime) * 1000.0;

        let s_i_ms = if cmap_warm_ms.is_finite() && cmap_warm_ms > 0.0 {
            cmap_warm_ms.max(MIN_S_I_MS)
        } else if cmap_exec_ms.is_finite() && cmap_exec_ms > 0.0 {
            cmap_exec_ms.max(MIN_S_I_MS)
        } else {
            DEFAULT_S_I_MS
        };
        let t_boot_ms = if cmap_cold_ms.is_finite() && cmap_cold_ms > 0.0 {
            cmap_cold_ms.max(1.0)
        } else {
            DEFAULT_T_BOOT_MS
        };
        FnStats { s_i_ms, t_boot_ms }
    }

    pub fn priority_weight(&self, fqdn: &str) -> (f64, f64) {
        let s_i = self.fn_stats(fqdn).s_i_ms;
        let w_0 = s_i.powf(-self.beta);
        (s_i, w_0)
    }

    /// Queue-ordering priority with starvation deadband `aging_grace_sec`:
    /// - `effective_wait_s = (wait_s - aging_grace_sec).max(0.0)`
    /// - `w_eff = w_0 * (1.0 + aging_alpha * effective_wait_s)`
    ///
    /// Used strictly inside `pricing_queue` ordering/popping; never passed downstream.
    /// When `wait_s <= aging_grace_sec`, returns `w_0` identically.
    pub fn effective_queue_priority(&self, fqdn: &str, wait_s: f64) -> f64 {
        let (_, w_0) = self.priority_weight(fqdn);
        if !wait_s.is_finite() || self.aging_alpha <= 0.0 {
            return w_0;
        }
        let effective_wait_s = (wait_s - self.aging_grace_sec).max(0.0);
        if effective_wait_s == 0.0 {
            w_0
        } else {
            w_0 * (1.0 + self.aging_alpha * effective_wait_s)
        }
    }

    fn container_snapshot(&self, fqdn: &str) -> (FqdnContainerSnapshot, Vec<DomainId>) {
        if let Some(cm) = self.container_manager.read().as_ref().and_then(Weak::upgrade) {
            let (idle_warm, idle_prewarm, total_containers) = cm.cpu_container_wave_counts(fqdn);
            let warm_domains: Vec<DomainId> = cm
                .idle_cpu_warm_domains(fqdn)
                .into_iter()
                .filter(|&d| d < self.num_domains)
                .collect();
            let has_idle = (idle_warm + idle_prewarm) > 0;
            // A prewarm container (or container with `last_domain == None`) has no prior domain
            // cache footprint, so placing it on any domain incurs zero migration cost.
            let has_unplaced_idle = has_idle && warm_domains.is_empty();
            return (
                FqdnContainerSnapshot {
                    has_idle,
                    has_unplaced_idle,
                    total_containers,
                },
                warm_domains,
            );
        }
        if let Some(flb) = self.fineloadbalancing.upgrade() {
            if let Some(st) = flb.idle_cpu_container_state(fqdn) {
                let has_idle = matches!(st, ContainerState::Warm | ContainerState::Prewarm);
                return (
                    FqdnContainerSnapshot {
                        has_idle,
                        has_unplaced_idle: has_idle,
                        total_containers: usize::from(has_idle),
                    },
                    Vec::new(),
                );
            }
        }
        (
            FqdnContainerSnapshot {
                has_idle: false,
                has_unplaced_idle: false,
                total_containers: 0,
            },
            Vec::new(),
        )
    }

    /// Poll FineSched PELT + runnable telemetry outside any coordinator lock (throttled to 2 ms)
    /// and feed `DomainLoadEstimator::on_pelt_sample`.
    pub fn refresh_pelt_if_due(&self) {
        let elapsed_ns = self.start_instant.elapsed().as_nanos() as u64;
        let prev = self.last_pelt_poll_ns.load(Ordering::Relaxed);
        if prev != 0 && elapsed_ns.saturating_sub(prev) < PELT_REFRESH_INTERVAL_NS {
            return;
        }
        if self
            .last_pelt_poll_ns
            .compare_exchange(prev, elapsed_ns, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let Some(flb) = self.fineloadbalancing.upgrade() else {
            return;
        };
        let mut samples = Vec::with_capacity(self.num_domains);
        for d in 0..self.num_domains {
            let gid = d as SchedGroupID;
            let pelt_util = flb
                .domain_pelt_like_utilization_percent(gid)
                .ok()
                .flatten()
                .map(|pct| (pct / 100.0).max(0.0));
            let runnable = flb.domain_runnable_load(gid);
            if let Some(util) = pelt_util {
                samples.push((d, util, runnable.unwrap_or(0.0)));
            }
        }
        if !samples.is_empty() {
            let mut est = self.estimator.write();
            for (d, util, runnable) in samples {
                est.on_pelt_sample(d, util, runnable);
            }
        }
    }

    // ========================================================================
    // Stage A/C Lifecycle Callbacks (1:1 Domain Attribution)
    // ========================================================================

    pub fn on_dispatch(&self, domain: DomainId, fqdn: &str, weight: f64) -> f64 {
        self.estimator.write().on_dispatch(domain, fqdn, weight)
    }

    pub fn on_complete(&self, domain: DomainId, fqdn: &str, weight: f64) {
        self.on_complete_with_footprint(domain, fqdn, weight, 1.0);
    }

    pub fn on_complete_with_footprint(
        &self,
        domain: DomainId,
        fqdn: &str,
        weight: f64,
        footprint_used: f64,
    ) {
        self.estimator
            .write()
            .on_complete(domain, fqdn, weight, footprint_used);
    }

    pub fn on_util_sample(
        &self,
        domain: DomainId,
        fqdn: &str,
        cpu_util_cores: f64,
        domain_rho: f64,
    ) {
        self.estimator
            .write()
            .on_util_sample(domain, fqdn, cpu_util_cores, domain_rho);
    }

    pub fn on_boot_start(&self, domain: DomainId, fqdn: &str) {
        self.on_boot_start_with_footprint(domain, fqdn, 0.0);
    }

    pub fn on_boot_start_with_footprint(&self, domain: DomainId, fqdn: &str, pending_footprint: f64) {
        *self.booting_by_fqdn.write().entry(fqdn.to_string()).or_insert(0) += 1;
        self.estimator
            .write()
            .on_boot_start_with_footprint(domain, fqdn, pending_footprint);
    }

    pub fn on_boot_end(&self, domain: DomainId, fqdn: &str) {
        self.on_boot_end_with_footprint(domain, fqdn, 0.0);
    }

    pub fn on_boot_end_with_footprint(&self, domain: DomainId, fqdn: &str, pending_footprint: f64) {
        {
            let mut boots = self.booting_by_fqdn.write();
            if let Some(cnt) = boots.get_mut(fqdn) {
                *cnt = cnt.saturating_sub(1);
                if *cnt == 0 {
                    boots.remove(fqdn);
                }
            }
        }
        self.estimator
            .write()
            .on_boot_end_with_footprint(domain, fqdn, pending_footprint);
    }

    pub fn on_pelt_sample(&self, domain: DomainId, pelt_util: f64, pelt_runnable: f64) {
        self.estimator
            .write()
            .on_pelt_sample(domain, pelt_util, pelt_runnable);
    }

    pub fn predict_domain(&self, domain: DomainId) -> DomainLoadEstimate {
        self.estimator.read().predict(domain)
    }

    pub fn domain_counts(&self, domain: DomainId) -> (u32, u32) {
        self.estimator.read().domain_counts(domain)
    }

    pub fn start_boot_guard(self: &Arc<Self>, domain: DomainId, fqdn: &str) -> PostedPriceBootGuard {
        self.start_boot_guard_with_footprint(domain, fqdn, 0.0)
    }

    pub fn start_boot_guard_with_footprint(
        self: &Arc<Self>,
        domain: DomainId,
        fqdn: &str,
        pending_footprint: f64,
    ) -> PostedPriceBootGuard {
        self.on_boot_start_with_footprint(domain, fqdn, pending_footprint);
        PostedPriceBootGuard {
            coordinator: self.clone(),
            domain,
            fqdn: fqdn.to_string(),
            pending_footprint,
            ended: false,
        }
    }

    /// Immediately upon `acquire_cores` returning `actual_domain`, attribute in-flight load
    /// (and boot start if cold) to `actual_domain` and emit dispatch-time telemetry.
    pub fn begin_dispatch(
        self: &Arc<Self>,
        tid: &TransactionId,
        fqdn: &str,
        actual_domain: DomainId,
        snapshot: Option<&DispatchPriceSnapshot>,
        express_ranked_first: Option<DomainId>,
    ) -> PostedPriceInvocationLifecycle {
        let (s_i_ms, w_0, is_cold) = if let Some(snap) = snapshot {
            (snap.s_i_ms, snap.w_0, snap.is_cold)
        } else {
            let (s_i, w_0) = self.priority_weight(fqdn);
            (s_i, w_0, false)
        };

        let footprint_used = self.on_dispatch(actual_domain, fqdn, w_0);
        let dispatch_guard = PostedPriceDispatchGuard {
            coordinator: self.clone(),
            domain: actual_domain,
            fqdn: fqdn.to_string(),
            weight: w_0,
            footprint_used,
            s_i_ms,
            completed: false,
        };
        let boot_guard = if is_cold {
            Some(self.start_boot_guard_with_footprint(actual_domain, fqdn, footprint_used))
        } else {
            None
        };
        let dispatch_rho = self.predict_domain(actual_domain).effective_rho;

        if let Some(snap) = snapshot {
            self.emit_dispatch_telemetry(tid, fqdn, snap, actual_domain);
        } else {
            let ranked_domain = express_ranked_first.unwrap_or(actual_domain);
            debug!(
                decision_time_ms = self.now_ms(),
                tid = %tid,
                fqdn = %fqdn,
                function_class = function_class(fqdn),
                policy = "posted_price",
                ranked_domain,
                actual_domain,
                spilled = actual_domain != ranked_domain,
                estimated_runtime_ms = s_i_ms,
                priority_weight = w_0,
                "[posted_price] express dispatch"
            );
        }

        PostedPriceInvocationLifecycle {
            actual_domain,
            s_i_ms,
            w_0,
            dispatch_rho,
            dispatch_guard,
            boot_guard,
        }
    }

    pub fn emit_dispatch_telemetry(
        &self,
        tid: &TransactionId,
        fqdn: &str,
        snapshot: &DispatchPriceSnapshot,
        actual_domain: DomainId,
    ) {
        let now_ms = self.now_ms();
        let fclass = function_class(fqdn);
        let spilled = actual_domain != snapshot.ranked_first;
        let load_estimator = self.estimator.read().estimator_name();
        for cand in &snapshot.candidates {
            let unit_cpu_toll = if cand.s_i_ms > 0.0 {
                (cand.private_cost + cand.externality_cost) / cand.s_i_ms
            } else {
                0.0
            };
            debug!(
                decision_time_ms = now_ms,
                tid = %tid,
                fqdn = %fqdn,
                function_class = fclass,
                policy = "posted_price",
                load_estimator = load_estimator,
                candidate_domain = cand.domain,
                ranked_domain = snapshot.ranked_first,
                actual_domain = actual_domain,
                final_domain = actual_domain,
                selected = cand.domain == actual_domain,
                spilled = spilled,
                admitted = !snapshot.is_cold || snapshot.cold_admitted,
                likely_cold = snapshot.is_cold,
                state_kind = cand.state_kind.as_str(),
                estimated_runtime_ms = cand.s_i_ms,
                priority_weight = cand.w_0,
                l1_rho = cand.l1_rho,
                bias_cores = cand.bias_cores,
                pred_pelt_cores = cand.pred_pelt_cores,
                effective_rho = cand.effective_rho,
                rho = cand.effective_rho,
                active_weight = cand.active_weight,
                pelt_util = ?cand.pelt_util,
                pelt_runnable = ?cand.pelt_runnable,
                sigma_d = cand.sigma_d,
                p_d = cand.p_d,
                private_cost = cand.private_cost,
                externality_cost = cand.externality_cost,
                state_cost = cand.state_cost,
                total_price = cand.total_price,
                total_marginal_toll = cand.total_price,
                benefit_B_i = cand.benefit_b_i,
                benefit = cand.benefit_b_i,
                toll_to_benefit = cand.toll_to_benefit,
                unit_congestion_price = unit_cpu_toll,
                net_gain = cand.benefit_b_i - cand.total_price,
                welfare_beta = self.beta,
                "[congestion_pricing] price candidate"
            );
        }
    }

    // ========================================================================
    // Stage C: Dispatch-Time Ranking (Read-Lock Only!) & Express Helper
    // ========================================================================

    /// Lightweight helper for express-lane (`hello`, warm `s_i < 2 ms`) invocations.
    /// Acquires only a brief read lock on `estimator` and returns `[warm_domain..., least_loaded...]`.
    pub fn rank_domains_express(&self, fqdn: &str) -> Vec<DomainId> {
        let (_, warm_domains) = self.container_snapshot(fqdn);
        self.rank_domains_express_with_warm(warm_domains.as_slice())
    }

    pub fn rank_domains_express_with_warm(&self, warm_domains: &[DomainId]) -> Vec<DomainId> {
        let n = self.num_domains;
        let rr = (self.rr_counter.fetch_add(1, Ordering::Relaxed) as usize) % n;
        let est_guard = self.estimator.read();
        let mut entries: Vec<(DomainId, bool, bool, DomainLoadEstimate)> = (0..n)
            .map(|d| {
                let (in_flight, _) = est_guard.domain_counts(d);
                let has_slot = in_flight < self.domain_cores(d);
                (d, has_slot, warm_domains.contains(&d), est_guard.predict(d))
            })
            .collect();
        drop(est_guard);

        // Keep all `n` domains in the returned slice:
        // 1. Domains with `in_flight_count < cores_per_domain` come first (warm & `effective_rho < 1.0`
        //    preferred, then remaining `< cores_per_domain` domains by ascending `effective_rho`).
        // 2. Domains with `in_flight_count >= cores_per_domain` are demoted to the back, sorted by
        //    ascending `effective_rho` so a concurrent completion during `acquire_cores` still picks
        //    the least-loaded domain and never sees an empty slice.
        entries.sort_by(|(da, sa, wa, ea), (db, sb, wb, eb)| {
            let warm_avail_a = *sa && *wa && ea.effective_rho < 1.0;
            let warm_avail_b = *sb && *wb && eb.effective_rho < 1.0;
            let tie_a = (*da + n - rr) % n;
            let tie_b = (*db + n - rr) % n;
            sb.cmp(sa)
                .then_with(|| warm_avail_b.cmp(&warm_avail_a))
                .then_with(|| OrderedFloat(ea.effective_rho).cmp(&OrderedFloat(eb.effective_rho)))
                .then_with(|| OrderedFloat(ea.active_weight).cmp(&OrderedFloat(eb.active_weight)))
                .then_with(|| wb.cmp(wa))
                .then_with(|| tie_a.cmp(&tie_b))
        });
        entries.into_iter().map(|(d, _, _, _)| d).collect()
    }

    /// Evaluate `P_i(d)` across all `d in 0..num_domains` under a brief read lock on `estimator`,
    /// returning `(ranked_domains, snapshot)`.
    pub fn rank_domains_for_dispatch(&self, fqdn: &str) -> (Vec<DomainId>, DispatchPriceSnapshot) {
        let (cont_snap, warm_domains) = self.container_snapshot(fqdn);
        let active_boots = self.booting_by_fqdn.read().get(fqdn).copied().unwrap_or(0);
        self.rank_domains_inner(
            fqdn,
            cont_snap.has_idle,
            cont_snap.has_unplaced_idle,
            cont_snap.total_containers + active_boots as usize,
            &warm_domains,
        )
    }

    /// Explicit state helper for unit testing Stage C domain ranking and locality-vs-spill.
    pub fn rank_domains_with_state(
        &self,
        fqdn: &str,
        has_idle: bool,
        warm_domains: &[DomainId],
        existing_replicas: usize,
    ) -> (Vec<DomainId>, DispatchPriceSnapshot) {
        let has_unplaced_idle = has_idle && warm_domains.is_empty();
        self.rank_domains_inner(fqdn, has_idle, has_unplaced_idle, existing_replicas, warm_domains)
    }

    fn rank_domains_inner(
        &self,
        fqdn: &str,
        has_idle: bool,
        has_unplaced_idle: bool,
        existing_replicas: usize,
        warm_domains: &[DomainId],
    ) -> (Vec<DomainId>, DispatchPriceSnapshot) {
        let fn_stats = self.fn_stats(fqdn);
        let s_i = fn_stats.s_i_ms;
        let t_boot = fn_stats.t_boot_ms;
        let n = self.num_domains;
        let rr = (self.rr_counter.fetch_add(1, Ordering::Relaxed) as usize) % n;
        let is_cold = !has_idle;

        // Brief read lock on `estimator` ONLY; never held across `acquire_cores`.
        let (estimates, diags, pelt_vals, has_slots, total_activity) = {
            let est_guard = self.estimator.read();
            let mut ests = Vec::with_capacity(n);
            let mut dgs = Vec::with_capacity(n);
            let mut pelts = Vec::with_capacity(n);
            let mut slots = Vec::with_capacity(n);
            for d in 0..n {
                ests.push(est_guard.predict(d));
                dgs.push(est_guard.diagnostics(d));
                pelts.push(est_guard.latest_pelt(d));
                slots.push(est_guard.domain_counts(d).0 < self.domain_cores(d));
            }
            let activity = est_guard.total_active_activity();
            (ests, dgs, pelts, slots, activity)
        };

        let mut candidates = Vec::with_capacity(n);
        for d in 0..n {
            let c_per_dom = self.domain_cores(d) as f64;
            let est = estimates[d];
            let diag = diags[d];
            let pelt = pelt_vals[d];
            let unit = domain_cpu_price(est, c_per_dom);
            let state_kind = if has_idle {
                if has_unplaced_idle || warm_domains.contains(&d) {
                    InvocationStateKind::WarmLocal
                } else {
                    InvocationStateKind::WarmRemote {
                        c_mig_ms: self.c_mig_ms,
                    }
                }
            } else {
                let c_boot_ms = t_boot * (1.0 + BOOT_CONGESTION_FACTOR * unit.sigma_d);
                // First replica unlocks warm execution (`Gamma_create = C_boot(d)`);
                // scale-out replicas credit baseline isolated `T_boot`, leaving congestion stretch.
                let gamma_create_ms = if existing_replicas == 0 {
                    c_boot_ms
                } else {
                    t_boot
                };
                InvocationStateKind::ColdBoot {
                    c_boot_ms,
                    gamma_create_ms,
                }
            };
            let mut cand = invocation_price(
                d,
                s_i,
                self.beta,
                est,
                pelt,
                c_per_dom,
                state_kind,
            );
            cand.l1_rho = diag.l1_rho;
            cand.bias_cores = diag.bias_cores;
            cand.pred_pelt_cores = diag.pred_pelt_cores;
            candidates.push(cand);
        }

        // Keep all `n` domains in `ranked_domains`: domains with `< cores_per_domain` active
        // invocations come first (ordered by ascending `total_price`), followed by full
        // `>= cores_per_domain` domains (ordered by ascending `total_price`).
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by(|&ia, &ib| {
            let ca = &candidates[ia];
            let cb = &candidates[ib];
            let tie_a = (ca.domain + n - rr) % n;
            let tie_b = (cb.domain + n - rr) % n;
            has_slots[ib]
                .cmp(&has_slots[ia])
                .then_with(|| OrderedFloat(ca.total_price).cmp(&OrderedFloat(cb.total_price)))
                .then_with(|| OrderedFloat(ca.effective_rho).cmp(&OrderedFloat(cb.effective_rho)))
                .then_with(|| tie_a.cmp(&tie_b))
        });

        let ranked_domains: Vec<DomainId> = order.iter().map(|&idx| candidates[idx].domain).collect();
        let best = &candidates[order[0]];
        let host_idle = total_activity.0 == 0 && total_activity.1 == 0;
        let cold_admitted = !is_cold || best.total_price <= best.benefit_b_i || host_idle;

        let snapshot = DispatchPriceSnapshot {
            ranked_first: best.domain,
            s_i_ms: best.s_i_ms,
            w_0: best.w_0,
            benefit_b_i: best.benefit_b_i,
            is_cold,
            cold_admitted,
            candidates,
        };
        (ranked_domains, snapshot)
    }

    /// Dispatch-time routing entry point: returns `(ranked_domains, Option<DispatchPriceSnapshot>)`.
    /// Warm express-lane invocations (`s_i < 2.0 ms`) use `rank_domains_express` (`snapshot = None`);
    /// all other invocations use `rank_domains_for_dispatch`.
    pub fn rank_for_item(
        &self,
        item: &Arc<EnqueuedInvocation>,
    ) -> (Vec<DomainId>, Option<DispatchPriceSnapshot>) {
        let fqdn = &item.registration.fqdn;
        let (cont_snap, warm_domains) = self.container_snapshot(fqdn);
        let s_i = self.fn_stats(fqdn).s_i_ms;
        if cont_snap.has_idle && s_i < EXPRESS_LANE_MAX_S_I_MS {
            let ranked = self.rank_domains_express_with_warm(&warm_domains);
            return (ranked, None);
        }
        let active_boots = self.booting_by_fqdn.read().get(fqdn).copied().unwrap_or(0);
        let (ranked, snapshot) = self.rank_domains_inner(
            fqdn,
            cont_snap.has_idle,
            cont_snap.has_unplaced_idle,
            cont_snap.total_containers + active_boots as usize,
            &warm_domains,
        );
        (ranked, Some(snapshot))
    }

    fn is_fqdn_dispatchable(&self, fqdn: &str) -> bool {
        let (cont_snap, warm_domains) = self.container_snapshot(fqdn);
        if cont_snap.has_idle {
            return true;
        }
        let active_boots = self.booting_by_fqdn.read().get(fqdn).copied().unwrap_or(0);
        let (_, snap) = self.rank_domains_inner(
            fqdn,
            false,
            false,
            cont_snap.total_containers + active_boots as usize,
            &warm_domains,
        );
        snap.cold_admitted
    }

    /// Select the highest-`w_eff` FQDN whose head item is currently eligible for dispatch
    /// (`WarmIdle`, or `Cold` with `P_i(d*) <= B_i`). Ties in `w_eff` are broken strictly by
    /// FIFO arrival timestamp (`enqueued_at`, then `seq`). Deferred cold FQDNs are skipped so
    /// they never head-of-line block `WarmIdle` invocations behind them.
    fn select_dispatchable_fqdn(&self, q: &PricingQueueState) -> Option<String> {
        if q.total_len == 0 {
            return None;
        }
        let now = Instant::now();
        let mut fqdn_heads: Vec<(&String, OrderedFloat<f64>, Instant, u64)> = q
            .by_fqdn
            .iter()
            .filter_map(|(fqdn, deque)| {
                let front = deque.front()?;
                let wait_s = now.saturating_duration_since(front.enqueued_at).as_secs_f64();
                let w_eff = self.effective_queue_priority(fqdn, wait_s);
                Some((fqdn, OrderedFloat(w_eff), front.enqueued_at, front.seq))
            })
            .collect();

        // Highest effective priority first; strict FIFO `(enqueued_at, seq)` tie-break.
        fqdn_heads.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.3.cmp(&b.3))
        });

        for (fqdn, _, _, _) in fqdn_heads {
            if self.is_fqdn_dispatchable(fqdn) {
                return Some(fqdn.clone());
            }
        }
        None
    }

    /// Pop the next dispatchable invocation in `w_0` priority order, skipping deferred cold boots.
    pub fn pop_dispatchable(&self) -> Option<Arc<EnqueuedInvocation>> {
        let mut q = self.queue.lock();
        let chosen_fqdn = self.select_dispatchable_fqdn(&q)?;
        let entry = {
            let deque = q.by_fqdn.get_mut(&chosen_fqdn)?;
            let popped = deque.pop_front()?;
            if deque.is_empty() {
                q.by_fqdn.remove(&chosen_fqdn);
            }
            q.total_len = q.total_len.saturating_sub(1);
            popped
        };
        drop(q);

        let wait_ms = entry.enqueued_at.elapsed().as_secs_f64() * 1000.0;
        let has_idle = self.container_snapshot(&entry.item.registration.fqdn).0.has_idle;
        entry
            .item
            .likely_cold
            .store(!has_idle, Ordering::Relaxed);
        self.telemetry
            .lock()
            .pricing_queue_ms
            .insert(entry.item.tid.clone(), wait_ms);
        Some(entry.item)
    }

    /// Re-insert an invocation at the front of its FQDN subqueue if core acquisition raced,
    /// preserving its original admission timestamp so FIFO tie-breaking and aging are unaffected.
    pub fn requeue_front(&self, item: &Arc<EnqueuedInvocation>) {
        let orig_enqueued_at = self
            .telemetry
            .lock()
            .invocation_starts
            .get(&item.tid)
            .copied()
            .unwrap_or_else(Instant::now);
        let mut q = self.queue.lock();
        let seq = q
            .by_fqdn
            .get(&item.registration.fqdn)
            .and_then(|d| d.front())
            .map(|e| e.seq.saturating_sub(1))
            .unwrap_or_else(|| q.alloc_seq());
        q.by_fqdn
            .entry(item.registration.fqdn.clone())
            .or_default()
            .push_front(QueuedEntry {
                seq,
                enqueued_at: orig_enqueued_at,
                item: item.clone(),
            });
        q.total_len += 1;
    }

    // ========================================================================
    // Post-Execution EWMA & Timing Telemetry
    // ========================================================================

    pub fn take_invocation_timing(&self, tid: &TransactionId) -> (f64, Instant) {
        let mut tel = self.telemetry.lock();
        let started_at = tel
            .invocation_starts
            .remove(tid)
            .unwrap_or_else(Instant::now);
        let pricing_queue_ms = tel.pricing_queue_ms.remove(tid).unwrap_or(0.0);
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
        self.telemetry
            .lock()
            .timing_samples
            .entry(class)
            .or_default()
            .push(sample);
    }

    /// Update per-function `s_i` / `t_boot` EWMAs from realized invocation duration.
    /// Does NOT touch in-flight counts (those are handled 1:1 by `PostedPriceDispatchGuard`).
    pub fn record_execution_observation(
        &self,
        domain_id: DomainId,
        fqdn: &str,
        wall_exec_ms: f64,
        is_cold: bool,
        cpu_exec_ms: Option<f64>,
    ) {
        let est = self.predict_domain(domain_id);
        let unit = domain_cpu_price(est, self.domain_cores(domain_id) as f64);
        let mut stats = self.fn_stats(fqdn);
        if !is_cold {
            let raw_ms = cpu_exec_ms.unwrap_or(wall_exec_ms).max(MIN_S_I_MS);
            let s_norm = if est.effective_rho <= 1.0 {
                raw_ms
            } else {
                (raw_ms / (1.0 + unit.sigma_d)).max(MIN_S_I_MS)
            };
            stats.s_i_ms = (0.9 * stats.s_i_ms + 0.1 * s_norm).max(MIN_S_I_MS);
        } else if wall_exec_ms.is_finite() && wall_exec_ms > 0.0 {
            stats.t_boot_ms = (0.9 * stats.t_boot_ms + 0.1 * wall_exec_ms).max(1.0);
        }
        self.fn_state.write().insert(fqdn.to_string(), stats);
    }
}

impl Drop for PostedPriceCoordinator {
    fn drop(&mut self) {
        let tel = self.telemetry.lock();
        for class in ["hello", "graph-bfs", "gzip", "pyaes", "other"] {
            let Some(samples) = tel.timing_samples.get(class) else {
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

impl InvokerCpuQueuePolicy for PostedPriceCoordinator {
    fn queue_len(&self) -> usize {
        self.queue.lock().total_len
    }

    fn est_queue_time(&self) -> f64 {
        let q = self.queue.lock();
        if q.total_len == 0 {
            return 0.0;
        }
        let total_ms: f64 = q
            .by_fqdn
            .iter()
            .map(|(fqdn, deque)| self.fn_stats(fqdn).s_i_ms * (deque.len() as f64))
            .sum();
        total_ms / 1000.0
    }

    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let q = self.queue.lock();
        let fqdn = self.select_dispatchable_fqdn(&q)?;
        q.by_fqdn.get(&fqdn)?.front().map(|e| e.item.clone())
    }

    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        self.pop_dispatchable()
            .expect("pop_queue called when peek_queue returned Some")
    }

    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
        let now = Instant::now();
        {
            let mut q = self.queue.lock();
            if q.total_len >= self.max_queue_len {
                anyhow::bail!(
                    "PostedPrice queue full ({}/{})",
                    q.total_len,
                    self.max_queue_len
                );
            }
            let seq = q.alloc_seq();
            q.by_fqdn
                .entry(item.registration.fqdn.clone())
                .or_default()
                .push_back(QueuedEntry {
                    seq,
                    enqueued_at: now,
                    item: item.clone(),
                });
            q.total_len += 1;
        }
        self.telemetry
            .lock()
            .invocation_starts
            .entry(item.tid.clone())
            .or_insert(now);
        Ok(())
    }

    fn preview_fqdn_counts(&self, limit: usize) -> HashMap<String, usize> {
        let q = self.queue.lock();
        let mut counts = HashMap::new();
        if limit == 0 || q.total_len == 0 {
            return counts;
        }
        let now = Instant::now();
        let mut fqdns: Vec<(&String, OrderedFloat<f64>, Instant, u64, usize)> = q
            .by_fqdn
            .iter()
            .filter_map(|(fqdn, deque)| {
                let front = deque.front()?;
                let wait_s = now.saturating_duration_since(front.enqueued_at).as_secs_f64();
                let w_eff = self.effective_queue_priority(fqdn, wait_s);
                Some((fqdn, OrderedFloat(w_eff), front.enqueued_at, front.seq, deque.len()))
            })
            .collect();
        fqdns.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.3.cmp(&b.3))
        });
        let mut remaining = limit;
        for (fqdn, _, _, _, len) in fqdns {
            if remaining == 0 {
                break;
            }
            let take = len.min(remaining);
            counts.insert(fqdn.clone(), take);
            remaining -= take;
        }
        counts
    }
}
