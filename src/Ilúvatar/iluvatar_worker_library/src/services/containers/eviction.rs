use crate::services::containers::container_pool::Subpool;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::Container;
use iluvatar_library::transaction::TransactionId;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::debug;

// NOTE: Rust will panic if the comparator doesn't implement total ordering.
// As the values used to sort containers _may_ change during sorting here, they must be pre-captured.
// Failure to do so will result in a panic and brick the system.

#[derive(Debug, Serialize, Deserialize)]
pub enum EvictionPolicy {
    /// Least recently used ordering for on-demand eviction
    LRU,
    /// Time-to-live, performs immediate eviction
    TTL { timout_sec: u64 },
    /// From 2020 FaaS paper
    GreedyDual,
}
impl Default for EvictionPolicy {
    fn default() -> Self {
        Self::LRU
    }
}

/// Main entry point for evicting using different policies. 
/// Return two lists: ordered list of containers for potential future eviction, and another for immediate eviction
pub fn order_pool_eviction(
    _ctr_mrg: &ContainerManager,
    policy: &EvictionPolicy,
    tid: &TransactionId,
    list: Subpool,
) -> (Subpool, Subpool) {
    debug!(tid = tid, eviction_policy = ?policy, "Computing eviction priorities");
    match policy {
        EvictionPolicy::LRU => lru_eviction(list),
        EvictionPolicy::TTL { timout_sec } => ttl_eviction(list, Duration::from_secs(*timout_sec)),
        EvictionPolicy::GreedyDual => greedy_dual_eviction(_ctr_mrg, list),
    }
}

fn lru_eviction(list: Subpool) -> (Subpool, Subpool) {
    let mut insts: Vec<(tokio::time::Instant, Container)> = list.into_iter().map(|c| (c.last_used(), c)).collect();
    insts.sort_unstable_by(|c1, c2| c1.0.cmp(&c2.0));
    for (last_used, c) in insts.iter() {
        debug!(container_id=%c.container_id(), fqdn=%c.fqdn(), last_used=?last_used, "Eviction: LRU eviction candidate");
    }
    (insts.into_iter().map(|c| c.1).collect(), vec![])
}

fn ttl_eviction(list: Subpool, timeout: Duration) -> (Subpool, Subpool) {
    let mut sort = vec![];
    let mut evict = vec![];
    for ctr in list.into_iter() {
        let last_used = ctr.last_used();
        if last_used.elapsed() >= timeout {
            debug!(container_id=%ctr.container_id(), fqdn=%ctr.fqdn(), elapsed=?last_used.elapsed(), timeout=?timeout, "Eviction: TTL eviction candidate");
            evict.push(ctr);
        } else {
            sort.push((last_used, ctr));
        }
    }
    sort.sort_unstable_by(|c1, c2| c1.0.cmp(&c2.0));
    (sort.into_iter().map(|c| c.1).collect(), evict)
}


fn greedy_dual_eviction(mgr: &ContainerManager, list: Subpool) -> (Subpool, Subpool) {
    let mut insts: Vec<(f64, Container)> = list
        .into_iter()
        .map(|c| {
            let priority = mgr
                .greedy_dual_priorities
                .get(c.container_id())
                .map(|r| *r)
                .unwrap_or_else(|| {
                    // Fallback calculation if priority is missing
		    // Shouldnt this be just the clock if the prior is missing? 
                    let clock = *mgr.greedy_dual_clock.read();
                    clock 
                });
            (priority, c)
        })
        .collect();

    // Sort ascending by priority (lowest priority first)
    insts.sort_unstable_by(|c1, c2| c1.0.partial_cmp(&c2.0).unwrap_or(std::cmp::Ordering::Equal));

    for (priority, c) in insts.iter() {
        debug!(container_id=%c.container_id(), fqdn=%c.fqdn(), priority=priority, "Eviction: GreedyDual candidate priority");
    }

    (insts.into_iter().map(|c| c.1).collect(), vec![])
}
