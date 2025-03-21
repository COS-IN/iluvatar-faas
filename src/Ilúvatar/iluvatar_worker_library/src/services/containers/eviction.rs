use crate::services::containers::container_pool::Subpool;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::Container;
use iluvatar_library::transaction::TransactionId;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, error};

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

/// Return two lists: ordered list of containers for potential future eviction, and another for immediate eviction
pub fn order_pool_eviction(
    _ctr_mrg: &ContainerManager,
    policy: &EvictionPolicy,
    tid: &TransactionId,
    list: Subpool,
) -> (Subpool, Subpool) {
    debug!(tid = tid, "Computing eviction priorities");
    match policy {
        EvictionPolicy::LRU => lru_eviction(list),
        EvictionPolicy::TTL { timout_sec } => ttl_eviction(list, Duration::from_secs(*timout_sec)),
        EvictionPolicy::GreedyDual => {
            error!(tid = tid, "GreedyDual eviction algorithm not implemented yet");
            (list, vec![])
        },
    }
}

fn lru_eviction(list: Subpool) -> (Subpool, Subpool) {
    let mut insts: Vec<(tokio::time::Instant, Container)> = list.into_iter().map(|c| (c.last_used(), c)).collect();
    insts.sort_unstable_by(|c1, c2| c1.0.cmp(&c2.0));
    (insts.into_iter().map(|c| c.1).collect(), vec![])
}

fn ttl_eviction(list: Subpool, timeout: Duration) -> (Subpool, Subpool) {
    let mut sort = vec![];
    let mut evict = vec![];
    for ctr in list.into_iter() {
        let last_used = ctr.last_used();
        if last_used.elapsed() >= timeout {
            evict.push(ctr);
        } else {
            sort.push((last_used, ctr));
        }
    }
    sort.sort_unstable_by(|c1, c2| c1.0.cmp(&c2.0));
    (sort.into_iter().map(|c| c.1).collect(), evict)
}
