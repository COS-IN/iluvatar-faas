use crate::services::invocation::dispatching::{queueing_dispatcher::DispatchPolicy, QueueMap, NO_ESTIMATE};
use crate::services::registration::RegisteredFunction;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::Compute;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Serialize, Deserialize)]
pub struct TinyLfuConfig {
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
}

fn default_cache_size() -> usize { 25 }
fn default_sample_size() -> usize { 500 }

impl Default for TinyLfuConfig {
    fn default() -> Self {
        Self {
            cache_size: 25,
            sample_size: 500,
        }
    }
}

pub struct TinyLfu {
    cmap: WorkerCharMap,
    config: Arc<TinyLfuConfig>,
    que_map: QueueMap,
    state: RwLock<TinyLfuState>,
}

struct TinyLfuState {
    sketch: HashMap<String, usize>,
    w: usize,
    // Using a VecDeque as a simple LRU queue (front = LRU victim, back = MRU)
    cache: VecDeque<String>,
}

impl TinyLfu {
    pub fn new(
        cmap: WorkerCharMap,
        config: &Option<Arc<TinyLfuConfig>>,
        que_map: QueueMap,
    ) -> Self {
        let config = config.as_ref().cloned().unwrap_or_else(|| Arc::new(TinyLfuConfig::default()));
        Self {
            cmap,
            config,
            que_map,
            state: RwLock::new(TinyLfuState {
                sketch: HashMap::new(),
                w: 0,
                cache: VecDeque::new(),
            }),
        }
    }
}

/// https://medium.com/@gati.sahu/tinylfu-smarter-cache-admission-for-modern-systems-409328980dd3

impl DispatchPolicy for TinyLfu {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let fqdn = &reg.fqdn;
        let mut state = self.state.write();

        // 1. Read existing frequencies BEFORE updating to avoid 1st-access bias
        let freq_candidate = *state.sketch.get(fqdn).unwrap_or(&0);

        // 2. Check Cache Hit
        if let Some(pos) = state.cache.iter().position(|x| x == fqdn) {
            // Promote to MRU (back of queue)
            let item = state.cache.remove(pos).unwrap();
            state.cache.push_back(item);

            // Record access & update aging
            record_access(&mut state, fqdn, self.config.sample_size);

            info!(tid = %tid, fqdn = %fqdn, "TinyLFU Dispatch (Hit) -> GPU");
            return get_estimates(Compute::GPU, reg, tid, &self.que_map, &self.cmap);
        }

        // 3. Admission Decision for Cache Miss
        let admit = if state.cache.len() < self.config.cache_size {
            true
        } else {
            // Compare candidate prior frequency against LRU victim (front of queue)
            let victim = state.cache.front().expect("cache cannot be empty when len >= cache_size");
            let freq_victim = *state.sketch.get(victim).unwrap_or(&0);

            if freq_candidate > freq_victim {
                let evicted = state.cache.pop_front().unwrap();
                info!(tid = %tid, admitted = %fqdn, evicted = %evicted, "TinyLFU: Evicted victim");
                true
            } else {
                info!(tid = %tid, rejected = %fqdn, victim = %victim, "TinyLFU: Admission rejected");
                false
            }
        };

        // 4. Update frequency regardless of hit or miss
        record_access(&mut state, fqdn, self.config.sample_size);

        if admit {
            state.cache.push_back(fqdn.clone());
            info!(tid = %tid, fqdn = %fqdn, "TinyLFU Dispatch (Admitted) -> GPU");
            get_estimates(Compute::GPU, reg, tid, &self.que_map, &self.cmap)
        } else {
            info!(tid = %tid, fqdn = %fqdn, "TinyLFU Dispatch (Miss) -> CPU");
            get_estimates(Compute::CPU, reg, tid, &self.que_map, &self.cmap)
        }
    }
}

fn record_access(state: &mut TinyLfuState, fqdn: &str, sample_size: usize) {
    *state.sketch.entry(fqdn.to_string()).or_insert(0) += 1;
    state.w += 1;

    if state.w >= sample_size {
        for val in state.sketch.values_mut() {
            *val /= 2;
        }
        // Retain only elements that still hold value OR are currently resident in cache
        let cached_set: std::collections::HashSet<&String> = state.cache.iter().collect();
        state.sketch.retain(|k, v| *v > 0 || cached_set.contains(k));
        state.w /= 2;
    }
}

fn get_estimates(
    compute: Compute,
    reg: &Arc<RegisteredFunction>,
    tid: &TransactionId,
    que_map: &QueueMap,
    cmap: &WorkerCharMap,
) -> (Compute, f64, f64) {
    let (est, load) = match que_map.get(&compute) {
        Some(q) => q.est_completion_time(reg, tid),
        None => {
            let ch = if compute == Compute::CPU {
                Chars::CpuExecTime
            } else {
                Chars::GpuExecTime
            };
            (cmap.get_avg(&reg.fqdn, ch), 0.0)
        }
    };
    (compute, load, est)
}
