use crate::clock::now;
use crate::linear_reg::LinearReg;
use crate::transaction::TransactionId;
use crate::types::{Compute, ResourceTimings};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::time::Instant;
use tracing::debug;
// A better Chars map system
//
// What we 'want' it to do:
//     Store these per-fqdn
//     Store only f64 values
//     Have a unique entry for all values in <T>, where T is some enum
//     Support a unique entry for MIN, MAX, AVG, LATEST of each <T>
//         Do we want to require external separate enum values for each compute, CpuExecTime, GpuExecTime, etc.?
//         Or have the cmap internally track CPU, GPU, etc. distinction?
//     Minimal locking on add / update and retrieval
//     An optional read-only wrapper to forward cmap to a simulation
//     Easy cloning of all of an FQDN's values
//     easy cloning / serializing to send to controller for LB purposes
//     Allow updating multiple enum entries in one call
//     Allow retrieving multiple enum entries in one call
//
// Not implemented yet:
//     cloning
//     exporting data
//     multiple retrieval

/// Trait ensuring that there is a maximal value that is also constant.
/// Used to index into the char map fqdn array.
pub trait Max {
    const MAX: usize;
    /// The number of enums
    const SIZE: usize = Self::MAX + 1;
}

pub trait CharMap<T: num_traits::AsPrimitive<usize> + Max> {
    // default implementations simplify RO version
    fn update(&self, _fqdn: &str, _key: T, _value: f64) {}
    fn update_2(&self, _fqdn: &str, _k1: T, _v1: f64, _k2: T, _v2: f64) {}
    fn update_3(&self, _fqdn: &str, _k1: T, _v1: f64, _k2: T, _v2: f64, _k3: T, _v3: f64) {}
    fn update_4(&self, _fqdn: &str, _k1: T, _v1: f64, _k2: T, _v2: f64, _k3: T, _v3: f64, _k4: T, _v4: f64) {}
    fn update_5(
        &self,
        _fqdn: &str,
        _k1: T,
        _v1: f64,
        _k2: T,
        _v2: f64,
        _k3: T,
        _v3: f64,
        _k4: T,
        _v4: f64,
        _k5: T,
        _v5: f64,
    ) {
    }

    fn get(&self, fqdn: &str, key: T, value: Value) -> f64;
    fn get_2(&self, fqdn: &str, k1: T, v1: Value, k2: T, v2: Value) -> (f64, f64);
    fn get_3(&self, fqdn: &str, k1: T, v1: Value, k2: T, v2: Value, k3: T, v3: Value) -> (f64, f64, f64);
    fn get_min(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Min)
    }
    fn get_max(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Max)
    }
    fn get_avg(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Avg)
    }
    fn get_latest(&self, fqdn: &str, key: T) -> f64 {
        self.get(fqdn, key, Value::Latest)
    }

    /// A read-only pointer to this char map.
    /// Others can still write to it.
    fn read_only(&self, slf: Arc<dyn CharMap<T>>) -> Arc<dyn CharMap<T>> {
        Arc::new(CharMapRO { inner: slf })
    }

    fn insert_gpu_load_est(&self, _fqdn: &str, _x: f64, _y: f64);
    fn predict_gpu_load_est(&self, _x: f64) -> f64;
    /// Returns [-1.0] if insufficient data exists for interpolation.
    fn func_predict_gpu_load_est(&self, _fqdn: &str, _x: f64) -> f64;
}

pub struct IatTracker {
    last_invoked: dashmap::DashMap<String, Instant>,
}
impl IatTracker {
    pub fn new() -> Self {
        Self {
            last_invoked: dashmap::DashMap::new(),
        }
    }

    pub fn track(&self, fqdn: &str) -> Option<f64> {
        match self.last_invoked.get_mut(fqdn) {
            None => {
                self.last_invoked.insert(fqdn.to_owned(), now());
                None
            },
            Some(mut l) => {
                let r = l.value().elapsed().as_secs_f64();
                *l.value_mut() = now();
                Some(r)
            },
        }
    }
}

#[derive(Clone, Copy)]
#[cfg_attr(test, derive(PartialEq, Debug, enum_iterator::Sequence))]
#[repr(usize)]
pub enum Chars {
    /// Running avg of _all_ times on CPU for invocations.
    /// Recorded by invoke_on_container_2
    CpuExecTime = 0,
    /// Time on CPU for a warm invocation.
    /// Recorded by invoke_on_container_2
    CpuWarmTime,
    /// Time on CPU for a pre-warmed invocation.
    /// The container was previously started, but no invocation was run on it
    /// Recorded by invoke_on_container_2
    CpuPreWarmTime,
    /// E2E time for a CPU cold start.
    /// Recorded by invoke_on_container_2
    CpuColdTime,
    /// Running avg of _all_ times on GPU for invocations.
    /// Recorded by invoke_on_container_2
    GpuExecTime,
    /// Time on GPU for a warm invocation.
    /// Recorded by invoke_on_container_2
    GpuWarmTime,
    /// Time on GPU for a pre-warmed invocation.
    /// The container was previously started, but no invocation was run on it
    /// Recorded by invoke_on_container_2
    GpuPreWarmTime,
    /// E2E time for a GPU cold start.
    /// Recorded by invoke_on_container_2
    GpuColdTime,
    /// The running avg IAT.
    /// Recorded by iluvatar_worker_library::worker_api::iluvatar_worker::IluvatarWorkerImpl::invoke and iluvatar_worker_library::worker_api::iluvatar_worker::IluvatarWorkerImpl::invoke_async
    IAT,
    /// The running avg memory usage, in MB
    /// Recorded in [ContainerManager::calc_container_pool_memory_usages]
    MemoryUsage,
    /// The running avg memory usage of GPU allocations, in MB
    /// Recorded by invoke_on_container_2
    GpuMemoryUsage,
    /// Total end to end latency: queuing plus execution
    E2ECpu,
    E2EGpu,

    /// Time estimate for the E2E GPU and CPU time
    EstGpu,
    EstCpu,

    /// Also store the error?
    QueueErrGpu,
    QueueErrCpu,
}
impl Chars {
    /// Get the Cold, Warm, and Execution time [Chars] specific to the given compute device.
    /// (Cold, Warm, PreWarm, Exec, E2E, QueueEstErr)
    pub fn get_chars(compute: &Compute) -> anyhow::Result<(Chars, Chars, Chars, Chars, Chars, Chars)> {
        if compute == &Compute::CPU {
            Ok((
                Chars::CpuColdTime,
                Chars::CpuWarmTime,
                Chars::CpuPreWarmTime,
                Chars::CpuExecTime,
                Chars::E2ECpu,
                Chars::QueueErrCpu,
            ))
        } else if compute == &Compute::GPU {
            Ok((
                Chars::GpuColdTime,
                Chars::GpuWarmTime,
                Chars::GpuPreWarmTime,
                Chars::GpuExecTime,
                Chars::E2EGpu,
                Chars::QueueErrGpu,
            ))
        } else {
            anyhow::bail!("Unknown compute to get Chars for registration: {:?}", compute)
        }
    }
}
impl Max for Chars {
    const MAX: usize = Self::QueueErrCpu as usize;
}
impl num_traits::AsPrimitive<usize> for Chars {
    #[inline(always)]
    fn as_(self) -> usize {
        self as usize
    }
}

#[derive(Clone, Copy)]
#[repr(usize)]
pub enum Value {
    Min = 0,
    Max,
    Avg,
    Latest,
}
impl Max for Value {
    const MAX: usize = Self::Latest as usize;
}

struct CharMapRO<T: num_traits::AsPrimitive<usize>> {
    inner: Arc<dyn CharMap<T>>,
}
impl<T: Max + num_traits::AsPrimitive<usize>> CharMap<T> for CharMapRO<T> {
    fn get(&self, fqdn: &str, key: T, value: Value) -> f64 {
        self.inner.get(fqdn, key, value)
    }
    fn get_2(&self, fqdn: &str, k1: T, v1: Value, k2: T, v2: Value) -> (f64, f64) {
        self.inner.get_2(fqdn, k1, v1, k2, v2)
    }
    fn get_3(&self, fqdn: &str, k1: T, v1: Value, k2: T, v2: Value, k3: T, v3: Value) -> (f64, f64, f64) {
        self.inner.get_3(fqdn, k1, v1, k2, v2, k3, v3)
    }
    fn insert_gpu_load_est(&self, _fqdn: &str, _x: f64, _y: f64) {
        // do nothing
    }

    fn predict_gpu_load_est(&self, x: f64) -> f64 {
        self.inner.predict_gpu_load_est(x)
    }

    fn func_predict_gpu_load_est(&self, fqdn: &str, x: f64) -> f64 {
        self.inner.func_predict_gpu_load_est(fqdn, x)
    }
}

#[inline(always)]
fn set_data(data: &mut Box<[f64]>, first_pos: usize, value: f64) {
    data[first_pos + Value::Min as usize] = value;
    data[first_pos + Value::Max as usize] = value;
    data[first_pos + Value::Avg as usize] = value;
    data[first_pos + Value::Latest as usize] = value;
}

#[inline(always)]
fn update_data(data: &mut Box<[f64]>, first_pos: usize, value: f64) {
    if data[first_pos].is_nan() {
        set_data(data, first_pos, value);
        return;
    }
    let min_pos = first_pos + Value::Min as usize;
    data[min_pos] = f64::min(data[min_pos], value);
    let max_pos = first_pos + Value::Max as usize;
    data[max_pos] = f64::max(data[max_pos], value);
    let avg_pos = first_pos + Value::Avg as usize;
    data[avg_pos] = data[avg_pos] * 0.9 + value * 0.1;
    data[first_pos + Value::Latest as usize] = value;
}

pub struct CharMapRW<const T: usize> {
    data: DashMap<String, Box<[f64]>>,
    gpu_load_lin_reg: RwLock<LinearReg>,
    func_gpu_load_lin_reg: DashMap<String, LinearReg>,
}
impl<T: Max + num_traits::AsPrimitive<usize>, const S: usize> CharMap<T> for CharMapRW<{ S }> {
    fn update(&self, fqdn: &str, key: T, value: f64) {
        let first_pos = key.as_() * (Value::SIZE);
        match self.data.get_mut(fqdn) {
            None => {
                let mut data = vec![f64::NAN; S * (Value::SIZE)].into_boxed_slice();
                set_data(&mut data, first_pos, value);
                self.data.insert(fqdn.to_string(), data);
            },
            Some(mut d) => {
                let arr = d.value_mut();
                update_data(arr, first_pos, value);
            },
        };
    }
    fn update_2(&self, fqdn: &str, k1: T, v1: f64, k2: T, v2: f64) {
        match self.data.get_mut(fqdn) {
            None => {
                let mut data = vec![f64::NAN; S * (Value::SIZE)].into_boxed_slice();
                set_data(&mut data, k1.as_() * (Value::SIZE), v1);
                set_data(&mut data, k2.as_() * (Value::SIZE), v2);
                self.data.insert(fqdn.to_string(), data);
            },
            Some(mut d) => {
                let data = d.value_mut();
                update_data(data, k1.as_() * (Value::SIZE), v1);
                update_data(data, k2.as_() * (Value::SIZE), v2);
            },
        };
    }
    fn update_3(&self, fqdn: &str, k1: T, v1: f64, k2: T, v2: f64, k3: T, v3: f64) {
        match self.data.get_mut(fqdn) {
            None => {
                let mut data = vec![f64::NAN; S * (Value::SIZE)].into_boxed_slice();
                set_data(&mut data, k1.as_() * (Value::SIZE), v1);
                set_data(&mut data, k2.as_() * (Value::SIZE), v2);
                set_data(&mut data, k3.as_() * (Value::SIZE), v3);
                self.data.insert(fqdn.to_string(), data);
            },
            Some(mut d) => {
                let data = d.value_mut();
                update_data(data, k1.as_() * (Value::SIZE), v1);
                update_data(data, k2.as_() * (Value::SIZE), v2);
                update_data(data, k3.as_() * (Value::SIZE), v3);
            },
        };
    }
    fn update_4(&self, fqdn: &str, k1: T, v1: f64, k2: T, v2: f64, k3: T, v3: f64, k4: T, v4: f64) {
        match self.data.get_mut(fqdn) {
            None => {
                let mut data = vec![f64::NAN; S * (Value::SIZE)].into_boxed_slice();
                set_data(&mut data, k1.as_() * (Value::SIZE), v1);
                set_data(&mut data, k2.as_() * (Value::SIZE), v2);
                set_data(&mut data, k3.as_() * (Value::SIZE), v3);
                set_data(&mut data, k4.as_() * (Value::SIZE), v4);
                self.data.insert(fqdn.to_string(), data);
            },
            Some(mut d) => {
                let data = d.value_mut();
                update_data(data, k1.as_() * (Value::SIZE), v1);
                update_data(data, k2.as_() * (Value::SIZE), v2);
                update_data(data, k3.as_() * (Value::SIZE), v3);
                update_data(data, k4.as_() * (Value::SIZE), v4);
            },
        };
    }
    fn update_5(&self, fqdn: &str, k1: T, v1: f64, k2: T, v2: f64, k3: T, v3: f64, k4: T, v4: f64, k5: T, v5: f64) {
        match self.data.get_mut(fqdn) {
            None => {
                let mut data = vec![f64::NAN; S * (Value::SIZE)].into_boxed_slice();
                set_data(&mut data, k1.as_() * (Value::SIZE), v1);
                set_data(&mut data, k2.as_() * (Value::SIZE), v2);
                set_data(&mut data, k3.as_() * (Value::SIZE), v3);
                set_data(&mut data, k4.as_() * (Value::SIZE), v4);
                set_data(&mut data, k5.as_() * (Value::SIZE), v5);
                self.data.insert(fqdn.to_string(), data);
            },
            Some(mut d) => {
                let data = d.value_mut();
                update_data(data, k1.as_() * (Value::SIZE), v1);
                update_data(data, k2.as_() * (Value::SIZE), v2);
                update_data(data, k3.as_() * (Value::SIZE), v3);
                update_data(data, k4.as_() * (Value::SIZE), v4);
                update_data(data, k5.as_() * (Value::SIZE), v5);
            },
        };
    }

    fn get(&self, fqdn: &str, key: T, value: Value) -> f64 {
        match self.data.get(fqdn) {
            None => 0.0,
            Some(d) => {
                let r = d.value()[(key.as_() * Value::SIZE) + value as usize];
                if r.is_nan() {
                    return 0.0;
                }
                r
            },
        }
    }

    fn get_2(&self, fqdn: &str, k1: T, v1: Value, k2: T, v2: Value) -> (f64, f64) {
        match self.data.get(fqdn) {
            None => (0.0, 0.0),
            Some(d) => {
                let r1 = d.value()[(k1.as_() * Value::SIZE) + v1 as usize];
                let r2 = d.value()[(k2.as_() * Value::SIZE) + v2 as usize];
                if r1.is_nan() {
                    return (0.0, 0.0);
                }
                (r1, r2)
            },
        }
    }
    fn get_3(&self, fqdn: &str, k1: T, v1: Value, k2: T, v2: Value, k3: T, v3: Value) -> (f64, f64, f64) {
        match self.data.get(fqdn) {
            None => (0.0, 0.0, 0.0),
            Some(d) => {
                let r1 = d.value()[(k1.as_() * Value::SIZE) + v1 as usize];
                if r1.is_nan() {
                    return (0.0, 0.0, 0.0);
                }
                let r2 = d.value()[(k2.as_() * Value::SIZE) + v2 as usize];
                let r3 = d.value()[(k3.as_() * Value::SIZE) + v3 as usize];
                (r1, r2, r3)
            },
        }
    }
    fn insert_gpu_load_est(&self, fqdn: &str, x: f64, y: f64) {
        self.gpu_load_lin_reg.write().insert(x, y);
        match self.func_gpu_load_lin_reg.get_mut(fqdn) {
            None => {
                let mut lr = LinearReg::new();
                lr.insert(x, y);
                self.func_gpu_load_lin_reg.insert(fqdn.to_string(), lr);
            },
            Some(mut lr) => lr.value_mut().insert(x, y),
        };
    }

    fn predict_gpu_load_est(&self, x: f64) -> f64 {
        self.gpu_load_lin_reg.read().predict(x)
    }
    /// Returns [-1.0] if insufficient data exists for interpolation.
    fn func_predict_gpu_load_est(&self, fqdn: &str, x: f64) -> f64 {
        match self.func_gpu_load_lin_reg.get(fqdn) {
            None => -1.0,
            Some(lr) => lr.value().predict(x),
        }
    }
}

impl<const S: usize> CharMapRW<S> {
    pub fn boxed<T: Max + num_traits::AsPrimitive<usize>>() -> Arc<dyn CharMap<T> + Send + Sync + 'static> {
        Arc::new(Self {
            data: DashMap::new(),
            gpu_load_lin_reg: RwLock::new(LinearReg::new()),
            func_gpu_load_lin_reg: DashMap::new(),
        })
    }
}

pub type WorkerCharMap = Arc<dyn CharMap<Chars> + Send + Sync + 'static>;
pub fn worker_char_map() -> WorkerCharMap {
    CharMapRW::<{ Chars::SIZE }>::boxed()
}

pub fn add_registration_timings(
    cmap: &WorkerCharMap,
    compute: Compute,
    resource_timings_json: &Option<ResourceTimings>,
    fqdn: &str,
    tid: &TransactionId,
) -> anyhow::Result<()> {
    if let Some(r) = resource_timings_json {
        for dev_compute in compute.into_iter() {
            if let Some(timings) = r.get(&dev_compute) {
                debug!(tid=tid, compute=%dev_compute, from_compute=%compute, fqdn=fqdn, timings=?r, "Registering timings for function");
                let (cold, warm, prewarm, exec, e2e, _) = Chars::get_chars(&dev_compute)?;
                for v in timings.cold_results_sec.iter() {
                    cmap.update(fqdn, exec, *v);
                }
                for v in timings.warm_results_sec.iter() {
                    cmap.update(fqdn, exec, *v);
                }
                for v in timings.cold_worker_duration_us.iter() {
                    cmap.update_2(fqdn, cold, *v as f64 / 1_000_000.0, e2e, *v as f64 / 1_000_000.0);
                }
                for v in timings.warm_worker_duration_us.iter() {
                    cmap.update_3(
                        fqdn,
                        warm,
                        *v as f64 / 1_000_000.0,
                        prewarm,
                        *v as f64 / 1_000_000.0,
                        e2e,
                        *v as f64 / 1_000_000.0,
                    );
                }
            }
        }
    };
    Ok(())
}

#[cfg(test)]
mod char_map_tests {
    use super::*;
    use enum_iterator::*;
    use rand::seq::IteratorRandom;

    #[test]
    fn compile_test() {
        let _cmap: WorkerCharMap = CharMapRW::<{ Chars::SIZE }>::boxed();
    }

    #[test]
    fn missing_get_returns_zero() {
        let cmap = worker_char_map();
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 0.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 0.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 0.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 0.0);
    }

    #[test]
    fn first_sets_all() {
        let cmap = worker_char_map();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 1.0);
    }

    #[test]
    fn get_helpers_work() {
        let cmap = worker_char_map();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        cmap.update("f1", Chars::CpuExecTime, 2.0);
        cmap.update("f1", Chars::CpuExecTime, 5.0);
        cmap.update("f1", Chars::CpuExecTime, 4.0);
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Min),
            cmap.get_min("f1", Chars::CpuExecTime)
        );
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Max),
            cmap.get_max("f1", Chars::CpuExecTime)
        );
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Avg),
            cmap.get_avg("f1", Chars::CpuExecTime)
        );
        assert_eq!(
            cmap.get("f1", Chars::CpuExecTime, Value::Latest),
            cmap.get_latest("f1", Chars::CpuExecTime)
        );
    }

    #[test]
    fn second_updates() {
        let cmap = worker_char_map();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        cmap.update("f1", Chars::CpuExecTime, 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0 * 0.9 + 2.0 * 0.1);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 2.0);
    }

    #[test]
    fn multi_set_works() {
        let cmap = worker_char_map();
        let chosen_chars = all::<Chars>().choose_multiple(&mut rand::rng(), 3);
        let err_msg = format!("Tested chars: {:?}", chosen_chars);

        cmap.update_2("f1", chosen_chars[0], 1.0, chosen_chars[1], 1.0);
        cmap.update_3("f1", chosen_chars[0], 2.0, chosen_chars[1], 2.0, chosen_chars[2], 2.0);
        assert_eq!(cmap.get("f1", chosen_chars[0], Value::Min), 1.0, "{}", err_msg);
        assert_eq!(cmap.get("f1", chosen_chars[0], Value::Max), 2.0, "{}", err_msg);
        assert_eq!(
            cmap.get("f1", chosen_chars[0], Value::Avg),
            1.0 * 0.9 + 2.0 * 0.1,
            "{}",
            err_msg
        );
        assert_eq!(cmap.get("f1", chosen_chars[0], Value::Latest), 2.0, "{}", err_msg);

        assert_eq!(cmap.get("f1", chosen_chars[1], Value::Min), 1.0, "{}", err_msg);
        assert_eq!(cmap.get("f1", chosen_chars[1], Value::Max), 2.0, "{}", err_msg);
        assert_eq!(
            cmap.get("f1", chosen_chars[1], Value::Avg),
            1.0 * 0.9 + 2.0 * 0.1,
            "{}",
            err_msg
        );
        assert_eq!(cmap.get("f1", chosen_chars[1], Value::Latest), 2.0, "{}", err_msg);

        assert_eq!(cmap.get("f1", chosen_chars[2], Value::Min), 2.0, "{}", err_msg);
        assert_eq!(cmap.get("f1", chosen_chars[2], Value::Max), 2.0, "{}", err_msg);
        assert_eq!(cmap.get("f1", chosen_chars[2], Value::Avg), 2.0, "{}", err_msg);
        assert_eq!(cmap.get("f1", chosen_chars[2], Value::Latest), 2.0, "{}", err_msg);
    }

    #[test]
    fn read_only_cannot_update() {
        let cmap = worker_char_map();
        cmap.update("f1", Chars::CpuExecTime, 1.0);
        cmap.update("f1", Chars::CpuExecTime, 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0 * 0.9 + 2.0 * 0.1);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 2.0);

        let ro_cmap = cmap.read_only(cmap.clone());
        ro_cmap.update("f1", Chars::CpuExecTime, 3.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 2.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.0 * 0.9 + 2.0 * 0.1);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 2.0);

        cmap.update("f1", Chars::CpuExecTime, 3.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Min), 1.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Max), 3.0);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Avg), 1.29);
        assert_eq!(cmap.get("f1", Chars::CpuExecTime, Value::Latest), 3.0);
    }

    #[test]
    fn write_to_all_chars() {
        let cmap = worker_char_map();
        for (i, char) in all::<Chars>().enumerate() {
            let val = 1.0 * (1 + i) as f64;
            cmap.update("f1", char, val);
            assert_eq!(cmap.get("f1", char, Value::Min), val);
            assert_eq!(cmap.get("f1", char, Value::Max), val);
            assert_eq!(cmap.get("f1", char, Value::Avg), val);
            assert_eq!(cmap.get("f1", char, Value::Latest), val);
        }

        for (i, char) in all::<Chars>().enumerate() {
            let val = 2.0 * (1 + i) as f64;
            cmap.update("f1", char, val);
            assert_eq!(cmap.get("f1", char, Value::Min), (1 + i) as f64);
            assert_eq!(cmap.get("f1", char, Value::Max), val);
            assert_eq!(cmap.get("f1", char, Value::Avg), val * 0.1 + (0.9 * (1 + i) as f64));
            assert_eq!(cmap.get("f1", char, Value::Latest), val);
        }
    }

    #[test]
    fn max_char_correct() {
        assert_eq!(last::<Chars>().unwrap() as usize, Chars::MAX);
    }

    #[test]
    fn size_char_correct() {
        assert_eq!(cardinality::<Chars>(), Chars::SIZE);
    }
}
