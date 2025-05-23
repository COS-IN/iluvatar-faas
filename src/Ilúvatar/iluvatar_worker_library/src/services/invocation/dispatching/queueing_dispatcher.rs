use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::dispatching::greedy_weight::GreedyWeights;
use crate::services::invocation::dispatching::{
    landlord::get_landlord, popular::get_popular, EnqueueingPolicy, QueueMap, NO_ESTIMATE,
};
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::invocation::queueing::gpu_mqfq::MQFQ;
use crate::services::invocation::queueing::{DeviceQueue, EnqueuedInvocation};
use crate::services::invocation::{
    async_tracker::AsyncHelper, cpu_q_invoke::CpuQueueingInvoker, gpu_q_invoke::GpuQueueingInvoker,
    InvocationResultPtr, Invoker, InvokerLoad,
};
use crate::services::registration::{RegisteredFunction, RegistrationService};
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::worker_api::config::StatusConfig;
use crate::worker_api::worker_config::{GPUResourceConfig, InvocationConfig};
use anyhow::Result;
use iluvatar_library::char_map::{Chars, Value, WorkerCharMap};
use iluvatar_library::clock::{get_global_clock, Clock};
use iluvatar_library::ring_buff::RingBuffer;
use iluvatar_library::threading::tokio_logging_thread;
use iluvatar_library::{bail_error, transaction::TransactionId, types::Compute};
use ordered_float::OrderedFloat;
use parking_lot::{Mutex, RwLock};
use rand::seq::IndexedRandom;
use rand::Rng;
use std::cmp::Ordering;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tracing::{debug, info, warn};

lazy_static::lazy_static! {
  pub static ref INVOKER_CPU_QUEUE_WORKER_TID: TransactionId = "InvokerCPUQueue".to_string();
  pub static ref INVOKER_GPU_QUEUE_WORKER_TID: TransactionId = "InvokerGPUQueue".to_string();
}
pub const DISPATCHER_INVOKER_LOG_TID: &str = "DISPATCHER_INVOKER_LOG";

pub trait DispatchPolicy: Send + Sync {
    /// Returns the selected device to enqueue the function's invocation, the load on that device, and est completion time on it.
    /// If est time is 0, estimate not provided.
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64);
}

#[allow(unused)]
pub struct PolymDispatchCtx {
    cmap: WorkerCharMap,
    /// cpu/gpu -> wt , based on device load.
    device_wts: HashMap<Compute, f64>,
    /// fn -> cpu_wt, gpu_wt , based on locality and speedup considerations.
    per_fn_wts: HashMap<String, (f64, f64)>,
    /// Most recent fn->device placement for each fn
    prev_dispatch: HashMap<String, Compute>,
    /// Previous dispatch of the function to CPU
    cpu_prev_t: HashMap<String, OffsetDateTime>,
    /// Previous dispatch of the function to GPU
    gpu_prev_t: HashMap<String, OffsetDateTime>,
    ///Total number of dispatches/rounds
    total_dispatch: u64,
    /// Total Number to CPU so far
    n_cpu: u64,
    /// Total number to GPU
    n_gpu: u64, //Init to 1 to avoid divide by 0
}

impl PolymDispatchCtx {
    pub fn boxed(cmap: &WorkerCharMap) -> Self {
        Self {
            cmap: cmap.clone(),
            device_wts: HashMap::from([(Compute::CPU, 1.0), (Compute::GPU, 1.0)]),
            per_fn_wts: HashMap::new(),
            prev_dispatch: HashMap::new(),
            cpu_prev_t: HashMap::new(),
            gpu_prev_t: HashMap::new(),
            total_dispatch: 1,
            n_cpu: 1,
            n_gpu: 1,
        }
    }

    pub fn select_device_for_fn(&mut self, fid: &str, compute: &Compute, now: OffsetDateTime) {
        self.total_dispatch += 1;
        self.prev_dispatch.insert(fid.to_string(), *compute);

        if compute == &Compute::CPU {
            self.n_cpu += 1;
            self.cpu_prev_t.insert(fid.to_string(), now);
        } else if compute == &Compute::GPU {
            self.n_gpu += 1;
            self.gpu_prev_t.insert(fid.to_string(), now);
        } else {
            tracing::error!("Unknown compute in dispatch {:?} {}", compute, fid);
        }
    }
}

#[allow(unused)]
pub struct QueueingDispatcher {
    async_functions: AsyncHelper,
    invocation_config: Arc<InvocationConfig>,
    cmap: WorkerCharMap,
    clock: Clock,
    policy: Arc<dyn DispatchPolicy>,
    que_map: QueueMap,
    dispatch_state: RwLock<PolymDispatchCtx>,
    gpu_config: Option<Arc<GPUResourceConfig>>,
}

#[allow(dyn_drop)]
/// An invoker implementation that enqueues invocations.
/// This struct creates separate queues for supported hardware devices, and sends invocations into those queues based on configuration.
impl QueueingDispatcher {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        invocation_config: Arc<InvocationConfig>,
        tid: &TransactionId,
        cmap: WorkerCharMap,
        cpu: Arc<CpuResourceTracker>,
        gpu: Option<Arc<GpuResourceTracker>>,
        gpu_config: &Option<Arc<GPUResourceConfig>>,
        reg: &Arc<RegistrationService>,
        rin_buff: &Arc<RingBuffer>,
        config: &Arc<StatusConfig>,
        #[cfg(feature = "power_cap")] energy: Arc<EnergyLimiter>,
    ) -> Result<Arc<Self>> {
        let cpu_q = Self::get_invoker_queue(
            &invocation_config,
            &cmap,
            &cont_manager,
            tid,
            &cpu,
            #[cfg(feature = "power_cap")]
            &energy,
        )?;
        let mut que_map = HashMap::from_iter([(Compute::CPU, cpu_q)]);
        if let Some(gpu_q) =
            Self::get_invoker_gpu_queue(&invocation_config, &cmap, &cont_manager, tid, &cpu, &gpu, gpu_config)?
        {
            que_map.insert(Compute::GPU, gpu_q);
        }
        let (_handle, rx) = tokio_logging_thread(
            config.report_freq_ms,
            DISPATCHER_INVOKER_LOG_TID.to_string(),
            rin_buff.clone(),
            Self::log_queue_info,
        )?;

        let policy = Self::get_dispatch_algo(&invocation_config, cmap.clone(), que_map.clone(), &gpu, reg, tid)?;
        let svc = Arc::new(QueueingDispatcher {
            que_map,
            policy,
            async_functions: AsyncHelper::new(),
            clock: get_global_clock(tid)?,
            invocation_config,
            dispatch_state: RwLock::new(PolymDispatchCtx::boxed(&cmap)),
            cmap,
            gpu_config: gpu_config.clone(),
        });
        rx.send(svc.clone())?;
        debug!(tid = tid, "Created QueueingInvoker");
        Ok(svc)
    }

    fn get_invoker_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &WorkerCharMap,
        cont_manager: &Arc<ContainerManager>,
        tid: &TransactionId,
        cpu: &Arc<CpuResourceTracker>,
        #[cfg(feature = "power_cap")] energy: &Arc<EnergyLimiter>,
    ) -> Result<Arc<dyn DeviceQueue>> {
        Ok(CpuQueueingInvoker::new(
            cont_manager.clone(),
            invocation_config.clone(),
            tid,
            cmap.clone(),
            cpu.clone(),
            #[cfg(feature = "power_cap")]
            energy.clone(),
        )?)
    }

    fn get_invoker_gpu_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &WorkerCharMap,
        cont_manager: &Arc<ContainerManager>,
        tid: &TransactionId,
        cpu: &Arc<CpuResourceTracker>,
        gpu: &Option<Arc<GpuResourceTracker>>,
        gpu_config: &Option<Arc<GPUResourceConfig>>,
    ) -> Result<Option<Arc<dyn DeviceQueue>>> {
        if gpu.is_none() || gpu_config.is_none() {
            info!(
                tid = tid,
                "GPU resource tracker or GPU config is missing, not creating gpu queue"
            );
            return Ok(None);
        }
        match invocation_config.queues.get(&Compute::GPU) {
            Some(q) => {
                if q == "serial" {
                    Ok(Some(GpuQueueingInvoker::new(
                        cont_manager.clone(),
                        invocation_config.clone(),
                        tid,
                        cmap.clone(),
                        cpu.clone(),
                        gpu.clone(),
                        gpu_config,
                    )?))
                } else if q == "mqfq" {
                    Ok(Some(MQFQ::new(
                        cont_manager.clone(),
                        cmap.clone(),
                        invocation_config.clone(),
                        cpu.clone(),
                        gpu,
                        gpu_config,
                        tid,
                    )?))
                } else {
                    anyhow::bail!("Unkonwn GPU queue {}", q);
                }
            },
            None => anyhow::bail!("GPU queue was not specified"),
        }
    }

    async fn log_queue_info(self: &Arc<Self>, tid: &TransactionId) -> Result<InvokerLoad> {
        let queue_lengths = self.queue_len();
        info!(tid=tid, num_running_funcs=self.running_funcs(), queue_info=%queue_lengths, "current queue info");
        Ok(queue_lengths)
    }

    fn make_enqueue(
        &self,
        reg: &Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
        insert_t: OffsetDateTime,
        est_comp_time: f64,
        insert_time_load: f64,
    ) -> Arc<EnqueuedInvocation> {
        Arc::new(EnqueuedInvocation::new(
            reg.clone(),
            json_args,
            tid.clone(),
            insert_t,
            est_comp_time,
            insert_time_load,
        ))
    }
    fn enqueue_compute(
        &self,
        reg: &Arc<RegisteredFunction>,
        json_args: String,
        tid: TransactionId,
        compute: Compute,
        insert_t: OffsetDateTime,
        est_comp_time: f64,
        insert_time_load: f64,
    ) -> Result<Arc<EnqueuedInvocation>> {
        let mut enqueues = 0;
        let mut args = Some(json_args);
        let mut enq = None;
        for c in compute.into_iter() {
            match self.que_map.get(&c) {
                None => warn!(tid=tid, compute=%c, "Tried to run invoke on compute with no queue"),
                Some(q) => {
                    let mut comp_time = est_comp_time;
                    let mut load = insert_time_load;
                    if comp_time == NO_ESTIMATE || load == NO_ESTIMATE {
                        let (est_time, est_load) = q.est_completion_time(reg, &tid);
                        if comp_time == NO_ESTIMATE {
                            comp_time = est_time;
                        }
                        if load == NO_ESTIMATE {
                            load = est_load;
                        }
                    }
                    if enq.is_none() {
                        if let Some(arg) = args.take() {
                            enq = Some(self.make_enqueue(reg, arg, &tid, insert_t, comp_time, load));
                        }
                    }
                    if let Some(e) = &enq {
                        q.enqueue_item(e)?;
                        enqueues += 1;
                    }
                },
            }
        }
        if enqueues == 0 {
            bail_error!(tid = tid, "Unable to enqueue function invocation, not matching compute");
        }
        enq.ok_or_else(|| anyhow::anyhow!("Enqueued item was never created"))
    }

    fn get_dispatch_algo(
        invocation_config: &Arc<InvocationConfig>,
        cmap: WorkerCharMap,
        que_map: QueueMap,
        gpu: &Option<Arc<GpuResourceTracker>>,
        reg: &Arc<RegistrationService>,
        tid: &TransactionId,
    ) -> Result<Arc<dyn DispatchPolicy>> {
        let policy = invocation_config
            .enqueueing_policy
            .as_ref()
            .unwrap_or(&EnqueueingPolicy::All);
        match policy {
            EnqueueingPolicy::All => Ok(Arc::new(All {})),
            EnqueueingPolicy::Random => Ok(Arc::new(Random {})),
            EnqueueingPolicy::AlwaysCPU => Ok(Arc::new(AlwaysCPU {})),
            EnqueueingPolicy::AlwaysGPU => Ok(Arc::new(AlwaysGPU {})),
            EnqueueingPolicy::ShortestExecTime => Ok(Arc::new(ShortestExecTime::new(cmap))),
            EnqueueingPolicy::EstCompTime => Ok(Arc::new(EstCompTime::new(que_map))),
            EnqueueingPolicy::UCB1 => Ok(Arc::new(Ucb1::new(&cmap, tid)?)),
            EnqueueingPolicy::MWUA => Ok(Arc::new(Mwua::new(&cmap, tid)?)),
            EnqueueingPolicy::HitTput => Ok(Arc::new(HitTput::new(que_map, &cmap, tid)?)),
            EnqueueingPolicy::EstSpeedup => Ok(Arc::new(EstSpeedup::new(invocation_config.clone(), cmap, que_map))),
            EnqueueingPolicy::RunningAvgEstSpeedup => {
                Ok(Arc::new(RunningAvgEstSpeedup::new(invocation_config, cmap, que_map)))
            },
            EnqueueingPolicy::QueueAdjustAvgEstSpeedup => Ok(Arc::new(QueueAdjustAvgEstSpeedup::new(
                invocation_config.clone(),
                cmap,
                que_map,
            ))),
            EnqueueingPolicy::Speedup => Ok(Arc::new(Speedup::new(invocation_config.clone(), cmap))),
            EnqueueingPolicy::Landlord
            | EnqueueingPolicy::LRU
            | EnqueueingPolicy::LFU
            | EnqueueingPolicy::LandlordFixed => get_landlord(*policy, &cmap, invocation_config, que_map),
            EnqueueingPolicy::Popular
            | EnqueueingPolicy::PopularEstTimeDispatch
            | EnqueueingPolicy::PopularQueueLenDispatch
            | EnqueueingPolicy::LeastPopular
            | EnqueueingPolicy::TopAvg => get_popular(*policy, &cmap, que_map),
            EnqueueingPolicy::GreedyWeights => {
                GreedyWeights::boxed(&cmap, que_map, &invocation_config.greedy_weight_config, reg, gpu)
            },
            EnqueueingPolicy::MICE => Ok(Arc::new(MICE::new(
                invocation_config.clone(),
                cmap.clone(),
                que_map,
                tid,
            )?)),
        }
    }

    /// Forms invocation data into a [EnqueuedInvocation] that is returned.
    /// The default implementation also calls [Invoker::add_item_to_queue] to optionally insert that item into the implementation's queue.
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args), fields(tid=tid)))]
    fn enqueue_new_invocation(
        &self,
        reg: &Arc<RegisteredFunction>,
        json_args: String,
        tid: TransactionId,
    ) -> Result<Arc<EnqueuedInvocation>> {
        debug!(tid = tid, "Enqueueing invocation");
        let insert_t = self.clock.now();
        if self.invocation_config.log_details() {
            debug!(tid = tid, "calc CPU est time");
            let ((cpu_est, cpu_load), cpu_tput) = match self.que_map.get(&Compute::CPU) {
                None => ((NO_ESTIMATE, NO_ESTIMATE), NO_ESTIMATE),
                Some(q) => (q.est_completion_time(reg, &tid), q.queue_tput()),
            };
            debug!(tid = tid, "calc GPU est time");
            let ((gpu_est, gpu_load), gpu_tput) = match self.que_map.get(&Compute::GPU) {
                None => ((NO_ESTIMATE, NO_ESTIMATE), NO_ESTIMATE),
                Some(q) => (q.est_completion_time(reg, &tid), q.queue_tput()),
            };
            info!(
                tid = tid,
                cpu_est = cpu_est,
                cpu_load = cpu_load,
                cpu_tput = cpu_tput,
                gpu_est = gpu_est,
                gpu_load = gpu_load,
                gpu_tput = gpu_tput,
                "Est e2e time"
            );
        }

        match reg.supported_compute {
            Compute::CPU => self.enqueue_compute(reg, json_args, tid, Compute::CPU, insert_t, NO_ESTIMATE, NO_ESTIMATE),
            Compute::GPU => self.enqueue_compute(reg, json_args, tid, Compute::GPU, insert_t, NO_ESTIMATE, NO_ESTIMATE),
            _ => {
                let (chosen_compute, load, est_time) = self.policy.choose(reg, &tid);
                if self.invocation_config.log_details() {
                    match chosen_compute {
                        Compute::GPU => info!(tid=tid, fqdn=%reg.fqdn, "Cache Hit"),
                        _ => info!(tid=tid, fqdn=%reg.fqdn, pot_creds=load, "Cache Miss"),
                    }
                }
                self.enqueue_compute(reg, json_args, tid, chosen_compute, insert_t, est_time, load)
            },
        }
    }
}

/// Given two weights, return 0 or 1 probabilistically
fn proportional_selection(wa: f64, wb: f64) -> Ordering {
    let wt = wa + wb;
    let wa = wa / wt;
    // let wb = wb / wt;
    let r = rand::rng().random_range(0.0..1.0);
    if r > wa {
        return Ordering::Less;
    }
    Ordering::Greater
}
struct HitTput {
    que_map: QueueMap,
    dispatch_state: RwLock<PolymDispatchCtx>,
    clock: Clock,
}
impl HitTput {
    pub fn new(que_map: QueueMap, cmap: &WorkerCharMap, tid: &TransactionId) -> Result<Self> {
        Ok(Self {
            que_map,
            dispatch_state: RwLock::new(PolymDispatchCtx::boxed(cmap)),
            clock: get_global_clock(tid)?,
        })
    }
}
impl DispatchPolicy for HitTput {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        // the cost is t_recent - t_global_min
        if let Some(gpu_queue) = self.que_map.get(&Compute::GPU) {
            if let Some(cpu_queue) = self.que_map.get(&Compute::CPU) {
                let (egpu, lgpu) = gpu_queue.est_completion_time(reg, tid);
                let (ecpu, lcpu) = cpu_queue.est_completion_time(reg, tid);

                let tnow = self.clock.now();

                let b = self.dispatch_state.read();
                let iat_gpu = match b.gpu_prev_t.get(&reg.fqdn) {
                    Some(tg) => (tnow - *tg).as_seconds_f64(),
                    _ => 10000.0, //infinity essentially
                };

                // let last_cpu = b.cpu_prev_t.get(fqdn);
                let iat_cpu = match b.cpu_prev_t.get(&reg.fqdn) {
                    Some(tg) => (tnow - *tg).as_seconds_f64(),
                    _ => 10000.0, //infinity essentially
                };

                let pgpu = gpu_queue.warm_hit_probability(reg, iat_gpu);
                let pcpu = cpu_queue.warm_hit_probability(reg, iat_cpu);

                let rgpu = pgpu / egpu;
                let rcpu = pcpu / ecpu;

                // choose the maximum of the two? or probabilistically?
                let n = proportional_selection(rcpu, rgpu);
                return match n {
                    Ordering::Less => (Compute::CPU, lcpu, ecpu),
                    _ => (Compute::GPU, lgpu, egpu),
                };
            }
        }
        (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
    }
}

/// Multiplicative Weights Update Algorithm
/// Shrinking dartboard : Geulen, Sascha, Berthold Vöcking, and Melanie Winkler.
/// "Regret Minimization for Online Buffering Problems Using the Weighted Majority Algorithm." COLT. 2010.
struct Mwua {
    dispatch_state: RwLock<PolymDispatchCtx>,
    clock: Clock,
    cmap: WorkerCharMap,
}
impl Mwua {
    pub fn new(cmap: &WorkerCharMap, tid: &TransactionId) -> Result<Self> {
        Ok(Self {
            cmap: cmap.clone(),
            dispatch_state: RwLock::new(PolymDispatchCtx::boxed(cmap)),
            clock: get_global_clock(tid)?,
        })
    }
}
impl DispatchPolicy for Mwua {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        // the cost is t_recent - t_global_min
        let eta = 0.3; // learning rate
        let fid = reg.fqdn.as_str();

        let lck = self.dispatch_state.read();
        let prev_wts = lck.per_fn_wts.get(fid).cloned().unwrap_or((1.0, 1.0));
        let prev_dispatch = lck.prev_dispatch.get(fid).cloned().unwrap_or(Compute::CPU);
        drop(lck);

        // Apply the reward/cost
        let t_other = if prev_dispatch == Compute::GPU {
            self.cmap.get_latest(fid, Chars::E2EGpu)
        } else {
            self.cmap.get_latest(fid, Chars::E2ECpu)
        };

        // global minimum best ever recorded
        let tmin = f64::min(
            self.cmap.get_min(fid, Chars::E2EGpu),
            self.cmap.get_min(fid, Chars::E2ECpu),
        );

        let cost = 1.0 - (eta * (t_other - tmin) / f64::min(t_other, 0.1));
        // Shrinking dartboard locality
        // With probability equal to cost, select the previous device, for improving locality
        let r = rand::rng().random_range(0.0..1.0);
        let use_prev = r < cost;

        // update weight?
        let new_wt = if prev_dispatch == Compute::GPU {
            prev_wts.1 * cost
        } else {
            prev_wts.0 * cost
        };

        // update the weight tuple
        let new_wts = if prev_dispatch == Compute::GPU {
            (prev_wts.0, new_wt)
        } else {
            (new_wt, prev_wts.1)
        };

        let selected_device = if use_prev {
            prev_dispatch
        } else {
            // 0 or 1
            let selected = proportional_selection(new_wts.0, new_wts.1);
            match selected {
                Ordering::Less => Compute::GPU,
                _ => Compute::CPU,
            }
        };

        let mut lck = self.dispatch_state.write();
        lck.select_device_for_fn(fid, &selected_device, self.clock.now());
        // update the weights
        lck.per_fn_wts.insert(fid.to_string(), new_wts);

        (selected_device, NO_ESTIMATE, NO_ESTIMATE)
    }
}

/// Upper-confidence bound on the execution latency. Or the E2E time?
/// https://jeremykun.com/2013/10/28/optimism-in-the-face-of-uncertainty-the-ucb1-algorithm/
struct Ucb1 {
    dispatch_state: RwLock<PolymDispatchCtx>,
    clock: Clock,
    cmap: WorkerCharMap,
}
impl Ucb1 {
    pub fn new(cmap: &WorkerCharMap, tid: &TransactionId) -> Result<Self> {
        Ok(Self {
            cmap: cmap.clone(),
            dispatch_state: RwLock::new(PolymDispatchCtx::boxed(cmap)),
            clock: get_global_clock(tid)?,
        })
    }
}
impl DispatchPolicy for Ucb1 {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        // device_wt = exec_time + sqrt(log steps/n), where n is number of times device has been selected for the function
        // Pick device with lowest weight and dispatch
        let (cpu_t, gpu_t) = self
            .cmap
            .get_2(&reg.fqdn, Chars::E2ECpu, Value::Avg, Chars::E2EGpu, Value::Avg);

        let lck = self.dispatch_state.read();
        let total_dispatch = lck.total_dispatch as f64;
        let n_cpu = lck.n_cpu as f64;
        let n_gpu = lck.n_gpu as f64;
        drop(lck);

        let cpu_ucb = (f64::log(total_dispatch, 2.0) / n_cpu).sqrt();
        let gpu_ucb = (f64::log(total_dispatch, 2.0) / n_gpu).sqrt();

        let cpu_wt = cpu_t + cpu_ucb;
        let gpu_wt = gpu_t + gpu_ucb;

        let device_wts = HashMap::from([(Compute::CPU, cpu_wt), (Compute::GPU, gpu_wt)]);

        let min_val_pair = device_wts.iter().min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap());

        // pick smallest of the two
        let selected_device = min_val_pair.unwrap().0;

        let mut lck = self.dispatch_state.write();
        lck.select_device_for_fn(&reg.fqdn, selected_device, self.clock.now());
        (*selected_device, NO_ESTIMATE, NO_ESTIMATE)
    }
}

struct All;
impl DispatchPolicy for All {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        (reg.supported_compute, NO_ESTIMATE, NO_ESTIMATE)
    }
}
struct Random;
impl DispatchPolicy for Random {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        // unwrap safe, has to have some compute entry to get this far
        let v: Vec<Compute> = reg.supported_compute.iter().collect();
        (*v.choose(&mut rand::rng()).unwrap(), NO_ESTIMATE, NO_ESTIMATE)
    }
}

struct AlwaysCPU;
impl DispatchPolicy for AlwaysCPU {
    fn choose(&self, _reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
    }
}

struct AlwaysGPU;
impl DispatchPolicy for AlwaysGPU {
    fn choose(&self, _reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        (Compute::GPU, NO_ESTIMATE, NO_ESTIMATE)
    }
}

struct EstCompTime {
    que_map: QueueMap,
}
impl EstCompTime {
    pub fn new(que_map: QueueMap) -> Self {
        Self { que_map }
    }
}
impl DispatchPolicy for EstCompTime {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let mut opts = vec![];
        for c in reg.supported_compute.into_iter() {
            if let Some(q) = self.que_map.get(&c) {
                opts.push((q.est_completion_time(reg, tid), c));
            }
        }
        if let Some(((est, load), c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
            return (*c, *load, *est);
        }
        (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
    }
}
struct ShortestExecTime {
    cmap: WorkerCharMap,
}
impl ShortestExecTime {
    pub fn new(cmap: WorkerCharMap) -> Self {
        Self { cmap }
    }
}
impl DispatchPolicy for ShortestExecTime {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        let mut opts = vec![];
        let (cpu, gpu) = self.cmap.get_2(
            &reg.fqdn,
            Chars::CpuExecTime,
            Value::Avg,
            Chars::GpuExecTime,
            Value::Avg,
        );
        if reg.supported_compute.contains(Compute::CPU) {
            opts.push((cpu, Compute::CPU));
        }
        if reg.supported_compute.contains(Compute::GPU) {
            opts.push((gpu, Compute::GPU));
        }
        if let Some((est, c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0)) {
            return (*c, NO_ESTIMATE, *est);
        }
        (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
    }
}

struct Speedup {
    cmap: WorkerCharMap,
    invocation_config: Arc<InvocationConfig>,
}
impl Speedup {
    pub fn new(invocation_config: Arc<InvocationConfig>, cmap: WorkerCharMap) -> Self {
        Self {
            cmap,
            invocation_config,
        }
    }
}
impl DispatchPolicy for Speedup {
    fn choose(&self, reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        let (cpu, gpu) = self.cmap.get_2(
            &reg.fqdn,
            Chars::CpuExecTime,
            Value::Avg,
            Chars::GpuExecTime,
            Value::Avg,
        );
        let ratio = cpu / gpu;
        if ratio > self.invocation_config.speedup_ratio.unwrap_or(4.0) {
            (Compute::GPU, NO_ESTIMATE, NO_ESTIMATE)
        } else {
            (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
        }
    }
}

struct EstSpeedup {
    que_map: QueueMap,
    cmap: WorkerCharMap,
    invocation_config: Arc<InvocationConfig>,
}
impl EstSpeedup {
    pub fn new(invocation_config: Arc<InvocationConfig>, cmap: WorkerCharMap, que_map: QueueMap) -> Self {
        Self {
            que_map,
            cmap,
            invocation_config,
        }
    }
}
impl DispatchPolicy for EstSpeedup {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let (cpu, gpu) = self.cmap.get_2(
            &reg.fqdn,
            Chars::CpuExecTime,
            Value::Avg,
            Chars::GpuExecTime,
            Value::Avg,
        );
        let ratio = cpu / gpu;
        if ratio > self.invocation_config.speedup_ratio.unwrap_or(4.0) {
            let mut opts = vec![];
            for c in reg.supported_compute.into_iter() {
                if let Some(q) = self.que_map.get(&c) {
                    opts.push((q.est_completion_time(reg, tid), c));
                }
            }
            match opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                Some(((est, load), c)) => (*c, *load, *est),
                None => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            }
        } else {
            (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
        }
    }
}

#[allow(unused)]
struct MICEState {
    cpu_thresh: f64,             // run on cpu if size below this
    gpu_thresh: f64,             // run on gpu if size below this
    control_interval: u32,       // M, number of invocations before we readjust thresholds. Countdown. Defaults 10
    cpu_dispatched: f64,         // Total service dispatched on CPU in this interval, so far
    gpu_dispatched: f64,         // Total service on GPU in this interval
    time_marker: OffsetDateTime, //When the current epoch started
    rho_cap_cpu: f64,
}

/// This is the machine-learned "ICE" policy from the paper:
/// On Sequential Dispatching Policies.  Esa Hyytiä et.al.
/// This paper has some more fundamental details: Minimizing Slowdown in Heterogeneous Size-Aware Dispatching Systems.
/// The basic idea is to have the lower backlogged server (in this case usually the CPU) to get first-pick.
/// Each server has thresholds for the job size. Thus if the CPU is less backlogged, then it gets to pick the invocation if it is less than tau_cpu in size. Else the GPU gets it.
/// The backlog is the estimated e2e latency.
/// Every M invocations, there will be some control loop to increment/decrement the (CPU) threshold based on the dispatched load in this M window, based on the observed/average service time of the invocation.
/// The other input needed is some load threshold for the devices (again mainly the CPU)
#[allow(unused)]
#[allow(non_camel_case_types)]
struct MICE {
    que_map: QueueMap,
    cmap: WorkerCharMap,
    invocation_config: Arc<InvocationConfig>,
    clock: Clock,
    //hmm, move this all part of mice-state under a single mutex?
    mstate: Mutex<MICEState>,
}
impl MICE {
    pub fn new(
        invocation_config: Arc<InvocationConfig>,
        cmap: WorkerCharMap,
        que_map: QueueMap,
        tid: &TransactionId,
    ) -> Result<Self> {
        let clock = get_global_clock(tid)?;
        Ok(Self {
            que_map,
            cmap,
            invocation_config,
            mstate: Mutex::new(MICEState {
                cpu_thresh: 10.0, // Should pass all these arguments
                gpu_thresh: 9999.0,
                control_interval: 10,
                cpu_dispatched: 0.0,
                gpu_dispatched: 0.0,
                time_marker: clock.now(),
                rho_cap_cpu: 100.0,
            }),
            clock,
        })
    }
}
impl DispatchPolicy for MICE {
    fn choose(&self, _reg: &Arc<RegisteredFunction>, _tid: &TransactionId) -> (Compute, f64, f64) {
        // let (x_cpu, x_gpu) = self.cmap.get_2(
        //     &reg.fqdn,
        //     Chars::CpuExecTime,
        //     Value::Avg,
        //     Chars::GpuExecTime,
        //     Value::Avg,
        // );

        //let delta_t = self.clock.now().as_seconds_f64() - self.time_marker ;
        //let rho_cpu = *self.cpu_dispatched.lock()/delta_t;
        //let rho_gpu = *self.gpu_dispatched.lock()/delta_t;

        let epsilon = 0.01; // This should be smaller than the function service times. 10 milliseconds seems ok.
        let mut ms = self.mstate.lock();
        ms.control_interval -= 1;
        let delta_t = (self.clock.now() - ms.time_marker).as_seconds_f64();

        match ms.control_interval {
            0 => {
                // Compute the new thresholds
                let rho_cpu = ms.cpu_dispatched / delta_t;
                if rho_cpu < ms.rho_cap_cpu {
                    ms.cpu_thresh += epsilon;
                } else {
                    ms.cpu_thresh -= epsilon;
                }
                // This is asymmetric and assumes GPU will need infinite threshold, so no update needed for it

                // reset the counters and the time?
                ms.control_interval = 10;
                ms.cpu_dispatched = 0.0;
                ms.gpu_dispatched = 0.0;
            },
            _ => {},
        };

        (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)

        // let mut opts = vec![];
        // for c in reg.supported_compute.into_iter() {
        //     if let Some(q) = self.que_map.get(&c) {
        //         opts.push((q.est_completion_time(reg, tid), c));
        //     }
        // }
        // match opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
        //     Some(((est, load), c)) => {
        // 	match c {
        // 	    &Compute::CPU => {
        // 		// CPU has lower backlog. Check if size of invok is under the threshold
        // 		if x_cpu < self.cpu_thresh {
        // 		let mut cd = self.cpu_dispatched.lock();
        // 		*cd += x_cpu ;
        // 		return (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE);
        // 	    }
        // 		else { // function is bigger than the current CPU threshold! GPU!

        // 	    }

        // 	    }
        // 	}
        //     }

        // }
    }
}

struct RunningAvgEstSpeedup {
    que_map: QueueMap,
    cmap: WorkerCharMap,
    running_avg_speedup: Mutex<f64>,
}
impl RunningAvgEstSpeedup {
    pub fn new(invocation_config: &Arc<InvocationConfig>, cmap: WorkerCharMap, que_map: QueueMap) -> Self {
        Self {
            running_avg_speedup: Mutex::new(invocation_config.speedup_ratio.unwrap_or(4.0)),
            que_map,
            cmap,
        }
    }
}
impl DispatchPolicy for RunningAvgEstSpeedup {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let (cpu, gpu) = self.cmap.get_2(
            &reg.fqdn,
            Chars::CpuExecTime,
            Value::Avg,
            Chars::GpuExecTime,
            Value::Avg,
        );
        let ratio = cpu / gpu;
        let mut avg = self.running_avg_speedup.lock();
        let new_avg = *avg * 0.9 + ratio * 0.1;
        *avg = new_avg;
        drop(avg);
        info!(tid = tid, new_avg = new_avg, "running avg");
        // let avg_scale = self.gpu_config.as_ref().map_or(1, |c| c.count) as f64;
        // if ratio > (new_avg / avg_scale) {
        if ratio > new_avg {
            let mut opts = vec![];
            for c in reg.supported_compute.into_iter() {
                if let Some(q) = self.que_map.get(&c) {
                    opts.push((q.est_completion_time(reg, tid), c));
                }
            }
            match opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                Some(((est, load), c)) => (*c, *load, *est),
                None => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            }
        } else {
            (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
        }
    }
}

struct QueueAdjustAvgEstSpeedup {
    que_map: QueueMap,
    cmap: WorkerCharMap,
    invocation_config: Arc<InvocationConfig>,
    running_avg_speedup: Mutex<f64>,
}
impl QueueAdjustAvgEstSpeedup {
    pub fn new(invocation_config: Arc<InvocationConfig>, cmap: WorkerCharMap, que_map: QueueMap) -> Self {
        Self {
            running_avg_speedup: Mutex::new(invocation_config.speedup_ratio.unwrap_or(4.0)),
            que_map,
            cmap,
            invocation_config,
        }
    }
}
impl DispatchPolicy for QueueAdjustAvgEstSpeedup {
    fn choose(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (Compute, f64, f64) {
        let (cpu, gpu) = self.cmap.get_2(
            &reg.fqdn,
            Chars::CpuExecTime,
            Value::Avg,
            Chars::GpuExecTime,
            Value::Avg,
        );
        let ratio = cpu / gpu;
        if ratio > *self.running_avg_speedup.lock() {
            let mut opts = vec![];
            for c in reg.supported_compute.into_iter() {
                if let Some(q) = self.que_map.get(&c) {
                    opts.push((q.est_completion_time(reg, tid), c));
                }
            }
            match opts.iter().min_by_key(|i| OrderedFloat(i.0 .0)) {
                Some(((est, load), c)) => {
                    if c == &Compute::GPU {
                        if let Some(gpu_queue) = self.que_map.get(c) {
                            let q_len = gpu_queue.queue_len();
                            let mut avg = self.running_avg_speedup.lock();
                            if q_len <= 1 {
                                *avg *= 0.99;
                            } else if q_len >= 3 {
                                // *avg = *avg * 1.05;
                                *avg = self.invocation_config.speedup_ratio.unwrap_or(4.0);
                            } else {
                                // *avg = *avg * 0.975;
                            }
                            debug!(tid = tid, new_avg = *avg, "running avg");
                        }
                    }
                    (*c, *load, *est)
                },
                None => (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE),
            }
        } else {
            (Compute::CPU, NO_ESTIMATE, NO_ESTIMATE)
        }
    }
}

#[tonic::async_trait]
impl Invoker for QueueingDispatcher {
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, reg, json_args), fields(tid=tid)))]
    async fn sync_invocation(
        &self,
        reg: Arc<RegisteredFunction>,
        json_args: String,
        tid: TransactionId,
    ) -> Result<InvocationResultPtr> {
        let queued = self.enqueue_new_invocation(&reg, json_args, tid.clone())?;
        queued.wait(&tid).await?;
        let result_ptr = queued.result_ptr.lock();
        match result_ptr.completed {
            true => {
                let e2etime = (self.clock.now() - queued.queue_insert_time).as_seconds_f64();
                if result_ptr.compute == Compute::GPU {
                    self.cmap.update(&reg.fqdn, Chars::E2EGpu, e2etime);
                } else {
                    self.cmap.update(&reg.fqdn, Chars::E2ECpu, e2etime);
                }
                info!(tid=tid, fqdn=%reg.fqdn, e2etime=%e2etime, compute=%result_ptr.compute, "Invocation complete");
                Ok(queued.result_ptr.clone())
            },
            false => {
                bail_error!(
                    tid = tid,
                    "Invocation was signaled completion but completion value was not set"
                )
            },
        }
    }
    fn async_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<String> {
        let invoke = self.enqueue_new_invocation(&reg, json_args, tid)?;
        self.async_functions.insert_async_invoke(invoke)
    }
    fn invoke_async_check(&self, cookie: &str, tid: &TransactionId) -> Result<iluvatar_rpc::rpc::InvokeResponse> {
        self.async_functions.invoke_async_check(cookie, tid)
    }

    /// The queue length of both CPU and GPU queues
    fn queue_len(&self) -> InvokerLoad {
        let mut running = 0;
        let mut loads = HashMap::new();
        for (compute, q) in self.que_map.iter() {
            running += q.running();
            loads.insert(*compute, q.queue_load());
        }
        InvokerLoad {
            num_running_funcs: running,
            queues: loads,
        }
    }

    /// The number of functions currently running
    fn running_funcs(&self) -> u32 {
        self.que_map.iter().map(|q| q.1.running()).sum()
    }

    /// Returns the estimated E2E in seconds time for the fqdn.
    fn est_e2e_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        if reg.supported_compute.contains(Compute::GPU) {
            match self.que_map.get(&Compute::GPU) {
                None => 0.0,
                Some(q) => q.est_completion_time(reg, tid).0,
            }
        } else if reg.supported_compute.contains(Compute::CPU) {
            match self.que_map.get(&Compute::CPU) {
                None => 0.0,
                Some(q) => q.est_completion_time(reg, tid).0,
            }
        } else {
            0.0
        }
    }
}
