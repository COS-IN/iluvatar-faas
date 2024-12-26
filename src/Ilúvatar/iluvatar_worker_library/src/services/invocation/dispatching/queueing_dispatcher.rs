use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::dispatching::{landlord::get_landlord, popular::get_popular, EnqueueingPolicy};
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::invocation::queueing::{concur_mqfq::ConcurMqfq, gpu_mqfq::MQFQ};
use crate::services::invocation::queueing::{DeviceQueue, EnqueuedInvocation};
use crate::services::invocation::{
    async_tracker::AsyncHelper, cpu_q_invoke::CpuQueueingInvoker, gpu_q_invoke::GpuQueueingInvoker,
    InvocationResultPtr, Invoker,
};
use crate::services::registration::RegisteredFunction;
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::worker_api::worker_config::{FunctionLimits, GPUResourceConfig, InvocationConfig};
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::characteristics_map::{Characteristics, Values};
use iluvatar_library::clock::{get_global_clock, Clock};
use iluvatar_library::types::ComputeEnum;
use iluvatar_library::{bail_error, transaction::TransactionId, types::Compute};
use ordered_float::OrderedFloat;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use std::{collections::HashMap, sync::Arc};
use time::OffsetDateTime;
use tracing::{debug, info};

lazy_static::lazy_static! {
  pub static ref INVOKER_CPU_QUEUE_WORKER_TID: TransactionId = "InvokerCPUQueue".to_string();
  pub static ref INVOKER_GPU_QUEUE_WORKER_TID: TransactionId = "InvokerGPUQueue".to_string();
}

pub trait DispatchPolicy: Send + Sync {
    fn choose(&mut self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Compute;
}

#[allow(unused)]
pub struct PolymDispatchCtx {
    cmap: Arc<CharacteristicsMap>,
    /// cpu/gpu -> wt , based on device load.
    device_wts: HashMap<Compute, f64>,
    /// fn -> cpu_wt, gpu_wt , based on locality and speedup considerations.
    per_fn_wts: HashMap<String, (f64, f64)>,
    /// Most recent fn->device placement for each fn
    prev_dispatch: HashMap<String, ComputeEnum>,
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
    pub fn boxed(cmap: &Arc<CharacteristicsMap>) -> Self {
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
}

pub struct QueueingDispatcher {
    async_functions: AsyncHelper,
    invocation_config: Arc<InvocationConfig>,
    cmap: Arc<CharacteristicsMap>,
    clock: Clock,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Option<Arc<dyn DeviceQueue>>,
    dispatch_state: RwLock<PolymDispatchCtx>,
    landlord: Mutex<Box<dyn DispatchPolicy>>,
    popular: Mutex<Box<dyn DispatchPolicy>>,
    running_avg_speedup: Mutex<f64>,
}

#[allow(dyn_drop)]
/// An invoker implementation that enqueues invocations.
/// This struct creates separate queues for supported hardware devices, and sends invocations into those queues based on configuration.
impl QueueingDispatcher {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        function_config: Arc<FunctionLimits>,
        invocation_config: Arc<InvocationConfig>,
        tid: &TransactionId,
        cmap: Arc<CharacteristicsMap>,
        cpu: Arc<CpuResourceTracker>,
        gpu: Option<Arc<GpuResourceTracker>>,
        gpu_config: &Option<Arc<GPUResourceConfig>>,
        #[cfg(feature = "power_cap")] energy: Arc<EnergyLimiter>,
    ) -> Result<Arc<Self>> {
        let cpu_q = Self::get_invoker_queue(
            &invocation_config,
            &cmap,
            &cont_manager,
            tid,
            &function_config,
            &cpu,
            #[cfg(feature = "power_cap")]
            &energy,
        )?;
        let gpu_q = Self::get_invoker_gpu_queue(
            &invocation_config,
            &cmap,
            &cont_manager,
            tid,
            &function_config,
            &cpu,
            &gpu,
            gpu_config,
        )?;
        let policy = invocation_config
            .enqueueing_policy
            .as_ref()
            .unwrap_or(&EnqueueingPolicy::All);
        let svc = Arc::new(QueueingDispatcher {
            landlord: Mutex::new(get_landlord(*policy, &cmap, &invocation_config, &cpu_q, &gpu_q)?),
            popular: Mutex::new(get_popular(*policy, &cmap, &invocation_config, &cpu_q, &gpu_q)?),
            cpu_queue: cpu_q,
            gpu_queue: gpu_q,
            async_functions: AsyncHelper::new(),
            clock: get_global_clock(tid)?,
            running_avg_speedup: Mutex::new(invocation_config.speedup_ratio.unwrap_or(4.0)),
            invocation_config,
            dispatch_state: RwLock::new(PolymDispatchCtx::boxed(&cmap)),
            cmap,
        });
        debug!(tid=%tid, "Created QueueingInvoker");
        Ok(svc)
    }

    fn get_invoker_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &Arc<CharacteristicsMap>,
        cont_manager: &Arc<ContainerManager>,
        tid: &TransactionId,
        function_config: &Arc<FunctionLimits>,
        cpu: &Arc<CpuResourceTracker>,
        #[cfg(feature = "power_cap")] energy: &Arc<EnergyLimiter>,
    ) -> Result<Arc<dyn DeviceQueue>> {
        Ok(CpuQueueingInvoker::new(
            cont_manager.clone(),
            function_config.clone(),
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
        cmap: &Arc<CharacteristicsMap>,
        cont_manager: &Arc<ContainerManager>,
        tid: &TransactionId,
        function_config: &Arc<FunctionLimits>,
        cpu: &Arc<CpuResourceTracker>,
        gpu: &Option<Arc<GpuResourceTracker>>,
        gpu_config: &Option<Arc<GPUResourceConfig>>,
    ) -> Result<Option<Arc<dyn DeviceQueue>>> {
        if gpu.is_none() || gpu_config.is_none() {
            info!(tid=%tid, "GPU resource tracker or GPU config is missing, not creating gpu queue");
            return Ok(None);
        }
        match invocation_config.queues.get(&ComputeEnum::gpu) {
            Some(q) => {
                if q == "serial" {
                    Ok(Some(GpuQueueingInvoker::new(
                        cont_manager.clone(),
                        function_config.clone(),
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
                } else if q == "concur_mqfq" {
                    Ok(Some(ConcurMqfq::new(
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

    fn make_enqueue(
        &self,
        reg: &Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
        insert_t: OffsetDateTime,
        est_comp_time: f64,
    ) -> Arc<EnqueuedInvocation> {
        Arc::new(EnqueuedInvocation::new(
            reg.clone(),
            json_args,
            tid.clone(),
            insert_t,
            est_comp_time,
        ))
    }
    fn enqueue_compute(&self, item: &Arc<EnqueuedInvocation>, compute: Compute) -> Result<u32> {
        let mut enqueues = 0;
        if compute.contains(Compute::CPU) {
            enqueues += 1;
            self.cpu_queue.enqueue_item(item)?;
        }
        if compute.contains(Compute::GPU) {
            self.enqueue_gpu_check(item)?;
            enqueues += 1;
        }
        Ok(enqueues)
    }
    fn enqueue_gpu_check(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        match self.gpu_queue.as_ref() {
            Some(q) => q.enqueue_item(item),
            None => anyhow::bail!("No queue present for compute '{}'", Compute::GPU),
        }
    }

    /// Forms invocation data into a [EnqueuedInvocation] that is returned.
    /// The default implementation also calls [Invoker::add_item_to_queue] to optionally insert that item into the implementation's queue.
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args), fields(tid=%tid)))]
    fn enqueue_new_invocation(
        &self,
        reg: &Arc<RegisteredFunction>,
        json_args: String,
        tid: TransactionId,
    ) -> Result<Arc<EnqueuedInvocation>> {
        debug!(tid=%tid, "Enqueueing invocation");
        let mut new_item = None;
        let insert_t = self.clock.now();
        let mut enqueues = 0;
        if self.invocation_config.log_details() {
            debug!(tid=%tid, "calc CPU est time");
            let cpu = self.cpu_queue.est_completion_time(reg, &tid);
            debug!(tid=%tid, "calc GPU est time");
            let gpu = self
                .gpu_queue
                .as_ref()
                .map_or(0.0, |q| q.est_completion_time(reg, &tid));
            info!(tid=%tid, cpu_est=cpu, gpu_est=gpu, "Est e2e time");
        }

        if reg.cpu_only() {
            let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
            self.cpu_queue.enqueue_item(&enq)?;
            return Ok(enq);
        }
        if reg.gpu_only() {
            let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
            self.enqueue_gpu_check(&enq)?;
            return Ok(enq);
        }

        let policy = self
            .invocation_config
            .enqueueing_policy
            .as_ref()
            .unwrap_or(&EnqueueingPolicy::All);
        match policy {
            EnqueueingPolicy::All => {
                let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
                if reg.supported_compute.contains(Compute::GPU) {
                    self.enqueue_gpu_check(&enq)?;
                    enqueues += 1;
                }
                if reg.supported_compute.contains(Compute::CPU) {
                    self.cpu_queue.enqueue_item(&enq)?;
                    enqueues += 1;
                }
                new_item = Some(enq);
            },
            EnqueueingPolicy::AlwaysCPU => {
                let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
                if reg.supported_compute.contains(Compute::CPU) {
                    self.cpu_queue.enqueue_item(&enq)?;
                    enqueues += 1;
                    new_item = Some(enq);
                } else {
                    anyhow::bail!(
                        "Cannot enqueue invocation using {:?} strategy because invocation does not support CPU-only",
                        EnqueueingPolicy::AlwaysCPU
                    );
                }
            },
            EnqueueingPolicy::AlwaysGPU => {
                if reg.supported_compute.contains(Compute::GPU) {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
                    self.enqueue_gpu_check(&enq)?;
                    new_item = Some(enq);
                    enqueues += 1;
                } else {
                    anyhow::bail!(
                        "Cannot enqueue invocation using {:?} strategy because invocation does not support CPU-only",
                        EnqueueingPolicy::AlwaysGPU
                    );
                }
            },
            EnqueueingPolicy::ShortestExecTime => {
                let mut opts = vec![];
                if reg.supported_compute.contains(Compute::CPU) {
                    opts.push((self.cmap.get_exec_time(&reg.fqdn), Compute::CPU));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    opts.push((self.cmap.get_gpu_exec_time(&reg.fqdn), Compute::GPU));
                }
                if let Some((est, c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0)) {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, *est);
                    enqueues += self.enqueue_compute(&enq, *c)?;
                    new_item = Some(enq);
                }
            },
            EnqueueingPolicy::EstCompTime => {
                let mut opts = vec![];
                if reg.supported_compute.contains(Compute::CPU) {
                    opts.push((self.cpu_queue.est_completion_time(reg, &tid), Compute::CPU));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    if let Some(gpu_queue) = &self.gpu_queue {
                        opts.push((gpu_queue.est_completion_time(reg, &tid), Compute::GPU));
                    }
                }
                if let Some((est, c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0)) {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, *est);
                    enqueues += self.enqueue_compute(&enq, *c)?;
                    if self.invocation_config.log_details() {
                        if c == &Compute::GPU {
                            info!(tid=%tid, fqdn=%enq.registration.fqdn, "Cache Hit");
                        } else {
                            info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=0.0, "Cache Miss");
                        }
                    }
                    new_item = Some(enq);
                }
            },
            EnqueueingPolicy::UCB1 => {
                anyhow::bail!("UCB1 not implemented");
                // self.ucb1_dispatch(reg.clone(), &tid.clone(), &enqueue)?;
                // enqueues += 1;
            },
            EnqueueingPolicy::MWUA => {
                anyhow::bail!("MWUA not implemented");
                // self.mwua_dispatch(reg.clone(), &tid.clone(), &enqueue)?;
                // enqueues += 1;
            },
            EnqueueingPolicy::HitTput => {
                anyhow::bail!("HitTput not implemented");
                // self.hit_tput_dispatch(reg.clone(), &tid.clone(), &enqueue)?;
                // enqueues += 1;
            },
            EnqueueingPolicy::EstSpeedup => {
                let cpu = self.cmap.avg_cpu_exec_t(&reg.fqdn);
                let gpu = self.cmap.avg_gpu_exec_t(&reg.fqdn);
                let ratio = cpu / gpu;
                if ratio > self.invocation_config.speedup_ratio.unwrap_or(4.0) {
                    let mut opts = vec![];
                    if reg.supported_compute.contains(Compute::CPU) {
                        opts.push((self.cpu_queue.est_completion_time(reg, &tid), Compute::CPU));
                    }
                    if reg.supported_compute.contains(Compute::GPU) {
                        if let Some(gpu_queue) = &self.gpu_queue {
                            opts.push((gpu_queue.est_completion_time(reg, &tid), Compute::GPU));
                        }
                    }
                    if let Some((est, c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0)) {
                        let enq = self.make_enqueue(reg, json_args, &tid, insert_t, *est);
                        enqueues += self.enqueue_compute(&enq, *c)?;
                        if self.invocation_config.log_details() {
                            if c == &Compute::GPU {
                                info!(tid=%tid, fqdn=%enq.registration.fqdn, "Cache Hit");
                            } else {
                                info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=1.0, "Cache Miss");
                            }
                        }
                        new_item = Some(enq);
                    }
                } else {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, cpu);
                    self.cpu_queue.enqueue_item(&enq)?;
                    if self.invocation_config.log_details() {
                        info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=0.0, "Cache Miss");
                    }
                    new_item = Some(enq);
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::RunningAvgEstSpeedup => {
                let cpu = self.cmap.avg_cpu_exec_t(&reg.fqdn);
                let gpu = self.cmap.avg_gpu_exec_t(&reg.fqdn);
                let ratio = cpu / gpu;
                let mut avg = self.running_avg_speedup.lock();
                let new_avg = *avg * 0.9 + ratio * 0.1;
                *avg = new_avg;
                drop(avg);
                info!(tid=%tid, new_avg=new_avg, "running avg");
                if ratio > new_avg {
                    let mut opts = vec![];
                    if reg.supported_compute.contains(Compute::CPU) {
                        opts.push((self.cpu_queue.est_completion_time(reg, &tid), Compute::CPU));
                    }
                    if reg.supported_compute.contains(Compute::GPU) {
                        if let Some(gpu_queue) = &self.gpu_queue {
                            opts.push((gpu_queue.est_completion_time(reg, &tid), Compute::GPU));
                        }
                    }
                    if let Some((est, c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0)) {
                        let enq = self.make_enqueue(reg, json_args, &tid, insert_t, *est);
                        enqueues += self.enqueue_compute(&enq, *c)?;
                        if self.invocation_config.log_details() {
                            if c == &Compute::GPU {
                                info!(tid=%tid, fqdn=%enq.registration.fqdn, "Cache Hit");
                            } else {
                                info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=1.0, "Cache Miss");
                            }
                        }
                        new_item = Some(enq);
                    }
                } else {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, cpu);
                    self.cpu_queue.enqueue_item(&enq)?;
                    if self.invocation_config.log_details() {
                        info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=0.0, "Cache Miss");
                    }
                    new_item = Some(enq);
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::QueueAdjustAvgEstSpeedup => {
                let cpu = self.cmap.avg_cpu_exec_t(&reg.fqdn);
                let gpu = self.cmap.avg_gpu_exec_t(&reg.fqdn);
                let ratio = cpu / gpu;
                if ratio > *self.running_avg_speedup.lock() {
                    let mut opts = vec![];
                    if reg.supported_compute.contains(Compute::CPU) {
                        opts.push((self.cpu_queue.est_completion_time(reg, &tid), Compute::CPU));
                    }
                    if reg.supported_compute.contains(Compute::GPU) {
                        if let Some(gpu_queue) = &self.gpu_queue {
                            opts.push((gpu_queue.est_completion_time(reg, &tid), Compute::GPU));
                        }
                    }
                    if let Some((est, c)) = opts.iter().min_by_key(|i| OrderedFloat(i.0)) {
                        let enq = self.make_enqueue(reg, json_args, &tid, insert_t, *est);
                        enqueues += self.enqueue_compute(&enq, *c)?;
                        if c == &Compute::GPU {
                            if self.invocation_config.log_details() {
                                info!(tid=%tid, fqdn=%enq.registration.fqdn, "Cache Hit");
                            }
                            if let Some(gpu_queue) = &self.gpu_queue {
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
                                info!(tid=%tid, new_avg=*avg, "running avg");
                            }
                        } else {
                            #[allow(clippy::collapsible_else_if)]
                            if self.invocation_config.log_details() {
                                info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=1.0, "Cache Miss");
                            }
                        }
                        new_item = Some(enq);
                    }
                } else {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, cpu);
                    self.cpu_queue.enqueue_item(&enq)?;
                    if self.invocation_config.log_details() {
                        info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=0.0, "Cache Miss");
                    }
                    new_item = Some(enq);
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::Speedup => {
                let cpu = self.cmap.avg_cpu_exec_t(&reg.fqdn);
                let gpu = self.cmap.avg_gpu_exec_t(&reg.fqdn);
                let ratio = cpu / gpu;
                if ratio > self.invocation_config.speedup_ratio.unwrap_or(4.0) {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, gpu);
                    self.enqueue_gpu_check(&enq)?;
                    if self.invocation_config.log_details() {
                        info!(tid=%tid, fqdn=%enq.registration.fqdn, "Cache Hit");
                    }
                    new_item = Some(enq);
                    enqueues += 1;
                } else {
                    let enq = self.make_enqueue(reg, json_args, &tid, insert_t, cpu);
                    self.cpu_queue.enqueue_item(&enq)?;
                    if self.invocation_config.log_details() {
                        info!(tid=%tid, fqdn=%enq.registration.fqdn, pot_creds=1.0, "Cache Miss");
                    }
                    new_item = Some(enq);
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::Landlord
            | EnqueueingPolicy::LRU
            | EnqueueingPolicy::LFU
            | EnqueueingPolicy::LandlordFixed => {
                let compute = self.landlord.lock().choose(&reg, &tid);
                let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
                enqueues += self.enqueue_compute(&enq, compute)?;
                new_item = Some(enq);
            },
            EnqueueingPolicy::Popular
            | EnqueueingPolicy::PopularEstTimeDispatch
            | EnqueueingPolicy::PopularQueueLenDispatch
            | EnqueueingPolicy::LeastPopular
            | EnqueueingPolicy::TopAvg
            | EnqueueingPolicy::TCPEstSpeedup => {
                let compute = self.popular.lock().choose(&reg, &tid);
                let enq = self.make_enqueue(reg, json_args, &tid, insert_t, 0.0);
                enqueues += self.enqueue_compute(&enq, compute)?;
                new_item = Some(enq);
            },
        }

        if enqueues == 0 {
            bail_error!(tid=%tid, "Unable to enqueue function invocation, not matching compute");
        }
        new_item.ok_or_else(|| anyhow::anyhow!("Enqueued item was never created"))
    }

    // Ideally should be in the ctx struct, but mutability?
    /// Should be in its struct, but mutable borrow etc
    #[allow(dead_code)]
    fn select_device_for_fn(&self, fid: String, device: ComputeEnum) {
        let mut d = self.dispatch_state.write();

        d.total_dispatch += 1;
        d.prev_dispatch.insert(fid.clone(), device);

        match device {
            ComputeEnum::cpu => {
                d.n_cpu += 1;
                d.cpu_prev_t.insert(fid.clone(), self.clock.now());
            },
            ComputeEnum::gpu => {
                d.n_gpu += 1;
                d.gpu_prev_t.insert(fid.clone(), self.clock.now());
            },
            _ => todo!(),
        }
    }

    /// Given two weights, return 0 or 1 probabilistically
    #[allow(dead_code)]
    fn proportional_selection(&self, wa: f64, wb: f64) -> i32 {
        // let mut rng = rand::thread_rng();
        let wt = wa + wb;
        let wa = wa / wt;
        // let wb = wb / wt;
        let r = rand::thread_rng().gen_range(0.0..1.0);
        if r > wa {
            return 1;
        }
        0
    }

    // https://jeremykun.com/2013/10/28/optimism-in-the-face-of-uncertainty-the-ucb1-algorithm/
    /// Upper-confidence bound on the execution latency. Or the E2E time?
    #[allow(dead_code)]
    fn ucb1_dispatch(
        &self,
        reg: Arc<RegisteredFunction>,
        _tid: &TransactionId,
        enqueue: &Arc<EnqueuedInvocation>,
    ) -> Result<()> {
        // device_wt = exec_time + sqrt(log steps/n), where n is number of times device has been selected for the function
        // Pick device with lowest weight and dispatch
        let fid = reg.fqdn.as_str(); //function name or fqdn?

        let cpu_t = self.cmap.avg_cpu_e2e_t(fid); // supposed to running average?
        let gpu_t = self.cmap.avg_gpu_e2e_t(fid);

        let total_dispatch = self.dispatch_state.read().total_dispatch as f64;
        let n_cpu = self.dispatch_state.read().n_cpu as f64;
        let n_gpu = self.dispatch_state.read().n_gpu as f64;

        let cpu_ucb = (f64::log(total_dispatch, 2.0) / n_cpu).sqrt();
        let gpu_ucb = (f64::log(total_dispatch, 2.0) / n_gpu).sqrt();

        let cpu_wt = cpu_t + cpu_ucb;
        let gpu_wt = gpu_t + gpu_ucb;

        let device_wts = HashMap::from([(ComputeEnum::cpu, cpu_wt), (ComputeEnum::gpu, gpu_wt)]);

        let min_val_pair = device_wts.iter().min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap());

        // pick smallest of the two
        let selected_device = min_val_pair.unwrap().0;

        self.select_device_for_fn(fid.to_string(), *selected_device);

        match selected_device {
            ComputeEnum::gpu => self.enqueue_gpu_check(enqueue),
            _ => self.cpu_queue.enqueue_item(enqueue),
        }
    }

    // Shrinking dartboard : Geulen, Sascha, Berthold Vöcking, and Melanie Winkler. "Regret Minimization for Online Buffering Problems Using the Weighted Majority Algorithm." COLT. 2010.
    /// Multiplicative Weights Update Algorithm
    #[allow(dead_code)]
    fn mwua_dispatch(
        &self,
        reg: Arc<RegisteredFunction>,
        _tid: &TransactionId,
        enqueue: &Arc<EnqueuedInvocation>,
    ) -> Result<()> {
        // the cost is t_recent - t_global_min
        let eta = 0.3; // learning rate
        let fid = reg.fqdn.as_str();

        let b = self.dispatch_state.read();
        let prev_wts = *b.per_fn_wts.get(fid).unwrap_or(&(1.0, 1.0));
        let prev_dispatch = *b.prev_dispatch.get(fid).unwrap_or(&ComputeEnum::cpu);
        drop(b);

        // Apply the reward/cost
        let t_other = match prev_dispatch {
            ComputeEnum::gpu => self.cmap.latest_gpu_e2e_t(fid),
            _ => self.cmap.latest_cpu_e2e_t(fid),
        };

        // global minimum best ever recorded
        let tmin = self.cmap.get_best_time(fid);

        let cost = 1.0 - (eta * (t_other - tmin) / f64::min(t_other, 0.1));
        // Shrinking dartboard locality
        // With probability equal to cost, select the previous device, for improving locality
        let r = rand::thread_rng().gen_range(0.0..1.0);
        let use_prev = r < cost;

        // update weight?
        let new_wt = match prev_dispatch {
            ComputeEnum::gpu => prev_wts.1 * cost,
            _ => prev_wts.0 * cost,
        };

        // update the weight tuple
        let new_wts = match prev_dispatch {
            ComputeEnum::gpu => (prev_wts.0, new_wt),
            _ => (new_wt, prev_wts.1),
        };

        let selected_device = if use_prev {
            prev_dispatch
        } else {
            // 0 or 1
            let selected = self.proportional_selection(new_wts.0, new_wts.1);
            match selected {
                1 => ComputeEnum::gpu,
                _ => ComputeEnum::cpu,
            }
        };

        self.select_device_for_fn(fid.to_string(), selected_device);
        // update the weights
        let mut d = self.dispatch_state.write();
        d.per_fn_wts.insert(fid.to_string(), new_wts);
        drop(d);

        match selected_device {
            ComputeEnum::gpu => self.enqueue_gpu_check(enqueue),
            _ => self.cpu_queue.enqueue_item(enqueue),
        }
    }

    /// Prob. of warm hit divided by avg e2e time. per-fn wts
    #[allow(dead_code)]
    fn hit_tput_dispatch(
        &self,
        reg: Arc<RegisteredFunction>,
        tid: &TransactionId,
        enqueue: &Arc<EnqueuedInvocation>,
    ) -> Result<()> {
        // the cost is t_recent - t_global_min
        if let Some(gpu_queue) = &self.gpu_queue {
            let egpu = gpu_queue.est_completion_time(&reg, tid);
            let ecpu = self.cpu_queue.est_completion_time(&reg, tid);

            let tnow = self.clock.now();

            let fqdn = &reg.fqdn;

            let b = self.dispatch_state.read();
            // let last_gpu = b.gpu_prev_t.get(fqdn);
            let iat_gpu = match b.gpu_prev_t.get(fqdn) {
                Some(tg) => (tnow - *tg).as_seconds_f64(),
                _ => 10000.0, //infinity essentially
            };

            // let last_cpu = b.cpu_prev_t.get(fqdn);
            let iat_cpu = match b.cpu_prev_t.get(fqdn) {
                Some(tg) => (tnow - *tg).as_seconds_f64(),
                _ => 10000.0, //infinity essentially
            };

            let pgpu = gpu_queue.warm_hit_probability(&reg, iat_gpu);
            let pcpu = self.cpu_queue.warm_hit_probability(&reg, iat_cpu);

            let rgpu = pgpu / egpu;
            let rcpu = pcpu / ecpu;

            // choose the maximum of the two? or probabilistically?
            let n = self.proportional_selection(rcpu, rgpu);
            match n {
                0 => self.enqueue_gpu_check(enqueue),
                _ => self.cpu_queue.enqueue_item(enqueue),
            }
        } else {
            anyhow::bail!("GPU queue was 'None' in hit_tput_dispatch");
        }
    }
}

#[tonic::async_trait]
impl Invoker for QueueingDispatcher {
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args), fields(tid=%tid)))]
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
                    self.cmap
                        .add(&reg.fqdn, Characteristics::E2EGpu, Values::F64(e2etime), false);
                    info!(tid=%tid, fqdn=%&reg.fqdn, e2etime=%e2etime, device=%"GPU", "Invocation complete");
                } else {
                    self.cmap
                        .add(&reg.fqdn, Characteristics::E2ECpu, Values::F64(e2etime), false);
                    info!(tid=%tid, fqdn=%&reg.fqdn, e2etime=%e2etime, device=%"CPU", "Invocation complete");
                }
                Ok(queued.result_ptr.clone())
            },
            false => {
                bail_error!(tid=%tid, "Invocation was signaled completion but completion value was not set")
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
    fn queue_len(&self) -> std::collections::HashMap<Compute, usize> {
        [
            (Compute::CPU, self.cpu_queue.queue_len()),
            (Compute::GPU, self.gpu_queue.as_ref().map_or(0, |g| g.queue_len())),
        ]
        .into_iter()
        .collect()
    }

    /// The number of functions currently running
    fn running_funcs(&self) -> u32 {
        self.cpu_queue.running() + self.gpu_queue.as_ref().map_or(0, |g| g.running())
    }
}
