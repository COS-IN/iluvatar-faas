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
    fn choose(&mut self, item: &Arc<EnqueuedInvocation>, tid: &TransactionId) -> Compute;
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
    /// tid->compute device map aargh
    dev_hist: HashMap<TransactionId, Compute>,
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
            dev_hist: HashMap::new(),
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

    fn enqueue_compute(&self, item: &Arc<EnqueuedInvocation>, compute: Compute) -> Result<u32> {
        let mut enqueues = 0;
        if compute.contains(Compute::CPU) {
            enqueues += 1;
            self.enqueue_cpu_check(item)?;
        }
        if compute.contains(Compute::GPU) {
            self.enqueue_gpu_check(item)?;
            enqueues += 1;
        }
        Ok(enqueues)
    }
    fn enqueue_cpu_check(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        Self::enqueue_check(&Some(&self.cpu_queue), item, Compute::CPU)
    }
    fn enqueue_gpu_check(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        Self::enqueue_check(&self.gpu_queue.as_ref(), item, Compute::GPU)
    }
    fn enqueue_check(
        q: &Option<&Arc<dyn DeviceQueue>>,
        item: &Arc<EnqueuedInvocation>,
        compute: Compute,
    ) -> Result<()> {
        match q {
            Some(q) => q.enqueue_item(item),
            None => anyhow::bail!("No queue present for compute '{}'", compute),
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
        let enqueue = Arc::new(EnqueuedInvocation::new(
            reg.clone(),
            json_args,
            tid.clone(),
            self.clock.now(),
        ));
        let mut enqueues = 0;
        if self.invocation_config.enqueuing_log_details.unwrap_or(false) {
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
            self.enqueue_cpu_check(&enqueue)?;
            return Ok(enqueue);
        }
        if reg.gpu_only() {
            self.enqueue_gpu_check(&enqueue)?;
            return Ok(enqueue);
        }

        let policy = self
            .invocation_config
            .enqueueing_policy
            .as_ref()
            .unwrap_or(&EnqueueingPolicy::All);
        match policy {
            EnqueueingPolicy::All => {
                if reg.supported_compute.contains(Compute::GPU) {
                    self.enqueue_gpu_check(&enqueue)?;
                    enqueues += 1;
                }
                if reg.supported_compute.contains(Compute::CPU) {
                    self.enqueue_cpu_check(&enqueue)?;
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::AlwaysCPU => {
                if reg.supported_compute.contains(Compute::CPU) {
                    self.enqueue_cpu_check(&enqueue)?;
                    enqueues += 1;
                } else {
                    anyhow::bail!(
                        "Cannot enqueue invocation using {:?} strategy because invocation does not support CPU-only",
                        EnqueueingPolicy::AlwaysCPU
                    );
                }
            },
            EnqueueingPolicy::AlwaysGPU => {
                if reg.supported_compute.contains(Compute::GPU) {
                    self.enqueue_gpu_check(&enqueue)?;
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
                    opts.push((self.cmap.get_exec_time(&reg.fqdn), Some(&self.cpu_queue), Compute::CPU));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    opts.push((
                        self.cmap.get_gpu_exec_time(&reg.fqdn),
                        self.gpu_queue.as_ref(),
                        Compute::GPU,
                    ));
                }
                let best = opts.iter().min_by_key(|i| ordered_float::OrderedFloat(i.0));
                if let Some((_, q, c)) = best {
                    Self::enqueue_check(q, &enqueue, *c)?;
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::EstCompTime => {
                let mut opts = vec![];
                if reg.supported_compute.contains(Compute::CPU) {
                    opts.push((
                        self.cpu_queue.est_completion_time(reg, &tid),
                        Some(&self.cpu_queue),
                        Compute::CPU,
                    ));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    if let Some(gpu_queue) = &self.gpu_queue {
                        opts.push((
                            gpu_queue.est_completion_time(reg, &tid),
                            self.gpu_queue.as_ref(),
                            Compute::GPU,
                        ));
                    }
                }
                let best = opts.iter().min_by_key(|i| ordered_float::OrderedFloat(i.0));
                if let Some((_, q, c)) = best {
                    Self::enqueue_check(q, &enqueue, *c)?;
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::UCB1 => {
                self.ucb1_dispatch(reg.clone(), &tid.clone(), &enqueue)?;
                enqueues += 1;
            },
            EnqueueingPolicy::MWUA => {
                self.mwua_dispatch(reg.clone(), &tid.clone(), &enqueue)?;
                enqueues += 1;
            },
            EnqueueingPolicy::HitTput => {
                self.hit_tput_dispatch(reg.clone(), &tid.clone(), &enqueue)?;
                enqueues += 1;
            },
            EnqueueingPolicy::Speedup => {
                let cpu = self.cmap.avg_cpu_exec_t(&enqueue.registration.fqdn);
                let gpu = self.cmap.avg_gpu_exec_t(&enqueue.registration.fqdn);
                let ratio = cpu / gpu;
                if ratio > 4.0 {
                    self.enqueue_gpu_check(&enqueue)?;
                    enqueues += 1;
                } else {
                    self.enqueue_cpu_check(&enqueue)?;
                    enqueues += 1;
                }
            },
            EnqueueingPolicy::Landlord => {
                let compute = self.landlord.lock().choose(&enqueue, &tid);
                let mut d = self.dispatch_state.write();
                d.dev_hist.insert(tid.clone(), compute);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::LRU => {
                let compute = self.landlord.lock().choose(&enqueue, &tid);
                let mut d = self.dispatch_state.write();
                d.dev_hist.insert(tid.clone(), compute);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::LFU => {
                let compute = self.landlord.lock().choose(&enqueue, &tid);
                let mut d = self.dispatch_state.write();
                d.dev_hist.insert(tid.clone(), compute);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::LandlordFixed => {
                let compute = self.landlord.lock().choose(&enqueue, &tid);
                let mut d = self.dispatch_state.write();
                d.dev_hist.insert(tid.clone(), compute);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::Popular => {
                let compute = self.popular.lock().choose(&enqueue, &tid);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::PopularEstTimeDispatch => {
                let compute = self.popular.lock().choose(&enqueue, &tid);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::PopularQueueLenDispatch => {
                let compute = self.popular.lock().choose(&enqueue, &tid);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::LeastPopular => {
                let compute = self.popular.lock().choose(&enqueue, &tid);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
            EnqueueingPolicy::TopAvg => {
                let compute = self.popular.lock().choose(&enqueue, &tid);
                enqueues += self.enqueue_compute(&enqueue, compute)?;
            },
        }

        if enqueues == 0 {
            bail_error!(tid=%enqueue.tid, "Unable to enqueue function invocation, not matching compute");
        }
        Ok(enqueue)
    }

    // Ideally should be in the ctx struct, but mutability?
    /// Should be in its struct, but mutable borrow etc
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
            _ => self.enqueue_cpu_check(enqueue),
        }
    }

    /// Prob. of warm hit divided by avg e2e time. per-fn wts
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
                _ => self.enqueue_cpu_check(enqueue),
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
                // TODO: update cmap E2E time, but CPU or GPU?
                let e2etime = (self.clock.now() - queued.queue_insert_time).as_seconds_f64();
                let d = self.dispatch_state.read();

                match d.dev_hist.get(&tid) {
                    Some(&Compute::GPU) => {
                        self.cmap
                            .add(&reg.fqdn, Characteristics::E2EGpu, Values::F64(e2etime), false);
                        info!(tid=%tid, fqdn=%&reg.fqdn, e2etime=%e2etime, device=%"GPU", "Invocation complete");
                    },
                    _ => {
                        self.cmap
                            .add(&reg.fqdn, Characteristics::E2ECpu, Values::F64(e2etime), false);
                        info!(tid=%tid, fqdn=%&reg.fqdn, e2etime=%e2etime, device=%"CPU", "Invocation complete");
                    },
                };
                Ok(queued.result_ptr.clone())
            },
            false => {
                anyhow::bail!("Invocation was signaled completion but completion value was not set")
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
        .iter()
        .cloned()
        .collect()
    }

    /// The number of functions currently running
    fn running_funcs(&self) -> u32 {
        self.cpu_queue.running() + self.gpu_queue.as_ref().map_or(0, |g| g.running())
    }
}
