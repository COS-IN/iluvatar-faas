use super::queueing::gpu_mqfq::MQFQ;
use super::queueing::{DeviceQueue, EnqueuedInvocation};
use super::{
    async_tracker::AsyncHelper, cpu_q_invoke::CpuQueueingInvoker, gpu_q_invoke::GpuQueueingInvoker,
    InvocationResultPtr, Invoker,
};
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::registration::RegisteredFunction;
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::services::{containers::containermanager::ContainerManager, invocation::queueing::EnqueueingPolicy};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::types::ComputeEnum;
use iluvatar_library::{logging::LocalTime, transaction::TransactionId, types::Compute};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, info};

lazy_static::lazy_static! {
  pub static ref INVOKER_CPU_QUEUE_WORKER_TID: TransactionId = "InvokerCPUQueue".to_string();
  pub static ref INVOKER_GPU_QUEUE_WORKER_TID: TransactionId = "InvokerGPUQueue".to_string();
}


pub struct PolymDispatchCtx {
    cmap: Arc<CharacteristicsMap>,
    /// cpu/gpu -> wt , based on device load.
    device_wts: HashMap<Compute, f64>,
    /// fn -> cpu_wt, gpu_wt , based on locality and speedup considerations.
    per_fn_wts: HashMap<String, (f64, f64)>,
    /// Most recent fn->device placement for each fn 
    prev_dispatch: HashMap<String, String>,
    fn_prev_t: HashMap<String, OffsetDateTime>,
    ///Total number of dispatches/rounds
    Total_dispatch: u64,
    n_cpu: u64,
    n_gpu: u64, //Init to to avoid divide by 0 
}

impl PolyDispatchCtx {
    pub fn boxed(cmap: &Arc<CharacteristicsMap>) -> Arc<Self> {
        Arc::new(Self {
            cmap: cmap.clone(),
            device_wts: HashMap::from([(Compute::CPU, 1.0), (Compute::GPU, 1.0)]),
            per_fn_wts: HashMap::new(),
            prev_dispatch: HashMap::new(),
            fn_prev_t: HashMap::new(),
        })
    }
    fn update_prev_t(&mut self, fid: String, t: OffsetDateTime) -> () {
        todo!();
    }
    fn update_locality(&mut self, fid: String, device: String) -> () {
        todo!();
    }

    // Based on the load/utilization etc?
    fn update_dev_wts(&mut self, device: String, wt: f64) -> () {
        todo!();
    }
    
    // Normalize the weights etc into probabilities?
    fn latency_rewards(&self, fid:String, device:String)
		       -> (Compute, delta:f64) {
	match device {
	    Compute::CPU => {
		let dev_lat = self.cmap.get_e2e_cpu(fid, False); //most recent 
		// Need to compare this to average latency of the /other/ device
		let other_lat = self.cmap.get_e2e_gpu(fid, True); // aggregate
	    }
	    _ => {
		let dev_lat = self.cmap.get_e2e_gpu(fid);
		let other_lat = self.cmap.get_e2e_cpu(fid, True); // aggregate		
	    }
	}
	let diff = other_lat - dev_lat ;
	
	device, diff 
    }

    /// CPU, GPU IAT previous dispatch 
    fn device_iats(&self, fid: String) -> (f64, f64) {

    }

    /// Set dispatch counters, history, etc. 
    fn select_device_for_fn(&mut self, fid: String, device:String) -> () {

    }

}

pub struct QueueingDispatcher {
    async_functions: AsyncHelper,
    invocation_config: Arc<InvocationConfig>,
    cmap: Arc<CharacteristicsMap>,
    clock: LocalTime,
    cpu_queue: Arc<dyn DeviceQueue>,
    gpu_queue: Arc<dyn DeviceQueue>,
    dispatch_state: Arc<PolymDispatchCtx>,
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
        gpu: Arc<GpuResourceTracker>,
        #[cfg(feature = "power_cap")] energy: Arc<EnergyLimiter>,
    ) -> Result<Arc<Self>> {
        let svc = Arc::new(QueueingDispatcher {
            cpu_queue: Self::get_invoker_queue(
                &invocation_config,
                &cmap,
                &cont_manager,
                tid,
                &function_config,
                &cpu,
                #[cfg(feature = "power_cap")]
                &energy,
            )?,
            gpu_queue: Self::get_invoker_gpu_queue(
                &invocation_config,
                &cmap,
                &cont_manager,
                tid,
                &function_config,
                &cpu,
                &gpu,
            )?,
            async_functions: AsyncHelper::new(),
            clock: LocalTime::new(tid)?,
            invocation_config,
            dispatch_state: PolyDispatchState::boxed(&cmap),
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
        gpu: &Arc<GpuResourceTracker>,
    ) -> Result<Arc<dyn DeviceQueue>> {
        match invocation_config.queues.get(&ComputeEnum::gpu).as_deref() {
            Some(q) => {
                if q == "serial" {
                    Ok(GpuQueueingInvoker::new(
                        cont_manager.clone(),
                        function_config.clone(),
                        invocation_config.clone(),
                        tid,
                        cmap.clone(),
                        cpu.clone(),
                        gpu.clone(),
                    )?)
                } else if q == "mqfq" {
                    Ok(MQFQ::new(
                        cont_manager.clone(),
                        cmap.clone(),
                        invocation_config.clone(),
                        cpu.clone(),
                        gpu.clone(),
                    )?)
                } else {
                    anyhow::bail!("Unkonwn GPU queue {}", q);
                }
            }
            None => anyhow::bail!("GPU queue was not specified"),
        }
    }

    /// Forms invocation data into a [EnqueuedInvocation] that is returned
    /// The default implementation also calls [Invoker::add_item_to_queue] to optionally insert that item into the implementation's queue
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

        if reg.supported_compute == Compute::CPU {
            self.cpu_queue.enqueue_item(&enqueue)?;
            return Ok(enqueue);
        }
        if reg.supported_compute == Compute::GPU {
            self.gpu_queue.enqueue_item(&enqueue)?;
            return Ok(enqueue);
        }

        let policy = self
            .invocation_config
            .enqueueing_policy
            .as_ref()
            .unwrap_or(&EnqueueingPolicy::All);
        match policy {
            EnqueueingPolicy::All => {
                if reg.supported_compute.contains(Compute::CPU) {
                    self.cpu_queue.enqueue_item(&enqueue)?;
                    enqueues += 1;
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    self.gpu_queue.enqueue_item(&enqueue)?;
                    enqueues += 1;
                }
            }
            EnqueueingPolicy::AlwaysCPU => {
                if reg.supported_compute.contains(Compute::CPU) {
                    self.cpu_queue.enqueue_item(&enqueue)?;
                    enqueues += 1;
                } else {
                    anyhow::bail!(
                        "Cannot enqueue invocation using {:?} strategy because it does not support CPU",
                        EnqueueingPolicy::AlwaysCPU
                    );
                }
            }
            EnqueueingPolicy::ShortestExecTime => {
                let mut opts = vec![];
                if reg.supported_compute.contains(Compute::CPU) {
                    opts.push((self.cmap.get_exec_time(&reg.fqdn), &self.cpu_queue));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    opts.push((self.cmap.get_gpu_exec_time(&reg.fqdn), &self.gpu_queue));
                }
                let best = opts.iter().min_by_key(|i| ordered_float::OrderedFloat(i.0));
                if let Some((_, q)) = best {
                    q.enqueue_item(&enqueue)?;
                    enqueues += 1;
                }
            }
            EnqueueingPolicy::EstCompTime => {
                let mut opts = vec![];
                if reg.supported_compute.contains(Compute::CPU) {
                    opts.push((self.cpu_queue.est_completion_time(reg, &tid), &self.cpu_queue));
                }
                if reg.supported_compute.contains(Compute::GPU) {
                    opts.push((self.gpu_queue.est_completion_time(reg, &tid), &self.gpu_queue));
                }
                let best = opts.iter().min_by_key(|i| ordered_float::OrderedFloat(i.0));
                if let Some((_, q)) = best {
                    q.enqueue_item(&enqueue)?;
                    enqueues += 1;
                }
            }
            EnqueueingPolicy::UCB1 => {
		let mut chosen_q = self.ucb1_dispatch(reg.clone(), &tid.clone(), enqueue.clone());
		chosen_q.enqueue_item(&enqueue)?;
		enqueues += 1 ;
            }
        }

        if enqueues == 0 {
            anyhow::bail!("Unable to enqueue function invocation, not matching compute");
        }
        Ok(enqueue)
    }

    // https://jeremykun.com/2013/10/28/optimism-in-the-face-of-uncertainty-the-ucb1-algorithm/
    /// Upper-confidence bound on the execution latency. Or the E2E time? 
    fn ucb1_dispatch(&self,  reg: Arc<RegisteredFunction>, tid: &TransactionId, enqueue: Arc<EnqueuedInvocation>) -> &Arc<dyn DeviceQueue> {
	// device_wt = exec_time + sqrt(log steps/n), where n is number of times device has been selected for the function
	// Pick device with lowest weight and dispatch
        let mut chosen_q ;
	let selected_device: String;
	let fid = reg.function_name.clone(); 
	let cpu_t = cmap.avg_cpu_exec_t(fid); // supposed to running average? 
	let gpu_t = cmap.avg_gpu_exec_t(fid);
	let T  = self.dispatch_state.Total_dispatch ; 
	let n_cpu = self.dispatch_state.n_cpu ;
	let n_gpu = self.dispatch_state.n_gpu ;
	
	let cpu_wt = cpu_t + math.sqrt(2.0*math.log(T)/float(n_cpu));
	let gpu_wt = cpu_t + math.sqrt(2.0*math.log(T)/float(n_gpu));
	let device_wts = Hashmap::new({("cpu", cpu_wt), ("gpu", gpu_wt)});
	// pick smallest of the two, gree
	selected_device = min_by_value(device_wts);

	self.dispatch_state.select_device_for_fn(fid, selected_device);
	
    }


    // 
    /// Multiplicative Weights Update Algorithm 
    fn mwua_dispatch(&self,  reg: Arc<RegisteredFunction>, tid: &TransactionId, enqueue: Arc<EnqueuedInvocation>) -> &Arc<dyn DeviceQueue> {
	// the cost is t_recent - t_global_min
	let eta = 0.3; // learning rate
	let mut chosen_q ;
	let selected_device: String;
	let fid = reg.function_name.clone(); 
	let cpu_t = cmap.latest_cpu_exec_t(fid); // supposed to running average? 
	let gpu_t = cmap.latest_gpu_exec_t(fid);
	let t_min = cmap.min_exec_t(fid); // global min
	let prev_dev, prev_t = self.dispatch_state.get_prev_exec_time(fid); //depending on device
	let cost = prev_t - t_min ;
	let pw_cpu, pw_gpu = self.dispatch_state.per_fn_wts.get(fid);

	// update weights w = w*(1.0 - (eta * cost));
	
	// choose proportional to the weights
	let vec_devices = Vec!["cpu", "gpu"]; 
	let choice = self.proportional_selection(w_cpu, w_gpu);
	selected_device = vec_devices[choice]; // Rust out of bounds etc unsafe?!?
	
	self.dispatch_state.select_device_for_fn(fid, selected_device);
    }

    /// Given two weights, return 0 or 1 probabilistically 
    fn proportional_selection(&self, wa:f64, wb:f64) -> i32 {
	let choice ;
	let wt = wa + wb ;
	let wa = wa/wt;
	let wb = wb/wt;
	let r = random(0, 1); //between 0 and 1 
	if r > wa {
	    return 1;
	}
	0 
    }

    /// Proportional to locality, performance, and load 
    fn prop_dispatch(&self,  reg: Arc<RegisteredFunction>, tid: &TransactionId, enqueue: Arc<EnqueuedInvocation>) -> &Arc<dyn DeviceQueue> {
	// ratios of the three main factors?
	let p_warm_cpu ;
	let p_warm_gpu ;
	let r_warm:f64 = p_warm_cpu/p_warm_gpu ;

	let exec_cpu ;
	let exec_gpu ; 
	let r_exec:f64 = exec_cpu/exec_gpu ; //lower is better, so inverse ? 

	let load_cpu ;
	let load_gpu ;
	let r_load:f64 = load_cpu/load_gpu; 

	let r_all = r_warm * r_exec * r_load ;

	let vec_devices = Vec!["cpu", "gpu"]; 
	let choice = self.proportional_selection(r_all, 1.0);
	
	self.dispatch_state.select_device_for_fn(fid, selected_device);
    }
    
    
    /// Multi-armed bandit based dispatch. Load-balancing using stale rewards (latency) and costs (load). Is sticky for locality.
    fn bandit1_dispatch(&self, reg: Arc<RegisteredFunction>, tid: &TransactionId, enqueue: Arc<EnqueuedInvocation>) -> &Arc<dyn DeviceQueue> {
	// Nothing to do for non-polymorphic functions
        if reg.cpu_only() {
            return &self.cpu_queue;
        }
        if reg.gpu_only() {
            return &self.gpu_queue;
        }
	
        let mut chosen_q ;
	let mut chosen_device: Compute; 
	let fid = reg.function_name.clone(); 

	// Three major sources
	// 1. Locality
	

	// 2. E2E latency and difference
	   
	// 3. Device loads 

	
	self.dispatch_state.update_device_loads();
	self.dispatch_state.update_fn_chars(); // implicit? 

	
	self.dispatch_state.update_prev_t(fid, OffsetDateTime::now_utc());
	self.dispatch_state.update_prev_dispath(fid, chosen_device); 
        return chosen_q ;
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
                info!(tid=%tid, "Invocation complete");
                Ok(queued.result_ptr.clone())
            }
            false => {
                anyhow::bail!("Invocation was signaled completion but completion value was not set")
            }
        }
    }
    fn async_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<String> {
        let invoke = self.enqueue_new_invocation(&reg, json_args, tid)?;
        self.async_functions.insert_async_invoke(invoke)
    }
    fn invoke_async_check(&self, cookie: &str, tid: &TransactionId) -> Result<crate::rpc::InvokeResponse> {
        self.async_functions.invoke_async_check(cookie, tid)
    }

    /// The queue length of both CPU and GPU queues
    fn queue_len(&self) -> std::collections::HashMap<Compute, usize> {
        [
            (Compute::CPU, self.cpu_queue.queue_len()),
            (Compute::GPU, self.gpu_queue.queue_len()),
        ]
        .iter()
        .cloned()
        .collect()
    }

    /// The number of functions currently running
    fn running_funcs(&self) -> u32 {
        self.cpu_queue.running() + self.gpu_queue.running()
    }
}
