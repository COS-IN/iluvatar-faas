use crate::services::load_balance::LoadBalancer;
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::char_map::{add_registration_timings, WorkerCharMap};
use iluvatar_library::types::Isolation;
use iluvatar_library::utils::port::Port;
use iluvatar_library::{bail_error, transaction::TransactionId, utils::calculate_fqdn};
use iluvatar_rpc::rpc::{RegisterRequest, RegisterWorkerRequest};
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use serde::{Deserialize, Serialize};
use std::hash::Hasher;
use std::sync::Arc;
use tracing::info;

#[allow(unused)]
#[derive(Deserialize, Serialize, Debug)]
pub struct RegisteredWorker {
    pub name: String,
    pub isolation: Isolation,
    pub host: String,
    pub port: Port,
    pub memory: i64,
    pub cpus: u32,
}
impl std::hash::Hash for RegisteredWorker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl RegisteredWorker {
    pub fn from(req: RegisterWorkerRequest) -> anyhow::Result<Self> {
        Ok(RegisteredWorker {
            name: req.name,
            isolation: Isolation::from(req.isolation),
            host: req.host,
            port: req.port as Port,
            memory: req.memory,
            cpus: req.cpus,
        })
    }
}

#[allow(unused)]
pub struct FunctionRegistration {
    functions: Arc<DashMap<String, Arc<RegisteredFunction>>>,
    worker_cmap: WorkerCharMap,
}
impl FunctionRegistration {
    pub fn boxed(worker_cmap: &WorkerCharMap) -> Arc<Self> {
        Arc::new(Self {
            functions: Arc::new(DashMap::new()),
            worker_cmap: worker_cmap.clone(),
        })
    }

    /// Return the function if it's been registered
    pub fn get_function(&self, fqdn: &str) -> Option<Arc<RegisteredFunction>> {
        self.functions.get(fqdn).map(|c| c.clone())
    }

    /// Return the function if it's been registered
    pub fn get_all_functions(&self) -> Vec<Arc<RegisteredFunction>> {
        self.functions.iter().map(|e| e.value().clone()).collect()
    }

    /// check if function has been registered already
    fn function_registered(&self, fqdn: &str) -> bool {
        self.functions.contains_key(fqdn)
    }

    /// Register a new function with workers
    pub async fn register_function(
        &self,
        req: RegisterRequest,
        tid: &TransactionId,
    ) -> Result<Arc<RegisteredFunction>> {
        let fqdn = calculate_fqdn(&req.function_name, &req.function_version);
        if self.function_registered(&fqdn) {
            bail_error!(tid = tid, fqdn = fqdn, "Function was already registered");
        } else {
            let function: RegisteredFunction = req.try_into()?;
            add_registration_timings(
                &self.worker_cmap,
                function.supported_compute,
                &function.all_resource_timings,
                &fqdn,
                tid,
            )?;
            let r = Arc::new(function);
            self.functions.insert(fqdn, r.clone());
            info!(tid = tid, fqdn = r.fqdn, "Function was registered");
            Ok(r)
        }
    }
}

#[allow(unused)]
pub struct WorkerRegistration {
    pub lb: LoadBalancer,
    workers: Arc<DashMap<String, Arc<RegisteredWorker>>>,
    worker_fact: Arc<WorkerAPIFactory>,
    function_reg: Arc<FunctionRegistration>,
}

impl WorkerRegistration {
    pub fn boxed(
        lb: LoadBalancer,
        worker_fact: Arc<WorkerAPIFactory>,
        function_reg: &Arc<FunctionRegistration>,
    ) -> Arc<Self> {
        Arc::new(Self {
            lb,
            workers: Arc::new(DashMap::new()),
            worker_fact,
            function_reg: function_reg.clone(),
        })
    }

    /// Register a new worker
    /// Prepare it with all registered functions too
    /// Send to load balancer
    pub async fn register_worker(
        &self,
        worker: RegisterWorkerRequest,
        tid: &TransactionId,
    ) -> Result<Arc<RegisteredWorker>> {
        if self.worker_registered(&worker.name) {
            bail_error!(tid = tid, worker = worker.name, "Worker was already registered");
        }

        let reg_worker = Arc::new(RegisteredWorker::from(worker)?);

        let mut api = self
            .worker_fact
            .get_worker_api(&reg_worker.name, &reg_worker.host, reg_worker.port, tid)
            .await?;
        for function in self.function_reg.get_all_functions() {
            match api
                .register(
                    function.function_name.clone(),
                    function.function_version.clone(),
                    function.image_name.clone(),
                    function.memory,
                    function.cpus,
                    function.parallel_invokes,
                    tid.clone(),
                    function.isolation_type,
                    function.supported_compute,
                    function.container_server,
                    function.all_resource_timings.as_ref(),
                    function.system_function,
                )
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    bail_error!(tid=tid, worker=reg_worker.name, error=%e, "New worker failed to register function")
                },
            };
        }
        self.lb.add_worker(reg_worker.clone(), tid);
        self.workers.insert(reg_worker.name.clone(), reg_worker.clone());
        info!(tid = tid, worker = reg_worker.name, "worker successfully registered");
        Ok(reg_worker)
    }

    /// check if worker has been registered already
    fn worker_registered(&self, name: &str) -> bool {
        self.workers.contains_key(name)
    }

    /// Register a new function with workers
    pub async fn new_function(
        &self,
        reg: Arc<RegisteredFunction>,
        tid: &TransactionId,
    ) -> Result<Arc<RegisteredFunction>> {
        for item in self.workers.iter() {
            let worker = item.value();
            let mut api = self
                .worker_fact
                .get_worker_api(&worker.name, &worker.host, worker.port, tid)
                .await?;
            match api
                .register(
                    reg.function_name.clone(),
                    reg.function_version.clone(),
                    reg.image_name.clone(),
                    reg.memory,
                    reg.cpus,
                    reg.parallel_invokes,
                    tid.clone(),
                    reg.isolation_type,
                    reg.supported_compute,
                    reg.container_server,
                    reg.all_resource_timings.as_ref(),
                    reg.system_function,
                )
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    bail_error!(tid=tid, worker=%worker.name, error=%e, "Worker failed to register new function")
                },
            };
        }
        info!(tid=tid, fqdn=%reg.fqdn, "Function was registered across workers");
        Ok(reg)
    }

    /// Get a lock-free iterable over all the currently registered workers
    pub fn iter_workers(&self) -> Vec<Arc<RegisteredWorker>> {
        self.workers.iter().map(|w| w.value().clone()).collect()
    }
}
