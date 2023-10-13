use crate::controller::structs::internal::{RegisteredFunction, RegisteredWorker};
use crate::controller::structs::json::RegisterFunction;
use crate::services::load_balance::LoadBalancer;
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::api_register::RegisterWorker;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::{bail_error, transaction::TransactionId, utils::calculate_fqdn};
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::sync::Arc;
use tracing::{error, info};

#[allow(unused)]
pub struct RegistrationService {
    pub lb: LoadBalancer,
    functions: Arc<DashMap<String, Arc<RegisteredFunction>>>,
    workers: Arc<DashMap<String, Arc<RegisteredWorker>>>,
    worker_fact: Arc<WorkerAPIFactory>,
}

impl RegistrationService {
    pub fn boxed(lb: LoadBalancer, worker_fact: Arc<WorkerAPIFactory>) -> Arc<Self> {
        Arc::new(RegistrationService {
            lb,
            functions: Arc::new(DashMap::new()),
            workers: Arc::new(DashMap::new()),
            worker_fact,
        })
    }

    /// Return the function if it's been registered
    pub fn get_function(&self, fqdn: &String) -> Option<Arc<RegisteredFunction>> {
        match self.functions.get(fqdn) {
            Some(c) => Some(c.clone()),
            None => None,
        }
    }

    /// Register a new worker
    /// Prepare it with all registered functions too
    /// Send to load balancer
    pub async fn register_worker(&self, worker: RegisterWorker, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
        if self.worker_registered(&worker.name) {
            bail_error!(tid=%tid, worker=%worker.name, "Worker was already registered");
        }

        let reg_worker = Arc::new(RegisteredWorker::from(worker));

        let mut api = self
            .worker_fact
            .get_worker_api(
                &reg_worker.name,
                &reg_worker.host,
                reg_worker.port,
                reg_worker.communication_method,
                tid,
            )
            .await?;
        for item in self.functions.iter() {
            let function = item.value();
            match api
                .register(
                    function.function_name.clone(),
                    function.function_version.clone(),
                    function.image_name.clone(),
                    function.memory,
                    function.cpus,
                    function.parallel_invokes,
                    tid.clone(),
                    Isolation::CONTAINERD,
                    Compute::CPU,
                    function.timings.as_ref(),
                )
                .await
            {
                Ok(_) => (),
                Err(e) => {
                    error!(tid=%tid, worker=%reg_worker.name, error=%e, "New worker failed to register function")
                }
            };
        }
        self.lb.add_worker(reg_worker.clone(), tid);
        self.workers.insert(reg_worker.name.clone(), reg_worker.clone());
        info!(tid=%tid, worker=%reg_worker.name, "worker successfully registered");
        Ok(reg_worker)
    }

    /// check if worker has been registered already
    fn worker_registered(&self, name: &String) -> bool {
        self.workers.contains_key(name)
    }

    /// check if function has been registered already
    fn function_registered(&self, fqdn: &String) -> bool {
        self.functions.contains_key(fqdn)
    }

    /// Register a new function with workers
    pub async fn register_function(&self, function: RegisterFunction, tid: &TransactionId) -> Result<()> {
        let fqdn = calculate_fqdn(&function.function_name, &function.function_version);
        if self.function_registered(&fqdn) {
            bail_error!(tid=%tid, fqdn=%fqdn, "Function was already registered");
        } else {
            for item in self.workers.iter() {
                let worker = item.value();
                let mut api = self
                    .worker_fact
                    .get_worker_api(
                        &worker.name,
                        &worker.host,
                        worker.port,
                        worker.communication_method,
                        tid,
                    )
                    .await?;
                match api
                    .register(
                        function.function_name.clone(),
                        function.function_version.clone(),
                        function.image_name.clone(),
                        function.memory,
                        function.cpus,
                        function.parallel_invokes,
                        tid.clone(),
                        Isolation::CONTAINERD,
                        Compute::CPU,
                        function.timings.as_ref(),
                    )
                    .await
                {
                    Ok(_) => (),
                    Err(e) => {
                        error!(tid=%tid, worker=%worker.name, error=%e, "Worker failed to register new function")
                    }
                };
            }
            let function = Arc::new(RegisteredFunction::from(function));
            self.functions.insert(fqdn.clone(), function);
        }

        info!(tid=%tid, fqdn=%fqdn, "Function was registered");
        Ok(())
    }

    /// Get a lock-free iterable over all the currently registered workers
    pub fn iter_workers(&self) -> Vec<Arc<RegisteredWorker>> {
        self.workers.iter().map(|w| w.value().clone()).collect()
    }
}
