use super::{
    invocation::Invoker,
    registration::{RegisteredFunction, RegistrationService},
};
use crate::services::containers::ContainerIsolationCollection;
use crate::worker_api::worker_config::WorkerConfig;
use anyhow::Result;
use iluvatar_library::types::ContainerServer;
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation},
};
use iluvatar_rpc::rpc::{HealthResponse, LanguageRuntime, RegisterRequest};
use std::sync::Arc;
use tracing::warn;

const TEST_FUNC_NAME: &str = "worker-health-test";
const TEST_FUNC_VERSION: &str = "1.0.0";
const TEST_FUNC_ARG: &str = "BADCAFE";
const TEST_FUNC_ARGS: &str = "{ \"name\":\"BADCAFE\" }";

/// result will look like {"body": {"greeting": greeting, "cold":was_cold, "start":start, "end":end, "latency": end-start} }
#[derive(serde::Deserialize)]
struct TestReturnFormat {
    body: TestBody,
}
#[derive(serde::Deserialize)]
#[allow(unused)]
struct TestBody {
    greeting: String,
    cold: bool,
    start: f64,
    end: f64,
    latency: f64,
}

pub struct WorkerHealthService {
    invoker_svc: Arc<dyn Invoker>,
    health_reg: Arc<RegisteredFunction>,
}

impl WorkerHealthService {
    pub async fn boxed(
        worker_config: WorkerConfig,
        invoker_svc: Arc<dyn Invoker>,
        reg: Arc<RegistrationService>,
        isos: ContainerIsolationCollection,
        tid: &TransactionId,
    ) -> Result<Arc<Self>> {
        let img_name = if worker_config
            .container_resources
            .gpu_resource
            .as_ref()
            .unwrap()
            .is_tegra
            .unwrap_or(false)
        {
            "docker.io/aarehman/hello-iluvatar-action:aarch64".to_string()
        } else {
            "docker.io/alfuerst/hello-iluvatar-action:latest".to_string()
        };

        let health_func = RegisterRequest {
            function_name: TEST_FUNC_NAME.to_string(),
            function_version: TEST_FUNC_VERSION.to_string(),
            image_name: img_name,
            memory: 75,
            cpus: 1,
            parallel_invokes: 1,
            transaction_id: tid.clone(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            // support all available isolations
            // TODO: health service should probably test each independently
            isolate: isos.keys().fold(Isolation::empty(), |acc, i| acc | *i).bits(),
            resource_timings_json: "{}".to_string(),
            container_server: ContainerServer::HTTP as u32,
            system_function: true,
        };

        let reg = reg
            .register(health_func, &iluvatar_library::transaction::HEALTH_TID)
            .await?;

        Ok(Arc::new(WorkerHealthService {
            invoker_svc,
            health_reg: reg,
        }))
    }

    fn unhealthy(&self) -> HealthResponse {
        HealthResponse { status: 1 }
    }

    fn healthy(&self) -> HealthResponse {
        HealthResponse { status: 0 }
    }

    /// see if the worker is healthy by trying to run a simple invocation and verifying results
    pub async fn check_health(&self, tid: &TransactionId) -> HealthResponse {
        match self
            .invoker_svc
            .sync_invocation(self.health_reg.clone(), TEST_FUNC_ARGS.to_string(), tid.clone())
            .await
        {
            Ok(result_ptr) => {
                let result = result_ptr.lock();
                match serde_json::from_str::<TestReturnFormat>(&result.result_json) {
                    Ok(obj) => {
                        if obj.body.greeting == format!("Hello {} from python!", TEST_FUNC_ARG) {
                            self.healthy()
                        } else {
                            warn!(tid=tid, greeting=%obj.body.greeting, "Received message from health function was incorrect");
                            self.unhealthy()
                        }
                    },
                    Err(e) => {
                        warn!(tid=tid, json=%result.result_json, error=%e, "Got invalid json from health function invocation");
                        self.unhealthy()
                    },
                }
            },
            Err(e) => {
                warn!(tid=tid, error=%e, "Got an error trying to run health function invocation");
                self.unhealthy()
            },
        }
    }
}
