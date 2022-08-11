use std::sync::Arc;
use iluvatar_library::transaction::TransactionId;
use crate::{rpc::{RegisterRequest, InvokeRequest, HealthResponse}};
use super::{invocation::invoker::InvokerService, containers::containermanager::ContainerManager};
use anyhow::Result;
use tracing::warn;

const TEST_FUNC_NAME: &str = "worker-health-test";
const TEST_FUNC_VERSION: &str = "1.0.0";
const TEST_FUNC_ARG: &str = "BADCAFE";
const TEST_FUNC_ARGS: &str = "{ \"name\":\"BADCAFE\" }";

/// result will look like {"body": {"greeting": greeting, "cold":was_cold, "start":start, "end":end, "latency": end-start} }
#[derive(serde::Deserialize)]
struct TestReturnFormat {
  body: TestBody
}

#[derive(serde::Deserialize)]
#[allow(unused)]
struct TestBody {
  greeting: String,
  cold: bool,
  start: f64,
  end: f64,
  latency: f64
}

pub struct WorkerHealthService {
  invoker_svc: Arc<InvokerService>
}

impl WorkerHealthService {
  pub async fn boxed(invoker_svc: Arc<InvokerService>, container_mgr: Arc<ContainerManager>, tid: &TransactionId) -> Result<Arc<Self>> {
    let health_func = RegisterRequest {
        function_name: TEST_FUNC_NAME.to_string(),
        function_version: TEST_FUNC_VERSION.to_string(),
        image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
        memory: 75,
        cpus: 1,
        parallel_invokes: 1,
        transaction_id: tid.clone(),
    };
    container_mgr.register(&health_func).await?;

    Ok(Arc::new(WorkerHealthService {
      invoker_svc
    }))
  }

  fn unhealthy(&self) -> HealthResponse {
    HealthResponse {
      status: 1,
    }
  }

  fn healthy(&self) -> HealthResponse {
    HealthResponse {
      status: 0,
    }
  }

  /// see if the worker is healthy by trying to run a simple invocation and verifying results
  pub async fn check_health(&self, tid: &TransactionId) -> HealthResponse {
    let request = InvokeRequest {
        function_name: TEST_FUNC_NAME.to_string(),
        function_version: TEST_FUNC_VERSION.to_string(),
        memory: 75,
        json_args: TEST_FUNC_ARGS.to_string(),
        transaction_id: tid.clone(),
    };
    match self.invoker_svc.invoke(request).await {
      Ok( (json, _dur) ) => {
        match serde_json::from_str::<TestReturnFormat>(&json) {
          Ok(obj) => {
            if obj.body.greeting == format!("Hello {} from python!", TEST_FUNC_ARG) {
              self.healthy()
            } else {
              warn!(tid=%tid, greeting=%obj.body.greeting, "Received message from health function was incorrect");
              self.unhealthy()
            }
          },
          Err(e) => {
            warn!(tid=%tid, json=%json, error=%e, "Got invalid json from health function invocation");
            self.unhealthy()
          },
        }
      },
      Err(e) => { 
        warn!(tid=%tid, error=%e, "Got an error trying to run health function invocation");
        self.unhealthy()
      },
    }
  }
}