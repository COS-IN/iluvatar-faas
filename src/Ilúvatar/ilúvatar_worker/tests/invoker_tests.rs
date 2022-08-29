#[macro_use]
pub mod utils;

use std::sync::Arc;
use iluvatar_worker_library::rpc::{RegisterRequest, PrewarmRequest, InvokeAsyncRequest};
use iluvatar_worker_library::rpc::InvokeRequest;
use iluvatar_library::transaction::TEST_TID;
use iluvatar_worker_library::worker_api::worker_config::{WorkerConfig, Configuration};
use iluvatar_worker_library::services::{containers::containermanager::ContainerManager, invocation::invoker::InvokerService};
use iluvatar_worker_library::services::{containers::LifecycleFactory};

#[cfg(test)]
mod invoke {
  use super::*;

  #[tokio::test]
  async fn invocation_works() {
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let req = InvokeRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      memory: 128,
      json_args:"{\"name\":\"TESTING\"}".to_string(),
      transaction_id: "testTID".to_string()
    };
    let result = invok_svc.invoke(req).await;
    match result {
      Ok( (json, dur) ) => {
        assert!(dur > 0, "Duration should not be <= 0!");
        assert_ne!(json, ""); 
      },
      Err(e) => panic!("Invocation failed: {}", e),
    }
  }

  #[tokio::test]
  async fn cold_start_works() {
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let req = InvokeRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      memory: 128,
      json_args:"{\"name\":\"TESTING\"}".to_string(),
      transaction_id: "testTID".to_string()
    };
    let result = invok_svc.invoke(req).await;
    match result {
      Ok( (json, dur) ) => {
        assert!(dur > 0, "Duration should not be <= 0!");
        assert_ne!(json, ""); 
      },
      Err(e) => panic!("Invocation failed: {}", e),
    }
  }
}

#[cfg(test)]
mod invoke_async {
  use core::panic;
  use std::time::Duration;
  use super::*;

  #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
  async fn invocation_works() {
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let req = InvokeAsyncRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      memory: 128,
      json_args:"{\"name\":\"TESTING\"}".to_string(),
      transaction_id: "testTID".to_string()
    };
    let result = InvokerService::invoke_async(invok_svc.clone(), req);
    let mut count = 0;
    match result {
      Ok( cookie ) => {
        assert_ne!(cookie, "");
        loop {
          let result = invok_svc.invoke_async_check(&cookie, &"testTID".to_string());
          match result {
            Ok(result) => {
              if result.success {
                assert_ne!(result.json_result, ""); 
                assert!(result.duration_ms > 0, "Duration should not be <= 0!");
                break
              } else {
                if result.json_result == "{ \"Error\": \"Invocation not found\" }" || result.json_result == "{ \"Error\": \"No result was captured\" }" {
                  panic!("Async invocation check after check: {:?}", result);
                } else if result.json_result == "{ \"Status\": \"Invocation not completed\" }" {
                  // keep waiting on invocation
                  tokio::time::sleep(Duration::from_millis(100)).await;
                  count += 1;
                  if count > 100 {
                    panic!("Waited too long for invocation result; cookie: {}; response: {:?}", cookie, result);
                  }
                  continue
                } else {
                  panic!("unkonwn response from async invocation check: {:?}", result);
                }
              }
            },
            Err(e) => panic!("Async invocation check failed: {}", e),
          }
        }
      },
      Err(e) => panic!("Async invocation failed: {}", e),
    }
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
  async fn cold_start_works() {
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let req = InvokeAsyncRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      memory: 128,
      json_args:"{\"name\":\"TESTING\"}".to_string(),
      transaction_id: "testTID".to_string()
    };
    let result = InvokerService::invoke_async(invok_svc.clone(), req);
    let mut count = 0;
    match result {
      Ok( cookie ) => {
        assert_ne!(cookie, "");
        loop {
          let result = invok_svc.invoke_async_check(&cookie, &"testTID".to_string());
          match result {
            Ok(result) => {
              if result.success {
                assert_ne!(result.json_result, ""); 
                assert!(result.duration_ms > 0, "Duration should not be <= 0!");
                break
              } else {
                if result.json_result == "{ \"Error\": \"Invocation not found\" }" || result.json_result == "{ \"Error\": \"No result was captured\" }" {
                  panic!("Async invocation check after check: {:?}", result);
                } else if result.json_result == "{ \"Status\": \"Invocation not completed\" }" {
                  // keep waiting on invocation
                  tokio::time::sleep(Duration::from_millis(100)).await;
                  count += 1;
                  if count > 100 {
                    panic!("Waited too long for invocation result; cookie: {}; response: {:?}", cookie, result);
                  }
                  continue
                } else {
                  panic!("unkonwn response from async invocation check: {:?}", result);
                }
              }
            },
            Err(e) => panic!("Async invocation check failed: {}", e),
          }
        }
      },
      Err(e) => panic!("Async invocation failed: {}", e),
    }
  }
}