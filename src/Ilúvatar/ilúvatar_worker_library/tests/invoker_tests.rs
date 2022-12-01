#[macro_use]
pub mod utils;

use std::sync::Arc;
use iluvatar_worker_library::rpc::{RegisterRequest, PrewarmRequest, InvokeAsyncRequest};
use iluvatar_worker_library::rpc::InvokeRequest;
use iluvatar_worker_library::worker_api::worker_config::WorkerConfig;
use iluvatar_worker_library::services::invocation::invoker_trait::Invoker;
use iluvatar_worker_library::services::containers::containermanager::ContainerManager;
use rstest::rstest;
use utils::{test_invoker_svc, clean_env};
use std::{time::Duration, collections::HashMap};

fn build_env(invoker_q: &str) -> HashMap<String, String> {
  let mut r = HashMap::new();
  r.insert("ILUVATAR_WORKER__invocation__queue_policy".to_string(), invoker_q.to_string());
  r.insert("ILUVATAR_WORKER__invocation__fcfs_bypass_duration_ms".to_string(), "20".to_string());
  r
}

#[cfg(test)]
mod invoke {
  use super::*;

  #[rstest]
  #[case("fcfs")]
  #[case("minheap")]
  #[case("fcfs_bypass")]
  // #[case("none")] // TODO: queueless does not do async invokes
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test]
  async fn invocation_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<dyn Invoker>) = test_invoker_svc(None, Some(&env)).await;
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
    let result = invok_svc.sync_invocation(req.function_name, req.function_version, req.json_args, req.transaction_id).await;
    match result {
      Ok( (json, dur) ) => {
        assert!(dur.as_micros() > 0, "Duration should not be <= 0!");
        assert_ne!(json, ""); 
      },
      Err(e) => panic!("Invocation failed: {}", e),
    }
    clean_env(&env);
  }

  #[rstest]
  #[case("fcfs")]
  #[case("minheap")]
  #[case("fcfs_bypass")]
  // #[case("none")] // TODO: queueless does not do async invokes
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test]
  async fn cold_start_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<dyn Invoker>) = test_invoker_svc(None, Some(&env)).await;
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
    let result = invok_svc.sync_invocation(req.function_name, req.function_version, req.json_args, req.transaction_id).await;
    match result {
      Ok( (json, dur) ) => {
        assert!(dur.as_micros() > 0, "Duration should not be <= 0!");
        assert_ne!(json, ""); 
      },
      Err(e) => panic!("Invocation failed: {}", e),
    }
  }
}

#[cfg(test)]
mod invoke_async {
  use super::*;

  #[rstest]
  #[case("fcfs")]
  #[case("minheap")]
  #[case("fcfs_bypass")]
  // #[case("none")] // TODO: queueless does not do async invokes
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
  async fn invocation_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<dyn Invoker>) = test_invoker_svc(None, Some(&env)).await;

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
    let result = invok_svc.async_invocation(req.function_name, req.function_version, req.json_args, req.transaction_id);
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
                assert!(result.duration_us > 0, "Duration should not be <= 0!");
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
    clean_env(&env);
  }

  #[rstest]
  #[case("fcfs")]
  #[case("minheap")]
  #[case("fcfs_bypass")]
  // #[case("none")] // TODO: queueless does not do async invokes
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
  async fn cold_start_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_cfg, cm, invok_svc): (WorkerConfig, Arc<ContainerManager>, Arc<dyn Invoker>) = test_invoker_svc(None, Some(&env)).await;
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
    let result = invok_svc.async_invocation(req.function_name, req.function_version, req.json_args, req.transaction_id);
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
                assert!(result.duration_us > 0, "Duration should not be <= 0!");
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