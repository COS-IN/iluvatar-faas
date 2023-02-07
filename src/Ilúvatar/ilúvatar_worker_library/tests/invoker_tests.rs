#[macro_use]
pub mod utils;

use std::sync::Arc;
use tokio::time::timeout;
use iluvatar_worker_library::rpc::{RegisterRequest, PrewarmRequest, InvokeAsyncRequest};
use iluvatar_worker_library::rpc::InvokeRequest;
use iluvatar_worker_library::services::invocation::invoker_trait::Invoker;
use iluvatar_worker_library::services::containers::containermanager::ContainerManager;
use rstest::rstest;
use utils::{test_invoker_svc, clean_env};
use std::{time::Duration, collections::HashMap};
use iluvatar_worker_library::services::containers::structs::ContainerTimeFormatter;
use iluvatar_worker_library::rpc::{LanguageRuntime, SupportedCompute, SupportedIsolation};

fn build_env(invoker_q: &str) -> HashMap<String, String> {
  let mut r = HashMap::new();
  r.insert("ILUVATAR_WORKER__invocation__queue_policy".to_string(), invoker_q.to_string());
  r.insert("ILUVATAR_WORKER__invocation__bypass_duration_ms".to_string(), "20".to_string());
  r.insert("ILUVATAR_WORKER__invocation__concurrency_udpate_check_ms".to_string(), "1000".to_string());
  r.insert("ILUVATAR_WORKER__invocation__max_load".to_string(), "10".to_string());
  r.insert("ILUVATAR_WORKER__invocation__max_concurrency".to_string(), "10".to_string());
  r
}

#[cfg(test)]
mod invoke {
  use super::*;

  #[rstest]
  #[case("fcfs")]
  #[case("minheap")]
  #[case("fcfs_bypass")]
  #[case("minheap_iat_bypass")]
  #[case("minheap_ed_bypass")]
  #[case("minheap_bypass")]
  #[case("minheap_iat")]
  #[case("minheap_ed")]
  #[case("none")]
  #[case("cold_pri")]
  #[case("scaling")]
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn invocation_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: vec![SupportedCompute::Cpu.into()],
      isolate: vec![SupportedIsolation::Containerd.into()],
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
    let formatter = ContainerTimeFormatter::new().unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
    let result = invok_svc.sync_invocation(req.function_name, req.function_version, req.json_args, req.transaction_id).await;
    match result {
      Ok( result_ptr ) => {
        let result = result_ptr.lock();
        let worker_result = result.worker_result.as_ref().unwrap_or_else(|| panic!("worker_result should have been set"));
        let parsed_start = formatter.parse_python_container_time(&worker_result.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.start, e));
        let parsed_end = formatter.parse_python_container_time(&worker_result.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.end, e));
        assert!(parsed_start < parsed_end, "Start and end times cannot be inversed!");
        assert!(result.duration.as_micros() > 0, "Duration should not be <= 0!");
        assert_ne!(result.result_json, ""); 
      },
      Err(e) => panic!("Invocation failed: {}", e),
    }
    clean_env(&env);
  }

  #[rstest]
  #[case("fcfs")]
  #[case("minheap")]
  #[case("fcfs_bypass")]
  #[case("minheap_iat_bypass")]
  #[case("minheap_ed_bypass")]
  #[case("minheap_bypass")]
  #[case("minheap_iat")]
  #[case("minheap_ed")]
  #[case("none")]
  #[case("cold_pri")]
  #[case("scaling")]
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn cold_start_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: vec![SupportedCompute::Cpu.into()],
      isolate: vec![SupportedIsolation::Containerd.into()],
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let req = InvokeRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      memory: 128,
      json_args:"{\"name\":\"TESTING\"}".to_string(),
      transaction_id: "testTID".to_string()
    };
    let formatter = ContainerTimeFormatter::new().unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
    let result = invok_svc.sync_invocation(req.function_name, req.function_version, req.json_args, req.transaction_id).await;
    match result {
      Ok( result_ptr ) => {
        let result = result_ptr.lock();
        let worker_result = result.worker_result.as_ref().unwrap_or_else(|| panic!("worker_result should have been set; json: {}", result.result_json));
        let parsed_start = formatter.parse_python_container_time(&worker_result.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.start, e));
        let parsed_end = formatter.parse_python_container_time(&worker_result.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.end, e));
        assert!(parsed_start < parsed_end, "Start and end times cannot be inversed!");
        assert!(result.duration.as_micros() > 0, "Duration should not be <= 0!");
        assert_ne!(result.result_json, ""); 
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
  #[case("minheap_iat_bypass")]
  #[case("minheap_ed_bypass")]
  #[case("minheap_bypass")]
  #[case("minheap_iat")]
  #[case("minheap_ed")]
  #[case("none")]
  #[case("cold_pri")]
  #[case("scaling")]
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn invocation_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;

    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: vec![SupportedCompute::Cpu.into()],
      isolate: vec![SupportedIsolation::Containerd.into()],
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
  #[case("minheap_iat_bypass")]
  #[case("minheap_ed_bypass")]
  #[case("minheap_bypass")]
  #[case("minheap_iat")]
  #[case("minheap_ed")]
  #[case("none")]
  #[case("cold_pri")]
  #[case("scaling")]
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn cold_start_works(#[case] invoker_q: &str) {
    let env = build_env(invoker_q);
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: vec![SupportedCompute::Cpu.into()],
      isolate: vec![SupportedIsolation::Containerd.into()],
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
                  if count > 200 {
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

use iluvatar_library::transaction::TransactionId;
use time::OffsetDateTime;
use tokio::task::JoinHandle;
type HANDLE = JoinHandle<Result<std::sync::Arc<parking_lot::Mutex<iluvatar_worker_library::services::invocation::invoker_structs::InvocationResult>>, anyhow::Error>>;

async fn get_start_end_time_from_invoke(handle: HANDLE, formatter: &ContainerTimeFormatter) -> (OffsetDateTime, OffsetDateTime) {
  let result = timeout(Duration::from_secs(20), handle).await
                                                .unwrap_or_else(|e| panic!("Error joining invocation thread handle: {:?}", e))
                                                .unwrap_or_else(|e| panic!("Error joining invocation thread handle: {:?}", e));
  match result {
    Ok( result_ptr ) => {
      let result = result_ptr.lock();
      let worker_result = result.worker_result.as_ref().unwrap_or_else(|| panic!("worker_result should have been set on '{:?}'", result));
      let parsed_start = formatter.parse_python_container_time(&worker_result.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.start, e));
      let parsed_end = formatter.parse_python_container_time(&worker_result.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.end, e));
      assert!(parsed_start < parsed_end, "Start and end times cannot be inversed!");
      assert!(result.duration.as_micros() > 0, "Duration should not be <= 0!");
      assert_ne!(result.result_json, "", "result_json should not be empty!");
      return (parsed_start, parsed_end);
    },
    Err(e) => panic!("Invocation failed: {}", e),
  }
}

fn test_invoke(invok_svc: &Arc<dyn Invoker>, function_name: &String, function_version: &String, json_args: &String, transaction_id: &TransactionId) -> HANDLE {
  let cln = invok_svc.clone();
  let f = function_name.clone();
  let v = function_version.clone();
  let j = json_args.clone();
  let t = transaction_id.clone();
  tokio::spawn(async move { cln.sync_invocation(f, v, j, t).await })
}

async fn prewarm(cm: &Arc<ContainerManager>, function_name: &String, function_version: &String, image: &String, transaction_id: &TransactionId) {
  let input = PrewarmRequest {
    function_name: function_name.clone(),
    function_version: function_version.clone(),
    cpu: 1,
    memory: 512,
    image_name: image.clone(),
    transaction_id: transaction_id.clone(),
  };
  timeout(Duration::from_secs(20), cm.prewarm(&input)).await
      .unwrap_or_else(|e| panic!("prewarm timout hit: {:?}", e))
      .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
}

#[cfg(test)]
mod fcfs_tests {
  use super::*;

  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn no_reordering() {
    let env = build_env("fcfs");
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let json_args = "{\"name\":\"TESTING\"}".to_string();
    let transaction_id = "testTID".to_string();
    let function_name = "test".to_string();
    let function_version = "0.1.1".to_string();
    let input = RegisterRequest {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: transaction_id.clone(),
      language: LanguageRuntime::Nolang.into(),
      compute: vec![SupportedCompute::Cpu.into()],
      isolate: vec![SupportedIsolation::Containerd.into()],
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let formatter = ContainerTimeFormatter::new().unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
    let first_invoke = test_invoke(&invok_svc, &function_name, &function_version, &json_args, &transaction_id);
    tokio::time::sleep(Duration::from_micros(10)).await;
    let second_invoke = test_invoke(&invok_svc, &function_name, &function_version, &json_args, &transaction_id);
    tokio::time::sleep(Duration::from_micros(10)).await;
    let third_invoke = test_invoke(&invok_svc, &function_name, &function_version, &json_args, &transaction_id);
    let (first_t, _) = get_start_end_time_from_invoke(first_invoke, &formatter).await;
    let (second_t, _) = get_start_end_time_from_invoke(second_invoke, &formatter).await;
    let (third_t, _) = get_start_end_time_from_invoke(third_invoke, &formatter).await;
    assert!(first_t < second_t, "First invoke did not start before second {} !< {}", first_t, second_t);
    assert!(second_t < third_t, "Second invoke did not start before third {} !< {}", second_t, third_t);
    clean_env(&env);
  }
}

#[cfg(test)]
mod minheap_tests {
  use super::*;
  
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn fast_put_first() {
    let env = build_env("minheap");
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let json_args = "{\"name\":\"TESTING\"}".to_string();
    let transaction_id = "testTID".to_string();
    let fast_name = "fast_test".to_string();
    let slow_name = "slow_test".to_string();
    let function_version = "0.1.1".to_string();
    let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
    let slow_img = "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest".to_string();

    prewarm(&cm, &fast_name, &function_version, &fast_img, &transaction_id).await;
    prewarm(&cm, &fast_name, &function_version, &fast_img, &transaction_id).await;
    prewarm(&cm, &slow_name, &function_version, &slow_img, &transaction_id).await;
    prewarm(&cm, &slow_name, &function_version, &slow_img, &transaction_id).await;

    let formatter = ContainerTimeFormatter::new().unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

    // warm exec time cache
    let first_invoke = test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &transaction_id);
    let second_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    get_start_end_time_from_invoke(first_invoke, &formatter).await;
    get_start_end_time_from_invoke(second_invoke, &formatter).await;

    let first_slow_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    tokio::time::sleep(Duration::from_micros(100)).await;
    let second_slow_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    let fast_invoke = test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &transaction_id);
    let (t1, _) = get_start_end_time_from_invoke(first_slow_invoke, &formatter).await;
    let (t2, _) = get_start_end_time_from_invoke(second_slow_invoke, &formatter).await;
    let (t3, _) = get_start_end_time_from_invoke(fast_invoke, &formatter).await;
    assert!(t1 < t2, "second invoke was out of order: {} !< {}", t1, t2);
    assert!(t1 < t3, "third invoke was out of order: {} !< {}", t1, t3);
    assert!(t3 < t2, "Fast invoke should not have been reordered to after slow: {} !< {}", t3, t2);

    clean_env(&env);
  }

  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn fast_not_moved() {
    let env = build_env("minheap");
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let json_args = "{\"name\":\"TESTING\"}".to_string();
    let transaction_id = "testTID".to_string();
    let fast_name = "fast_test".to_string();
    let slow_name = "slow_test".to_string();
    let function_version = "0.1.1".to_string();
    let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
    let slow_img = "docker.io/alfuerst/video_processing-iluvatar-action:latest".to_string();

    prewarm(&cm, &fast_name, &function_version, &fast_img, &transaction_id).await;
    prewarm(&cm, &fast_name, &function_version, &fast_img, &transaction_id).await;
    prewarm(&cm, &slow_name, &function_version, &slow_img, &transaction_id).await;
    prewarm(&cm, &slow_name, &function_version, &slow_img, &transaction_id).await;

    let formatter = ContainerTimeFormatter::new().unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

    // warm exec time cache
    get_start_end_time_from_invoke(test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &transaction_id), &formatter).await;
    // get_start_end_time_from_invoke(test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &transaction_id), &formatter).await;
    // get_start_end_time_from_invoke(test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id), &formatter).await;
    get_start_end_time_from_invoke(test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id), &formatter).await;

    let first_slow_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    tokio::time::sleep(Duration::from_micros(10)).await;
    let fast_invoke = test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &transaction_id);
    let second_slow_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    let (t1, _) = get_start_end_time_from_invoke(first_slow_invoke, &formatter).await;
    let (t2, _) = get_start_end_time_from_invoke(fast_invoke, &formatter).await;
    let (t3, _) = get_start_end_time_from_invoke(second_slow_invoke, &formatter).await;
    assert!(t1 < t2, "second invoke was out of order: {} !< {}", t1, t2);
    assert!(t1 < t3, "third invoke was out of order: {} !< {}", t1, t3);
    assert!(t2 < t3, "Fast invoke should not have been reordered to after slow: {} !< {}", t2, t3);

    clean_env(&env);
  }
}

#[cfg(test)]
mod fcfs_bypass_tests {
  use std::time::SystemTime;

use super::*;
  
  #[ignore="Must be run serially because of env var clashing"]
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn fast_bypass_limits() {
    let env = build_env("fcfs_bypass");
    let (_log, _cfg, cm, invok_svc) = test_invoker_svc(None, Some(&env), None).await;
    let json_args = "{\"name\":\"TESTING\"}".to_string();
    let transaction_id = "testTID".to_string();
    let fast_name = "fast_test".to_string();
    let slow_name = "slow_test".to_string();
    let function_version = "0.1.1".to_string();
    let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
    let slow_img = "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest".to_string();

    prewarm(&cm, &fast_name, &function_version, &fast_img, &transaction_id).await;
    prewarm(&cm, &fast_name, &function_version, &fast_img, &transaction_id).await;
    prewarm(&cm, &slow_name, &function_version, &slow_img, &transaction_id).await;
    prewarm(&cm, &slow_name, &function_version, &slow_img, &transaction_id).await;

    let formatter = ContainerTimeFormatter::new().unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

    // warm exec time cache
    let first_invoke = test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &"fastId".to_string());
    let second_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &"slowID".to_string());
    get_start_end_time_from_invoke(first_invoke, &formatter).await;
    get_start_end_time_from_invoke(second_invoke, &formatter).await;

    let first_slow_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    tokio::time::sleep(Duration::from_micros(100)).await;
    assert_eq!(invok_svc.running_funcs(), 1, "Should have one thing running");
    let second_slow_invoke = test_invoke(&invok_svc, &slow_name, &function_version, &json_args, &transaction_id);
    assert_eq!(invok_svc.running_funcs(), 1, "Should have one thing running");
    let start = SystemTime::now();
    let fast_invoke = test_invoke(&invok_svc, &fast_name, &function_version, &json_args, &transaction_id);
    let mut found = false;
    while start.elapsed().expect("Time elapsed failed") < Duration::from_secs(4) {
      if invok_svc.running_funcs() > 1 {
        found = true;
        break;
      }
    }
    let (t1_s, t1_e) = get_start_end_time_from_invoke(first_slow_invoke, &formatter).await;
    let (t2_s, t2_e) = get_start_end_time_from_invoke(fast_invoke, &formatter).await;
    let (t3_s, t3_e) = get_start_end_time_from_invoke(second_slow_invoke, &formatter).await;
    assert!(t1_s < t3_s, "third invoke was out of order: {} !< {}", t1_s, t3_s);
    assert!(t3_e > t1_e, "second slow invoke started before first finished: {} !> {}", t3_e, t1_e);
    assert!(t2_s < t3_s, "Fast invoke should not have been reordered to start before slow: {} !< {}", t2_s, t3_s);
    assert!(t2_e < t3_e, "Fast invoke should have finished before slow: {} !< {}", t2_e, t3_e);
    assert!(found, "`found` was never 2");

    clean_env(&env);
  }
}
