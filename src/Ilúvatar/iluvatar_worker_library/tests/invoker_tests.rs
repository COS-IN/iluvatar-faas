#[macro_use]
pub mod utils;

use iluvatar_library::clock::{now, ContainerTimeFormatter};
use iluvatar_library::transaction::TEST_TID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::{ContainerState, InvokeAsyncRequest, InvokeRequest, RegisterRequest};
use rstest::rstest;
use std::time::Duration;
use utils::test_invoker_svc;

/// Only 1 CPU to force serial execution
fn build_serial_overrides(invoker_q: &str) -> Vec<(String, String)> {
    vec![
        ("invocation.queue_policies.cpu".to_string(), invoker_q.to_string()),
        (
            "container_resources.cpu_resource.concurrency_update_check_ms".to_string(),
            "0".to_string(),
        ),
        ("container_resources.cpu_resource.max_load".to_string(), "1".to_string()),
        (
            "container_resources.cpu_resource.max_oversubscribe".to_string(),
            "1".to_string(),
        ),
        ("container_resources.cpu_resource.count".to_string(), "1".to_string()),
    ]
}
fn basic_reg_req(image: &str, name: &str) -> RegisterRequest {
    RegisterRequest {
        function_name: name.to_string(),
        function_version: "0.1.1".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: image.to_string(),
        transaction_id: "testTID".to_string(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        ..Default::default()
    }
}

#[cfg(test)]
mod invoke {
    use super::*;

    #[rstest]
    #[case("fcfs")]
    #[case("minheap")]
    #[case("minheap_iat")]
    #[case("minheap_ed")]
    #[case("none")]
    #[case("cold_pri")]
    #[case("scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invocation_works(#[case] invoker_q: &str) {
        let env = build_serial_overrides(invoker_q);
        let (_log, _cfg, cm, invok_svc, _reg, _, _) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/hello-iluvatar-action:latest", "test"),
                &TEST_TID,
            )
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let req = InvokeRequest {
            function_name: "test".to_string(),
            function_version: "0.1.1".to_string(),
            json_args: "{\"name\":\"TESTING\"}".to_string(),
            transaction_id: "testTID".to_string(),
        };
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let result = invok_svc.sync_invocation(reg, req.json_args, req.transaction_id).await;
        match result {
            Ok(result_ptr) => {
                let result = result_ptr.lock();
                let worker_result = result
                    .worker_result
                    .as_ref()
                    .unwrap_or_else(|| panic!("worker_result should have been set"));
                let parsed_start = formatter
                    .parse_python_container_time(&worker_result.start)
                    .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.start, e));
                let parsed_end = formatter
                    .parse_python_container_time(&worker_result.end)
                    .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.end, e));
                assert!(parsed_start < parsed_end, "Start and end times cannot be inversed!");
                assert!(result.duration.as_micros() > 0, "Duration should not be <= 0!");
                assert_ne!(result.result_json, "");
                assert_eq!(result.compute, Compute::CPU);
                assert_eq!(result.container_state, ContainerState::Prewarm);
            },
            Err(e) => panic!("Invocation failed: {}", e),
        }
    }

    #[rstest]
    #[case("fcfs")]
    #[case("minheap")]
    #[case("minheap_iat")]
    #[case("minheap_ed")]
    #[case("none")]
    #[case("cold_pri")]
    #[case("scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_start_works(#[case] invoker_q: &str) {
        let env = build_serial_overrides(invoker_q);
        let (_log, _cfg, _cm, invok_svc, _reg, _, _) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/hello-iluvatar-action:latest", "test"),
                &TEST_TID,
            )
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let req = InvokeRequest {
            function_name: "test".to_string(),
            function_version: "0.1.1".to_string(),
            json_args: "{\"name\":\"TESTING\"}".to_string(),
            transaction_id: "testTID".to_string(),
        };
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let result = invok_svc.sync_invocation(reg, req.json_args, req.transaction_id).await;
        match result {
            Ok(result_ptr) => {
                let result = result_ptr.lock();
                let worker_result = result
                    .worker_result
                    .as_ref()
                    .unwrap_or_else(|| panic!("worker_result should have been set; json: {}", result.result_json));
                let parsed_start = formatter
                    .parse_python_container_time(&worker_result.start)
                    .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.start, e));
                let parsed_end = formatter
                    .parse_python_container_time(&worker_result.end)
                    .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", worker_result.end, e));
                assert!(parsed_start < parsed_end, "Start and end times cannot be inversed!");
                assert!(result.duration.as_micros() > 0, "Duration should not be <= 0!");
                assert_ne!(result.result_json, "");
                assert_eq!(result.compute, Compute::CPU);
                assert_eq!(result.container_state, ContainerState::Cold);
            },
            Err(e) => panic!("Invocation failed: {}", e),
        }
    }
}

#[cfg(test)]
mod invoke_async {
    use super::*;
    use iluvatar_library::transaction::gen_tid;

    #[rstest]
    #[case("fcfs")]
    #[case("minheap")]
    #[case("minheap_iat")]
    #[case("minheap_ed")]
    #[case("none")]
    #[case("cold_pri")]
    #[case("scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invocation_works(#[case] invoker_q: &str) {
        let env = build_serial_overrides(invoker_q);
        let (_log, _cfg, cm, invok_svc, _reg, _, _) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/hello-iluvatar-action:latest", "test"),
                &TEST_TID,
            )
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let req = InvokeAsyncRequest {
            function_name: "test".to_string(),
            function_version: "0.1.1".to_string(),
            json_args: "{\"name\":\"TESTING\"}".to_string(),
            transaction_id: "testTID".to_string(),
        };
        let result = invok_svc.async_invocation(reg, req.json_args, req.transaction_id);
        let start = now();
        match result {
            Ok(cookie) => {
                assert_ne!(cookie, "");
                loop {
                    let result = invok_svc.invoke_async_check(&cookie, &"testTID".to_string());
                    match result {
                        Ok(result) => {
                            if result.success {
                                assert_ne!(result.json_result, "");
                                assert!(result.duration_us > 0, "Duration should not be <= 0!");
                                break;
                            } else if result.json_result == "{ \"Error\": \"Invocation not found\" }"
                                || result.json_result == "{ \"Error\": \"No result was captured\" }"
                            {
                                panic!("Async invocation check after check: {:?}", result);
                            } else if result.json_result == "{ \"Status\": \"Invocation not completed\" }" {
                                // keep waiting on invocation
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                if start.elapsed() > Duration::from_secs(10) {
                                    panic!(
                                        "Waited too long for invocation result; cookie: {}; response: {:?}",
                                        cookie, result
                                    );
                                }
                                continue;
                            } else {
                                panic!("unkonwn response from async invocation check: {:?}", result);
                            }
                        },
                        Err(e) => panic!("Async invocation check failed: {}", e),
                    }
                }
            },
            Err(e) => panic!("Async invocation failed: {}", e),
        }
    }

    #[rstest]
    #[case("fcfs")]
    #[case("minheap")]
    #[case("minheap_iat")]
    #[case("minheap_ed")]
    #[case("none")]
    #[case("cold_pri")]
    #[case("scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_start_works(#[case] invoker_q: &str) {
        let env = build_serial_overrides(invoker_q);
        let tid = gen_tid();
        let (_log, _cfg, _cm, invok_svc, _reg, _, _) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/hello-iluvatar-action:latest", "test"),
                &tid,
            )
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let req = InvokeAsyncRequest {
            function_name: "test".to_string(),
            function_version: "0.1.1".to_string(),
            json_args: "{\"name\":\"TESTING\"}".to_string(),
            transaction_id: tid,
        };
        let result = invok_svc.async_invocation(reg, req.json_args, req.transaction_id);
        let mut count = 0;
        match result {
            Ok(cookie) => {
                assert_ne!(cookie, "");
                loop {
                    let result = invok_svc.invoke_async_check(&cookie, &"testTID".to_string());
                    match result {
                        Ok(result) => {
                            if result.success {
                                assert_ne!(result.json_result, "");
                                assert!(result.duration_us > 0, "Duration should not be <= 0!");
                                break;
                            } else if result.json_result == "{ \"Error\": \"Invocation not found\" }"
                                || result.json_result == "{ \"Error\": \"No result was captured\" }"
                            {
                                panic!("Async invocation check after check: {:?}", result);
                            } else if result.json_result == "{ \"Status\": \"Invocation not completed\" }" {
                                // keep waiting on invocation
                                tokio::time::sleep(Duration::from_millis(100)).await;
                                count += 1;
                                if count > 200 {
                                    panic!(
                                        "Waited too long for invocation result; cookie: {}; response: {:?}",
                                        cookie, result
                                    );
                                }
                                continue;
                            } else {
                                panic!("unkonwn response from async invocation check: {:?}", result);
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
