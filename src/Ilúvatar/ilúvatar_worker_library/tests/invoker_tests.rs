#[macro_use]
pub mod utils;

use iluvatar_library::transaction::TEST_TID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_worker_library::rpc::{
    ContainerState, InvokeAsyncRequest, InvokeRequest, LanguageRuntime, RegisterRequest,
};
use iluvatar_worker_library::services::containers::structs::ContainerTimeFormatter;
use rstest::rstest;
use std::time::{Duration, SystemTime};
use utils::{
    background_test_invoke, cust_register, get_start_end_time_from_invoke, prewarm, register, test_invoker_svc,
};

fn build_overrides(invoker_q: &str) -> Vec<(String, String)> {
    let mut r = vec![];
    r.push(("invocation.queue_policy".to_string(), invoker_q.to_string()));
    r.push(("invocation.concurrency_update_check_ms".to_string(), "1000".to_string()));
    r.push(("invocation.max_load".to_string(), "10".to_string()));
    r.push(("invocation.max_concurrency".to_string(), "10".to_string()));
    r
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
        language: LanguageRuntime::Nolang.into(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::CONTAINERD.bits(),
        resource_timings_json: "".to_string(),
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
        let env = build_overrides(invoker_q);
        let (_log, _cfg, cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest", "test"),
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
            }
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
        let env = build_overrides(invoker_q);
        let (_log, _cfg, _cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest", "test"),
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
            }
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
    #[case("minheap_iat")]
    #[case("minheap_ed")]
    #[case("none")]
    #[case("cold_pri")]
    #[case("scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invocation_works(#[case] invoker_q: &str) {
        let env = build_overrides(invoker_q);
        let (_log, _cfg, cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest", "test"),
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
        let start = std::time::Instant::now();
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
                            } else {
                                if result.json_result == "{ \"Error\": \"Invocation not found\" }"
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
                            }
                        }
                        Err(e) => panic!("Async invocation check failed: {}", e),
                    }
                }
            }
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
        let env = build_overrides(invoker_q);
        let (_log, _cfg, _cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let reg = _reg
            .register(
                basic_reg_req("docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest", "test"),
                &TEST_TID,
            )
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let req = InvokeAsyncRequest {
            function_name: "test".to_string(),
            function_version: "0.1.1".to_string(),
            json_args: "{\"name\":\"TESTING\"}".to_string(),
            transaction_id: "testTID".to_string(),
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
                            } else {
                                if result.json_result == "{ \"Error\": \"Invocation not found\" }"
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
                            }
                        }
                        Err(e) => panic!("Async invocation check failed: {}", e),
                    }
                }
            }
            Err(e) => panic!("Async invocation failed: {}", e),
        }
    }
}

#[cfg(test)]
mod fcfs_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_reordering() {
        let env = build_overrides("fcfs");
        let (_log, _cfg, _cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let json_args = "{\"name\":\"TESTING\"}".to_string();
        let transaction_id = "testTID".to_string();
        let function_name = "test".to_string();

        let func_reg = register(
            &_reg,
            &"docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest".to_string(),
            &function_name,
            &transaction_id,
        )
        .await;
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let first_invoke = background_test_invoke(&invok_svc, &func_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(10)).await;
        let second_invoke = background_test_invoke(&invok_svc, &func_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(10)).await;
        let third_invoke = background_test_invoke(&invok_svc, &func_reg, &json_args, &transaction_id);
        let (first_t, _) = get_start_end_time_from_invoke(first_invoke, &formatter).await;
        let (second_t, _) = get_start_end_time_from_invoke(second_invoke, &formatter).await;
        let (third_t, _) = get_start_end_time_from_invoke(third_invoke, &formatter).await;
        assert!(
            first_t < second_t,
            "First invoke did not start before second {} !< {}",
            first_t,
            second_t
        );
        assert!(
            second_t < third_t,
            "Second invoke did not start before third {} !< {}",
            second_t,
            third_t
        );
    }
}

#[cfg(test)]
mod minheap_tests {
    use super::*;
    use crate::utils::test_invoke;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_put_first() {
        let env = build_overrides("minheap");
        let (_log, _cfg, cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let json_args = "{\"name\":\"TESTING\"}".to_string();
        let transaction_id = "testTID".to_string();
        let fast_name = "fast_test".to_string();
        let slow_name = "slow_test".to_string();
        let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
        let slow_img = "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest".to_string();

        let fast_reg = register(&_reg, &fast_img, &fast_name, &transaction_id).await;
        let slow_reg = cust_register(&_reg, &slow_img, &slow_name, 512, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;

        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

        // warm exec time cache
        test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id).await;
        test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id).await;
        test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id).await;
        test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id).await;

        let first_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(100)).await;
        let second_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        let fast_invoke = background_test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id);
        let (t1, _) = get_start_end_time_from_invoke(first_slow_invoke, &formatter).await;
        let (t2, _) = get_start_end_time_from_invoke(second_slow_invoke, &formatter).await;
        let (t3, _) = get_start_end_time_from_invoke(fast_invoke, &formatter).await;
        assert!(t1 < t2, "second invoke was out of order: {} !< {}", t1, t2);
        assert!(t1 < t3, "third invoke was out of order: {} !< {}", t1, t3);
        assert!(
            t3 < t2,
            "Fast invoke should not have been reordered to after slow: {} !< {}",
            t3,
            t2
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_not_moved() {
        let env = build_overrides("minheap");
        let (_log, _cfg, cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let json_args = "{\"name\":\"TESTING\"}".to_string();
        let transaction_id = "testTID".to_string();
        let fast_name = "fast_test".to_string();
        let slow_name = "slow_test".to_string();
        let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
        let slow_img = "docker.io/alfuerst/video_processing-iluvatar-action:latest".to_string();

        let fast_reg = register(&_reg, &fast_img, &fast_name, &transaction_id).await;
        let slow_reg = cust_register(&_reg, &slow_img, &slow_name, 512, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;

        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

        // warm exec time cache
        get_start_end_time_from_invoke(
            background_test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id),
            &formatter,
        )
        .await;
        // get_start_end_time_from_invoke(background_test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id), &formatter).await;
        // get_start_end_time_from_invoke(background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id), &formatter).await;
        get_start_end_time_from_invoke(
            background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id),
            &formatter,
        )
        .await;

        let first_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(10)).await;
        let fast_invoke = background_test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id);
        let second_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        let (t1, _) = get_start_end_time_from_invoke(first_slow_invoke, &formatter).await;
        let (t2, _) = get_start_end_time_from_invoke(fast_invoke, &formatter).await;
        let (t3, _) = get_start_end_time_from_invoke(second_slow_invoke, &formatter).await;
        assert!(t1 < t2, "second invoke was out of order: {} !< {}", t1, t2);
        assert!(t1 < t3, "third invoke was out of order: {} !< {}", t1, t3);
        assert!(
            t2 < t3,
            "Fast invoke should not have been reordered to after slow: {} !< {}",
            t2,
            t3
        );
    }
}

fn build_bypass_overrides(invoker_q: &str) -> Vec<(String, String)> {
    let mut r = vec![];
    r.push(("invocation.queue_policy".to_string(), invoker_q.to_string()));
    r.push(("invocation.bypass_duration_ms".to_string(), "20".to_string()));
    r.push(("invocation.concurrency_update_check_ms".to_string(), "1000".to_string()));
    r.push(("invocation.max_load".to_string(), "10".to_string()));
    r.push(("invocation.max_concurrency".to_string(), "10".to_string()));
    r
}

#[cfg(test)]
mod bypass_tests {
    use super::*;

    #[rstest]
    #[case("fcfs")]
    #[case("minheap")]
    #[case("minheap_iat")]
    #[case("minheap_ed")]
    #[case("cold_pri")]
    #[case("scaling")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_bypass_limits(#[case] invoker_q: &str) {
        let env = build_bypass_overrides(invoker_q);
        let (_log, _cfg, cm, invok_svc, _reg) = test_invoker_svc(None, Some(env), None).await;
        let json_args = "{\"name\":\"TESTING\"}".to_string();
        let transaction_id = "testTID".to_string();
        let fast_name = "fast_test".to_string();
        let slow_name = "slow_test".to_string();
        let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
        let slow_img = "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest".to_string();

        let fast_reg = register(&_reg, &fast_img, &fast_name, &transaction_id).await;
        let slow_reg = cust_register(&_reg, &slow_img, &slow_name, 512, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;

        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

        // warm exec time cache
        let first_invoke = background_test_invoke(&invok_svc, &fast_reg, &json_args, &"fastId".to_string());
        let second_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &"slowID".to_string());
        get_start_end_time_from_invoke(first_invoke, &formatter).await;
        get_start_end_time_from_invoke(second_invoke, &formatter).await;

        let first_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(100)).await;
        assert_eq!(invok_svc.running_funcs(), 1, "Should have one thing running");
        let second_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        assert_eq!(invok_svc.running_funcs(), 1, "Should have one thing running");
        let start = SystemTime::now();
        let fast_invoke = background_test_invoke(&invok_svc, &fast_reg, &json_args, &transaction_id);
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
        assert!(
            t3_e > t1_e,
            "second slow invoke started before first finished: {} !> {}",
            t3_e,
            t1_e
        );
        assert!(
            t2_s < t3_s,
            "Fast invoke should not have been reordered to start before slow: {} !< {}",
            t2_s,
            t3_s
        );
        assert!(
            t2_e < t3_e,
            "Fast invoke should have finished before slow: {} !< {}",
            t2_e,
            t3_e
        );
        assert!(found, "`found` was never 2");
    }
}
