#[macro_use]
pub mod utils;

use crate::utils::{short_sim_args, sim_args};
use iluvatar_library::clock::ContainerTimeFormatter;
use iluvatar_library::transaction::TEST_TID;
use rstest::rstest;
use std::time::Duration;
use utils::{
    background_test_invoke, cust_register, get_start_end_time_from_invoke, prewarm, register, sim_test_services,
};

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

#[cfg(test)]
mod fcfs_tests {
    use super::*;
    use crate::utils::sim_args;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn no_reordering() {
        let env = build_serial_overrides("fcfs");
        let (_log, _cfg, _cm, invok_svc, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let json_args = sim_args().unwrap();
        let transaction_id = "testTID".to_string();
        let function_name = "test".to_string();

        let func_reg = register(
            &reg,
            "docker.io/alfuerst/json_dumps_loads-iluvatar-action:latest",
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
    use crate::utils::{sim_args, test_invoke};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fast_put_first() {
        let env = build_serial_overrides("minheap");
        let (_log, _cfg, cm, invok_svc, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let json_args = sim_args().unwrap();
        let short_args = short_sim_args().unwrap();
        let transaction_id = "testTID".to_string();
        let fast_name = "fast_test".to_string();
        let slow_name = "slow_test".to_string();
        let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
        let slow_img = "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest".to_string();

        let fast_reg = register(&reg, &fast_img, &fast_name, &transaction_id).await;
        let slow_reg = cust_register(&reg, &slow_img, &slow_name, 512, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;

        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

        // warm exec time cache
        test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id).await;
        test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id).await;
        test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id).await;
        test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id).await;

        let first_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(100)).await;
        let second_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        let fast_invoke = background_test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id);
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
        let env = build_serial_overrides("minheap");
        let (_log, _cfg, cm, invok_svc, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let json_args = sim_args().unwrap();
        let short_args = short_sim_args().unwrap();
        let transaction_id = "testTID".to_string();
        let fast_name = "fast_test".to_string();
        let slow_name = "slow_test".to_string();
        let fast_img = "docker.io/alfuerst/hello-iluvatar-action:latest".to_string();
        let slow_img = "docker.io/alfuerst/video_processing-iluvatar-action:latest".to_string();

        let fast_reg = register(&reg, &fast_img, &fast_name, &transaction_id).await;
        let slow_reg = cust_register(&reg, &slow_img, &slow_name, 512, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;

        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

        // warm exec time cache
        test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id).await;
        test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id).await;
        test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id).await;
        test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id).await;
        tokio::time::sleep(Duration::from_micros(100)).await;

        let first_slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
        tokio::time::sleep(Duration::from_micros(20)).await;
        let fast_invoke = background_test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id);
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
    vec![
        ("invocation.queue_policies.cpu".to_string(), invoker_q.to_string()),
        ("invocation.bypass_duration_ms".to_string(), "800".to_string()),
        (
            "container_resources.cpu_resource.concurrency_update_check_ms".to_string(),
            "1000".to_string(),
        ),
        (
            "container_resources.cpu_resource.max_load".to_string(),
            "10".to_string(),
        ),
        (
            "container_resources.cpu_resource.max_oversubscribe".to_string(),
            "10".to_string(),
        ),
    ]
}

#[cfg(test)]
mod bypass_tests {
    use super::*;
    use iluvatar_library::clock::now;

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
        let (_log, _cfg, cm, invok_svc, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let json_args = sim_args().unwrap();
        let short_args = short_sim_args().unwrap();
        let transaction_id = "testTID".to_string();

        let fast_reg = register(
            &reg,
            "docker.io/alfuerst/hello-iluvatar-action:latest",
            "fast_test",
            &transaction_id,
        )
        .await;
        let slow_reg = cust_register(
            &reg,
            "docker.io/alfuerst/cnn_image_classification-iluvatar-action:latest",
            "slow_test",
            512,
            &transaction_id,
        )
        .await;
        prewarm(&cm, &fast_reg, &transaction_id).await;
        prewarm(&cm, &slow_reg, &transaction_id).await;

        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));

        let loop_start = now();
        let mut found = false;
        while loop_start.elapsed().as_secs() < 20 {
            let slow_invoke = background_test_invoke(&invok_svc, &slow_reg, &json_args, &transaction_id);
            let fast_invoke = background_test_invoke(&invok_svc, &fast_reg, &short_args, &transaction_id);
            let start = now();
            while start.elapsed() < Duration::from_secs(2) {
                if invok_svc.running_funcs() > 1 {
                    found = true;
                    break;
                }
            }
            let (_t1_s, _t1_e) = get_start_end_time_from_invoke(slow_invoke, &formatter).await;
            let (_t2_s, _t2_e) = get_start_end_time_from_invoke(fast_invoke, &formatter).await;
        }
        assert!(found, "`found` was never 2");
    }
}
