#[macro_use]
pub mod utils;

use crate::utils::{sim_args, sim_test_services};
use iluvatar_library::transaction::{gen_tid, TEST_TID};
use iluvatar_library::types::Compute;
use iluvatar_library::types::Isolation;
use iluvatar_rpc::rpc::RegisterRequest;

fn gpu_reg() -> RegisterRequest {
    RegisterRequest {
        function_name: gen_tid(),
        function_version: "test".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
        transaction_id: "testTID".to_string(),
        compute: Compute::GPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        ..Default::default()
    }
}

fn build_gpu_env(overrun: f64, num_gpus: u32) -> Vec<(String, String)> {
    vec![
        (
            "container_resources.gpu_resource.count".to_string(),
            num_gpus.to_string(),
        ),
        (
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "1".to_string(),
        ),
        (
            "container_resources.gpu_resource.use_hook".to_string(),
            "true".to_string(),
        ),
        ("invocation.queues.gpu".to_string(), "concur_mqfq".to_string()),
        (
            "invocation.mqfq_config.allowed_overrun".to_string(),
            format!("{}", overrun),
        ),
    ]
}

// #[cfg(test)]
mod flowq_tests {
    use super::*;
    use crate::utils::{background_test_invoke, resolve_invoke};
    use iluvatar_library::clock::ContainerTimeFormatter;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn insert_set_active() {
        let env = build_gpu_env(20.0, 1);
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::GPU, "Invoke compute must be GPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// run two different funcs on different GPUs
    async fn two_funcs_invokes_split() {
        let formatter = ContainerTimeFormatter::new(&TEST_TID).unwrap();
        let env = build_gpu_env(20.0, 2);
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func1 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let func2 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let args = sim_args().unwrap();
        let invoke1 = background_test_invoke(&invoker, &func1, args.as_str(), &"invoke1".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await; // queue insertion can be kind of racy, sleep to ensure ordering
        let invoke2 = background_test_invoke(&invoker, &func2, args.as_str(), &"invoke2".to_string());

        let r1 = resolve_invoke(invoke1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let r2 = resolve_invoke(invoke2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        assert_eq!(r1.lock().compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(r2.lock().compute, Compute::GPU, "First invoke should run on GPU");

        let r1_lck = r1.lock();
        let r1_res = r1_lck.worker_result.as_ref().unwrap();
        let r1_end = formatter
            .parse_python_container_time(&r1_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r1_res.end, e));

        let r2_lck = r2.lock();
        let r2_res = r2_lck.worker_result.as_ref().unwrap();
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));
        let r2_end = formatter
            .parse_python_container_time(&r2_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.end, e));
        assert!(r1_end < r2_end, "Out of order 1 {} !< {}", r1_end, r2_end);
        assert!(r1_end > r2_start, "Out of order 2 {} !> {}", r1_end, r2_start);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// run two different funcs on different GPUs
    async fn two_funcs_invokes_split_2() {
        let formatter = ContainerTimeFormatter::new(&TEST_TID).unwrap();
        let env = build_gpu_env(20.0, 2);
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func1 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let args = sim_args().unwrap();
        let invoke1 = background_test_invoke(&invoker, &func1, args.as_str(), &"invoke1".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await; // queue insertion can be kind of racy, sleep to ensure ordering
        let func2 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let invoke2 = background_test_invoke(&invoker, &func2, args.as_str(), &"invoke2".to_string());

        let r1 = resolve_invoke(invoke1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let r2 = resolve_invoke(invoke2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        assert_eq!(r1.lock().compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(r2.lock().compute, Compute::GPU, "First invoke should run on GPU");

        let r1_lck = r1.lock();
        let r1_res = r1_lck.worker_result.as_ref().unwrap();
        let r1_end = formatter
            .parse_python_container_time(&r1_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r1_res.end, e));

        let r2_lck = r2.lock();
        let r2_res = r2_lck.worker_result.as_ref().unwrap();
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));
        let r2_end = formatter
            .parse_python_container_time(&r2_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.end, e));
        assert!(r1_end < r2_end, "Out of order 1 {} !< {}", r1_end, r2_end);
        assert!(r1_end > r2_start, "Out of order 2 {} !> {}", r1_end, r2_start);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    /// run same func twice on different GPUs
    async fn concurrent_invokes_split() {
        let formatter = ContainerTimeFormatter::new(&TEST_TID).unwrap();
        let env = build_gpu_env(20.0, 2);
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let args = sim_args().unwrap();
        let invoke1 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke1".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await; // queue insertion can be kind of racy, sleep to ensure ordering
        let invoke2 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke2".to_string());

        let r1 = resolve_invoke(invoke1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let r2 = resolve_invoke(invoke2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        assert_eq!(r1.lock().compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(r2.lock().compute, Compute::GPU, "First invoke should run on GPU");

        let r1_lck = r1.lock();
        let r1_res = r1_lck.worker_result.as_ref().unwrap();
        let r1_end = formatter
            .parse_python_container_time(&r1_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r1_res.end, e));

        let r2_lck = r2.lock();
        let r2_res = r2_lck.worker_result.as_ref().unwrap();
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));
        let r2_end = formatter
            .parse_python_container_time(&r2_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.end, e));
        assert!(r1_end < r2_end, "Out of order 1 {} !< {}", r1_end, r2_end);
        assert!(r1_end > r2_start, "Out of order 2 {} !> {}", r1_end, r2_start);
    }
}
