#[macro_use]
pub mod utils;

use crate::utils::{sim_args, sim_invoker_svc, test_invoke};
use iluvatar_library::transaction::TEST_TID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_worker_library::rpc::{LanguageRuntime, RegisterRequest};
use iluvatar_worker_library::services::containers::structs::ContainerState;

fn cpu_reg() -> RegisterRequest {
    RegisterRequest {
        function_name: "test".to_string(),
        function_version: "test".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
        transaction_id: "testTID".to_string(),
        language: LanguageRuntime::Nolang.into(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::CONTAINERD.bits(),
        resource_timings_json: "".to_string(),
    }
}

fn gpu_reg() -> RegisterRequest {
    RegisterRequest {
        function_name: "test".to_string(),
        function_version: "test".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
        transaction_id: "testTID".to_string(),
        language: LanguageRuntime::Nolang.into(),
        compute: Compute::GPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        resource_timings_json: "".to_string(),
    }
}

fn build_gpu_env() -> Vec<(String, String)> {
    let mut r = vec![];
    r.push((
        "container_resources.resource_map.gpu.count".to_string(),
        "1".to_string(),
    ));
    r
}

#[cfg(test)]
mod container_available {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_prewarm_available_after_prewarm() {
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {}", e));

        assert_eq!(
            cm.container_available(&func.fqdn, Compute::CPU),
            ContainerState::Prewarm
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_prewarm_available_after_prewarm() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {}", e));

        assert_eq!(
            cm.container_available(&func.fqdn, Compute::GPU),
            ContainerState::Prewarm
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_warm_available_after_invoke() {
        let (_log, _cfg, cm, invoker, reg, _cmap) = sim_invoker_svc(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;

        assert_eq!(cm.container_available(&func.fqdn, Compute::CPU), ContainerState::Warm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_warm_available_after_invoke() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, invoker, reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;

        assert_eq!(cm.container_available(&func.fqdn, Compute::GPU), ContainerState::Warm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_nothing_cold() {
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_available(&func.fqdn, Compute::CPU), ContainerState::Cold);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_nothing_cold() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_available(&func.fqdn, Compute::GPU), ContainerState::Cold);
    }
}

#[cfg(test)]
mod container_exists {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_prewarm_available_after_prewarm() {
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {}", e));
        tokio::time::sleep(std::time::Duration::from_micros(100)).await;

        assert_eq!(cm.container_exists(&func.fqdn, Compute::CPU), ContainerState::Prewarm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_prewarm_available_after_prewarm() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {}", e));

        assert_eq!(cm.container_exists(&func.fqdn, Compute::GPU), ContainerState::Prewarm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_warm_available_after_invoke() {
        let (_log, _cfg, cm, invoker, reg, _cmap) = sim_invoker_svc(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;

        assert_eq!(cm.container_exists(&func.fqdn, Compute::CPU), ContainerState::Warm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_warm_available_after_invoke() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, invoker, reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;

        assert_eq!(cm.container_exists(&func.fqdn, Compute::GPU), ContainerState::Warm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_nothing_cold() {
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_exists(&func.fqdn, Compute::CPU), ContainerState::Cold);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_nothing_cold() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap) = sim_invoker_svc(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_exists(&func.fqdn, Compute::GPU), ContainerState::Cold);
    }
}
