#[macro_use]
pub mod utils;

use crate::utils::{short_sim_args, sim_args, sim_test_services, test_invoke};
use iluvatar_library::transaction::{gen_tid, TEST_TID};
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::containers::structs::ContainerState;
use std::time::Duration;

fn cpu_reg() -> RegisterRequest {
    RegisterRequest {
        function_name: gen_tid(),
        function_version: "test".to_string(),
        cpus: 1,
        memory: 128,
        parallel_invokes: 1,
        image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
        transaction_id: "testTID".to_string(),
        compute: Compute::CPU.bits(),
        isolate: Isolation::DOCKER.bits(),
        ..Default::default()
    }
}

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

fn build_gpu_env() -> Vec<(String, String)> {
    vec![
        ("container_resources.gpu_resource.count".to_string(), "1".to_string()),
        (
            "container_resources.gpu_resource.memory_mb".to_string(),
            "1024".to_string(),
        ),
    ]
}

#[cfg(test)]
mod container_available {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_prewarm_available_after_prewarm() {
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
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
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
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
        let (_log, _cfg, cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
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
        let (_log, _cfg, cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;
        assert_eq!(cm.container_available(&func.fqdn, Compute::GPU), ContainerState::Warm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_nothing_cold() {
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_available(&func.fqdn, Compute::CPU), ContainerState::Cold);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_nothing_cold() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
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
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
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
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
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
        let (_log, _cfg, cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
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
        let (_log, _cfg, cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;

        assert_eq!(cm.container_exists(&func.fqdn, Compute::GPU), ContainerState::Warm);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_nothing_cold() {
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_exists(&func.fqdn, Compute::CPU), ContainerState::Cold);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_nothing_cold() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        assert_eq!(cm.container_exists(&func.fqdn, Compute::GPU), ContainerState::Cold);
    }
}

#[cfg(test)]
mod container_ordering {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_lru_two_containers_ordered() {
        let env = vec![("container_resources.pool_freq_ms".to_string(), "500".to_string())];
        let (_log, cfg, cm, invoker, reg_svc, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let reg1 = cpu_reg();
        let reg2 = cpu_reg();
        let func1 = reg_svc
            .register(reg1.clone(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let func2 = reg_svc
            .register(reg2.clone(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func1, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        test_invoke(&invoker, &func2, sim_args().unwrap().as_str(), &"invoke2".to_string()).await;
        tokio::time::sleep(Duration::from_millis(cfg.container_resources.pool_freq_ms * 2)).await;

        let list = cm.prioritized_list.read();
        assert_eq!(list.len(), 2);
        for i in 0..(list.len() - 1) {
            let t1 = list[i].last_used();
            let t2 = list[i + 1].last_used();

            assert!(t1 < t2, "{:?} !< {:?}", t1, t2);
        }
        assert_eq!(list[0].function().fqdn, func1.fqdn);
        assert_eq!(list[1].function().fqdn, func2.fqdn);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_lru_many_containers_ordered() {
        let mut env = vec![("container_resources.pool_freq_ms".to_string(), "500".to_string())];
        env.push(("container_resources.memory_mb".to_string(), "100000".to_string()));
        let (_log, cfg, cm, invoker, reg_svc, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        for _ in 0..10 {
            let reg = cpu_reg();
            let func = reg_svc
                .register(reg.clone(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            test_invoke(
                &invoker,
                &func,
                short_sim_args().unwrap().as_str(),
                &"invoke1".to_string(),
            )
            .await;
        }
        tokio::time::sleep(Duration::from_millis(cfg.container_resources.pool_freq_ms * 2)).await;

        let list = cm.prioritized_list.read();
        assert_eq!(list.len(), 10);
        for i in 0..(list.len() - 1) {
            let t1 = list[i].last_used();
            let t2 = list[i + 1].last_used();
            assert!(t1 < t2, "{:?} !< {:?}", t1, t2);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_lru_two_containers_ordered() {
        let mut env = build_gpu_env();
        env.push(("container_resources.pool_freq_ms".to_string(), "500".to_string()));
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "16".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        let (_log, cfg, cm, invoker, reg_svc, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let reg1 = gpu_reg();
        let reg2 = gpu_reg();
        let func1 = reg_svc
            .register(reg1.clone(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let func2 = reg_svc
            .register(reg2.clone(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        test_invoke(&invoker, &func1, sim_args().unwrap().as_str(), &"invoke1".to_string()).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        test_invoke(&invoker, &func2, sim_args().unwrap().as_str(), &"invoke2".to_string()).await;
        tokio::time::sleep(Duration::from_millis(cfg.container_resources.pool_freq_ms * 2)).await;

        let list = cm.prioritized_gpu_list.read();
        assert_eq!(list.len(), 2);
        for i in 0..(list.len() - 1) {
            let t1 = list[i].last_used();
            let t2 = list[i + 1].last_used();

            assert!(t1 < t2, "{:?} !< {:?}", t1, t2);
        }
        assert_eq!(list[0].function().fqdn, func1.fqdn);
        assert_eq!(list[1].function().fqdn, func2.fqdn);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_lru_many_containers_ordered() {
        let mut env = build_gpu_env();
        env.push(("container_resources.pool_freq_ms".to_string(), "500".to_string()));
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "16".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        env.push(("container_resources.memory_mb".to_string(), "100000".to_string()));
        let (_log, cfg, cm, invoker, reg_svc, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        for _ in 0..10 {
            let reg = gpu_reg();
            let func = reg_svc
                .register(reg.clone(), &TEST_TID)
                .await
                .unwrap_or_else(|e| panic!("Registration failed: {}", e));
            test_invoke(
                &invoker,
                &func,
                short_sim_args().unwrap().as_str(),
                &"invoke".to_string(),
            )
            .await;
        }
        tokio::time::sleep(Duration::from_millis(cfg.container_resources.pool_freq_ms * 2)).await;

        let list = cm.prioritized_gpu_list.read();
        assert_eq!(list.len(), 10);
        for i in 0..(list.len() - 1) {
            let t1 = list[i].last_used();
            let t2 = list[i + 1].last_used();
            assert!(t1 < t2, "{:?} !< {:?}", t1, t2);
        }
    }
}
