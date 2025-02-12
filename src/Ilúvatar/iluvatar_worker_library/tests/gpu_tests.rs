#[macro_use]
pub mod utils;

use crate::utils::sim_test_services;
use iluvatar_library::transaction::TEST_TID;

fn build_gpu_env(num: u32, memory_mb: u32) -> Vec<(String, String)> {
    vec![
        ("container_resources.gpu_resource.count".to_string(), format!("{}", num)),
        (
            "container_resources.gpu_resource.memory_mb".to_string(),
            format!("{}", memory_mb),
        ),
    ]
}

#[cfg(test)]
mod gpu_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn current_num_physical_gpus() {
        let env = build_gpu_env(2, 1024);
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu.physical_gpus(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn virtual_gpus_created_driver() {
        let mut env = build_gpu_env(2, 1024);
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "16".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu.physical_gpus(), 2);
        assert_eq!(gpu.total_gpus(), 2 * 16);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn virtual_gpus_created_mem() {
        let mut env = build_gpu_env(2, 1024);
        env.push((
            "container_resources.gpu_resource.per_func_memory_mb".to_string(),
            "128".to_string(),
        ));
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu.physical_gpus(), 2);
        assert_eq!(gpu.total_gpus(), 2 * (1024 / 128));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn acquire_gpu_changes_state() {
        let mut env = build_gpu_env(2, 1024);
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "16".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu_svc = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu_svc.physical_gpus(), 2);
        assert_eq!(gpu_svc.total_gpus(), 2 * 16);
        let gpu1 = gpu_svc.acquire_gpu(&TEST_TID);
        assert!(gpu1.is_some());

        let gpu2 = gpu_svc.acquire_gpu(&TEST_TID);
        assert!(gpu2.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_acquire_none_returns_gpu0() {
        let mut env = build_gpu_env(2, 1024);
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "16".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu_svc = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu_svc.physical_gpus(), 2);
        assert_eq!(gpu_svc.total_gpus(), 2 * 16);
        assert_eq!(gpu_svc.outstanding(), 0);
        let gpu = gpu_svc.acquire_gpu(&TEST_TID);
        assert!(gpu.is_some());
        let token = gpu_svc
            .try_acquire_resource(None, &TEST_TID)
            .expect("Should get GPU token");
        assert_eq!(gpu_svc.outstanding(), 1);
        drop(token);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_acquire_returns_given_gpu() {
        let mut env = build_gpu_env(2, 1024);
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "16".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu_svc = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu_svc.physical_gpus(), 2);
        assert_eq!(gpu_svc.total_gpus(), 2 * 16);
        assert_eq!(gpu_svc.outstanding(), 0);
        let gpu = gpu_svc.acquire_gpu(&TEST_TID);
        assert!(gpu.is_some());
        let token = gpu_svc
            .try_acquire_resource(gpu.as_ref(), &TEST_TID)
            .expect("Should get GPU token");
        assert_eq!(gpu_svc.outstanding(), 1);
        drop(token);
        assert_eq!(gpu_svc.outstanding(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn alloc_least_loaded_gpu() {
        let mut env = build_gpu_env(2, 1024);
        env.push((
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "2".to_string(),
        ));
        env.push((
            "container_resources.gpu_resource.use_driver_hook".to_string(),
            "true".to_string(),
        ));
        let (_log, _cfg, _cm, _invoker, _reg_svc, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let gpu_svc = gpu.unwrap_or_else(|| panic!("GPU resource tracker should have been present"));
        assert_eq!(gpu_svc.physical_gpus(), 2);
        assert_eq!(gpu_svc.total_gpus(), 2 * 2);
        assert_eq!(gpu_svc.outstanding(), 0);
        let gpu1 = gpu_svc.acquire_gpu(&TEST_TID).expect("should return a valid GPU");
        let gpu2 = gpu_svc.acquire_gpu(&TEST_TID).expect("should return a valid GPU");
        let gpu3 = gpu_svc.acquire_gpu(&TEST_TID).expect("should return a valid GPU");
        let gpu4 = gpu_svc.acquire_gpu(&TEST_TID).expect("should return a valid GPU");
        assert_eq!(gpu1.gpu_uuid, gpu3.gpu_uuid);
        assert_eq!(gpu2.gpu_uuid, gpu4.gpu_uuid);
    }
}
