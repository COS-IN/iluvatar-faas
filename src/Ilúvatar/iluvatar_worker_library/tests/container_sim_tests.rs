#[macro_use]
pub mod utils;

use crate::utils::{background_test_invoke, resolve_invoke, sim_args, sim_test_services, wait_for_queue_len};
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::{threading::EventualItem, transaction::TEST_TID};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::containers::structs::ContainerState;
use rstest::rstest;

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
        isolate: Isolation::CONTAINERD.bits(),
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
            "2048".to_string(),
        ),
        (
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "1".to_string(),
        ),
    ]
}

fn two_gpu_env() -> Vec<(String, String)> {
    vec![
        ("container_resources.gpu_resource.count".to_string(), "2".to_string()),
        (
            "container_resources.gpu_resource.memory_mb".to_string(),
            "2048".to_string(),
        ),
        (
            "container_resources.gpu_resource.funcs_per_device".to_string(),
            "1".to_string(),
        ),
    ]
}

mod compute_iso_matching {
    use super::*;
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_docker_works() {
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_ctr_works() {
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_compute_works() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::CPU);
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::GPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefer_ctd_container() {
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let request = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: (Isolation::DOCKER | Isolation::CONTAINERD).bits(),
            ..Default::default()
        };
        let reg = reg
            .register(request, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cant_request_not_registered_compute_gpu() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(cpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        assert_error!(
            cm.prewarm(&func, &TEST_TID, Compute::GPU).await,
            "Registration did not contain requested compute",
            "Perwarm did not error correctly"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cant_request_not_registered_compute_cpu() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        assert_error!(
            cm.prewarm(&func, &TEST_TID, Compute::CPU).await,
            "Registration did not contain requested compute",
            "Perwarm did not error correctly"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_gpu_fails_register() {
        let (_log, _cfg, _cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg.register(req, &TEST_TID).await;
        assert_error!(
            func,
            "Could not register function for compute GPU because the worker has no devices of that type!",
            ""
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_fpga_fails_register() {
        let (_log, _cfg, _cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, None, None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::FPGA.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg.register(req, &TEST_TID).await;
        assert_error!(
            func,
            "Could not register function for compute FPGA because the worker has no devices of that type!",
            ""
        );
    }
}

#[cfg(test)]
mod gpu {
    use super::*;
    use iluvatar_library::clock::ContainerTimeFormatter;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_container_must_use_docker() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let request = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            ..Default::default()
        };
        let func = reg
            .register(request, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let err = cm.prewarm(&func, &TEST_TID, Compute::GPU).await;
        assert_error!(
            err,
            "GPU only supported with Docker isolation",
            "Prewarm succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_forces_docker() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: (Isolation::DOCKER | Isolation::CONTAINERD).bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
        assert_eq!(c.container.compute_type(), Compute::CPU);
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::GPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn polymorphic_invoke_only_one_compute() {
        let env = build_gpu_env();
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(
            invoke.compute.into_iter().count(),
            1,
            "Invoke compute was {:?}",
            invoke.compute
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_docker_works() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let _c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returned_container_ok() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        let c1_cont = c1.container.clone();
        drop(c1);
        let c2 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert!(c1_cont.is_healthy());
        assert_eq!(c1_cont.container_id(), c2.container.container_id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_not_used() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        let c1_cont = c1.container.clone();
        c1_cont.mark_unhealthy();
        assert!(
            !c1_cont.is_healthy(),
            "Container should not be healthy after being marked"
        );
        drop(c1);
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        let c2 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert!(c2.container.is_healthy(), "New container should be healthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_resource_limiting() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let _c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(_) => panic!("Should have gotten prewarmed container"),
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        let err = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(_) => panic!("Should not have gotten prewarmed container"),
        };
        assert_error!(
            err,
            "No GPU available to launch container",
            "Only one GPU available, can't have two live containers!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_container_removed() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let reg2 = RegisterRequest {
            function_name: "test2".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func2 = reg
            .register(reg2, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration 2 failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let ctr = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(_) => panic!("Should have gotten prewarmed container"),
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("Creating container should have succeeded: {}", e));
        assert_eq!(
            ctr.container.function().function_name,
            func.function_name,
            "Got incorrect container"
        );
        drop(ctr);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let ctr = match cm.acquire_container(&func2, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(_) => panic!("Should not have gotten prewarmed container"),
        }
        .unwrap_or_else(|e| panic!("Creating container should have succeeded: {}", e));

        assert_eq!(
            ctr.container.function().function_name,
            "test2",
            "Got incorrect container"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn multiple_invokes_blocks() {
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let env = build_gpu_env();
        let (_log, _cfg, cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let args = sim_args().unwrap();

        let invoke1 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke1".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await; // queue insertion can be kind of racy, sleep to ensure ordering
        let invoke2 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke2".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let invoke3 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke3".to_string());
        let r1 = resolve_invoke(invoke1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let _ = r1
            .lock()
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 1 '{:?}' did not have a result", *r1.lock()))
            .unwrap();

        let r2 = resolve_invoke(invoke2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));

        assert_eq!(r1.lock().compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(
            r1.lock().container_state,
            ContainerState::Prewarm,
            "Invoke 1 should be prewarm"
        );
        assert!(r1.lock().duration.as_micros() > 0, "Invoke should have duration time");

        let r1_lck = r1.lock();
        let r1_res = r1_lck.worker_result.as_ref().unwrap();

        let r1_end = formatter
            .parse_python_container_time(&r1_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r1_res.end, e));
        let r2_lck = r2.lock();
        let r2_res = r2_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r2_lck))
            .unwrap();
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));

        assert_eq!(r2_lck.compute, Compute::GPU, "Second invoke should run on GPU");
        assert_eq!(r2_lck.container_state, ContainerState::Warm, "Invoke 2 should be warm");
        assert!(r2_lck.duration.as_micros() > 0, "Invoke 2 should have duration time");
        assert!(
            r1_end < r2_start,
            "Invoke 2 should have started {} after invoke 1 ended {}",
            r2_start,
            r1_end
        );

        let r3 = resolve_invoke(invoke3)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let r3_lck = r3.lock();
        let r3_res = r3_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 3 '{:?}' did not have a result", *r3_lck))
            .unwrap();
        let r3_start = formatter
            .parse_python_container_time(&r3_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));
        let r2_end = formatter
            .parse_python_container_time(&r2_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));

        assert_eq!(r3_lck.compute, Compute::GPU, "Third invoke should run on GPU");
        assert_eq!(r3_lck.container_state, ContainerState::Warm, "Invoke 3 should be warm");
        assert!(r3_lck.duration.as_micros() > 0, "Invoke 3 should have duration time");
        assert!(
            r2_end < r3_start,
            "Invoke 3 should have started {} after invoke 2 ended {}",
            r3_start,
            r2_end
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn diff_funcs_multiple_invokes_blocks() {
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let env = build_gpu_env();
        let (_log, _cfg, cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func1 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let r2 = RegisterRequest {
            function_name: "test2".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func2 = reg
            .register(r2, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func1, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let args = sim_args().unwrap();

        let invoke1 = background_test_invoke(&invoker, &func1, args.as_str(), &"invoke1".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await; // queue insertion can be kind of racy, sleep to ensure ordering
        let invoke2 = background_test_invoke(&invoker, &func2, args.as_str(), &"invoke2".to_string());
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let invoke3 = background_test_invoke(&invoker, &func1, args.as_str(), &"invoke3".to_string());
        let r1 = resolve_invoke(invoke1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        r1.lock()
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 1 '{:?}' did not have a result", *r1.lock()))
            .unwrap();

        let r2 = resolve_invoke(invoke2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));

        assert_eq!(r1.lock().compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(
            r1.lock().container_state,
            ContainerState::Prewarm,
            "Invoke 2 should be prewarm"
        );
        assert!(r1.lock().duration.as_micros() > 0, "Invoke should have duration time");

        let r1_end = formatter
            .parse_python_container_time(&r1.lock().worker_result.as_ref().unwrap().end)
            .unwrap_or_else(|e| {
                panic!(
                    "Failed to parse time '{}' because {}",
                    r1.lock().worker_result.as_ref().unwrap().end,
                    e
                )
            });
        let r2_lck = r2.lock();
        let r2_res = r2_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r2_lck))
            .unwrap();
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));

        assert_eq!(r2_lck.compute, Compute::GPU, "Second invoke should run on GPU");
        assert_eq!(r2_lck.container_state, ContainerState::Cold, "Invoke 2 should be cold");
        assert!(r2_lck.duration.as_micros() > 0, "Invoke 2 should have duration time");
        assert!(
            r1_end < r2_start,
            "Invoke 2 should have started {} after invoke 1 ended {}",
            r2_start,
            r1_end
        );

        let r3 = resolve_invoke(invoke3)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let r3_lck = r3.lock();
        let r3_res = r3_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r3_lck))
            .unwrap();
        let r3_start = formatter
            .parse_python_container_time(&r3_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));
        let r2_end = formatter
            .parse_python_container_time(&r2_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));

        assert_eq!(r3_lck.compute, Compute::GPU, "Third invoke should run on GPU");
        assert_eq!(r3_lck.container_state, ContainerState::Cold, "Invoke 3 should be cold");
        assert!(r3_lck.duration.as_micros() > 0, "Invoke 3 should have duration time");
        assert!(
            r2_end < r3_start,
            "Invoke 3 should have started {} after invoke 2 ended {}",
            r3_start,
            r2_end
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_gpu_allowed() {
        let env = two_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let func = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(_) => panic!("Should have gotten prewarmed container"),
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("First acquire container failed: {:?}", e));
        let c2 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(_) => panic!("Should not have gotten prewarmed container"),
        }
        .unwrap_or_else(|e| panic!("Second acquire container failed: {:?}", e));
        assert_ne!(
            c1.container.device_resource().as_ref().unwrap().gpu_uuid,
            c2.container.device_resource().as_ref().unwrap().gpu_uuid,
            "Two containers cannot have same GPU"
        );
    }
}

#[cfg(test)]
mod gpu_queueuing {
    use super::*;
    use iluvatar_library::clock::ContainerTimeFormatter;

    #[rstest]
    #[case("fcfs")]
    #[case("oldest_batch")]
    #[case("sjf")]
    #[case("eedf")]
    #[case("sized_batch")]
    #[case("paella")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn queues_work(#[case] invoker_q: &str) {
        let mut env = build_gpu_env();
        env.push(("invocation.queue_policies.gpu".to_string(), invoker_q.to_string()));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn oldest_queued_batch_run() {
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let mut env = build_gpu_env();
        env.push(("invocation.queue_policies.gpu".to_string(), "oldest_batch".to_string()));
        let (_log, _cfg, _cm, invoker, reg, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        println!("{:?}", _cfg);
        let func1 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let reg2 = RegisterRequest {
            function_name: "test2".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func2 = reg
            .register(reg2, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        let gpu_lck = gpu
            .unwrap_or_else(|| panic!("No GPU resource"))
            .try_acquire_resource(None, &TEST_TID)
            .expect("Should return GPU permit"); // hold GPU to force queueing
        let inv1 = background_test_invoke(&invoker, &func1, &sim_args().unwrap(), &TEST_TID);
        wait_for_queue_len(&invoker, Compute::GPU, 1).await;
        let inv2 = background_test_invoke(&invoker, &func2, &sim_args().unwrap(), &TEST_TID);
        wait_for_queue_len(&invoker, Compute::GPU, 2).await;
        let inv3 = background_test_invoke(&invoker, &func1, &sim_args().unwrap(), &TEST_TID);
        wait_for_queue_len(&invoker, Compute::GPU, 3).await;
        drop(gpu_lck);

        let inv1 = resolve_invoke(inv1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let inv3 = resolve_invoke(inv3)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let inv2 = resolve_invoke(inv2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));

        let r1_lck = inv1.lock();
        let r1_res = r1_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 1 '{:?}' did not have a result", *r1_lck))
            .unwrap();
        assert_eq!(r1_lck.compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(r1_lck.container_state, ContainerState::Cold, "Invoke 1 should be cold");
        assert!(r1_lck.duration.as_micros() > 0, "Invoke 1 should have duration time");

        let r2_lck = inv2.lock();
        let r2_res = r2_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r2_lck))
            .unwrap();
        assert_eq!(r2_lck.compute, Compute::GPU, "Second invoke should run on GPU");
        assert_eq!(r2_lck.container_state, ContainerState::Cold, "Invoke 2 should be cold");
        assert!(r2_lck.duration.as_micros() > 0, "Invoke 2 should have duration time");

        let r3_lck = inv3.lock();
        let r3_res = r3_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 3 '{:?}' did not have a result", *r3_lck))
            .unwrap();
        assert_eq!(r3_lck.compute, Compute::GPU, "Third invoke should run on GPU");
        // assert_eq!(
        //     r3_lck.container_state,
        //     ContainerState::Warm,
        //     "Invoke 3 should be warm because of batching"
        // );
        assert!(r3_lck.duration.as_micros() > 0, "Invoke 3 should have duration time");

        let r1_end = formatter
            .parse_python_container_time(&r1_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));
        let r3_start = formatter
            .parse_python_container_time(&r3_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));
        let r3_end = formatter
            .parse_python_container_time(&r3_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));

        assert!(
            r1_end < r3_start,
            "Invoke 3 should have started {} after invoke 1 ended {}",
            r3_start,
            r1_end
        );
        assert!(
            r3_end < r2_start,
            "Invoke 3 should have ended {} before invoke 2 started {}",
            r3_end,
            r2_start
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn fcfs_ordering_kept() {
        let mut env = build_gpu_env();
        env.push(("invocation.queue_policies.GPU".to_string(), "fcfs".to_string()));
        let formatter = ContainerTimeFormatter::new(&TEST_TID)
            .unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
        let (_, _, _, invoker, reg, _cmap, gpu) = sim_test_services(None, Some(env), None).await;
        let func1 = reg
            .register(gpu_reg(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let reg2 = RegisterRequest {
            function_name: "test2".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func2 = reg
            .register(reg2, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));

        let gpu_lck = gpu
            .unwrap_or_else(|| panic!("No GPU resource"))
            .try_acquire_resource(None, &TEST_TID)
            .expect("Should return GPU permit"); // hold GPU to force queueing
        let inv1 = background_test_invoke(&invoker, &func1, &sim_args().unwrap(), &TEST_TID);
        wait_for_queue_len(&invoker, Compute::GPU, 1).await;
        let inv2 = background_test_invoke(&invoker, &func2, &sim_args().unwrap(), &TEST_TID);
        wait_for_queue_len(&invoker, Compute::GPU, 2).await;
        let inv3 = background_test_invoke(&invoker, &func1, &sim_args().unwrap(), &TEST_TID);
        wait_for_queue_len(&invoker, Compute::GPU, 3).await;
        drop(gpu_lck);

        let inv1 = resolve_invoke(inv1)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let inv2 = resolve_invoke(inv2)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
        let inv3 = resolve_invoke(inv3)
            .await
            .unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));

        let r1_lck = inv1.lock();
        let r1_res = r1_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 1 '{:?}' did not have a result", *r1_lck))
            .unwrap();
        assert_eq!(r1_lck.compute, Compute::GPU, "First invoke should run on GPU");
        assert_eq!(r1_lck.container_state, ContainerState::Cold, "Invoke 1 should be cold");
        assert!(r1_lck.duration.as_micros() > 0, "Invoke 1 should have duration time");

        let r2_lck = inv2.lock();
        let r2_res = r2_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r2_lck))
            .unwrap();
        assert_eq!(r2_lck.compute, Compute::GPU, "Second invoke should run on GPU");
        assert_eq!(r2_lck.container_state, ContainerState::Cold, "Invoke 2 should be cold");
        assert!(r2_lck.duration.as_micros() > 0, "Invoke 2 should have duration time");

        let r3_lck = inv3.lock();
        let r3_res = r3_lck
            .worker_result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Invoke 3 '{:?}' did not have a result", *r3_lck))
            .unwrap();
        assert_eq!(r3_lck.compute, Compute::GPU, "Third invoke should run on GPU");
        assert_eq!(
            r3_lck.container_state,
            ContainerState::Cold,
            "Invoke 3 should be warm because of batching"
        );
        assert!(r3_lck.duration.as_micros() > 0, "Invoke 3 should have duration time");

        let r1_end = formatter
            .parse_python_container_time(&r1_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));
        let r2_start = formatter
            .parse_python_container_time(&r2_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));
        let r2_end = formatter
            .parse_python_container_time(&r2_res.end)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));
        let r3_start = formatter
            .parse_python_container_time(&r3_res.start)
            .unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));

        assert!(
            r1_end < r2_start,
            "Invoke 2 should have started {} after invoke 1 ended {}",
            r2_start,
            r1_end
        );
        assert!(
            r2_end < r3_start,
            "Invoke 3 should have started {} after invoke 2 ended {}",
            r3_start,
            r2_end
        );
    }
}

#[cfg(test)]
mod clean_tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_cpu_container_removed() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.compute_type(), Compute::CPU);
        drop(c);
        let removed = cm
            .remove_idle_containers(&TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("clean failed: {}", e));
        assert_eq!(removed.len(), 1);
        let cpus = removed
            .get(&Compute::CPU)
            .unwrap_or_else(|| panic!("Did not have a cpu removal, but: {:?}", removed));
        assert_eq!(*cpus, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_gpu_container_removed() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::GPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        drop(c);
        let removed = cm
            .remove_idle_containers(&TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("clean failed: {}", e));
        assert_eq!(removed.len(), 1);
        let cpus = removed
            .get(&Compute::GPU)
            .unwrap_or_else(|| panic!("Did not have a GPU removal, but: {:?}", removed));
        assert_eq!(*cpus, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_container_removed() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        assert_eq!(c.container.compute_type(), Compute::CPU);
        drop(c);
        let removed = cm
            .remove_idle_containers(&TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("clean failed: {}", e));
        assert_eq!(removed.len(), 1);
        let cpus = removed
            .get(&Compute::CPU)
            .unwrap_or_else(|| panic!("Did not have a cpu removal, but: {:?}", removed));
        assert_eq!(*cpus, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn gpu_container_removed() {
        let env = build_gpu_env();
        let (_log, _cfg, cm, _invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        drop(c);
        let removed = cm
            .remove_idle_containers(&TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("clean failed: {}", e));
        assert_eq!(removed.len(), 1);
        let gpus = removed
            .get(&Compute::GPU)
            .unwrap_or_else(|| panic!("Did not have a GPU removal, but: {:?}", removed));
        assert_eq!(*gpus, 1);
    }
}

mod enqueueing_tests {
    use super::*;
    use iluvatar_library::char_map::Chars;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn picks_gpu_queueing_exec() {
        let mut env = build_gpu_env();
        env.push((
            "invocation.enqueueing_policy".to_string(),
            "ShortestExecTime".to_string(),
        ));
        let (_log, _cfg, _cm, invoker, reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cmap.update(&func.fqdn, Chars::CpuExecTime, 1.0);
        cmap.update(&func.fqdn, Chars::GpuExecTime, 0.5);
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::GPU, "Item should have been run on CPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn picks_cpu_queueing_exec() {
        let mut env = build_gpu_env();
        env.push((
            "invocation.enqueueing_policy".to_string(),
            "ShortestExecTime".to_string(),
        ));
        let (_log, _cfg, _cm, invoker, reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cmap.update(&func.fqdn, Chars::CpuExecTime, 0.5);
        cmap.update(&func.fqdn, Chars::GpuExecTime, 1.0);
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::CPU, "Item should have been run on CPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn always_cpu() {
        let mut env = build_gpu_env();
        env.push(("invocation.enqueueing_policy".to_string(), "AlwaysCPU".to_string()));
        let (_log, _cfg, _cm, invoker, reg, _cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::CPU, "Item should have been run on CPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_faster_cpu_path_chosen() {
        let mut env = build_gpu_env();
        env.push(("invocation.enqueueing_policy".to_string(), "EstCompTime".to_string()));
        let (_log, _cfg, _cm, invoker, reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cmap.update(&func.fqdn, Chars::CpuColdTime, 1.0);
        cmap.update(&func.fqdn, Chars::GpuColdTime, 0.5);
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::GPU, "Item should have been run on GPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_faster_gpu_path_chosen() {
        let env = vec![
            ("container_resources.gpu_resource.count".to_string(), "1".to_string()),
            (
                "container_resources.gpu_resource.memory_mb".to_string(),
                "2048".to_string(),
            ),
            (
                "container_resources.gpu_resource.funcs_per_device".to_string(),
                "1".to_string(),
            ),
            ("container_resources.cpu_resource.count".to_string(), "1".to_string()),
            (
                "container_resources.cpu_resource.max_oversubscribe".to_string(),
                "1".to_string(),
            ),
            ("invocation.enqueueing_policy".to_string(), "EstCompTime".to_string()),
        ];
        let (_log, _cfg, _cm, invoker, reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cmap.update(&func.fqdn, Chars::CpuColdTime, 0.1);
        cmap.update(&func.fqdn, Chars::GpuColdTime, 1.5);
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::CPU, "Item should have been run on CPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_faster_cpu_path_chosen() {
        let mut env = build_gpu_env();
        env.push(("invocation.enqueueing_policy".to_string(), "EstCompTime".to_string()));
        let (_log, _cfg, cm, invoker, reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cmap.update(&func.fqdn, Chars::CpuPreWarmTime, 1.0);
        cmap.update(&func.fqdn, Chars::GpuPreWarmTime, 1.5);
        cm.prewarm(&func, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::CPU, "Item should have been run on CPU");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_faster_gpu_path_chosen() {
        let env = vec![
            ("container_resources.gpu_resource.count".to_string(), "1".to_string()),
            (
                "container_resources.gpu_resource.memory_mb".to_string(),
                "2048".to_string(),
            ),
            (
                "container_resources.gpu_resource.funcs_per_device".to_string(),
                "1".to_string(),
            ),
            ("container_resources.cpu_resource.count".to_string(), "1".to_string()),
            (
                "container_resources.cpu_resource.max_oversubscribe".to_string(),
                "1".to_string(),
            ),
            ("invocation.enqueueing_policy".to_string(), "EstCompTime".to_string()),
            ("invocation.queues.GPU".to_string(), "serial".to_string()),
        ];
        let (_log, _cfg, cm, invoker, reg, cmap, _gpu) = sim_test_services(None, Some(env), None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: (Compute::CPU | Compute::GPU).bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let func = reg
            .register(req, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        cmap.update(&func.fqdn, Chars::CpuPreWarmTime, 1.0);
        cmap.update(&func.fqdn, Chars::GpuPreWarmTime, 0.5);
        cm.prewarm(&func, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        cm.prewarm(&func, &TEST_TID, Compute::GPU)
            .await
            .unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
        let invoke = invoker
            .sync_invocation(func.clone(), sim_args().unwrap(), TEST_TID.clone())
            .await
            .unwrap_or_else(|e| panic!("Invocation failed: {}", e));
        let invoke = invoke.lock();
        assert_eq!(invoke.compute, Compute::GPU, "Item should have been run on GPU");
    }
}
