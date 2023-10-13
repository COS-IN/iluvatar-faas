#[macro_use]
pub mod utils;

use crate::utils::test_invoker_svc;
use iluvatar_library::threading::EventualItem;
use iluvatar_library::transaction::TEST_TID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_worker_library::rpc::LanguageRuntime;
use iluvatar_worker_library::rpc::RegisterRequest;
use iluvatar_worker_library::services::containers::containerd::containerdstructs::ContainerdContainer;
use iluvatar_worker_library::services::containers::structs::{cast, ContainerState};
use reqwest;

fn basic_reg_req() -> RegisterRequest {
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

fn basic_reg_req_docker() -> RegisterRequest {
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
        isolate: Isolation::DOCKER.bits(),
        resource_timings_json: "".to_string(),
    }
}

#[cfg(test)]
mod registration {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_works() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        reg.register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeat_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        reg.register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let err = reg.register(basic_reg_req(), &TEST_TID).await;
        assert_error!(
            err,
            "Function test-test is already registered!",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invokes_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 0,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Illegal parallel invokes set, must be 1",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn name_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Invalid function name",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn version_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Invalid function version",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpus_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 0,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Illegal cpu allocation request '0'",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_small_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 0,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Illegal memory allocation request '0'",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_large_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 1000000,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Illegal memory allocation request '1000000'",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn image_invalid_registration_fails_ctr() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let bad_img = "docker.io/library/alpine:lasdijbgoie";
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: bad_img.to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        match err {
            Ok(_) => panic!("registration succeeded when it should have failed!"),
            Err(e) => {
                let e_str = e.to_string();
                if !(e_str.contains(bad_img)
                    && e_str.contains("failed to resolve reference")
                    && e_str.contains("not found"))
                {
                    panic!("unexpected error: {:?}", e);
                }
            }
        };
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn image_invalid_registration_fails_docker() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let bad_img = "docker.io/library/alpine:lasdijbgoie";
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: bad_img.to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        match err {
            Ok(_) => panic!("registration succeeded when it should have failed!"),
            Err(e) => {
                let e_str = e.to_string();
                if !e_str.contains("Failed to pull docker image with exit code") {
                    panic!("unexpected error: {:?}", e);
                }
            }
        };
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_isolate_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::empty().bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Could not register function with no specified isolation!",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invalid_isolate_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::INVALID.bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Could not register function with isolation(s): INVALID",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extra_isolate_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: (Isolation::DOCKER | Isolation::CONTAINERD | Isolation::INVALID).bits(),
            resource_timings_json: "".to_string(),
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Could not register function with isolation(s): INVALID",
            "registration succeeded when it should have failed!"
        );
    }
}

#[cfg(test)]
mod prewarm {
    use super::*;
    use iluvatar_worker_library::services::containers::docker::dockerstructs::DockerContainer;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_get_container() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
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
        let cast_container = cast::<ContainerdContainer>(&c.container, &TEST_TID).unwrap();
        assert_eq!(cast_container.task.running, true);
        assert_eq!(c.container.function().function_name, "test");
        assert_eq!(c.container.function().function_version, "test");
        assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_get_container_docker() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
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
        let cast_container = cast::<DockerContainer>(&c.container, &TEST_TID).unwrap();
        assert_eq!(cast_container.function.function_name, "test");
        assert_eq!(cast_container.function.function_version, "test");
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefer_ctd_container() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let request = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: (Isolation::DOCKER | Isolation::CONTAINERD).bits(),
            resource_timings_json: "".to_string(),
        };
        let reg = _reg
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
        let cast_container = match cast::<ContainerdContainer>(&c.container, &TEST_TID) {
            Ok(c) => c,
            Err(e) => panic!("{:?}", e),
        };
        assert_eq!(cast_container.function.function_name, "test");
        assert_eq!(cast_container.function.function_version, "test");
        assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }
}

#[cfg(test)]
mod get_container {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cant_double_acquire() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");

        let c2 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten cold-start container");
        assert_ne!(c1.container.container_id(), c2.container.container_id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mem_limit() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let request = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 2048,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            language: LanguageRuntime::Nolang.into(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            resource_timings_json: "".to_string(),
        };
        let reg = _reg
            .register(request, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let _c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");

        let c2 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        };
        match c2 {
            Ok(_c2) => panic!("should have gotten an error instead of something"),
            Err(_c2) => {}
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn container_alive() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;

        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c2 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");

        let cast_container = cast::<ContainerdContainer>(&c2.container, &TEST_TID).unwrap();

        let client = reqwest::Client::new();
        let result = client.get(&cast_container.base_uri).send().await.unwrap();
        assert_eq!(result.status(), 200);
    }
}

#[cfg(test)]
mod remove_container {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_container_deleted() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");
        c1.container.mark_unhealthy();

        let c1_cont = c1.container.clone();
        drop(c1);
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let cast_container = cast::<ContainerdContainer>(&c1_cont, &TEST_TID).unwrap();

        let client = reqwest::Client::new();
        let result = client.get(&cast_container.base_uri).send().await;
        match result {
            Ok(result) => panic!("Unpexpected result when container should be gone {:?}", result),
            Err(e) => {
                if e.is_request() {
                    if let Some(status) = e.status() {
                        assert_eq!(status, 111, "unexpected return status {:?} with error {:?}", status, e);
                    }
                } else {
                    panic!("Unexpected error connecting to gone container {:?}", e);
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_container_not_gettable() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");
        c1.container.mark_unhealthy();

        let c1_cont = c1.container.clone();
        drop(c1);

        let c2 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");
        assert_ne!(
            c1_cont.container_id(),
            c2.container.container_id(),
            "Second container should have different ID because container is gone"
        );
    }
}

#[cfg(test)]
mod container_state {
    use std::time::Duration;

    use super::*;
    use tokio::time::timeout;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarmed() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");

        assert_eq!(
            c1.container.state(),
            ContainerState::Prewarm,
            "Container's state should have been prewarmed"
        );
        assert!(c1.container.is_healthy(), "Container should be healthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarmed_docker() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");

        assert_eq!(
            c1.container.state(),
            ContainerState::Prewarm,
            "Container's state should have been prewarmed"
        );
        assert!(c1.container.is_healthy(), "Container should be healthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(_) => panic!("should not have gotten read container"),
        }
        .unwrap_or_else(|e| panic!("acquire_container failed: {:?}", e));

        assert_eq!(
            c1.container.state(),
            ContainerState::Cold,
            "Container's state should have been cold"
        );
        assert!(c1.container.is_healthy(), "Container should be healthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_docker() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(_) => panic!("should not have gotten read container"),
        }
        .unwrap_or_else(|e| panic!("acquire_container failed: {:?}", e));

        assert_eq!(
            c1.container.state(),
            ContainerState::Cold,
            "Container's state should have been cold"
        );
        assert!(c1.container.is_healthy(), "Container should be healthy");
    }

    use crate::utils::{register, test_invoke};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm() {
        let (_log, _cfg, cm, invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = register(
            &_reg,
            "docker.io/alfuerst/hello-iluvatar-action:latest",
            "test",
            &TEST_TID,
        )
        .await;
        let _result = test_invoke(&invoker, &reg, "{}", &TEST_TID).await;

        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => timeout(Duration::from_secs(20), f).await.expect("Timeout error"),
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten container");

        assert_eq!(
            c1.container.state(),
            ContainerState::Warm,
            "Container's state should have been warm"
        );
        assert!(c1.container.is_healthy(), "Container should be healthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_docker() {
        let (_log, _cfg, cm, invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        invoker
            .sync_invocation(reg.clone(), "{}".to_string(), "TEST_TID".to_string())
            .await
            .expect("Basic invocation should succeed");

        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten container");

        assert_eq!(
            c1.container.state(),
            ContainerState::Warm,
            "Container's state should have been warm"
        );
        assert!(c1.container.is_healthy(), "Container should be healthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten container");
        c1.container.mark_unhealthy();

        assert_eq!(
            c1.container.state(),
            ContainerState::Unhealthy,
            "Container's state should have been Unhealthy"
        );
        assert_eq!(c1.container.is_healthy(), false, "Container should be unhealthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_docker() {
        let (_log, _cfg, cm, _invoker, _reg) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        let c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten container");
        c1.container.mark_unhealthy();

        assert_eq!(
            c1.container.state(),
            ContainerState::Unhealthy,
            "Container's state should have been Unhealthy"
        );
        assert_eq!(c1.container.is_healthy(), false, "Container should be unhealthy");
    }
}
