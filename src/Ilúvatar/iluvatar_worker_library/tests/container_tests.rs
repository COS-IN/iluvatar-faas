#[macro_use]
pub mod utils;

use crate::utils::test_invoker_svc;
use iluvatar_library::threading::EventualItem;
use iluvatar_library::transaction::TEST_TID;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_rpc::rpc::RegisterRequest;
use iluvatar_worker_library::services::containers::structs::{cast, ContainerState};

fn basic_reg_req_docker() -> RegisterRequest {
    RegisterRequest {
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
    }
}

#[cfg(test)]
mod registration {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_works() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        reg.register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeat_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        reg.register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Registration failed: {}", e));
        let err = reg.register(basic_reg_req_docker(), &TEST_TID).await;
        assert_error!(
            err,
            "Function test-test is already registered!",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn invokes_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 0,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
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
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
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
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
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
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 0,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
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
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 0,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
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
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 1000000,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Illegal memory allocation request '1000000'",
            "registration succeeded when it should have failed!"
        );
    }

    #[ignore]
    // ignored because containerd testing is currently broken
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn image_invalid_registration_fails_ctr() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let bad_img = "docker.io/library/alpine:lasdijbgoie";
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: bad_img.to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Failed to pull image",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn image_invalid_registration_fails_docker() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let bad_img = "docker.io/library/alpine:lasdijbgoie";
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: bad_img.to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Failed to pull image",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn no_isolate_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::empty().bits(),
            ..Default::default()
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
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::INVALID.bits(),
            ..Default::default()
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Could not register function with isolation(s): Isolation(INVALID)",
            "registration succeeded when it should have failed!"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn extra_isolate_invalid_registration_fails() {
        let (_log, _cfg, _cm, _invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let input = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            parallel_invokes: 1,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: (Isolation::DOCKER | Isolation::INVALID).bits(),
            ..Default::default()
        };
        let err = reg.register(input, &TEST_TID).await;
        assert_error!(
            err,
            "Could not register function with isolation(s): Isolation(INVALID)",
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
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        let _cast_container = cast::<DockerContainer>(&c.container).unwrap();
        assert_eq!(c.container.function().function_name, "test");
        assert_eq!(c.container.function().function_version, "test");
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prewarm_get_container_docker() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        let cast_container = cast::<DockerContainer>(&c.container).unwrap();
        assert_eq!(cast_container.function.function_name, "test");
        assert_eq!(cast_container.function.function_version, "test");
        assert_eq!(c.container.container_type(), Isolation::DOCKER);
        assert_eq!(c.container.compute_type(), Compute::CPU);
    }
}

#[cfg(test)]
mod get_container {
    use super::*;
    use iluvatar_worker_library::services::containers::docker::dockerstructs::DockerContainer;
    use iluvatar_worker_library::services::containers::structs::ContainerT;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cant_double_acquire() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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

        let c2 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten cold-start container");
        assert_ne!(c1.container.container_id(), c2.container.container_id());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mem_limit() {
        let (_log, cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
        let request = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: cfg.container_resources.memory_mb,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            ..Default::default()
        };
        let reg = _reg
            .register(request, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));
        cm.prewarm(&reg, &TEST_TID, Compute::CPU)
            .await
            .unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
        let _c1 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(_) => panic!("Should get prewarmed container"),
            EventualItem::Now(n) => n,
        }
        .expect("should have gotten prewarmed container");
        assert_eq!(_c1.container.fqdn(), &reg.fqdn);
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        let c2 = match cm.acquire_container(&reg, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        };
        match c2 {
            Ok(_c2) => panic!("should have gotten an error instead of something"),
            Err(_c2) => {},
        }
        assert_eq!(_c1.container.fqdn(), &reg.fqdn);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn container_alive() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;

        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
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

        let cast_container = cast::<DockerContainer>(&c2.container).unwrap();

        let _result = cast_container
            .client
            .invoke("{}", &TEST_TID, cast_container.container_id())
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod remove_container {
    use super::*;
    use iluvatar_worker_library::services::containers::docker::dockerstructs::DockerContainer;
    use iluvatar_worker_library::services::containers::structs::ContainerT;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_container_deleted() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        c1.container.mark_unhealthy();

        let c1_cont = c1.container.clone();
        drop(c1);
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let cast_container = cast::<DockerContainer>(&c1_cont).unwrap();

        let result = cast_container
            .client
            .invoke("{}", &TEST_TID, cast_container.container_id())
            .await;
        assert_error!(
            result,
            "HTTP error when trying to connect to container",
            format!("Unpexpected result when container should be gone {:?}", result)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_container_not_gettable() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
    async fn prewarmed_docker() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_docker() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        let (_log, _cfg, cm, invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        let (_log, _cfg, cm, invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
        let reg = _reg
            .register(basic_reg_req_docker(), &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("registration failed: {:?}", e));

        test_invoke(&invoker, &reg, "{}", &TEST_TID).await;

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
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        assert!(!c1.container.is_healthy(), "Container should be unhealthy");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unhealthy_docker() {
        let (_log, _cfg, cm, _invoker, _reg, _, _) = test_invoker_svc(None, None, None).await;
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
        assert!(!c1.container.is_healthy(), "Container should be unhealthy");
    }
}

#[cfg(test)]
mod server_invokable {
    use super::*;
    use crate::utils::test_invoke;
    use iluvatar_library::types::ContainerServer;
    use rstest::rstest;
    use std::time::Duration;
    use tokio::time::timeout;

    #[rstest]
    #[case("http")]
    #[case("unix")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn docker_severs_work(#[case] server: &str) {
        let image = format!("docker.io/alfuerst/hello-iluvatar-action-{}:latest", server);
        let (_log, _cfg, cm, invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: image,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::DOCKER.bits(),
            container_server: server.parse::<ContainerServer>().unwrap() as u32,
            ..Default::default()
        };
        let reg = reg.register(req, &TEST_TID).await.expect("register failed");
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

    #[rstest]
    // ignored because containerd testing is currently broken
    #[ignore]
    #[case("http")]
    #[ignore]
    #[case("unix")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn containerd_severs_work(#[case] server: &str) {
        let image = format!("docker.io/alfuerst/hello-iluvatar-action-{}:latest", server);
        let (_log, _cfg, cm, invoker, reg, _, _) = test_invoker_svc(None, None, None).await;
        let req = RegisterRequest {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: image,
            transaction_id: "testTID".to_string(),
            compute: Compute::CPU.bits(),
            isolate: Isolation::CONTAINERD.bits(),
            container_server: server.parse::<ContainerServer>().unwrap() as u32,
            ..Default::default()
        };
        let reg = reg.register(req, &TEST_TID).await.expect("register failed");
        let _result = test_invoke(&invoker, &reg, "{}", &TEST_TID).await;
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
}
