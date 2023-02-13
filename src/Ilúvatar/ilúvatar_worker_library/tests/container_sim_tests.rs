#[macro_use]
pub mod utils;

use iluvatar_worker_library::rpc::{RegisterRequest, LanguageRuntime};
use iluvatar_library::transaction::TEST_TID;
use crate::utils::sim_invoker_svc;
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::threading::EventualItem;

fn cpu_reg() -> RegisterRequest {
  RegisterRequest {
    function_name: "test".to_string(),
    function_version: "test".to_string(),
    cpus: 1, memory: 128, parallel_invokes: 1,
    image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
    transaction_id: "testTID".to_string(),
    language: LanguageRuntime::Nolang.into(),
    compute: Compute::CPU.bits(),
    isolate: Isolation::CONTAINERD.bits(),
  }
}

fn gpu_reg() -> RegisterRequest {
  RegisterRequest {
    function_name: "test".to_string(),
    function_version: "test".to_string(),
    cpus: 1, memory: 128, parallel_invokes: 1,
    image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
    transaction_id: "testTID".to_string(),
    language: LanguageRuntime::Nolang.into(),
    compute: Compute::GPU.bits(),
    isolate: Isolation::DOCKER.bits(),
  }
}

mod compute_iso_matching {
  use super::*;
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn cpu_docker_works() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let req =   RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1, memory: 128, parallel_invokes: 1,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: Compute::CPU.bits(),
      isolate: Isolation::DOCKER.bits(),
    };
    let func = reg.register(req, &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::CPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert_eq!(c.container.container_type(), Isolation::DOCKER);
    assert_eq!(c.container.compute_type(), Compute::CPU);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn cpu_ctr_works() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(cpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::CPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
    assert_eq!(c.container.compute_type(), Compute::CPU);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn two_compute_works() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let req =   RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1, memory: 128, parallel_invokes: 1,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: (Compute::CPU|Compute::GPU).bits(),
      isolate: Isolation::DOCKER.bits(),
    };
    let func = reg.register(req, &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert_eq!(c.container.container_type(), Isolation::DOCKER);
    assert_eq!(c.container.compute_type(), Compute::CPU);
    let c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert_eq!(c.container.container_type(), Isolation::DOCKER);
    assert_eq!(c.container.compute_type(), Compute::GPU);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn cant_request_not_registered_compute_gpu() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(cpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    assert_error!(cm.prewarm(&func, &TEST_TID, Compute::GPU).await, "Registration did not contain requested compute", "Perwarm did not error correctly");
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn cant_request_not_registered_compute_cpu() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    assert_error!(cm.prewarm(&func, &TEST_TID, Compute::CPU).await, "Registration did not contain requested compute", "Perwarm did not error correctly");
  }
}

#[cfg(test)]
mod gpu {
  use super::*;
  
  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_container_must_use_docker() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let request = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1, memory: 128, parallel_invokes: 1,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: Compute::GPU.bits(),
      isolate: Isolation::CONTAINERD.bits(),
    };
    let func = reg.register(request, &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let err = cm.prewarm(&func, &TEST_TID, Compute::GPU).await;
    assert_error!(err, "GPU only supported with Docker isolation", "Prewarm succeeded when it should have failed!");
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_forces_docker() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let req =   RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1, memory: 128, parallel_invokes: 1,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: (Compute::CPU|Compute::GPU).bits(),
      isolate: (Isolation::DOCKER|Isolation::CONTAINERD).bits(),
    };
    let func = reg.register(req, &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let c = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert_eq!(c.container.container_type(), Isolation::CONTAINERD);
    assert_eq!(c.container.compute_type(), Compute::CPU);
    let c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert_eq!(c.container.container_type(), Isolation::DOCKER);
    assert_eq!(c.container.compute_type(), Compute::GPU);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_docker_works() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let _c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn returned_container_ok() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    let c1_cont = c1.container.clone();
    drop(c1);
    let c2 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert!(c1_cont.is_healthy());
    assert_eq!(c1_cont.container_id(), c2.container.container_id());
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn unhealthy_not_used() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    let c1_cont = c1.container.clone();
    c1_cont.mark_unhealthy();
    assert!(!c1_cont.is_healthy(), "Container should not be healthy after being marked");
    drop(c1);
    let c2 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert!(c2.container.is_healthy(), "New container should be healthy");
    assert_ne!(c1_cont.container_id(), c2.container.container_id(), "New container should be different!");
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_resource_limiting() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let _c1 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(_) => panic!("Should have gotten prewarmed container"),
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    let err = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(_) => panic!("Should not have gotten prewarmed container"),
    };
    assert_error!(err, "No GPU available to launch container", "Only one gpu available, can't have two live containers!");
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_container_removed() {
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, None, None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let reg2 = RegisterRequest {
      function_name: "test2".to_string(),
      function_version: "test".to_string(),
      cpus: 1, memory: 128, parallel_invokes: 1,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: Compute::GPU.bits(),
      isolate: Isolation::DOCKER.bits(),
    };
    let func2 = reg.register(reg2, &TEST_TID).await.unwrap_or_else(|e| panic!("Registration 2 failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let ctr = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(_) => panic!("Should have gotten prewarmed container"),
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("Creating container should have succeeded: {}", e));
    assert_eq!(ctr.container.function().function_name, "test", "Got incorrect container");
    drop(ctr);

    let ctr = match cm.acquire_container(&func2, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(_) => panic!("Should not have gotten prewarmed container"),
    }.unwrap_or_else(|e| panic!("Creating container should have succeeded: {}", e));
    
    assert_eq!(ctr.container.function().function_name, "test2", "Got incorrect container");
  }
}
