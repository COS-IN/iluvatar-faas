#[macro_use]
pub mod utils;

use std::collections::HashMap;
use iluvatar_worker_library::rpc::{RegisterRequest, LanguageRuntime};
use iluvatar_library::transaction::TEST_TID;
use crate::utils::{sim_invoker_svc, clean_env, background_test_invoke, resolve_invoke, sim_args};
use iluvatar_library::types::{Compute, Isolation};
use iluvatar_library::threading::EventualItem;
use iluvatar_worker_library::services::containers::structs::ContainerState;

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

fn build_gpu_env() -> HashMap<String, String> {
  let mut r = HashMap::new();
  r.insert("ILUVATAR_WORKER__container_resources__resource_map__GPU__count".to_string(), "1".to_string());
  r.insert("ILUVATAR_WORKER__invocation__concurrent_invokes".to_string(), "5".to_string());
  r
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
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
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
    clean_env(&env);
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
  use iluvatar_worker_library::services::containers::structs::ContainerTimeFormatter;

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
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
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
    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_docker_works() {
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let _c = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn returned_container_ok() {
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
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
    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn unhealthy_not_used() {
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
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
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    let c2 = match cm.acquire_container(&func, &TEST_TID, Compute::GPU) {
      EventualItem::Future(f) => f.await,
      EventualItem::Now(n) => n,
    }.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    assert!(c2.container.is_healthy(), "New container should be healthy");
    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_resource_limiting() {
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
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
    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
  async fn gpu_container_removed() {
    let env = build_gpu_env();
    let (_log, _cfg, cm, _invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
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
    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn multiple_invokes_blocks() {
    let formatter = ContainerTimeFormatter::new(&TEST_TID).unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
    let env = build_gpu_env();
    let (_log, _cfg, cm, invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
    let func = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let args = sim_args();

    let invoke1 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke1".to_string());
    tokio::time::sleep(std::time::Duration::from_micros(10)).await; // queue insertion can be kind of racy, sleep to ensure ordering
    let invoke2 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke2".to_string());
    tokio::time::sleep(std::time::Duration::from_micros(10)).await;
    let invoke3 = background_test_invoke(&invoker, &func, args.as_str(), &"invoke3".to_string());
    let r1 = resolve_invoke(invoke1).await.unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
    let r1_lck = r1.lock();
    let r1_res = r1_lck.worker_result.as_ref().ok_or_else(|| anyhow::anyhow!("Invoke 1 '{:?}' did not have a result", *r1_lck)).unwrap();
    
    let r2 = resolve_invoke(invoke2).await.unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));

    assert_eq!(r1_lck.compute, Compute::GPU, "First invoke should run on GPU");
    assert_eq!(r1_lck.container_state, ContainerState::Prewarm, "Invoke 1 should be prewarm");
    assert!(r1_lck.duration.as_micros() > 0, "Invoke should have duration time");

    let r1_end = formatter.parse_python_container_time(&r1_res.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r1_res.end, e));
    let r2_lck = r2.lock();
    let r2_res = r2_lck.worker_result.as_ref().ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r2_lck)).unwrap();
    let r2_start = formatter.parse_python_container_time(&r2_res.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));

    assert_eq!(r2_lck.compute, Compute::GPU, "Second invoke should run on GPU");
    assert_eq!(r2_lck.container_state, ContainerState::Warm, "Invoke 2 should be warm");
    assert!(r2_lck.duration.as_micros() > 0, "Invoke 2 should have duration time");
    assert!(r1_end < r2_start, "Invoke 2 should have started {} after invoke 1 ended {}", r2_start, r1_end);

    let r3 = resolve_invoke(invoke3).await.unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
    let r3_lck = r3.lock();
    let r3_res = r3_lck.worker_result.as_ref().ok_or_else(|| anyhow::anyhow!("Invoke 3 '{:?}' did not have a result", *r3_lck)).unwrap();
    let r3_start = formatter.parse_python_container_time(&r3_res.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));
    let r2_end = formatter.parse_python_container_time(&r2_res.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));

    assert_eq!(r3_lck.compute, Compute::GPU, "Third invoke should run on GPU");
    assert_eq!(r3_lck.container_state, ContainerState::Warm, "Invoke 3 should be warm");
    assert!(r3_lck.duration.as_micros() > 0, "Invoke 3 should have duration time");
    assert!(r2_end < r3_start, "Invoke 3 should have started {} after invoke 2 ended {}", r3_start, r2_end);

    clean_env(&env);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn diff_funcs_multiple_invokes_blocks() {
    let formatter = ContainerTimeFormatter::new(&TEST_TID).unwrap_or_else(|e| panic!("ContainerTimeFormatter failed because {}", e));
    let env = build_gpu_env();
    let (_log, _cfg, cm, invoker, reg) = sim_invoker_svc(None, Some(&env), None).await;
    let func1 = reg.register(gpu_reg(), &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let r2 = RegisterRequest {
      function_name: "test2".to_string(),
      function_version: "test".to_string(),
      cpus: 1, memory: 128, parallel_invokes: 1,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string(),
      language: LanguageRuntime::Nolang.into(),
      compute: Compute::GPU.bits(),
      isolate: Isolation::DOCKER.bits(),
    };
    let func2 = reg.register(r2, &TEST_TID).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    cm.prewarm(&func1, &TEST_TID, Compute::GPU).await.unwrap_or_else(|e| panic!("Prewarm failed: {}", e));
    let args = sim_args();

    let invoke1 = background_test_invoke(&invoker, &func1, args.as_str(), &"invoke1".to_string());
    tokio::time::sleep(std::time::Duration::from_micros(10)).await; // queue insertion can be kind of racy, sleep to ensure ordering
    let invoke2 = background_test_invoke(&invoker, &func2, args.as_str(), &"invoke2".to_string());
    tokio::time::sleep(std::time::Duration::from_micros(10)).await;
    let invoke3 = background_test_invoke(&invoker, &func1, args.as_str(), &"invoke3".to_string());
    let r1 = resolve_invoke(invoke1).await.unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
    let r1_lck = r1.lock();
    let r1_res = r1_lck.worker_result.as_ref().ok_or_else(|| anyhow::anyhow!("Invoke 1 '{:?}' did not have a result", *r1_lck)).unwrap();
    
    let r2 = resolve_invoke(invoke2).await.unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));

    assert_eq!(r1_lck.compute, Compute::GPU, "First invoke should run on GPU");
    assert_eq!(r1_lck.container_state, ContainerState::Prewarm, "Invoke 2 should be prewarm");
    assert!(r1_lck.duration.as_micros() > 0, "Invoke should have duration time");

    let r1_end = formatter.parse_python_container_time(&r1_res.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r1_res.end, e));
    let r2_lck = r2.lock();
    let r2_res = r2_lck.worker_result.as_ref().ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r2_lck)).unwrap();
    let r2_start = formatter.parse_python_container_time(&r2_res.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r2_res.start, e));

    assert_eq!(r2_lck.compute, Compute::GPU, "Second invoke should run on GPU");
    assert_eq!(r2_lck.container_state, ContainerState::Cold, "Invoke 2 should be warm");
    assert!(r2_lck.duration.as_micros() > 0, "Invoke 2 should have duration time");
    assert!(r1_end < r2_start, "Invoke 2 should have started {} after invoke 1 ended {}", r2_start, r1_end);

    let r3 = resolve_invoke(invoke3).await.unwrap_or_else(|e| panic!("Invoke failed: {:?}", e));
    let r3_lck = r3.lock();
    let r3_res = r3_lck.worker_result.as_ref().ok_or_else(|| anyhow::anyhow!("Invoke 2 '{:?}' did not have a result", *r3_lck)).unwrap();
    let r3_start = formatter.parse_python_container_time(&r3_res.start).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.start, e));
    let r2_end = formatter.parse_python_container_time(&r2_res.end).unwrap_or_else(|e| panic!("Failed to parse time '{}' because {}", r3_res.end, e));

    assert_eq!(r3_lck.compute, Compute::GPU, "Third invoke should run on GPU");
    assert_eq!(r3_lck.container_state, ContainerState::Cold, "Invoke 3 should be warm");
    assert!(r3_lck.duration.as_micros() > 0, "Invoke 3 should have duration time");
    assert!(r2_end < r3_start, "Invoke 3 should have started {} after invoke 2 ended {}", r3_start, r2_end);

    clean_env(&env);
  }
}
