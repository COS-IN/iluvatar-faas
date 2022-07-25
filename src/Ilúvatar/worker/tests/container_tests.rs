#[macro_use]
pub mod utils;

use std::sync::Arc;
use iluvatar_lib::rpc::{RegisterRequest, PrewarmRequest};
use iluvatar_lib::services::containers::containermanager::ContainerManager;
use iluvatar_lib::services::LifecycleFactory;
use iluvatar_lib::utils::calculate_fqdn;
use iluvatar_lib::worker_api::worker_config::{WorkerConfig, Configuration};
use iluvatar_lib::transaction::TEST_TID;
use iluvatar_lib::services::{invocation::invoker::InvokerService};
use iluvatar_lib::services::containers::structs::cast;
use iluvatar_lib::services::containers::containerd::containerdstructs::ContainerdContainer;

#[cfg(test)]
mod registration {
  use super::*;
  
  #[tokio::test]
  async fn registration_works() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
  }
  
  #[tokio::test]
  async fn repeat_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    cm.register(&input).await.unwrap_or_else(|e| panic!("Registration failed: {}", e));
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    assert_error!(err, "Function test-test is already registered!", "registration succeeded when it should have failed!");
  }
  
  #[tokio::test]
  async fn invokes_invalid_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 0,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    assert_error!(err, "Illegal parallel invokes set, must be 1", "registration succeeded when it should have failed!");
  }
  
  #[tokio::test]
  async fn name_invalid_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    assert_error!(err, "Invalid function name", "registration succeeded when it should have failed!");
  }
  
  #[tokio::test]
  async fn version_invalid_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "".to_string(),
      cpus: 1,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    assert_error!(err, "Invalid function version", "registration succeeded when it should have failed!");
  }
  
  #[tokio::test]
  async fn cpus_invalid_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 0,
      memory: 128,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    assert_error!(err, "Illegal cpu allocation request", "registration succeeded when it should have failed!");
  }
  
  #[tokio::test]
  async fn memory_invalid_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 0,
      image_name: "docker.io/library/alpine:latest".to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    assert_error!(err, "Illegal memory allocation request", "registration succeeded when it should have failed!");
  }
  
  #[tokio::test]
  async fn image_invalid_registration_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let bad_img = "docker.io/library/alpine:lasdijbgoie";
    let input = RegisterRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpus: 1,
      memory: 128,
      image_name: bad_img.to_string(),
      parallel_invokes: 1,
      transaction_id: "testTID".to_string()
    };
    let err = cm.register(&input).await;
    match err {
      Ok(_) => panic!("registration succeeded when it should have failed!"),
      Err(e) => {
        let e_str = e.to_string();
        if !(e_str.contains(bad_img) && e_str.contains("failed to resolve reference") && e_str.contains("not found")) {
          panic!("unexpected error: {:?}", e);
        }
      },
    };
  }
}

#[cfg(test)]
mod prewarm {
  use super::*;
  #[tokio::test]
  async fn no_prewarm_fails() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      ..Default::default()
    };
    let err = cm.prewarm(&input).await;
    match err {
      Ok(_) => panic!("registration succeeded when it should have failed!"),
      Err(e) => {
        let e_str = e.to_string();
        if !(e_str.contains("was not registered") && e_str.contains("Attempted registration failed because")) {
          panic!("unexpected error: {:?}", e);
        }
      },
    };
  }

  #[tokio::test]
  async fn prewarm_noreg_works() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "test".to_string(),
      cpu: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
  }

  #[tokio::test]
  async fn prewarm_get_container() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let fqdn = calculate_fqdn(&"test".to_string(), &"0.1.1".to_string());
    let c = cm.acquire_container(&fqdn, &TEST_TID).await.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    // let cast_c = c.container.clone() as Arc<ContainerdContainer>; 
    let cast_container = cast::<ContainerdContainer>(&c.container, &TEST_TID).unwrap();
    assert_eq!(cast_container.task.running, true);
    assert_eq!(c.container.function().function_name, "test");
    assert_eq!(c.container.function().function_version, "0.1.1");
  }
}

#[cfg(test)]
mod get_container {
  use super::*;
  use iluvatar_lib::transaction::TEST_TID;
use reqwest;

  #[tokio::test]
  async fn cant_double_acquire() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let fqdn = calculate_fqdn(&"test".to_string(), &"0.1.1".to_string());
    let c1 = cm.acquire_container(&fqdn, &TEST_TID).await.expect("should have gotten prewarmed container");

    let c2 = cm.acquire_container(&fqdn, &TEST_TID).await.expect("should have gotten cold-start container");
    assert_ne!(c1.container.container_id(), c2.container.container_id());
  }

  #[tokio::test]
  async fn mem_limit() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 256,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let fqdn = calculate_fqdn(&"test".to_string(), &"0.1.1".to_string());
    let _c1 = cm.acquire_container(&fqdn, &TEST_TID).await.expect("should have gotten prewarmed container");

    let c2 = cm.acquire_container(&fqdn, &TEST_TID).await; //.unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
    match c2 {
      Ok(_c2) => print!("should have gotten an error instead of something"),
      Err(_c2) => {},
    }
  }

  fn cust_test(arg: Arc<dyn std::any::Any + Send + Sync + 'static>) -> Arc<dyn std::any::Any + Send + Sync + 'static> {
    arg
  }

  #[tokio::test]
  async fn container_alive() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 256,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let fqdn = calculate_fqdn(&"test".to_string(), &"0.1.1".to_string());
    let c2 = cm.acquire_container(&fqdn, &TEST_TID).await.expect("should have gotten prewarmed container");
    let c3 = cust_test(c2.container);

    let cast_container = cast::<ContainerdContainer>(&c2.container, &TEST_TID).unwrap();

    let client = reqwest::Client::new();
    let result = client.get(&cast_container.base_uri)
      .send()
      .await.unwrap();
      assert_eq!(result.status(), 200);
  }
}

#[cfg(test)]
mod remove_container {
  use super::*;
  use reqwest;

  #[tokio::test]
  async fn removed_container_gone() {
    let (_cfg, cm, _invoker): (WorkerConfig, Arc<ContainerManager>, Arc<InvokerService>) = invoker_svc!();
    let input = PrewarmRequest {
      function_name: "test".to_string(),
      function_version: "0.1.1".to_string(),
      cpu: 1,
      memory: 128,
      image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
      transaction_id: "testTID".to_string()
    };
    cm.prewarm(&input).await.unwrap_or_else(|e| panic!("prewarm failed: {:?}", e));
    let fqdn = calculate_fqdn(&"test".to_string(), &"0.1.1".to_string());
    let c1 = cm.acquire_container(&fqdn, &TEST_TID).await.expect("should have gotten prewarmed container");
    
    let c1_cont = c1.container.clone();
    drop(c1);

    cm.remove_container(c1_cont.clone(), true, &TEST_TID).await.unwrap_or_else(|e| panic!("remove container failed: {:?}", e));
    let cast_container = cast::<ContainerdContainer>(&c1_cont, &TEST_TID).unwrap();

    let client = reqwest::Client::new();
    let result = client.get(&cast_container.base_uri)
      .send()
      .await;
    match result {
    Ok(result) => panic!("Unpexpected result when container should be gone {:?}", result),
    Err(e) => {
        if e.is_request() {
          if let Some(status) = e.status() {
            assert_eq!(status, 111, "unexpected return status {:?} with error {:?}", status, e);
          }
        }
        else {
          panic!("Unexpected error connecting to gone container {:?}", e);
        }
      },
    }
    // assert_ne!(result.status(), 111, "unexpected return status for container {:?}", c1_cont);

    let c2 = cm.acquire_container(&fqdn, &TEST_TID).await.expect("should have gotten prewarmed container");
    assert_ne!(c1_cont.container_id(), c2.container.container_id(), "Second container should have different ID because container is gone");
  }

}
