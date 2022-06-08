use crate::containers::containerlife;

use iluvatar_lib::rpc::RegisterRequest;
use anyhow::Result;
use std::collections::HashMap;
use std::cell::RefCell;
use std::sync::Arc;

// TODO:
// use std::sync::Mutex;
// use tokio::sync::Mutex;
use parking_lot::RwLock;


#[derive(Debug)]
struct RegisteredFunction {
  function_name: String,
  function_version: String,
  image_name: String,
  memory: u32,
  cpus: u32,
}

#[derive(Debug)]
pub struct ContainerManager {
  registered_functions: Arc<RwLock<RefCell<HashMap<String, RegisteredFunction>>>>,
}

impl ContainerManager {
  pub fn new() -> ContainerManager {
    ContainerManager {
      registered_functions: Arc::new(RwLock::new(RefCell::new(HashMap::new()))),
    }
  }

  pub async fn register(&self, request: &RegisterRequest) -> Result<()> {
    let mut life = containerlife::ContainerLifecycle::new();

    let fqn = format!("{}/{}", request.function_name, request.function_version);

    { // read lock
      let acquired_reg = self.registered_functions.read();
      if acquired_reg.borrow().contains_key(&fqn) {
        anyhow::bail!("Function {} is already registered!", fqn);
      }
    }

    life.ensure_image(&request.image_name).await?;
    let registration = RegisteredFunction {
      function_name: request.function_name.clone(),
      function_version: request.function_version.clone(),
      image_name: request.image_name.clone(),
      memory: request.memory,
      cpus: request.cpus,
    };

    { // write lock
      let acquired_reg = self.registered_functions.write();
      acquired_reg.borrow_mut().insert(fqn, registration);
    }
    return Ok(());
  }
}