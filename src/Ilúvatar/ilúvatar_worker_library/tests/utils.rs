use std::{sync::Arc, collections::HashMap};

use iluvatar_library::{transaction::TEST_TID, logging::{start_tracing, LoggingConfig}, characteristics_map::{CharacteristicsMap, AgExponential}};
use iluvatar_worker_library::{worker_api::config::{Configuration, WorkerConfig}, services::{containers::{LifecycleFactory, containermanager::ContainerManager}, invocation::{InvokerFactory, invoker_trait::Invoker}}};

#[macro_export]
macro_rules! assert_error {
  ($err:expr, $exp:expr, $noerr:expr) => {
    match $err {
      Ok(_) => panic!("{}", $noerr),
      Err(e) => {
        assert_eq!(e.to_string(), $exp);
      },
    };
  };
}

/// Creates/sets up the structs needed to test an invoker setup
/// The [env] will set process-level environment vars to adjust config. Clean these up with [clean_env]
/// Passing [log] = Some(true)  will enable logging to stdout. Only pass this on one test as it will be set globally and can only be done once
/// [config_pth] is an optional path to config to load
pub async fn test_invoker_svc(config_pth: Option<String>, env: Option<&HashMap<String, String>>, log: Option<bool>) -> (Option<impl Drop>, WorkerConfig, Arc<ContainerManager>, Arc<dyn Invoker>) {
  iluvatar_library::utils::file::ensure_temp_dir().unwrap();
  let log = log.unwrap_or(false);
  let test_cfg_pth = config_pth.unwrap_or_else(|| "tests/resources/worker.dev.json".to_string());
  if let Some(env) = env {
    for (k,v) in env {
      std::env::set_var(k,v);
    }  
  }
  let cfg = Configuration::boxed(false, &test_cfg_pth).unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
  let fake_logging = Arc::new(LoggingConfig {
    level: cfg.logging.level.clone(),
    directory: "".to_string(),
    basename: "".to_string(),
    spanning: cfg.logging.spanning.clone(),
    flame: "".to_string(),
    span_energy_monitoring: false,
  });
  let _log = match log {
    true => Some(start_tracing(fake_logging, cfg.graphite.clone(), &"TEST".to_string(), &TEST_TID).unwrap_or_else(|e| panic!("Failed to load start tracing for test: {}", e))),
    false => None,
  };
  let cmap = Arc::new(CharacteristicsMap::new(AgExponential::new(0.6)));
  let factory = LifecycleFactory::new(cfg.container_resources.clone(), cfg.networking.clone(), cfg.limits.clone());
  let lifecycle = factory.get_lifecycle_service(&TEST_TID, true).await.unwrap_or_else(|e| panic!("Failed to create lifecycle: {}", e));

  let cm = ContainerManager::boxed(cfg.limits.clone(), cfg.container_resources.clone(), lifecycle.clone(), &TEST_TID).await.unwrap_or_else(|e| panic!("Failed to create container manger for test: {}", e));
  let invoker_fact = InvokerFactory::new(cm.clone(), cfg.limits.clone(), cfg.invocation.clone(), cmap);
  let invoker = invoker_fact.get_invoker_service(&TEST_TID).unwrap_or_else(|e| panic!("Failed to create invoker service because: {}", e));
  (_log, cfg, cm, invoker)
}

pub fn clean_env(env: &HashMap<String, String>) {
  for (k,_) in env {
    std::env::remove_var(k);
  }  
}
