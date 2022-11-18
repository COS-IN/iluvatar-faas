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

#[macro_export]
macro_rules! invoker_svc {
  () => {
    {
      iluvatar_library::utils::file::ensure_temp_dir().unwrap();
      let cfg = Configuration::boxed(false, &"tests/resources/worker.dev.json".to_string()).unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
      let factory = LifecycleFactory::new(cfg.container_resources.clone(), cfg.networking.clone(), cfg.limits.clone());
      let lifecycle = factory.get_lifecycle_service(&TEST_TID, true).await.unwrap_or_else(|e| panic!("Failed to create lifecycle: {}", e));

      let cm = ContainerManager::boxed(cfg.limits.clone(), cfg.container_resources.clone(), lifecycle.clone(), &TEST_TID).await;
      let invoker_fact = InvokerFactory::new(cm.clone(), cfg.limits.clone(), cfg.invocation.clone());
      let invoker = invoker_fact.get_invoker_service(&TEST_TID).unwrap_or_else(|e| panic!("Failed to create invoker service because: {}", e));
      (cfg, cm, invoker)
    }
  };
}