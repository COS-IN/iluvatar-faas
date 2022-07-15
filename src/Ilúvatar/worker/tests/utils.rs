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
      iluvatar_lib::utils::file::ensure_temp_dir().unwrap();
      let cfg = Configuration::boxed(false, &"tests/resources/worker.dev.json".to_string()).unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
      let factory = LifecycleFactory::new(cfg.clone());
      let lifecycle = factory.get_lifecycle_service(&TEST_TID, true).await.unwrap_or_else(|e| panic!("Failed to create lifecycke: {}", e));

      let cm = Arc::new(ContainerManager::new(cfg.limits.clone(), cfg.container_resources.clone(), lifecycle.clone()).await.unwrap());
      let invoker = InvokerService::boxed(cm.clone(), &TEST_TID, cfg.limits.clone());
      (cfg, cm, invoker)
    }
  };
}