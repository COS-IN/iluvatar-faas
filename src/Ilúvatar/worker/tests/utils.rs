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
macro_rules! container_mgr {
  () => {
    {
      iluvatar_lib::utils::ensure_temp_dir().unwrap();
      let cfg = Configuration::boxed(Some("tests/resources/worker.json")).unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
      let nm = NamespaceManager::boxed(cfg.clone(), &TEST_TID);
      nm.ensure_bridge(&TEST_TID).unwrap();
      let cm = ContainerManager::new(cfg.clone(), nm.clone()).await.unwrap();
      (cfg, nm, cm)
    }
  };
}

#[macro_export]
macro_rules! invoker_svc {
  () => {
    {
      iluvatar_lib::utils::ensure_temp_dir().unwrap();
      let cfg = Configuration::boxed(Some("tests/resources/worker.json")).unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
      let nm = NamespaceManager::boxed(cfg.clone(), &TEST_TID);
      nm.ensure_bridge(&TEST_TID).unwrap();
      let cm = Arc::new(ContainerManager::new(cfg.clone(), nm.clone()).await.unwrap());
      let invoker = Arc::new(InvokerService::new(cm.clone()));
      (cfg, nm, cm, invoker)
    }
  };
}