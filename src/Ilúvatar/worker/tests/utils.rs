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
      let cfg = Configuration::boxed(Some("tests/resources/worker.json")).unwrap_or_else(|e| panic!("Failed to load config file for test: {}", e));
      let mut nm = NamespaceManager::new(cfg.clone());
      nm.ensure_bridge().unwrap();
      let nm = Arc::new(nm);
      let cm = ContainerManager::new(cfg.clone(), nm.clone());
      (cfg, nm, cm)
    }
  };
}
