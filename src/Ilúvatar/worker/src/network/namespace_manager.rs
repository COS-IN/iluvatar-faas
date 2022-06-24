use crate::network::network_structs::ContdNamespace;
use crate::{config::WorkerConfig, network::network_structs::Namespace};
use std::sync::Arc;
use std::{process::Command, collections::HashMap};
use anyhow::Result;
use iluvatar_lib::transaction::{TransactionId, NAMESPACE_POOL_WORKER_TID};
use iluvatar_lib::{utils, bail_error};
use parking_lot::Mutex;
use std::env;
use std::fs::File;
use std::io::Write;
use log::*;
use guid_create::GUID;

#[derive(Debug)]
#[allow(unused)]
pub struct NamespaceManager {
  config: WorkerConfig,
  net_conf_path: String,
  pool: NamespacePool,
}

type NamespacePool = Arc<Mutex<Vec<Arc<Namespace>>>>;

const CNI_PATH_VAR: &str = "CNI_PATH";
const NETCONFPATH_VAR: &str = "NETCONFPATH";

impl NamespaceManager {
  fn new(config: WorkerConfig) -> NamespaceManager {
    return NamespaceManager {
      config,
      net_conf_path: utils::TEMP_DIR.to_string(),
      pool: Arc::new(Mutex::new(Vec::new())),
    }
  }

  pub fn boxed(config: WorkerConfig, tid: &TransactionId) -> Arc<NamespaceManager> {
    let ns = Arc::new(NamespaceManager::new(config.clone()));
    debug!("[{}] creating namespace manager", tid);

    if config.networking.use_pool {
      let cln = ns.clone();
      info!("[{}] launching namespace pool monitor thread", tid);
      let _handle = tokio::spawn(async move {
        NamespaceManager::monitor_pool(config, cln).await;
      });
    }
    ns
  }

  async fn monitor_pool(config: WorkerConfig, nm: Arc<NamespaceManager>) {
    let tid: &TransactionId = &NAMESPACE_POOL_WORKER_TID;
    loop {
      while nm.pool_size() < config.networking.pool_size {
        let ns = match nm.create_namespace(&GUID::rand().to_string(), tid) {
            Ok(ns) => ns,
            Err(e) => {
              error!("[{}] Failed creating namespace in monitor: {}", tid, e);
              break;
            },
        };
        match nm.return_namespace(Arc::new(ns), tid) {
            Ok(_) => {},
            Err(e) => error!("[{}] Failed giving namespace to pool: {}", tid, e),
        };
      }
      tokio::time::sleep(std::time::Duration::from_secs(config.networking.pool_freq_sec)).await;
    }
  }

  pub fn ensure_bridge(&self, tid: &TransactionId) -> Result<()> {
    info!("[{}] Ensuring network bridge", tid);

    let temp_file = utils::temp_file(&"il_worker_br".to_string(), "json")?;

    let mut file = File::create(temp_file)?;
    let bridge_json = include_str!("../resources/cni/il_worker_br.json");
    writeln!(&mut file, "{}", bridge_json)?;

    let mut env: HashMap<String, String> = env::vars().collect();
    env.insert(CNI_PATH_VAR.to_string(), self.config.networking.cni_plugin_bin.clone());
    env.insert(NETCONFPATH_VAR.to_string(), self.net_conf_path.to_string());

    let name = "mk_bridge_throwaway".to_string();

    if ! self.namespace_exists(&name) {
      debug!("[{}] Namespace '{}' does not exists, making", tid, name);
      NamespaceManager::create_namespace_internal(&name, tid)?;
    } else {
      debug!("[{}] Namespace '{}' already exists, skipping", tid, name);
    }

    let nspth = NamespaceManager::net_namespace(&name);

    if self.bridge_exists(&nspth)? {
      debug!("[{}] Bridge already exists, skipping", tid);
      return Ok(());
    }

    let mut cmd = Command::new(self.config.networking.cnitool.clone());
    cmd.args(["add", &self.config.networking.cni_name.as_str(), &nspth.as_str()])
            .envs(&env);
    debug!("[{}] Command to create network bridge: '{:?}'", tid, cmd);

    let out = cmd.output();
    debug!("[{}] Output from creating network bridge: '{:?}'", tid, out);
    match out {
        Ok(output) => {
          if let Some(status) = output.status.code() {
            if status == 0 {
              Ok(())
            } else {
              panic!("Failed to create bridge with exit code '{}' and error '{:?}'", status, output)
            }
          } else {
            panic!("Failed to create bridge with no exit code and error '{:?}'", output)
          }
        },
        Err(e) => {
          panic!("Failed to create bridge with error '{:?}'", e)
        },
    }
  }

  fn bridge_exists(&self, nspth: &String) -> Result<bool> {
    let mut env: HashMap<String, String> = env::vars().collect();
    env.insert(CNI_PATH_VAR.to_string(), self.config.networking.cni_plugin_bin.clone());
    env.insert(NETCONFPATH_VAR.to_string(), self.net_conf_path.to_string());

    let output = Command::new(self.config.networking.cnitool.clone())
            .args(["check", &self.config.networking.cni_name.as_str(), &nspth.as_str()])
            .envs(&env)
            .output()?;
    if let Some(status) = output.status.code() {
      if status == 0 {
        Ok(true)
      } else {
        Ok(false)
      }
    } else {
      panic!("Error checking bridge status '{:?}'", output)
    }
  }
  
  pub fn net_namespace(name: &String) -> String {
    format!("/run/netns/{}", name)
  }

  pub fn namespace_exists(&self, name: &String) -> bool {
    let nspth = NamespaceManager::net_namespace(name);
    std::path::Path::new(&nspth).exists()
  }
  
  fn create_namespace_internal(name: &String, tid: &TransactionId) -> Result<()> {
    let out = Command::new("ip")
            .args(["netns", "add", name])
            .output()?;

    debug!("[{}] internal create namespace '{}' via ip: '{:?}'", tid, name, out);
    if let Some(status) = out.status.code() {
      if status == 0 {
        return Ok(());
      } else {
        bail_error!("[{}] Failed to create internal namespace with exit code '{}' and error '{:?}'", tid, status, out)
      }
    } else {
      bail_error!("[{}] Failed to create delete with no exit code and error '{:?}'", tid, out)
    }
  }

  /// cleanup_addresses
  /// The addresses returned by the bridge plugin of cnitool will have subnet information
  /// E.g. 10.10.0.3/16
  /// We don't need it and can discard it here
  fn cleanup_addresses(ns: &mut ContdNamespace) {
    for ip in &mut ns.ips[..] {
      if ip.address.contains("/") {
        let v: Vec<&str> = ip.address.split("/").collect();
        ip.address = v[0].to_string();
      }
    }

  }

  pub fn pool_size(&self) -> usize {
    return self.pool.lock().len();
  }
  
  pub fn create_namespace(&self, name: &String, tid: &TransactionId) -> Result<Namespace> {
    info!("[{}] Creating new namespace: {}", tid, name);
    let mut env: HashMap<String, String> = env::vars().collect();
    env.insert(CNI_PATH_VAR.to_string(), self.config.networking.cni_plugin_bin.clone());
    env.insert(NETCONFPATH_VAR.to_string(), self.net_conf_path.to_string());
  
    let nspth = NamespaceManager::net_namespace(name);
    NamespaceManager::create_namespace_internal(&name, tid)?;

    let out = Command::new(self.config.networking.cnitool.clone())
              .args(["add", &self.config.networking.cni_name.as_str(), &nspth.as_str()])
              .envs(&env)
              .output()?;

    match serde_json::from_slice(&out.stdout) {
        Ok(mut ns) => {
          NamespaceManager::cleanup_addresses(&mut ns);
          debug!("[{}] Namespace '{}' created. Output: '{:?}'", tid, &name, ns);
          Ok(Namespace {
            name: name.to_string(),
            namespace: ns
          })
        },
        Err(e) => bail_error!("[{}] JSON error in create_namespace: {}", tid, e),
    }
  }

  pub fn get_namespace(&self, tid: &TransactionId) -> Result<Arc<Namespace>> {
    let mut locked = self.pool.lock();
    if self.config.networking.use_pool && locked.len() > 0 {
      match locked.pop() {
        Some(ns) =>{
          debug!("[{}] Assigning namespace {}", tid, ns.name);
          return Ok(ns);
        },
        None => bail_error!("[{}] Namespace pool of length {} should have had thing in it", tid, locked.len()),
      }
    } else {
      debug!("[{}] Creating new namespace, pool is empty", tid);
      let ns = Arc::new(self.create_namespace(&GUID::rand().to_string(), tid)?);
      return Ok(ns);
    }
  }

  pub fn return_namespace(&self, ns: Arc<Namespace>, tid: &TransactionId) -> Result<()> {
    debug!("[{}] Namespace {} being returned", tid, ns.name);
    if self.config.networking.use_pool {  
      let mut locked = self.pool.lock();
      locked.push(ns);
      return Ok(());
    } else {
      return self.delete_namespace(&ns.name, tid);
    }
  }

  fn delete_namespace(&self, name: &String, tid: &TransactionId) -> Result<()> {
    let out = Command::new("ip")
            .args(["netns", "delete", name])
            .output()?;

    debug!("[{}] internal delete namespace '{}' via ip: '{:?}'", tid, name, out);
    if let Some(status) = out.status.code() {
      if status == 0 {
        return Ok(());
      } else {
        bail_error!("[{}] Failed to delete namespace with exit code '{}' and error '{:?}'", tid, status, out)
      }
    } else {
      bail_error!("[{}] Failed to delete delete with no exit code and error '{:?}'", tid, out)
    }
  }
}
