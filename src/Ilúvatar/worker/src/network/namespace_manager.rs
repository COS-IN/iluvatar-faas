use crate::{config::WorkerConfig, network::network_structs::Namespace};
use std::{process::Command, collections::HashMap};
use anyhow::Result;
use iluvatar_lib::utils;
use std::env;
use std::fs::File;
use std::io::Write;
use log::*;

#[derive(Debug)]
#[allow(unused)]
pub struct NamespaceManager {
  config: WorkerConfig,
  net_conf_path: String,
}

const CNI_PATH_VAR: &str = "CNI_PATH";
const NETCONFPATH_VAR: &str = "NETCONFPATH";

impl NamespaceManager {
  pub fn new(config: WorkerConfig) -> NamespaceManager {
    return NamespaceManager {
      config,
      net_conf_path: ".".to_string()
    }
  }

  pub fn ensure_bridge(&mut self) -> Result<()> {
    info!("Ensuring network bridge");

    let temp_file = utils::temp_file(&"il_worker_br".to_string(), "json")?;
    self.net_conf_path = utils::TEMP_DIR.to_string();

    let mut file = File::create(temp_file)?;
    let bridge_json = include_str!("../resources/cni/il_worker_br.json");
    writeln!(&mut file, "{}", bridge_json)?;

    let mut env: HashMap<String, String> = env::vars().collect();
    env.insert(CNI_PATH_VAR.to_string(), self.config.networking.cni_plugin_bin.clone());
    env.insert(NETCONFPATH_VAR.to_string(), self.net_conf_path.to_string());

    let name = "mk_bridge_throwaway".to_string();

    if ! self.namespace_exists(&name) {
      debug!("Namespace '{}' does not exists, making", name);
      self.create_namespace_internal(&name)?;
    } else {
      debug!("Namespace '{}' already exists, skipping", name);
    }

    let nspth = self.net_namespace(&name);

    if self.bridge_exists(&nspth)? {
      debug!("Bridge already exists, skipping");
      return Ok(());
    }

    let mut cmd = Command::new(self.config.networking.cnitool.clone());
    cmd.args(["add", &self.config.networking.cni_name.as_str(), &nspth.as_str()])
            .envs(&env);
    debug!("Command to create network bridge: '{:?}'", cmd);

    let out = cmd.output();
    debug!("Output from creating network bridge: '{:?}'", out);
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
  
  pub fn net_namespace(&self, name: &String) -> String {
    format!("/run/netns/{}", name)
  }

  pub fn namespace_exists(&self, name: &String) -> bool {
    let nspth = self.net_namespace(name);
    std::path::Path::new(&nspth).exists()
  }
  
  fn create_namespace_internal(&self, name: &String) -> Result<()> {
    let out = Command::new("ip")
            .args(["netns", "add", name])
            .output()?;

    debug!("internal create namespace '{}' via ip: '{:?}'", name, out);
    if let Some(status) = out.status.code() {
      if status == 0 {
        return Ok(());
      } else {
        anyhow::bail!("Failed to create internal namespace with exit code '{}' and error '{:?}'", status, out)
      }
    } else {
      anyhow::bail!("Failed to create bridge with no exit code and error '{:?}'", out)
    }
  }

  /// cleanup_addresses
  /// The addresses returned by the bridge plugin of cnitool will have subnet information
  /// E.g. 10.10.0.3/16
  /// We don't need it and can discard it here
  fn cleanup_addresses(&self, ns: &mut Namespace) {
    for ip in &mut ns.ips[..] {
      if ip.address.contains("/") {
        let v: Vec<&str> = ip.address.split("/").collect();
        ip.address = v[0].to_string();
      }
    }

  }

  pub fn create_namespace(&self, name: &String) -> Result<Namespace> {
    let mut env: HashMap<String, String> = env::vars().collect();
    env.insert(CNI_PATH_VAR.to_string(), self.config.networking.cni_plugin_bin.clone());
    env.insert(NETCONFPATH_VAR.to_string(), self.net_conf_path.to_string());
  
    let nspth = self.net_namespace(name);
    self.create_namespace_internal(&name)?;

    let out = Command::new(self.config.networking.cnitool.clone())
              .args(["add", &self.config.networking.cni_name.as_str(), &nspth.as_str()])
              .envs(&env)
              .output()?;

    let mut ns: Namespace = serde_json::from_slice(&out.stdout)?;
    self.cleanup_addresses(&mut ns);
    debug!("Namespace '{}' created. Output: '{:?}'", name, ns);
    
    Ok(ns)
  }

  pub fn remove_namespace(&self, _name: String) -> Result<()> {
    // TODO: this function
    Ok(())
  }
}
