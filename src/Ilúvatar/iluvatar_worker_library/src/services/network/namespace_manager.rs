use crate::services::network::network_structs::{ContdNamespace, Namespace};
use crate::worker_api::worker_config::NetworkingConfig;
use anyhow::Result;
use guid_create::GUID;
use iluvatar_library::threading::os_thread;
use iluvatar_library::transaction::{TransactionId, NAMESPACE_POOL_WORKER_TID};
use iluvatar_library::{
    bail_error, utils,
    utils::{execute_cmd, execute_cmd_checked},
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::{env, fs::File, io::Write};
use tracing::{debug, error, info, warn};

#[derive(Debug)]
#[allow(unused)]
pub struct NamespaceManager {
    config: Arc<NetworkingConfig>,
    pool: NamespacePool,
    _worker_thread: Option<JoinHandle<()>>,
}

lazy_static::lazy_static! {
  pub static ref BRIDGE_EXISTS: Mutex<bool> = Mutex::new(false);
}
type NamespacePool = Arc<Mutex<Vec<Arc<Namespace>>>>;

const CNI_PATH_VAR: &str = "CNI_PATH";
const NETCONFPATH_VAR: &str = "NETCONFPATH";
const NAMESPACE_IDENTIFIER: &str = "ilunns-";
const BRIDGE_NET_ID: &str = "mk_bridge_throwaway";

impl NamespaceManager {
    fn new(config: Arc<NetworkingConfig>, worker_thread: Option<JoinHandle<()>>) -> Self {
        NamespaceManager {
            config,
            pool: Arc::new(Mutex::new(Vec::new())),
            _worker_thread: worker_thread,
        }
    }

    pub fn boxed(config: Arc<NetworkingConfig>, tid: &TransactionId, ensure_bridge: bool) -> Result<Arc<Self>> {
        debug!(tid = tid, "creating namespace manager");
        Self::ensure_net_config_file(tid, &config)?;
        if ensure_bridge {
            Self::ensure_bridge(tid, &config)?;
        }
        Ok(match config.use_pool {
            false => Arc::new(Self::new(config.clone(), None)),
            true => {
                info!(tid = tid, "Launching namespace pool monitor thread");
                let (handle, tx) = os_thread(
                    config.pool_freq_ms,
                    NAMESPACE_POOL_WORKER_TID.clone(),
                    Arc::new(Self::monitor_pool),
                )?;
                let ns = Arc::new(Self::new(config.clone(), Some(handle)));
                tx.send(ns.clone())?;
                ns
            },
        })
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self), fields(tid=tid)))]
    fn monitor_pool(&self, tid: &TransactionId) {
        while self.pool_size() < self.config.pool_size {
            let ns = match self.create_namespace(&self.generate_net_namespace_name(), tid) {
                Ok(ns) => ns,
                Err(e) => {
                    error!(tid=tid, error=%e, "Failed creating namespace in monitor");
                    break;
                },
            };
            match self.return_namespace(Arc::new(ns), tid) {
                Ok(_) => {},
                Err(e) => error!(tid=tid, error=%e, "Failed giving namespace to pool"),
            };
        }
    }

    /// the environment variables to run the cni tool with
    fn cmd_environment(config: &Arc<NetworkingConfig>) -> HashMap<String, String> {
        let mut env: HashMap<String, String> = env::vars().collect();
        env.insert(CNI_PATH_VAR.to_string(), config.cni_plugin_bin.clone());
        env.insert(NETCONFPATH_VAR.to_string(), utils::file::TEMP_DIR.to_string());
        env
    }

    /// makes sure the bridge necessary for container networking
    pub fn ensure_bridge(tid: &TransactionId, config: &Arc<NetworkingConfig>) -> Result<()> {
        let mut bridge_check = BRIDGE_EXISTS.lock();
        if *bridge_check {
            return Ok(());
        }
        info!(tid = tid, "Ensuring network bridge");
        // multiple workers on one machine can compete over this
        // catch an error if we aren't the race winner and try again, will do nothing if bridge exists
        match Self::try_ensure_bridge(tid, config) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!(tid=tid, error=%e, "Error on first attempt making bridge, retring");
                std::thread::sleep(std::time::Duration::from_millis(config.pool_freq_ms));
                Self::try_ensure_bridge(tid, config)
            },
        }?;
        *bridge_check = true;
        Ok(())
    }

    /// The path to the resolv.conf file created by the namespace manager, for DNS resolution
    pub fn resolv_conf_path() -> String {
        utils::file::temp_file_pth("resolv", "conf")
    }

    /// Create a resolv.conf file to be available globally
    fn make_resolv_conf(tid: &TransactionId) -> Result<()> {
        let resolv_conf = include_str!("../../resources/resolv.conf").to_string();
        let temp_file = Self::resolv_conf_path();
        let mut file = match File::create(temp_file) {
            Ok(f) => f,
            Err(e) => anyhow::bail!("[{}] error creating 'resolv' temp file: {}", tid, e),
        };
        match writeln!(&mut file, "{}", resolv_conf) {
            Ok(_) => Ok(()),
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to write 'resolv' conf file"),
        }
    }

    fn ensure_net_config_file(tid: &TransactionId, config: &Arc<NetworkingConfig>) -> Result<()> {
        let temp_file = utils::file::temp_file_pth("il_worker_br", "conf");

        let mut file = match File::create(temp_file) {
            Ok(f) => f,
            Err(e) => anyhow::bail!("[{}] error creating 'il_worker_br' temp file: {}", tid, e),
        };
        let bridge_json = include_str!("../../resources/cni/il_worker_br.json")
            .to_string()
            .replace("$BRIDGE", &config.bridge);

        match writeln!(&mut file, "{}", bridge_json) {
            Ok(_) => Ok(()),
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to write 'il_worker_br' conf file"),
        }
    }

    fn try_ensure_bridge(tid: &TransactionId, config: &Arc<NetworkingConfig>) -> Result<()> {
        if !Self::hardware_exists(tid, config)? {
            anyhow::bail!(
                "Hardware interface '{}' does not exist or cannot be found!",
                config.hardware_interface
            );
        }
        Self::make_resolv_conf(tid)?;

        let env = Self::cmd_environment(config);
        let name = BRIDGE_NET_ID.to_string();

        if !Self::namespace_exists(&name) {
            debug!(tid=tid, namespace=%name, "Namespace does not exists, making");
            Self::create_namespace_internal(&name, tid)?;
        } else {
            debug!(tid=tid, namespace=%name, "Namespace already exists, skipping");
        }

        let nspth = Self::net_namespace(&name);

        if Self::bridge_exists(&nspth, tid, config)? {
            debug!(tid = tid, "Bridge already exists, skipping");
            return Ok(());
        }

        let output = execute_cmd(
            &config.cnitool,
            vec!["add", config.cni_name.as_str(), nspth.as_str()],
            Some(&env),
            tid,
        );
        debug!(tid=tid, output=?output, "Output from creating network bridge");
        match output {
            Ok(output) => {
                if let Some(status) = output.status.code() {
                    if status != 0 {
                        anyhow::bail!(
                            "[{}] Failed to create bridge with exit code '{}' and error '{:?}'",
                            tid,
                            status,
                            output
                        );
                    }
                } else {
                    anyhow::bail!(
                        "[{}] Failed to create bridge with no exit code and error '{:?}'",
                        tid,
                        output
                    );
                }
            },
            Err(e) => {
                anyhow::bail!("[{}] Failed to create bridge with error '{:?}'", tid, e);
            },
        };

        // https://unix.stackexchange.com/questions/248504/bridged-interfaces-do-not-have-internet-access
        match execute_cmd_checked(
            "/usr/sbin/iptables",
            [
                "-t",
                "nat",
                "-A",
                "POSTROUTING",
                "-o",
                &config.hardware_interface,
                "-j",
                "MASQUERADE",
            ],
            None,
            tid,
        ) {
            Ok(_) => debug!(tid = tid, "Setting nat on hardware interface succeded"),
            Err(e) => bail_error!(tid=tid, error=%e, "Setting nat on hardware interface failed"),
        };
        match execute_cmd_checked(
            "/usr/sbin/iptables",
            vec![
                "-A",
                "FORWARD",
                "-m",
                "conntrack",
                "--ctstate",
                "RELATED,ESTABLISHED",
                "-j",
                "ACCEPT",
            ],
            None,
            tid,
        ) {
            Ok(_) => debug!(tid = tid, "Setting conntrack succeded"),
            Err(e) => bail_error!(tid=tid, error=%e, "Setting conntrack failed"),
        };
        match execute_cmd_checked(
            "/usr/sbin/iptables",
            vec![
                "-A",
                "FORWARD",
                "-i",
                config.bridge.as_str(),
                "-o",
                config.hardware_interface.as_str(),
                "-j",
                "ACCEPT",
            ],
            None,
            tid,
        ) {
            Ok(_) => debug!(tid = tid, "Forwarding bridge to interface succeded"),
            Err(e) => bail_error!(tid=tid, error=%e, "Forwarding bridge to interface failed"),
        };
        match execute_cmd_checked("/sbin/sysctl", vec!["-w", "net.ipv4.conf.all.forwarding=1"], None, tid) {
            Ok(_) => debug!(tid = tid, "Setting upv4 forwarding succeeded"),
            Err(e) => bail_error!(tid=tid, error=%e, "Setting upv4 forwarding failed"),
        };
        Ok(())
    }

    /// Return `true` if the hardware network interface exists
    fn hardware_exists(tid: &TransactionId, config: &Arc<NetworkingConfig>) -> Result<bool> {
        let env = Self::cmd_environment(config);
        let output = execute_cmd("/usr/sbin/ifconfig", vec![&config.hardware_interface], Some(&env), tid);
        match output {
            Ok(output) => {
                if let Some(status) = output.status.code() {
                    if status == 0 {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    bail_error!(tid=tid, output=?output, "No error code when checking hardware interface status");
                }
            },
            Err(e) => bail_error!(tid=tid, error=%e, "Error checking hardware interface status"),
        }
    }

    fn bridge_exists(nspth: &str, tid: &TransactionId, config: &Arc<NetworkingConfig>) -> Result<bool> {
        let env = Self::cmd_environment(config);
        let output = execute_cmd(
            &config.cnitool,
            vec!["check", config.cni_name.as_str(), nspth],
            Some(&env),
            tid,
        );
        match output {
            Ok(output) => {
                if let Some(status) = output.status.code() {
                    if status == 0 {
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    bail_error!(tid=tid, output=?output, "Error checking bridge status");
                }
            },
            Err(e) => bail_error!(tid=tid, error=%e, "Error checking bridge status"),
        }
    }

    /// Format the network namespace name to the full path
    pub fn net_namespace(name: &str) -> String {
        format!("/run/netns/{}", name)
    }

    fn namespace_exists(name: &str) -> bool {
        let nspth = Self::net_namespace(name);
        std::path::Path::new(&nspth).exists()
    }

    fn create_namespace_internal(name: &str, tid: &TransactionId) -> Result<()> {
        let out = match execute_cmd_checked("/bin/ip", vec!["netns", "add", name], None, tid) {
            Ok(out) => out,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to launch 'ip netns add' command"),
        };

        debug!(tid=tid, namespace=%name, output=?out, "internal create namespace via ip");
        if let Some(status) = out.status.code() {
            if status == 0 {
                Ok(())
            } else {
                bail_error!(tid=tid, status=?status, stdout=?out, "Failed to create internal namespace")
            }
        } else {
            bail_error!(tid=tid, stdout=?out, "Failed to create internal namespace with no exit code")
        }
    }

    /// The addresses returned by the bridge plugin of cnitool will have subnet information
    /// E.g. 10.10.0.3/16
    /// We don't need it and can discard it here
    fn cleanup_addresses(&self, ns: &mut ContdNamespace) {
        for ip in &mut ns.ips[..] {
            if ip.address.contains('/') {
                let v: Vec<&str> = ip.address.split('/').collect();
                ip.address = v[0].to_string();
            }
        }
    }

    pub fn pool_size(&self) -> usize {
        self.pool.lock().len()
    }

    fn create_namespace(&self, name: &str, tid: &TransactionId) -> Result<Namespace> {
        debug!(tid=tid, namespace=%name, "Creating new namespace");
        let env = Self::cmd_environment(&self.config);
        let nspth = Self::net_namespace(name);
        Self::create_namespace_internal(name, tid)?;

        let out = execute_cmd(
            &self.config.cnitool,
            vec!["add", self.config.cni_name.as_str(), nspth.as_str()],
            Some(&env),
            tid,
        )?;

        let stdout = String::from_utf8_lossy(&out.stdout);
        match serde_json::from_str(&stdout) {
            Ok(mut ns) => {
                self.cleanup_addresses(&mut ns);
                debug!(tid=tid, namespace=%name, containerd_namespace=?ns, "Namespace created");
                Ok(Namespace {
                    name: name.to_string(),
                    namespace: ns,
                })
            },
            Err(e) => {
                bail_error!(tid=tid, error=%e, output=?out, "JSON error in create_namespace")
            },
        }
    }

    pub fn get_namespace(&self, tid: &TransactionId) -> Result<Arc<Namespace>> {
        let mut locked = self.pool.lock();
        if self.config.use_pool && !locked.is_empty() {
            match locked.pop() {
                Some(ns) => {
                    debug!(tid=tid, namespace=%ns.name, "Assigning namespace");
                    Ok(ns)
                },
                None => {
                    bail_error!(tid=tid, length=%locked.len(), "Namespace pool should have had a thing in it")
                },
            }
        } else {
            drop(locked);
            debug!(tid = tid, "Creating new namespace, pool is empty");
            let ns = Arc::new(self.create_namespace(&self.generate_net_namespace_name(), tid)?);
            Ok(ns)
        }
    }

    fn generate_net_namespace_name(&self) -> String {
        format!("{}{}", NAMESPACE_IDENTIFIER, GUID::rand())
    }

    fn is_owned_namespace(&self, ns: &str) -> bool {
        ns.starts_with(NAMESPACE_IDENTIFIER)
    }

    pub fn return_namespace(&self, ns: Arc<Namespace>, tid: &TransactionId) -> Result<()> {
        debug!(tid=tid, namespace=%ns.name, "Namespace being returned");
        if self.config.use_pool {
            let mut locked = self.pool.lock();
            locked.push(ns);
            Ok(())
        } else {
            self.delete_namespace(ns.name.as_str(), tid)
        }
    }

    fn delete_namespace(&self, name: &str, tid: &TransactionId) -> Result<()> {
        let env = Self::cmd_environment(&self.config);
        let nspth = Self::net_namespace(name);
        let out = execute_cmd(
            &self.config.cnitool,
            vec!["del", self.config.cni_name.as_str(), nspth.as_str()],
            Some(&env),
            tid,
        )?;

        debug!(tid=tid, namespace=%name, output=?out, "internal del namespace via cnitool");
        if let Some(status) = out.status.code() {
            if status != 0 {
                bail_error!(tid=tid, stdout=?out, status=?status, "cnitool failed to del namespace")
            }
        } else {
            bail_error!(tid=tid, stdout=?out, "cnitool failed to del with no exit code")
        }

        let out = execute_cmd("/bin/ip", vec!["netns", "delete", name], None, tid)?;
        debug!(tid=tid, namespace=%name, output=?out, "internal delete namespace via ip");
        if let Some(status) = out.status.code() {
            if status != 0 {
                bail_error!(tid=tid, stdout=?out, status=?status, "Failed to delete namespace")
            }
        } else {
            bail_error!(tid=tid, stdout=?out, "Failed to delete with no exit code")
        }
        Ok(())
    }

    pub async fn clean(&self, svc: Arc<Self>, tid: &TransactionId) -> Result<()> {
        info!(tid = tid, "Deleting all owned namespaces");
        let out = execute_cmd("/bin/ip", vec!["netns"], None, tid)?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        let lines = stdout.split('\n');
        let mut handles = vec![];
        for line in lines {
            if self.is_owned_namespace(line) {
                let split: Vec<&str> = line.split(' ').collect();
                let name = split[0].to_string();
                let tid_c = tid.clone();
                let svc_c = svc.clone();
                handles.push(tokio::task::spawn_blocking(move || {
                    svc_c.delete_namespace(&name, &tid_c)
                }));
            }
        }

        let mut failed = 0;
        let num_handles = handles.len();
        for h in handles {
            match h.await {
                Ok(r) => match r {
                    Ok(_r) => (),
                    Err(e) => {
                        error!(tid=tid, error=%e, "Encountered an error on network namespace cleanup");
                        failed += 1;
                    },
                },
                Err(e) => {
                    error!(tid=tid, error=%e, "Encountered an error joining thread for network namespace cleanup");
                    failed += 1;
                },
            }
        }
        if failed > 0 {
            anyhow::bail!(
                "There were {} errors encountered cleaning up network namespace, out of {} namespaces",
                failed,
                num_handles
            );
        }

        let bridge_nspth = Self::net_namespace(BRIDGE_NET_ID);
        let env = Self::cmd_environment(&self.config);
        if Self::namespace_exists(BRIDGE_NET_ID) {
            let _output = execute_cmd(
                &self.config.cnitool,
                vec!["del", self.config.cni_name.as_str(), bridge_nspth.as_str()],
                Some(&env),
                tid,
            )?;
            self.delete_namespace(BRIDGE_NET_ID, tid)?;
        }
        let _out = execute_cmd(
            "/bin/ip",
            vec!["link", "delete", self.config.bridge.as_str(), "type", "bridge"],
            Some(&env),
            tid,
        )?;

        Ok(())
    }
}
